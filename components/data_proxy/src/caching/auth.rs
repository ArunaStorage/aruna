use super::cache::Cache;
use crate::structs::CheckAccessResult;
use crate::structs::DbPermissionLevel;
use crate::structs::Missing;
use crate::structs::Object;
use crate::structs::ObjectLocation;
use crate::structs::ObjectType;
use crate::structs::ResourceIds;
use crate::structs::ResourceResults;
use crate::trace_err;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::DataClass;
use diesel_ulid::DieselUlid;
use http::Method;
use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use s3s::auth::Credentials;
use s3s::path::S3Path;
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tonic::metadata::MetadataMap;
use tracing::debug;
use tracing::error;
use tracing::trace;

pub struct AuthHandler {
    pub cache: Arc<Cache>,
    pub self_id: DieselUlid,
    pub self_secret: String,
    pub encoding_key: (i32, EncodingKey),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ArunaTokenClaims {
    iss: String, // Currently always 'aruna'
    sub: String, // User_ID / DataProxy_ID
    exp: usize,  // Expiration timestamp
    aud: String, // Valid audiences
    // Token_ID; None if OIDC or DataProxy-DataProxy interaction ?
    #[serde(skip_serializing_if = "Option::is_none")]
    tid: Option<String>,
    // Intent: <endpoint-ulid>_<action>
    #[serde(skip_serializing_if = "Option::is_none")]
    it: Option<Intent>,
}

#[repr(u8)]
#[non_exhaustive]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord)]
pub enum Action {
    All = 0,
    CreateSecrets = 1,
    Impersonate = 2,
    FetchInfo = 3,
    DpExchange = 4,
}

impl From<u8> for Action {
    #[tracing::instrument(level = "trace", skip(input))]
    fn from(input: u8) -> Self {
        match input {
            0 => Action::All,
            1 => Action::CreateSecrets,
            2 => Action::Impersonate,
            3 => Action::FetchInfo,
            4 => Action::DpExchange,
            _ => panic!("Invalid action"),
        }
    }
}

#[derive(Debug)]
pub struct Intent {
    pub target: DieselUlid,
    pub action: Action,
}

impl Serialize for Intent {
    #[tracing::instrument(level = "trace", skip(self, serializer))]
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer
            .serialize_str(format!("{}_{:?}", self.target, self.action.clone() as u8).as_str())
    }
}

impl<'de> Deserialize<'de> for Intent {
    #[tracing::instrument(level = "trace", skip(deserializer))]
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let temp = String::deserialize(deserializer)?;
        let split = temp.split('_').collect::<Vec<&str>>();

        Ok(Intent {
            target: DieselUlid::from_str(split[0])
                .map_err(|_| serde::de::Error::custom("Invalid UUID"))?,
            action: u8::from_str(split[1])
                .map_err(|_| serde::de::Error::custom("Invalid Action"))?
                .into(),
        })
    }
}

impl AuthHandler {
    #[tracing::instrument(
        level = "trace",
        skip(cache, self_id, encode_secret, encoding_key_serial)
    )]
    pub fn new(
        cache: Arc<Cache>,
        self_id: DieselUlid,
        self_secret: String,
        encode_secret: String,
        encoding_key_serial: i32,
    ) -> Self {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let encoding_key = trace_err!(EncodingKey::from_ed_pem(private_pem.as_bytes())).unwrap();

        Self {
            cache,
            self_id,
            self_secret,
            encoding_key: (encoding_key_serial, encoding_key),
        }
    }

    #[tracing::instrument(level = "trace", skip(self, token))]
    pub fn check_permissions(&self, token: &str) -> Result<(DieselUlid, Option<String>)> {
        let kid = trace_err!(decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid")))?;
        let (_, dec_key) = trace_err!(self.cache.get_pubkey(trace_err!(i32::from_str(&kid))?))?;
        let claims = self.extract_claims(token, &dec_key)?;

        if let Some(it) = claims.it {
            match it.action {
                Action::All => Ok((DieselUlid::from_str(&claims.sub)?, claims.tid)),
                Action::CreateSecrets => {
                    if it.target == self.self_id {
                        Ok((DieselUlid::from_str(&claims.sub)?, claims.tid))
                    } else {
                        error!("Token is not valid for this Dataproxy");
                        bail!("Token is not valid for this Dataproxy")
                    }
                }
                Action::DpExchange => {
                    if it.target == self.self_id {
                        Ok((DieselUlid::from_str(&claims.sub)?, None))
                    } else {
                        error!("Token is not valid for this Dataproxy");
                        bail!("Token is not valid for this Dataproxy")
                    }
                }
                _ => {
                    error!("Action not allowed for Dataproxy");
                    bail!("Action not allowed for Dataproxy")
                }
            }
        } else {
            // No intent, no Dataproxy/Action check
            Ok((trace_err!(DieselUlid::from_str(&claims.sub))?, claims.tid))
        }
    }

    #[tracing::instrument(level = "trace", skip(self, token, dec_key))]
    pub(crate) fn extract_claims(
        &self,
        token: &str,
        dec_key: &DecodingKey,
    ) -> Result<ArunaTokenClaims> {
        let mut validation = Validation::new(Algorithm::EdDSA);
        validation.set_audience(&["proxy"]);

        let token = trace_err!(decode::<ArunaTokenClaims>(
            token,
            dec_key,
            &validation //&Validation::new(Algorithm::EdDSA)
        ))?;
        Ok(token.claims)
    }

    #[tracing::instrument(level = "debug", skip(self, creds, method, path))]
    pub async fn check_access(
        &self,
        creds: Option<&Credentials>,
        method: &Method,
        path: &S3Path,
    ) -> Result<CheckAccessResult> {
        let db_perm_from_method = DbPermissionLevel::from(method);
        trace!(extracted_perm = ?db_perm_from_method);
        if let Some(b) = path.as_bucket() {
            trace!("detected as_bucket");
            if method == Method::POST || method == Method::PUT {
                trace!("detected POST/PUT");
                let user = trace_err!(self
                    .cache
                    .get_user_by_key(
                        &trace_err!(creds.ok_or_else(|| anyhow!("Unknown user")))?.access_key
                    )
                    .ok_or_else(|| anyhow!("Unknown user")))?;

                let token_id = if user.user_id.to_string() == user.access_key {
                    None
                } else {
                    Some(user.access_key.clone())
                };

                return Ok(CheckAccessResult {
                    user_id: Some(user.user_id.to_string()),
                    token_id,
                    resource_ids: None,
                    missing_resources: Some(Missing {
                        p: Some(b.to_string()),
                        c: None,
                        d: None,
                        o: None,
                    }),
                    object: None,
                    bundle: None,
                });
            } else {
                trace!("detected not POST/PUT");
                let user = trace_err!(self
                    .cache
                    .get_user_by_key(
                        &trace_err!(creds.ok_or_else(|| anyhow!("Unknown user")))?.access_key
                    )
                    .ok_or_else(|| anyhow!("Unknown user")))?;

                return Ok(CheckAccessResult::new(
                    Some(user.user_id.to_string()),
                    Some(user.access_key),
                    None,
                    Some(Missing {
                        p: Some(b.to_string()),
                        c: None,
                        d: None,
                        o: None,
                    }),
                    None,
                    None,
                ));
            }
        }

        let ((obj, loc), ids, missing, bundle) = self.extract_object_from_path(path, method)?;
        if let Some(bundle) = bundle {
            debug!(bundle, "bundle_detected");
            if obj.object_type == ObjectType::Bundle {
                // Check if user has access to Bundle Object
                let user = trace_err!(self
                    .cache
                    .get_user_by_key(
                        &trace_err!(creds.ok_or_else(|| anyhow!("Unknown user")))?.access_key
                    )
                    .ok_or_else(|| anyhow!("Unknown user")))?;

                for (res, perm) in user.permissions {
                    // ResourceIds only contain Bundle Id as Project
                    if ids.check_if_in(res) && ((perm >= db_perm_from_method) || user.admin) {
                        let token_id = if user.user_id.to_string() == user.access_key {
                            None
                        } else {
                            Some(user.access_key.clone())
                        };

                        return Ok(CheckAccessResult {
                            user_id: Some(user.user_id.to_string()),
                            token_id,
                            resource_ids: None,
                            missing_resources: None, // Bundles are standalone
                            object: Some((obj, None)), // Bundles can't have a location
                            bundle: Some(bundle),
                        });
                    }
                }
            }

            if db_perm_from_method == DbPermissionLevel::Read && obj.data_class == DataClass::Public
            {
                debug!("public_bundle");
                return Ok(CheckAccessResult {
                    user_id: None,
                    token_id: None,
                    resource_ids: None,
                    missing_resources: None,
                    object: Some((obj, loc)),
                    bundle: Some(bundle),
                });
            } else {
                debug!("bundle_not_public");
                let user = trace_err!(self
                    .cache
                    .get_user_by_key(
                        &trace_err!(creds.ok_or_else(|| anyhow!("Unknown user")))?.access_key
                    )
                    .ok_or_else(|| anyhow!("Unknown user")))?;
                if !user.admin {
                    for (res, perm) in user.permissions {
                        if ids.check_if_in(res) && (perm >= db_perm_from_method) {
                            let token_id = if user.user_id.to_string() == user.access_key {
                                None
                            } else {
                                Some(user.access_key.clone())
                            };
                            trace!(resource_id = ?res, permission = ?perm, "permission match found");
                            return Ok(CheckAccessResult::new(
                                Some(user.user_id.to_string()),
                                token_id,
                                Some(ids),
                                missing,
                                Some((obj, loc)),
                                Some(bundle),
                            ));
                        }
                    }
                } else {
                    trace!("AdminUser signed bundle");
                    return Ok(CheckAccessResult::new(
                        Some(user.user_id.to_string()),
                        None,
                        Some(ids),
                        missing,
                        Some((obj, loc)),
                        Some(bundle),
                    ));
                }
            }
        }

        if db_perm_from_method == DbPermissionLevel::Read && obj.data_class == DataClass::Public {
            debug!("public_object");
            return Ok(CheckAccessResult::new(
                None,
                None,
                Some(ids),
                missing,
                Some((obj, loc)),
                None,
            ));
        } else if let Some(creds) = creds {
            debug!("private object");
            let user = trace_err!(self
                .cache
                .get_user_by_key(&creds.access_key)
                .ok_or_else(|| anyhow!("Unknown user")))?;

            for (res, perm) in user.permissions {
                if ids.check_if_in(res) && ((perm >= db_perm_from_method) || user.admin) {
                    let token_id = if user.user_id.to_string() == user.access_key {
                        None
                    } else {
                        Some(user.access_key.clone())
                    };
                    trace!(resource_id = ?res, permission = ?perm, "permission match found");
                    return Ok(CheckAccessResult::new(
                        Some(user.user_id.to_string()),
                        token_id,
                        Some(ids),
                        missing,
                        Some((obj, loc)),
                        None,
                    ));
                }
            }
        }

        Err(anyhow!("Invalid permissions"))
    }

    #[tracing::instrument(
        level = "trace",
        skip(self, vec_vec_ids, access_key, target_perm_level, get_secret)
    )]
    pub fn check_ids(
        &self,
        vec_vec_ids: &Vec<Vec<ResourceIds>>,
        access_key: &str,
        target_perm_level: DbPermissionLevel,
        get_secret: bool,
    ) -> Result<Option<String>> {
        let user = trace_err!(self
            .cache
            .get_user_by_key(access_key)
            .ok_or_else(|| anyhow!("Unknown user")))?;

        'id_vec: for vec_ids in vec_vec_ids {
            for (res, perm) in &user.permissions {
                for id in vec_ids {
                    if id.check_if_in(*res) && *perm >= target_perm_level {
                        continue 'id_vec;
                    }
                }
            }
            error!(?user.permissions, ?vec_vec_ids, "Invalid permissions");
            return Err(anyhow!("Invalid permissions"));
        }

        if get_secret {
            trace!("secret SOME");
            Ok(Some(user.secret.clone()))
        } else {
            trace!("secret NONE");
            Ok(None)
        }
    }

    #[tracing::instrument(level = "trace", skip(self, path, _method))]
    #[allow(clippy::type_complexity)]
    pub fn extract_object_from_path(
        &self,
        path: &S3Path,
        _method: &Method,
    ) -> Result<(
        (Object, Option<ObjectLocation>),
        ResourceIds,
        Option<Missing>,
        Option<String>,
    )> {
        if let Some((bucket, mut path)) = path.as_object() {
            debug!("bucket as object");
            if bucket == "objects" {
                debug!("special bucket detected");
                path = path.trim_matches('/');
                debug!("{path:?}");
                // Why
                // objects/<resource-id>/<resource-name>
                // ?
                if let Some((prefix, name)) = path.split_once('/') {
                    let id = trace_err!(DieselUlid::from_str(prefix))?;
                    trace!(?id, "extracted id from path");
                    let obj = trace_err!(self
                        .cache
                        .resources
                        .get(&id)
                        .ok_or_else(|| anyhow!("No object found in path")))?
                    .value()
                    .clone();

                    let mut ids = trace_err!(self.cache.get_resource_ids_from_id(id))?.0;
                    ids.sort();

                    trace!(?ids, "extracted ids from path");
                    return Ok((
                        obj,
                        trace_err!(ids.last().ok_or_else(|| anyhow!("No object found in path")))?
                            .clone(),
                        None,
                        Some(name.to_string()),
                    ));
                }
            } else if bucket == "bundles" {
                debug!("special bundle bucket detected");
                path = path.trim_matches('/');
                if let Some((prefix, name)) = path.split_once('/') {
                    let id = trace_err!(DieselUlid::from_str(prefix))?;
                    trace!(?id, "extracted bundle id from path");

                    let (bundle_object, _) = trace_err!(self
                        .cache
                        .resources
                        .get(&id)
                        .ok_or_else(|| anyhow!("No object found in path")))?
                    .value()
                    .clone();

                    // Check if Bundle is empty
                    if let Some(children) = &bundle_object.children {
                        if children.is_empty() {
                            error!(?children, "Empty bundle: Children is empty");
                            bail!("Empty bundle: Children is empty")
                        }
                    } else {
                        error!("empty bundle, children is None");
                        bail!("Empty bundle: Children is None")
                    }

                    return Ok((
                        (bundle_object, None),
                        ResourceIds::Project(id),
                        None,
                        Some(name.to_string()),
                    ));
                }
            }
        }

        let ResourceResults { found, missing } =
            ResourceResults::from_path(path, self.cache.clone())?;
        trace!(?found);
        trace!(?missing);

        let missing = if missing.is_empty() {
            None
        } else {
            Some(missing.into())
        };

        let resource_id = trace_err!(found
            .last()
            .ok_or_else(|| anyhow!("No object found in path")))?
        .clone();

        let (object, location) = trace_err!(self
            .cache
            .resources
            .get(&resource_id.get_id())
            .ok_or_else(|| anyhow!("No object found in path")))?
        .value()
        .clone();

        trace!(
            ?object,
            ?location,
            ?resource_id,
            ?missing,
            "extracted object from path"
        );

        Ok(((object, location), resource_id, missing, None))
    }

    #[tracing::instrument(level = "trace", skip(self, user_id, tid))]
    pub(crate) fn sign_impersonating_token(
        &self,
        user_id: impl Into<String>,
        tid: Option<impl Into<String>>,
    ) -> Result<String> {
        let claims = ArunaTokenClaims {
            iss: self.self_id.to_string(),
            sub: user_id.into(),
            aud: "aruna".to_string(),
            exp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .add(Duration::from_secs(15 * 60))
                .as_secs() as usize,
            tid: tid.map(|x| x.into()),
            it: Some(Intent {
                target: self.self_id,
                action: Action::Impersonate,
            }),
        };

        trace_err!(self.sign_token(claims))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub(crate) fn sign_notification_token(&self) -> Result<String> {
        let claims = ArunaTokenClaims {
            iss: self.self_id.to_string(),
            sub: self.self_id.to_string(),
            aud: "aruna".to_string(),
            exp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .add(Duration::from_secs(60 * 60 * 24 * 365 * 10))
                .as_secs() as usize,
            tid: None,
            it: Some(Intent {
                target: self.self_id,
                action: Action::FetchInfo,
            }),
        };

        trace_err!(self.sign_token(claims))
    }

    #[tracing::instrument(level = "trace", skip(self, target_endpoint))]
    pub(crate) fn sign_dataproxy_token(&self, target_endpoint: DieselUlid) -> Result<String> {
        let claims = ArunaTokenClaims {
            iss: self.self_id.to_string(),
            sub: self.self_id.to_string(),
            aud: "proxy".to_string(),
            exp: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)?
                .add(Duration::from_secs(15 * 60))
                .as_secs() as usize,
            tid: None,
            it: Some(Intent {
                target: target_endpoint,
                action: Action::DpExchange,
            }),
        };

        trace_err!(self.sign_token(claims))
    }
    #[tracing::instrument(level = "trace", skip(self, claims))]
    pub(crate) fn sign_token(&self, claims: ArunaTokenClaims) -> Result<String> {
        let header = Header {
            kid: Some(format!("{}", &self.encoding_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        let token = trace_err!(jsonwebtoken::encode(&header, &claims, &self.encoding_key.1))?;

        Ok(token)
    }
}

#[tracing::instrument(level = "trace", skip(md))]
pub fn get_token_from_md(md: &MetadataMap) -> Result<String> {
    let token_string = trace_err!(md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found")))?
    .to_str()?;

    let split = token_string.split(' ').collect::<Vec<_>>();

    if split.len() != 2 {
        error!(split_len = split.len(), "wrong token length, expected: 2");
        return Err(anyhow!("Authorization flow error"));
    }

    if split[0] != "Bearer" {
        error!(split = split[0], "wrong token type, expected: Bearer");
        return Err(anyhow!("Authorization flow error"));
    }

    if split[1].is_empty() {
        error!(?split, "empty token");
        return Err(anyhow!("Authorization flow error"));
    }
    Ok(split[1].to_string())
}
