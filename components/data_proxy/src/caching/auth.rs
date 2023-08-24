use super::cache::Cache;
use crate::structs::CheckAccessResult;
use crate::structs::DbPermissionLevel;
use crate::structs::Missing;
use crate::structs::Object;
use crate::structs::ObjectLocation;
use crate::structs::ResourceIds;
use crate::structs::ResourceString;
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

pub struct AuthHandler {
    pub cache: Arc<Cache>,
    pub self_id: DieselUlid,
    pub encoding_key: (i32, EncodingKey),
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ArunaTokenClaims {
    iss: String, // Currently always 'aruna'
    sub: String, // User_ID / DataProxy_ID
    exp: usize,  // Expiration timestamp
    // Token_ID; None if OIDC or ... ?
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
    //DpExchange = 4,
}

impl From<u8> for Action {
    fn from(input: u8) -> Self {
        match input {
            0 => Action::All,
            1 => Action::CreateSecrets,
            2 => Action::Impersonate,
            3 => Action::FetchInfo,
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
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer
            .serialize_str(format!("{}_{:?}", self.target, self.action.clone() as u8).as_str())
    }
}

impl<'de> Deserialize<'de> for Intent {
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
    pub fn new(
        cache: Arc<Cache>,
        self_id: DieselUlid,
        encode_secret: String,
        encoding_key_serial: i32,
    ) -> Self {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let encoding_key = EncodingKey::from_ed_pem(private_pem.as_bytes()).unwrap();

        Self {
            cache,
            self_id,
            encoding_key: (encoding_key_serial, encoding_key),
        }
    }

    pub fn check_permissions(&self, token: &str) -> Result<(DieselUlid, Option<String>)> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;
        let (_, dec_key) = self.cache.get_pubkey(i32::from_str(&kid)?)?;
        let claims = self.extract_claims(token, &dec_key)?;

        if let Some(it) = claims.it {
            if it.action == Action::CreateSecrets && it.target == self.self_id {
                return Ok((DieselUlid::from_str(&claims.sub)?, claims.tid));
            }
        }

        bail!("Invalid permissions")
    }

    pub(crate) fn extract_claims(
        &self,
        token: &str,
        dec_key: &DecodingKey,
    ) -> Result<ArunaTokenClaims> {
        let token = decode::<ArunaTokenClaims>(token, dec_key, &Validation::new(Algorithm::EdDSA))?;
        Ok(token.claims)
    }

    pub async fn check_access(
        &self,
        creds: Option<&Credentials>,
        method: &Method,
        path: &S3Path,
    ) -> Result<CheckAccessResult> {
        let db_perm_from_method = DbPermissionLevel::from(method);
        if let Some(b) = path.as_bucket() {
            if method == Method::POST || method == Method::PUT {
                let user = self
                    .cache
                    .get_user_by_key(&creds.ok_or_else(|| anyhow!("Unknown user"))?.access_key)
                    .ok_or_else(|| anyhow!("Unknown user"))?;

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
                });

                // let get_object = self
                //     .cache
                //     .get_res_by_res_string(crate::structs::ResourceString::Project(b.to_string()))
                //     .ok_or_else(|| anyhow!("Unknown object"))?;

                // let obj = &self
                //     .cache
                //     .resources
                //     .get(&get_object.get_id())
                //     .ok_or_else(|| anyhow!("Unknown object"))?
                //     .0;

                // for (res, perm) in user.permissions {
                //     if get_object.check_if_in(res) && perm >= db_perm_from_method {
                //         return Ok(CheckAccessResult::new(
                //             Some(user.user_id.to_string()),
                //             Some(user.access_key),
                //             Some(get_object),
                //             None,
                //             Some(obj.clone()),
                //         ));
                //     }
                // }
            } else {
                let user = self
                    .cache
                    .get_user_by_key(&creds.ok_or_else(|| anyhow!("Unknown user"))?.access_key)
                    .ok_or_else(|| anyhow!("Unknown user"))?;

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
                ));
            }
        }

        let (ids, (obj, loc), missing) = self.extract_object_from_path(path, method)?;
        if db_perm_from_method == DbPermissionLevel::Read && obj.data_class == DataClass::Public {
            return Ok(CheckAccessResult::new(
                None,
                None,
                Some(ids),
                missing,
                Some((obj, loc)),
            ));
        } else if let Some(creds) = creds {
            let user = self
                .cache
                .get_user_by_key(&creds.access_key)
                .ok_or_else(|| anyhow!("Unknown user"))?;

            for (res, perm) in user.permissions {
                if ids.check_if_in(res) && perm >= db_perm_from_method {
                    let token_id = if user.user_id.to_string() == user.access_key {
                        None
                    } else {
                        Some(user.access_key.clone())
                    };

                    return Ok(CheckAccessResult::new(
                        Some(user.user_id.to_string()),
                        token_id,
                        Some(ids),
                        missing,
                        Some((obj, loc)),
                    ));
                }
            }
        }

        Err(anyhow!("Invalid permissions"))
    }

    #[allow(clippy::type_complexity)]
    pub fn extract_object_from_path(
        &self,
        path: &S3Path,
        _method: &Method,
    ) -> Result<(
        ResourceIds,
        (Object, Option<ObjectLocation>),
        Option<Missing>,
    )> {
        let res_strings = ResourceString::try_from(path)?;

        let mut found = Vec::new();
        let mut missing = Vec::new();

        for resource in res_strings.into_parts() {
            if let Some(e) = self.cache.get_res_by_res_string(resource.clone()) {
                found.push(e);
            } else {
                missing.push(resource.clone());
            }
        }
        found.sort();

        let resource_id = found
            .last()
            .ok_or_else(|| anyhow!("No object found in path"))?
            .clone();

        let (object, location) = self
            .cache
            .resources
            .get(&resource_id.get_id())
            .ok_or_else(|| anyhow!("No object found in path"))?
            .value()
            .clone();

        Ok((resource_id, (object, location), Some(missing.into())))
    }

    pub(crate) fn sign_impersonating_token(
        &self,
        user_id: impl Into<String>,
        tid: Option<impl Into<String>>,
    ) -> Result<String> {
        let claims = ArunaTokenClaims {
            iss: "aruna_dataproxy".to_string(),
            sub: user_id.into(),
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

        self.sign_token(claims)
    }

    pub(crate) fn sign_notification_token(&self) -> Result<String> {
        let claims = ArunaTokenClaims {
            iss: "aruna_dataproxy".to_string(),
            sub: self.self_id.to_string(),
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

        self.sign_token(claims)
    }

    pub(crate) fn sign_token(&self, claims: ArunaTokenClaims) -> Result<String> {
        let header = Header {
            kid: Some(format!("{}", &self.encoding_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        let token = jsonwebtoken::encode(&header, &claims, &self.encoding_key.1)?;

        Ok(token)
    }
}

pub fn get_token_from_md(md: &MetadataMap) -> Result<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))?
        .to_str()?;

    let split = token_string.split(' ').collect::<Vec<_>>();

    if split.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            split.len()
        );
        return Err(anyhow!("Authorization flow error"));
    }

    if split[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid token type, expected: Bearer, got: {:?}",
            split[0]
        );

        return Err(anyhow!("Authorization flow error"));
    }

    if split[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid token length, expected: >0, got: {:?}",
            split[1].len()
        );

        return Err(anyhow!("Authorization flow error"));
    }

    Ok(split[1].to_string())
}
