use crate::caching::cache::Cache;
use crate::helpers::is_method_read;
use crate::structs::AccessKeyPermissions;
use crate::structs::CheckAccessResult;
use crate::structs::DbPermissionLevel;
use crate::structs::Object;
use crate::structs::ObjectType;
use crate::structs::ResourceState;
use crate::structs::ResourceStates;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::ResourceVariant;
use aruna_rust_api::api::storage::models::v2::UserAttributes;
use axum::http::method;
use diesel_ulid::DieselUlid;
use http::HeaderMap;
use http::HeaderValue;
use http::Method;
use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use s3s::auth::Credentials;
use s3s::path::S3Path;
use s3s::s3_error;
use s3s::S3Error;
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tonic::metadata::MetadataMap;
use tracing::error;

use super::rule_engine::RuleEngine;
use super::rule_structs::ObjectRuleInputBuilder;
use super::rule_structs::RequestInfo;
use super::rule_structs::RootRuleInput;
use super::rule_structs::RootRuleInputBuilder;
use super::rule_structs::UserRuleInfo;

pub struct AuthHandler {
    cache: Arc<Cache>,
    self_id: DieselUlid,
    rule_engine: RuleEngine,
    encoding_key: (i32, EncodingKey),
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
        encode_secret: String,
        encoding_key_serial: i32,
    ) -> Result<Self> {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let encoding_key = EncodingKey::from_ed_pem(private_pem.as_bytes()).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        Ok(Self {
            cache,
            self_id,
            rule_engine: RuleEngine::new()?,
            encoding_key: (encoding_key_serial, encoding_key),
        })
    }

    #[tracing::instrument(level = "trace", skip(self, token))]
    pub fn check_permissions(&self, token: &str) -> Result<(DieselUlid, Option<String>)> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        let (_, dec_key) = self
            .cache
            .get_pubkey(i32::from_str(&kid).map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?)
            .map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
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
            Ok((
                DieselUlid::from_str(&claims.sub).map_err(|e| {
                    tracing::error!(error = ?e, msg = e.to_string());
                    e
                })?,
                claims.tid,
            ))
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

        let token = decode::<ArunaTokenClaims>(
            token,
            dec_key,
            &validation, //&Validation::new(Algorithm::EdDSA)
        )
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(token.claims)
    }

    // ----------------- AUTHORIZATION -----------------

    #[tracing::instrument(level = "debug", skip(self, creds, method, path))]
    pub async fn check_access(
        &self,
        creds: Option<&Credentials>,
        method: &Method,
        path: &S3Path,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        match path {
            S3Path::Root => self.handle_root(method, creds, headers),
            S3Path::Bucket { bucket } => {
                // Buckets are handled the same for GET and POST
                self.handle_bucket(bucket, method, creds, headers).await
            }
            S3Path::Object { bucket, key } => {
                if is_method_read(method) {
                    // "GET" style methods
                    // &Method::GET | &Method::HEAD | &Method::OPTIONS
                    // 2 special cases: objects, bundles
                    self.handle_object_get(bucket, key, method, creds, headers)
                        .await
                } else {
                    // "POST" style = modifying methods
                    // &Method::POST | &Method::PUT | &Method::DELETE | &Method::PATCH | (&Method::CONNECT | &Method::TRACE)
                    self.handle_object_post(bucket, key, creds, headers).await
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, creds))]
    pub fn handle_root(
        &self,
        method: &Method,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        if let Some((
            AccessKeyPermissions {
                user_id,
                access_key,
                ..
            },
            attributes,
        )) = self.extract_access_key_perms(creds)
        {
            self.rule_engine
                .evaluate_root(
                    RootRuleInputBuilder::new()
                        .attributes(attributes)
                        .method(method.to_string())
                        .headers(
                            headers
                                .iter()
                                .map(|(k, v)| {
                                    (k.to_string(), v.to_str().unwrap_or_default().to_string())
                                })
                                .collect(),
                        )
                        .build()
                        .map_err(|e| s3_error!(MalformedACLError, "Rule has wrong context"))?,
                )
                .map_err(|e| s3_error!(AccessDenied, "Forbidden by rule"))?;
            return Ok(CheckAccessResult {
                user_id: Some(user_id.to_string()),
                token_id: Some(access_key.to_string()),
                ..Default::default()
            });
        }
        Err(s3_error!(AccessDenied, "Missing access key"))
    }

    #[tracing::instrument(level = "trace", skip(self, bucket_name, creds, headers))]
    pub async fn handle_bucket(
        &self,
        bucket_name: &str,
        method: &Method,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        // TODO: Decide how to handle public bucket access
        // Query the User -> Must exist
        let (access_key_info, attributes) =
            self.extract_access_key_perms(creds).ok_or_else(|| {
                error!("No such user");
                s3_error!(AccessDenied, "Access Denied")
            })?;

        // Query the project and extract the headers
        let (cors_headers, project) =
            if let Some(project) = self.cache.get_full_resource_by_path(bucket_name) {
                // Check if the project is partially synced -> FAIL
                if project.is_partial_sync(self.self_id) {
                    error!("Invalid Bucket Name (partial synced)");
                    return Err(s3_error!(
                        InvalidBucketName,
                        "Invalid Bucket Name (partial synced)"
                    ));
                }
                (project.project_get_headers(method, headers), project)
            } else {
                error!("No such bucket");
                return Err(s3_error!(NoSuchBucket, "No such bucket"));
            };

        // Extract the permission level from the method READ == "GET" and friends, WRITE == "POST" and friends
        let db_perm_from_method = DbPermissionLevel::from(method);

        if access_key_info
            .permissions
            .get(&project.id)
            .ok_or_else(|| {
                error!("No permissions found");
                s3_error!(AccessDenied, "Access Denied")
            })?
            < &db_perm_from_method
        {
            error!("Insufficient permissions");
            return Err(s3_error!(AccessDenied, "Access Denied"));
        }

        // Create a "resource_state" struct that tracks what is missing, what is found and what is not set
        let resource_state = Some(
            ResourceState::from_list(&[&project])
                .map_err(|_| s3_error!(InternalError, "Internal Error"))?,
        );

        self.rule_engine
            .evaluate_object(
                ObjectRuleInputBuilder::new()
                    .attributes(attributes)
                    .method(method.to_string())
                    .headers(
                        headers
                            .iter()
                            .map(|(k, v)| {
                                (k.to_string(), v.to_str().unwrap_or_default().to_string())
                            })
                            .collect(),
                    )
                    .project(project.clone())
                    .build().map_err(|_| s3_error!(MalformedACLError, "Rule has wrong context"))?,
            )
            .map_err(|_| s3_error!(AccessDenied, "Forbidden by rule"))?;

        Ok(CheckAccessResult::new(
            Some(access_key_info.user_id.to_string()),
            Some(access_key_info.access_key),
            resource_state,
            Some((project, None)),
            None,
            cors_headers,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, bucket_name, key_name, creds, headers))]
    pub async fn handle_object_get(
        &self,
        bucket_name: &str,
        key_name: &str,
        method: &Method,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        // Cache objects special cases
        match bucket_name {
            "objects" => {
                return self.handle_special_objects(key_name, creds, headers).await;
            }
            "bundles" => {
                return self.handle_bundles(key_name, creds, headers).await;
            }
            _ => {}
        }

        // Query the Object -> Might be public
        let object = self
            .cache
            .get_full_resource_by_path(&format!("{bucket_name}/{key_name}"))
            .ok_or_else(|| {
                error!("No such object");
                s3_error!(NoSuchKey, "No such object")
            })?;

        // Fail if the object is not a regular object
        if object.object_type != ObjectType::Object {
            error!("Invalid object type");
            return Err(s3_error!(NoSuchKey, "No such object"));
        }

        let (project, headers) = self
            .get_project_and_headers(bucket_name, method, headers)
            .ok_or_else(|| {
                error!("No such project");
                s3_error!(NoSuchBucket, "No such bucket")
            })?;

        // Paths
        let mut prefix = key_into_prefix(key_name);
        prefix.remove(object.name.as_str());
        prefix.remove(project.name.as_str());

        if prefix.len() > 2 {
            error!("This should not happen: Detected more than 4 Objects in the path");
            return Err(s3_error!(InternalError, "Invalid key parsing"));
        }

        let mut objects = vec![project];
        for prefix in prefix.iter() {
            objects.push(
                self.cache
                    .get_full_resource_by_path(prefix)
                    .ok_or_else(|| {
                        error!("No such object");
                        s3_error!(NoSuchKey, "No such object")
                    })?,
            );
        }
        objects.push(object);

        // Create a "resource_state" struct that tracks what is missing, what is found and what is not set
        let resource_state = Some(
            ResourceState::from_list(&objects)
                .map_err(|_| s3_error!(InternalError, "Internal Error"))?,
        );

        Ok(CheckAccessResult::new(
            Some(user.user_id.to_string()),
            Some(user.access_key),
            resource_state,
            None,
            None,
            headers,
        ))
    }

    #[tracing::instrument(level = "trace", skip(self, bucket_name, key_name, creds, headers))]
    pub async fn handle_object_post(
        &self,
        bucket_name: &str,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self, key_name, creds, headers))]
    pub async fn handle_special_objects(
        &self,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self, key_name, creds, headers))]
    pub async fn handle_bundles(
        &self,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult, S3Error> {
        todo!()
    }

    // ----------------- HELPERS -----------------

    #[tracing::instrument(level = "trace", skip(self, creds))]
    pub fn extract_access_key_perms(
        &self,
        creds: Option<&Credentials>,
    ) -> Option<(AccessKeyPermissions, HashMap<String, String>)> {
        if let Some(creds) = creds {
            if let Some(key) = self.cache.get_key_perms(&creds.access_key) {
                if let Some(user) = self.cache.get_user_attributes(&key.user_id) {
                    return Some((key, user));
                }
            }
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(self, bucket, method, headers))]
    fn get_project_and_headers(
        &self,
        bucket: &str,
        method: &Method,
        headers: &HeaderMap<HeaderValue>,
    ) -> Option<(Object, Option<HashMap<String, String>>)> {
        let project = self.cache.get_full_resource_by_path(bucket)?;
        let headers = project.project_get_headers(method, headers);
        Some((project, headers))
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

        self.sign_token(claims).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })
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

        self.sign_token(claims).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })
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

        self.sign_token(claims).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })
    }
    #[tracing::instrument(level = "trace", skip(self, claims))]
    pub(crate) fn sign_token(&self, claims: ArunaTokenClaims) -> Result<String> {
        let header = Header {
            kid: Some(format!("{}", &self.encoding_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        let token = jsonwebtoken::encode(&header, &claims, &self.encoding_key.1).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;

        Ok(token)
    }
}

#[tracing::instrument(level = "trace", skip(md))]
pub fn get_token_from_md(md: &MetadataMap) -> Result<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))
        .map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?
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

#[tracing::instrument(level = "trace", skip(key))]
pub fn key_into_prefix(key: &str) -> HashSet<String> {
    let mut parts = HashSet::new();
    let mut prefix = String::new();
    for s in key.splitn(4, "/") {
        prefix.push_str(s);
        parts.insert(prefix.clone());
        prefix.push('/');
    }
    parts
}
