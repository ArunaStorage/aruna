use super::cache::Cache;
use crate::helpers::is_method_read;
use crate::structs::AccessKeyPermissions;
use crate::structs::CheckAccessResult;
use crate::trace_err;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
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
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::ops::Add;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use tonic::metadata::MetadataMap;
use tracing::error;

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
    ) -> Self {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let encoding_key = trace_err!(EncodingKey::from_ed_pem(private_pem.as_bytes())).unwrap();

        Self {
            cache,
            self_id,
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
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        match path {
            S3Path::Root => Ok(self.handle_root(creds)),
            S3Path::Bucket { bucket } => {
                if is_method_read(method) {
                    // "GET" style methods
                    // &Method::GET | &Method::HEAD | &Method::OPTIONS
                    self.handle_bucket_get(bucket, creds, headers).await
                } else {
                    // "POST" style = modifying methods
                    // &Method::POST | &Method::PUT | &Method::DELETE | &Method::PATCH | (&Method::CONNECT | &Method::TRACE)
                    self.handle_bucket_post(bucket, creds, headers).await
                }
            }
            S3Path::Object { bucket, key } => {
                if is_method_read(method) {
                    // "GET" style methods
                    // &Method::GET | &Method::HEAD | &Method::OPTIONS
                    // 2 special cases: objects, bundles
                    self.handle_object_get(bucket, key, creds, headers).await
                } else {
                    // "POST" style = modifying methods
                    // &Method::POST | &Method::PUT | &Method::DELETE | &Method::PATCH | (&Method::CONNECT | &Method::TRACE)
                    self.handle_object_post(bucket, key, creds, headers).await
                }
            }
        }
    }

    #[tracing::instrument(level = "trace", skip(self, creds))]
    pub fn extract_access_key_perms(
        &self,
        creds: Option<&Credentials>,
    ) -> Option<AccessKeyPermissions> {
        if let Some(creds) = creds {
            return self.cache.get_key_perms(&creds.access_key);
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(self, creds))]
    pub fn handle_root(&self, creds: Option<&Credentials>) -> CheckAccessResult {
        if let Some(AccessKeyPermissions {
            user_id,
            access_key,
            ..
        }) = self.extract_access_key_perms(creds)
        {
            return CheckAccessResult {
                user_id: Some(user_id.to_string()),
                token_id: Some(access_key.to_string()),
                ..Default::default()
            };
        }
        CheckAccessResult::default()
    }

    #[tracing::instrument(level = "trace", skip(self, bucket_name, creds, headers))]
    pub async fn handle_bucket_get(
        &self,
        bucket_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        todo!()
    }


    #[tracing::instrument(level = "trace", skip(self, bucket_name, creds, headers))]
    pub async fn handle_bucket_post(
        &self,
        bucket_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        todo!()
    }


    #[tracing::instrument(level = "trace", skip(self, bucket_name, key_name, creds, headers))]
    pub async fn handle_object_get(
        &self,
        bucket_name: &str,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        match bucket_name {
            "objects" => {
                return self.handle_objects(key_name, creds, headers).await;
            }
            "bundles" => {
                return self.handle_bundles(key_name, creds, headers).await;
            }
            _ => {}
        }
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self, bucket_name, key_name, creds, headers))]
    pub async fn handle_object_post(
        &self,
        bucket_name: &str,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self, key_name, creds, headers))]
    pub async fn handle_objects(
        &self,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self, key_name, creds, headers))]
    pub async fn handle_bundles(
        &self,
        key_name: &str,
        creds: Option<&Credentials>,
        headers: &HeaderMap<HeaderValue>,
    ) -> Result<CheckAccessResult> {
        todo!()
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
