use crate::caching::cache::ResourceStrings;
use crate::structs::DbPermissionLevel;
use crate::structs::Object;

use super::cache::Cache;
use super::cache::ResourceIds;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::DataClass;
use diesel_ulid::DieselUlid;
use http::Method;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use s3s::auth::Credentials;
use s3s::path::S3Path;
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::MetadataMap;

pub struct AuthHandler {
    pub cache: Arc<Cache>,
    pub self_id: DieselUlid,
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
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    All = 0,
    Notifications = 1,
    CreateSecrets = 2,
}

impl From<u8> for Action {
    fn from(input: u8) -> Self {
        match input {
            0 => Action::All,
            1 => Action::Notifications,
            2 => Action::CreateSecrets,
            _ => panic!("Invalid action"),
        }
    }
}

#[derive(Debug)]
pub struct Intent {
    target: DieselUlid,
    action: Action,
}

impl Serialize for Intent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(
            format!(
                "{}_{:?}",
                self.target,
                self.action.clone() as u8
            )
            .as_str(),
        )
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
    pub fn new(cache: Arc<Cache>, self_id: DieselUlid) -> Self {
        Self { cache, self_id }
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
        let token = decode::<ArunaTokenClaims>(
            token,
            dec_key,
            &Validation::new(jsonwebtoken::Algorithm::EdDSA),
        )?;
        Ok(token.claims)
    }

    pub async fn check_access(
        &self,
        creds: Option<&Credentials>,
        method: &Method,
        path: &S3Path,
    ) -> Result<()> {
        let (ids, obj) = self.extract_object_from_path(path)?;
        let db_perm_from_method = DbPermissionLevel::from(method);

        if db_perm_from_method == DbPermissionLevel::READ && obj.data_class == DataClass::Public {
            return Ok(());
        } else if let Some(creds) = creds {
            let user = self
                .cache
                .get_user_by_key(&creds.access_key)
                .ok_or_else(|| anyhow!("Unknown user"))?;

            for (res_id, perm) in user.permissions {
                if ids.check_if_in(res_id) && perm >= db_perm_from_method {
                    return Ok(());
                }
            }
        }

        Err(anyhow!("Invalid permissions"))
    }

    pub fn extract_object_from_path(&self, path: &S3Path) -> Result<(ResourceIds, Object)> {
        let res_strings = ResourceStrings::try_from(path)?.0;
        for res in res_strings {
            if let Some(e) = self.cache.get_res_by_res_string(res) {
                return Ok((
                    e.clone(),
                    self.cache
                        .resources
                        .get(&e.get_id())
                        .ok_or_else(|| anyhow!("Unknown object"))?
                        .value()
                        .0
                        .clone(),
                ));
            }
        }
        Err(anyhow!("No object found in path"))
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
