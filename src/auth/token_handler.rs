use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use base64::engine::general_purpose;
use base64::Engine;
use diesel_ulid::DieselUlid;
use jsonwebtoken::Algorithm;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;

use crate::caching::cache::Cache;
use crate::caching::structs::PubKey;
use crate::database::enums::DbPermissionLevel;

#[derive(Deserialize, Debug)]
struct KeyCloakResponse {
    #[serde(alias = "realm")]
    _realm: String,
    public_key: String,
    #[serde(alias = "token-service")]
    _token_service: String,
    #[serde(alias = "account-service")]
    _account_service: String,
    #[serde(alias = "tokens-not-before")]
    _tokens_not_before: i64,
}

/// This contains claims for ArunaTokens
/// containing two fields
///
/// - tid: UUID from the specific token
/// - exp: When this token expires (by default very large number)
///
#[derive(Debug, Serialize, Deserialize)]
struct ArunaTokenClaims {
    iss: String,
    // User_id / Dataproxy_id
    sub: String,
    // Token ID
    #[serde(skip_serializing_if = "Option::is_none")]
    tid: Option<String>,
    exp: usize,
}

pub struct TokenHandler {
    cache: Arc<Cache>,
    oidc_realminfo: String,
    oidc_pubkey: Arc<RwLock<Option<DecodingKey>>>,
}

impl TokenHandler {
    pub fn new(cache: Arc<Cache>, oidc_realminfo: String) -> Self {
        TokenHandler {
            cache,
            oidc_realminfo,
            oidc_pubkey: Arc::new(RwLock::new(None)),
        }
    }

    pub async fn process_token(
        &self,
        token: &str,
    ) -> Result<(
        Option<DieselUlid>,
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
    )> {
        let decoded = general_purpose::STANDARD.decode(token)?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded)?;

        match claims.iss.as_str() {
            "oidc.test.com" => self.validate_oidc_only(token).await,
            "aruna" => self.validate_aruna(token).await,
            _ => return Err(anyhow!("Unknown issuer")),
        }
    }

    async fn validate_aruna(
        &self,
        token: &str,
    ) -> Result<(
        Option<DieselUlid>,
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
    )> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;

        let key = self
            .cache
            .pubkeys
            .get(&kid.parse::<i32>()?)
            .ok_or_else(|| anyhow!("Unspecified kid"))?
            .clone();

        let dec_key = match key {
            PubKey::DataProxy(k) => {
                let claims =
                    decode::<ArunaTokenClaims>(token, &k, &Validation::new(Algorithm::EdDSA))?;
                let uid = DieselUlid::from_str(&claims.claims.sub)?;
                return Ok((Some(uid), vec![], true));
            }
            PubKey::Server(k) => k,
        };
        let claims =
            decode::<ArunaTokenClaims>(token, &dec_key, &Validation::new(Algorithm::EdDSA))?;

        let uid = DieselUlid::from_str(&claims.claims.sub)?;

        let user = self.cache.get_user(&uid);

        if let Some(user) = user {
            let perms = user.get_permissions(None)?;
            return Ok((Some(user.id), perms, false));
        }
        bail!("Invalid user")
    }

    async fn validate_oidc_only(
        &self,
        token: &str,
    ) -> Result<(
        Option<DieselUlid>,
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
    )> {
        let header = decode_header(token)?;
        // Validate key
        let read = {
            let lock = self.oidc_pubkey.try_read().unwrap();
            lock.clone()
        };
        let token_data = match read {
            Some(pk) => decode::<ArunaTokenClaims>(token, &pk, &Validation::new(header.alg))?,
            None => decode::<ArunaTokenClaims>(
                token,
                &self.get_token_realminfo().await?,
                &Validation::new(header.alg),
            )?,
        };

        let user = self.cache.get_user_by_oidc(&token_data.claims.sub)?;

        let perms = user.get_permissions(None)?;

        Ok((Some(user.id), perms, false))
    }

    async fn get_token_realminfo(&self) -> Result<DecodingKey> {
        let resp = reqwest::get(&self.oidc_realminfo)
            .await?
            .json::<KeyCloakResponse>()
            .await?;
        let dec_key = DecodingKey::from_rsa_pem(
            format!(
                "{}\n{}\n{}",
                "-----BEGIN PUBLIC KEY-----", resp.public_key, "-----END PUBLIC KEY-----"
            )
            .as_bytes(),
        )?;
        let pks = self.oidc_pubkey.clone();
        let mut lck = pks.write().unwrap();
        *lck = Some(dec_key.clone());
        Ok(dec_key)
    }
}
