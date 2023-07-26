use anyhow::anyhow;
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
    sub: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    uid: Option<String>,
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

    pub async fn process_token(&self, token: &str) -> Result<(DieselUlid, Option<DieselUlid>)> {
        let decoded = general_purpose::STANDARD.decode(token)?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded)?;

        let checked_claims = match claims.iss.as_str() {
            "oidc.test.com" => self.validate_oidc_only(token).await?,
            "aruna" => self.validate_aruna(token).await?,
            _ => return Err(anyhow!("Unknown issuer")),
        };

        let (user_id, token_id) = match checked_claims.uid {
            Some(uid) => (
                DieselUlid::from_str(&uid)?,
                Some(DieselUlid::from_str(&checked_claims.sub)?),
            ),
            None => (None, None),
        };

        Ok((user_id, token_id))
    }

    async fn validate_aruna(&self, token: &str) -> Result<ArunaTokenClaims> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;

        let key = self
            .cache
            .cache
            .pubkeys
            .get(&kid.parse::<i32>()?)
            .ok_or_else(|| anyhow!("Unspecified kid"))?
            .clone();

        let dec_key = match key {
            PubKey::DataProxy(k) => k,
            PubKey::Server(k) => k,
        };
        Ok(decode::<ArunaTokenClaims>(token, &dec_key, &Validation::new(Algorithm::EdDSA))?.claims)
    }

    async fn validate_oidc_only(&self, token: &str) -> Result<String> {
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
        Ok(token_data.claims.sub)
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
