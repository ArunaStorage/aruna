use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use base64::engine::general_purpose;
use base64::Engine;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use jsonwebtoken::encode;
use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;

use crate::caching::cache::Cache;
use crate::caching::structs::PubKey;
use crate::database::connection::Database;
use crate::database::dsls::pub_key_dsl::PubKey as DbPubKey;
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
/// containing 3 mandatory and 2 optional fields.
///
/// - iss: Toen issuer which is currently 'aruna' everytime
/// - sub: User_ID or Endpoint_ID
/// - tid: UUID from the specific token
/// - exp: When this token expires (by default very large number)
/// - intent: Combination of specific endpoint and action.
///           Strongly restricts the usability of the token.
#[derive(Debug, Serialize, Deserialize)]
struct ArunaTokenClaims {
    iss: String, // Currently always 'aruna'
    sub: String, // User_ID / DataProxy_ID
    exp: usize,  // Expiration timestamp
    // Token_ID; None if OIDC or ... ?
    #[serde(skip_serializing_if = "Option::is_none")]
    tid: Option<String>,
    // Intent: <endpoint-ulid>_<action>
    #[serde(skip_serializing_if = "Option::is_none")]
    intent: Option<String>,
}

pub struct TokenHandler {
    cache: Arc<Cache>,
    oidc_realminfo: String,
    oidc_pubkey: Arc<RwLock<Option<DecodingKey>>>,
    signing_info: Arc<RwLock<(i64, EncodingKey, DecodingKey)>>, //<PublicKey Serial; PrivateKey; PublicKey>
}

impl TokenHandler {
    pub async fn new(
        cache: Arc<Cache>,
        database: Arc<Database>,
        oidc_realminfo: String,
        encode_secret: String,
        decode_secret: String,
    ) -> anyhow::Result<Self> {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let public_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            decode_secret
        );

        // Read encoding and decoding key; On error panic, we do not want malformed keys.
        let encoding_key = EncodingKey::from_ed_pem(private_pem.as_bytes()).unwrap();
        let decoding_key = DecodingKey::from_ed_pem(public_pem.as_bytes()).unwrap();

        // Check if public key already exists in database/cache
        let pubkey_serial: i64 = if let Some(key_serial) = cache.get_pubkey_serial(&decode_secret) {
            key_serial as i64
        } else {
            // Add public key to database and cache
            let client = database.get_client().await.unwrap();
            let pub_key = DbPubKey::create_without_id(None, &decode_secret, &client).await?;

            cache.add_pubkey(
                pub_key.id as i32,
                PubKey::Server((decode_secret, decoding_key.clone())),
            );

            // Notification --> Announcement::PubKey::New ?

            pub_key.id as i64
        };

        // Return initialized TokenHandler
        Ok(TokenHandler {
            cache,
            oidc_realminfo,
            oidc_pubkey: Arc::new(RwLock::new(None)),
            signing_info: Arc::new(RwLock::new((pubkey_serial, encoding_key, decoding_key))),
        })
    }

    ///ToDo: Rust Doc
    pub fn sign_user_token(
        &self,
        user_id: &DieselUlid,
        token_id: &DieselUlid,
        expires_at: Option<prost_wkt_types::Timestamp>,
    ) -> Result<String> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: user_id.to_string(),
            exp: if expires_at.is_none() {
                // Add 10 years to token lifetime if  expiry unspecified
                (Utc::now().timestamp() as usize) + 315360000
            } else {
                expires_at.unwrap().seconds as usize
            },
            tid: Some(token_id.to_string()),
            intent: None,
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        Ok(encode(&header, &claims, &signing_key.1)?)
    }

    /// Signing function to create a token that on lives only for a short period
    /// and is only applicable for a specific Endpoint. If an intent is provided
    /// the token is additionally restricted to the specific action.
    pub fn sign_dataproxy_slt(
        &self,
        user_id: &DieselUlid,     // User id of original
        endpoint_id: &DieselUlid, // Endpoint the token is signed for
        token_id: Option<String>, // None if original request came with OIDC
        action: Option<String>,   // Some Dataproxy action to restrict token usage scope
    ) -> Result<String> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: user_id.to_string(),
            exp: (Utc::now().timestamp() as usize) + 86400, // One day for now.
            tid: token_id,
            intent: if let Some(definite_action) = action {
                Some(format!("{}_{}", endpoint_id, definite_action))
            } else {
                Some(endpoint_id.to_string())
            },
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        Ok(encode(&header, &claims, &signing_key.1)?)
    }

    }

    pub async fn process_token(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,         // Proxy or Token Id
        Option<DieselUlid>, // User_id
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
    )> {
        let decoded = general_purpose::STANDARD.decode(token)?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded)?;

        match claims.iss.as_str() {
            "oidc.test.com" => self.validate_oidc_only(token).await,
            "aruna" => self.validate_aruna(token).await,
            _ => Err(anyhow!("Unknown issuer")),
        }
    }

    ///ToDo: Rust Doc
    async fn validate_aruna(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,
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

        let (_, dec_key) = match key {
            PubKey::DataProxy((_, key)) => {
                let claims =
                    decode::<ArunaTokenClaims>(token, &key, &Validation::new(Algorithm::EdDSA))?;

                let sub_id = DieselUlid::from_str(&claims.claims.sub)?;

                let (option_user, perms) = match claims.claims.tid {
                    Some(uid) => {
                        let uid = DieselUlid::from_str(&uid)?;
                        (
                            Some(uid),
                            self.cache
                                .get_user(&uid)
                                .ok_or_else(|| anyhow!("Invalid user"))?
                                .get_permissions(None)?,
                        )
                    }
                    None => (None, vec![]),
                };
                return Ok((sub_id, option_user, perms, true));
            }
            PubKey::Server(k) => k,
        };
        let claims =
            decode::<ArunaTokenClaims>(token, &dec_key, &Validation::new(Algorithm::EdDSA))?;

        let uid = DieselUlid::from_str(&claims.claims.sub)?;

        let user = self.cache.get_user(&uid);

        let token = match claims.claims.tid {
            Some(uid) => Some(DieselUlid::from_str(&uid)?),
            None => None,
        };

        if let Some(user) = user {
            let perms = user.get_permissions(token)?;
            return Ok((user.id, token, perms, false));
        }
        bail!("Invalid user")
    }

    async fn validate_oidc_only(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,
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

        Ok((user.id, None, perms, false))
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
