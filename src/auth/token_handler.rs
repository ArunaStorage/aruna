use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use base64::engine::general_purpose;
use base64::Engine;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use hmac::{Hmac, Mac};
use jsonwebtoken::encode;
use jsonwebtoken::Algorithm;
use jsonwebtoken::EncodingKey;
use jsonwebtoken::Header;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::RwLock;

use crate::caching::cache::Cache;
use crate::caching::structs::PubKeyEnum;
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
pub(crate) struct ArunaTokenClaims {
    iss: String,     // Currently always 'aruna'
    pub sub: String, // User_ID / DataProxy_ID
    exp: usize,      // Expiration timestamp
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
type HmacSha256 = Hmac<Sha256>;

impl Serialize for Intent {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer
            .serialize_str(format!("{}_{:?}", self.target, self.action.clone() as u8).as_str())
    }
}

impl Serialize for Action {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(format!("{:?}", self.clone() as u8).as_str())
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

impl<'de> Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let deserialized_string = String::deserialize(deserializer)?;
        let action_byte = u8::from_str(&deserialized_string)
            .map_err(|_| serde::de::Error::custom("Conversion to u8 failed"))?;

        Action::try_from(action_byte)
            .map_err(|_| serde::de::Error::custom("Conversion to Action failed"))
    }
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
    ) -> Result<Self> {
        let private_pem = format!(
            "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
            encode_secret
        );
        let public_pem = format!(
            "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
            decode_secret
        );

        // Read encoding and decoding key; On error panic, we do not want malformed keys.
        let encoding_key = EncodingKey::from_ed_pem(private_pem.as_bytes())?;
        let decoding_key = DecodingKey::from_ed_pem(public_pem.as_bytes())?;

        // Check if public key already exists in database/cache
        let pubkey_serial: i64 = if let Some(key_serial) = cache.get_pubkey_serial(&decode_secret) {
            key_serial as i64
        } else {
            // Add public key to database and cache
            let client = database.get_client().await?;
            let pub_key = DbPubKey::create_or_get_without_id(None, &decode_secret, &client).await?;

            cache.add_pubkey(
                pub_key.id as i32,
                PubKeyEnum::Server((decode_secret, decoding_key.clone())), //ToDo: Server ID?
            );

            // Notification --> Announcement::PubKey::New?

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
    pub fn get_current_pubkey_serial(&self) -> i64 {
        // Gets the signing key info -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();
        signing_key.0
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
            exp: if let Some(expiration) = expires_at {
                expiration.seconds as usize
            } else {
                // Add 10 years to token lifetime if  expiry unspecified
                (Utc::now().timestamp() as usize) + 315360000
            },
            tid: Some(token_id.to_string()),
            it: None,
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
        token_id: Option<String>, // None if original request came with OIDC
        intent: Option<Intent>,   // Some Dataproxy action to restrict token usage scope
    ) -> Result<String> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().unwrap();

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: user_id.to_string(),
            exp: (Utc::now().timestamp() as usize) + 86400, // One day for now.
            tid: token_id,
            it: intent,
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        Ok(encode(&header, &claims, &signing_key.1)?)
    }

    pub async fn process_token(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,         // Proxy or Token Id
        Option<DieselUlid>, // User_id
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
        Option<Intent>,
    )> {
        let split = token
            .split('.')
            .nth(1)
            .ok_or_else(|| anyhow!("Invalid token"))?;
        let decoded = general_purpose::STANDARD_NO_PAD.decode(split)?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded)?;

        match claims.iss.as_str() {
            "aruna" => self.validate_server_token(token).await,
            "aruna_dataproxy" => self.validate_dataproxy_token(token).await,
            "localhost.test" => self.validate_oidc_token(token).await,
            _ => Err(anyhow!("Unknown issuer")),
        }
    }

    ///ToDo: Rust Doc
    async fn validate_server_token(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,                           // User_ID or Endpoint_ID
        Option<DieselUlid>,                   // Maybe Token_ID
        Vec<(DieselUlid, DbPermissionLevel)>, // Associated Permissions
        bool,                                 //Option<DieselUlid> extrahiert aus Claims.sub (?)
        Option<Intent>,
    )> {
        // Extract pubkey id from JWT header
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;

        // Fetch pubkey from cache
        let cached_key = self
            .cache
            .pubkeys
            .get(&kid.parse::<i32>()?)
            .ok_or_else(|| anyhow!("Unspecified kid"))?
            .clone();

        // Check if pubkey is from ArunaServer or Dataproxy.
        let (_, dec_key) = match cached_key {
            PubKeyEnum::Server(key) => key,
            PubKeyEnum::DataProxy((_, _, _)) => {
                return Err(anyhow::anyhow!("Token not signed from ArunaServer"))
            }
        };

        // Decode claims with pubkey
        let claims =
            decode::<ArunaTokenClaims>(token, &dec_key, &Validation::new(Algorithm::EdDSA))?;

        // Fetch user from cache
        let uid = DieselUlid::from_str(&claims.claims.sub)?;
        let user = self.cache.get_user(&uid);

        // Convert token id if present
        let token = match claims.claims.tid {
            Some(token_id) => Some(DieselUlid::from_str(&token_id)?),
            None => None,
        };

        // Fetch permissions associated with token
        if let Some(user) = user {
            let perms = user.get_permissions(token)?;
            return Ok((user.id, token, perms, false, None));
        }
        bail!("Invalid user")
    }

    ///ToDo: Rust Doc
    async fn validate_dataproxy_token(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,
        Option<DieselUlid>,
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
        Option<Intent>,
    )> {
        // Extract pubkey id from JWT header
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;

        // Fetch pubkey from cache
        let key = self
            .cache
            .pubkeys
            .get(&kid.parse::<i32>()?)
            .ok_or_else(|| anyhow!("Unspecified kid"))?
            .clone();

        // Check if pubkey is from ArunaServer or Dataproxy.
        match key {
            PubKeyEnum::DataProxy((_, key, endpoint_id)) => {
                // Decode claims with pubkey
                let claims =
                    decode::<ArunaTokenClaims>(token, &key, &Validation::new(Algorithm::EdDSA))?;

                // Intent is mandatory with Dataproxy signed tokens
                if let Some(intent) = claims.claims.it {
                    // Check if endpoint id matches the id associated with the pubkey
                    if endpoint_id != intent.target {
                        bail!("Invalid intent target id")
                    }

                    // Convert claims sub to ULID
                    let sub_id = DieselUlid::from_str(&claims.claims.sub)?;

                    // Check if intent action is valid
                    match intent.action {
                        //Case 1: Dataproxy notification fetch
                        Action::FetchInfo => Ok((sub_id, None, vec![], true, Some(intent))),
                        //Case 2: Dataproxy user impersonation
                        Action::Impersonate => {
                            // Fetch user from cache
                            let user = self.cache.get_user(&sub_id);

                            // Convert token id if present
                            let token = match claims.claims.tid {
                                Some(token_id) => Some(DieselUlid::from_str(&token_id)?),
                                None => None,
                            };

                            // Fetch permissions associated with token
                            if let Some(user) = user {
                                let perms = user.get_permissions(token)?;
                                return Ok((user.id, token, perms, true, Some(intent)));
                            }
                            bail!("Invalid user provided")
                        }
                        _ => bail!("Invalid Dataproxy signed token intent"),
                    }
                } else {
                    bail!("Missing intent in Dataproxy signed token")
                }
            }
            PubKeyEnum::Server(_) => Err(anyhow::anyhow!("Token not signed from Dataproxy")),
        }
    }

    pub(crate) async fn process_oidc_token(&self, token: &str) -> Result<ArunaTokenClaims> {
        // Read current oidc public key
        let read = {
            let lock = self.oidc_pubkey.try_read().unwrap();
            lock.clone()
        };
        // Extract header from JWT
        let header = decode_header(token)?;

        // Decode JWT claims
        let token_data = match read {
            Some(pubkey) => {
                decode::<ArunaTokenClaims>(token, &pubkey, &Validation::new(header.alg))?
            }
            None => decode::<ArunaTokenClaims>(
                token,
                &self.get_token_realminfo().await?,
                &Validation::new(header.alg),
            )?,
        };

        Ok(token_data.claims)
    }

    ///ToDo: Rust Doc
    async fn validate_oidc_token(
        &self,
        token: &str,
    ) -> Result<(
        DieselUlid,
        Option<DieselUlid>,
        Vec<(DieselUlid, DbPermissionLevel)>,
        bool,
        Option<Intent>,
    )> {
        let claims = self.process_oidc_token(token).await?;
        // Fetch user from oidc provider
        let user = self.cache.get_user_by_oidc(&claims.sub)?;
        let perms = user.get_permissions(None)?;

        Ok((user.id, None, perms, false, None))
    }

    /// Fetches the public key from the OIDC provider.
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
    pub async fn sign_hook_secret(
        &self,
        cache: Arc<Cache>,
        object_id: DieselUlid,
        hook_id: DieselUlid,
    ) -> Result<(String, i32)> {
        let serial = self.get_current_pubkey_serial() as i32;
        let key = cache
            .get_pubkey(serial)
            .ok_or_else(|| anyhow!("Pubkey not found"))?
            .get_key_string();
        let mut mac = HmacSha256::new_from_slice(key.as_bytes())?;
        let sign = format!("{}{}", object_id.to_string(), hook_id.to_string());
        mac.update(sign.as_bytes());
        Ok((
            general_purpose::STANDARD.encode(mac.finalize().into_bytes()),
            serial,
        ))
    }
    pub fn verify_hook_secret(
        &self,
        cache: Arc<Cache>,
        secret: String,
        object_id: DieselUlid,
        hook_id: DieselUlid,
        pubkey_serial: i32,
    ) -> Result<()> {
        let key = cache
            .get_pubkey(pubkey_serial)
            .ok_or_else(|| anyhow!("No pubkey found"))?
            .get_key_string();
        let mut mac = HmacSha256::new_from_slice(key.as_bytes())?;
        let sign = format!("{}{}", object_id.to_string(), hook_id.to_string());
        mac.update(sign.as_bytes());
        mac.verify_slice(&secret.as_bytes())?;
        Ok(())
    }
}

// Token tests
#[cfg(test)]
mod tests {}
