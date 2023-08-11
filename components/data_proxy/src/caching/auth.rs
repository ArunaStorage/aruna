use super::cache::Cache;
use anyhow::anyhow;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

pub(crate) struct AuthHandler {
    pub cache: Arc<RwLock<Cache>>,
}

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

#[repr(u8)]
#[non_exhaustive]
#[derive(Debug, Serialize, Deserialize)]
pub enum Action {
    Notifications = 0,
    CreateSecrets = 1,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Intent {
    target: DieselUlid,
    #[serde(flatten)]
    action: Action,
}

impl AuthHandler {
    pub fn new(cache: Arc<RwLock<Cache>>) -> Self {
        Self { cache }
    }

    pub fn check_permissions(&self, token: &str) -> Result<bool> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;
        match self.cache.read() {
            Ok(cache) => {
                let (pk, dec_key) = cache.get_pubkey(kid);
            }
            Err(_) => Ok(false),
        }
    }

    pub fn extract_claims(&self, token: &str, dec_key: &DecodingKey) -> Result<ArunaTokenClaims> {
        let token = decode::<ArunaTokenClaims>(
            token,
            dec_key,
            &Validation::new(jsonwebtoken::Algorithm::EdDSA),
        )?;
        Ok(token.claims)
    }

    // /// Signing function to create a token for a specific endpoint to fetch all notifications
    // /// of its consumer.
    // pub fn sign_proxy_notifications_token(
    //     &self,
    //     endpoint_id: &DieselUlid,
    //     token_id: &DieselUlid,
    // ) -> Result<String> {
    //     // Gets the signing key -> if this returns a poison error this should also panic
    //     // We dont want to allow poisoned / malformed encoding keys and must crash at this point
    //     let signing_key = self.signing_info.read().unwrap();

    //     let claims = ArunaTokenClaims {
    //         iss: "aruna".to_string(),
    //         sub: endpoint_id.to_string(),
    //         exp: (Utc::now().timestamp() as usize) + 315360000, // 10 years for now.
    //         tid: None,
    //         intent: Some(format!("{}_{}", endpoint_id, "notification")),
    //     };

    //     let header = Header {
    //         kid: Some(format!("{}", signing_key.0)),
    //         alg: Algorithm::EdDSA,
    //         ..Default::default()
    //     };

    //     Ok(encode(&header, &claims, &signing_key.1)?)
    // }

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
}
