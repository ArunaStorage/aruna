use super::cache::Cache;
use anyhow::anyhow;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use jsonwebtoken::{decode, decode_header, DecodingKey, Validation};
use serde::Deserializer;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::{Arc, RwLock};

pub struct AuthHandler {
    pub cache: Arc<RwLock<Cache>>,
    pub self_id: DieselUlid,
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
                self.target.to_string(),
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
    pub fn new(cache: Arc<RwLock<Cache>>, self_id: DieselUlid) -> Self {
        Self { cache, self_id }
    }

    pub fn check_permissions(&self, token: &str) -> Result<Option<DieselUlid>> {
        let kid = decode_header(token)?
            .kid
            .ok_or_else(|| anyhow!("Unspecified kid"))?;
        match self.cache.read() {
            Ok(cache) => {
                let (pk, dec_key) = cache.get_pubkey(i32::from_str(&kid)?)?;
                let claims = self.extract_claims(token, &dec_key)?;

                if let Some(it) = claims.it {
                    if it.action == Action::CreateSecrets && it.target == self.self_id {
                        return Ok(Some(DieselUlid::from_str(&claims.sub)?));
                    }
                }

                Ok(None)
            }
            Err(_) => Ok(None),
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
}
