use std::sync::{Arc, RwLock};

use super::{
    controller::Controller,
    request::{AuthMethod, Request, Requester},
};
use crate::{
    context::Context,
    error::ArunaError,
    models::{ArunaTokenClaims, Audience, IssuerType},
    storage::store::Store,
};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, DecodingKey, Header};
use serde::de::DeserializeOwned;
use ulid::Ulid;

impl Controller {
    pub(super) async fn authorize_token<'a, R: Request>(
        &self,
        token: Option<String>,
        request: &'a R,
    ) -> Result<Option<Requester>, ArunaError> {
        let ctx = request.get_context();

        let Some(token) = token else {
            if matches!(ctx, Context::Public) {
                return Ok(None);
            } else {
                tracing::error!("No token provided");
                return Err(ArunaError::Unauthorized);
            }
        };

        let token_handler = self.get_token_handler();
        let requester = tokio::task::spawn_blocking(move || token_handler.process_token(&token))
            .await
            .map_err(|e| {
                tracing::error!(?e, "Error joining thread");
                ArunaError::ServerError("Internal server error".to_string())
            })??;

        self.authorize(&requester, request).await?;

        Ok(Some(requester))
    }

    pub(super) async fn authorize<'a, R: Request>(
        &self,
        user: &Requester,
        request: &'a R,
    ) -> Result<(), ArunaError> {
        let ctx = request.get_context();

        match ctx {
            Context::Public => Ok(()),
            Context::UserOnly => {
                if matches!(user, Requester::User { .. }) {
                    Ok(())
                } else {
                    tracing::error!("ServiceAccount not allowed");
                    Err(ArunaError::Forbidden(
                        "SerivceAccounts are not allowed".to_string(),
                    ))
                }
            }
            Context::GlobalAdmin => Ok(()),
            Context::Permission {
                min_permission,
                source,
            } => {
                let store = self.get_store();
                let user_id = user.get_id();
                let source = source.clone();
                let perm = tokio::task::spawn_blocking(move || {
                    store.read().unwrap().get_permissions(&source, &user_id)
                })
                .await
                .map_err(|_| {
                    tracing::error!("Error joining thread");
                    ArunaError::Unauthorized
                })??;
                if &perm >= min_permission {
                    Ok(())
                } else {
                    tracing::error!("Insufficient permission");
                    return Err(ArunaError::Forbidden("Permission denied".to_string()));
                }
            }
            Context::Permissions {
                first_min_permission,
                first_source,
                second_min_permission,
                second_source,
            } => {
                let store = self.get_store();
                let user_id = user.get_id();
                let first_source = first_source.clone();

                let first_perm = tokio::task::spawn_blocking(move || {
                    store
                        .read()
                        .unwrap() // Unwrap of poison lock
                        .get_permissions(&first_source, &user_id)
                })
                .await
                .map_err(|_| {
                    tracing::error!("Error joining thread");
                    ArunaError::Unauthorized
                })??;

                let second_source = second_source.clone();
                let store = self.get_store();
                let second_perm = tokio::task::spawn_blocking(move || {
                    store
                        .read()
                        .unwrap() // Unwrap of poison lock
                        .get_permissions(&second_source, &user_id)
                })
                .await
                .map_err(|_| {
                    tracing::error!("Error joining thread");
                    ArunaError::Unauthorized
                })??;

                if &first_perm >= first_min_permission && &second_perm >= second_min_permission {
                    Ok(())
                } else {
                    tracing::error!("Insufficient permission");
                    return Err(ArunaError::Forbidden("Permission denied".to_string()));
                }
            }
        }
    }
}

pub struct TokenHandler {
    store: Arc<RwLock<Store>>,
}

impl TokenHandler {
    pub fn new(store: Arc<RwLock<Store>>) -> Self {
        Self { store }
    }

    pub fn read(&self) -> std::sync::RwLockReadGuard<Store> {
        self.store.read().unwrap()
    }

    ///ToDo: Rust Doc
    pub fn sign_user_token(
        &self,
        is_service_account: bool,
        token_idx: u16,
        user_id: &Ulid, // User or ServiceAccount
        scope: Option<String>,
        expires_at: Option<u64>,
    ) -> Result<String, ArunaError> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let store = self.read();
        let (kid, encoding_key) = store.get_encoding_key();
        let is_sa_u8 = if is_service_account { 1u8 } else { 0u8 };

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: user_id.to_string(),
            exp: if let Some(expiration) = expires_at {
                expiration
            } else {
                // Add 10 years to token lifetime if  expiry unspecified
                (Utc::now().timestamp() as u64) + 315360000
            },
            info: Some((is_sa_u8, token_idx)),
            scope,
            aud: Some(Audience::String("aruna".to_string())),
        };

        let header = Header {
            kid: Some(format!("{}", kid)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        encode(&header, &claims, encoding_key).map_err(|e| {
            tracing::error!("User token signing failed: {:?}", e);
            ArunaError::ServerError("Error creating token".to_string())
        })
    }

    fn process_token(&self, token: &str) -> Result<Requester, ArunaError> {
        // Split the token into header and payload
        let mut split = token.split('.').map(b64_decode);

        // Decode and deserialize a potential header to get the key id
        let header = deserialize_field::<Header>(&mut split)?;
        // Decode and deserialize a potential payload to get the claims and issuer
        let unvalidated_claims = deserialize_field::<ArunaTokenClaims>(&mut split)?;

        let store = self.read();
        let (issuer_type, decoding_key, audiences) = store
            .get_issuer_info(
                unvalidated_claims.iss.to_string(),
                header.kid.ok_or_else(|| {
                    tracing::error!("No kid specified in token");
                    ArunaError::Unauthorized
                })?,
            )
            .ok_or_else(|| {
                tracing::error!("No issuer found");
                ArunaError::Unauthorized
            })?;

        let claims = Self::get_validate_claims(token, header.alg, decoding_key, &audiences)?;

        match issuer_type {
            IssuerType::OIDC => self.validate_oidc_token(&claims),
            IssuerType::ARUNA => self.extract_token_info(&claims),
        }
    }

    fn get_validate_claims(
        token: &str,
        alg: Algorithm,
        decoding_key: &DecodingKey,
        aud: &[String],
    ) -> Result<ArunaTokenClaims, ArunaError> {
        let mut validation = jsonwebtoken::Validation::new(alg);
        validation.set_audience(aud);
        let tokendata = jsonwebtoken::decode::<ArunaTokenClaims>(token, decoding_key, &validation)
            .map_err(|e| {
                tracing::error!(?e, "Error decoding token header");
                ArunaError::Unauthorized
            })?;
        Ok(tokendata.claims)
    }

    ///ToDo: Rust Doc
    fn extract_token_info(&self, subject: &ArunaTokenClaims) -> Result<Requester, ArunaError> {
        let user_id = Ulid::from_string(&subject.sub).map_err(|_| {
            tracing::error!("Invalid token id provided");
            ArunaError::Unauthorized
        })?;

        let store = self.store.read().expect("Poisoned lock");

        let Some((is_service_account, token_idx)) = subject.info else {
            tracing::error!("No token info provided");
            return Err(ArunaError::Unauthorized);
        };
        match is_service_account {
            0u8 => {
                // False

                store.ensure_token_exists(&user_id, token_idx)?;

                Ok(Requester::User {
                    user_id,
                    auth_method: AuthMethod::Aruna(token_idx),
                })
            }
            1u8 => {
                // True
                store.ensure_token_exists(&user_id, token_idx)?;
                let group_id = store.get_group_from_sa(&user_id)?;

                Ok(Requester::ServiceAccount {
                    service_account_id: user_id,
                    token_id: token_idx,
                    group_id,
                })
            }
            _ => {
                tracing::error!("Invalid service account flag");
                Err(ArunaError::Unauthorized)
            }
        }
    }

    ///ToDo: Rust Doc
    fn validate_oidc_token(&self, claims: &ArunaTokenClaims) -> Result<Requester, ArunaError> {
        let _oidc_mapping = (claims.iss.clone(), claims.sub.clone());
        // Fetch user from oidc provider
        //self.controller.get_user_by_oidc(oidc_mapping)
        todo!()
    }
}

/// Convert a base64 encoded field into a deserialized struct
/// SAFETY: These fields are untrusted and should be handled with care
pub(crate) fn deserialize_field<T: DeserializeOwned>(
    iterator: &mut impl Iterator<Item = Result<Vec<u8>, ArunaError>>,
) -> Result<T, ArunaError> {
    serde_json::from_slice::<T>(&iterator.next().ok_or_else(|| {
        tracing::error!("No header found in token");
        ArunaError::Unauthorized
    })??)
    .map_err(|e| {
        tracing::error!(?e, "Error deserializing token header");
        ArunaError::Unauthorized
    })
}

pub(crate) fn b64_decode<T: AsRef<[u8]>>(input: T) -> Result<Vec<u8>, ArunaError> {
    general_purpose::URL_SAFE_NO_PAD.decode(input).map_err(|e| {
        tracing::error!(?e, "Error decoding base64");
        ArunaError::Unauthorized
    })
}
