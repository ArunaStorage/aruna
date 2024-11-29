use std::{str::FromStr, sync::Arc};

use super::{
    controller::Controller,
    request::{AuthMethod, Request, Requester, SerializedResponse, WriteRequest},
};
use crate::{
    context::{BatchPermission, Context},
    error::ArunaError,
    models::{
        models::{ArunaTokenClaims, Audience, IssuerKey, IssuerType, Scope},
        requests::{AddOidcProviderRequest, AddOidcProviderResponse},
    },
    storage::store::Store,
};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, DecodingKey, Header};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use ulid::Ulid;

impl Controller {
    #[tracing::instrument(level = "trace", skip(self, request))]
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
        let store = self.get_store();
        let user = user.clone();
        match ctx {
            Context::Public => Ok(()),
            Context::NotRegistered => Ok(()), // Must provide valid oidc_token
            Context::UserOnly => validate_user_only(user, store).await,
            Context::SubscriberOwnerOf(subscriber_id) => {
                validate_subscriber_of(user, subscriber_id, store).await
            }
            Context::GlobalAdmin => Err(ArunaError::Forbidden(String::new())), // TODO: Impl global
            // admins
            Context::Permission {
                min_permission,
                source,
            } => validate_permission_batch(user, &[BatchPermission{ min_permission, source }], store).await,
            Context::PermissionBatch(permissions) => {
                validate_permission_batch(user, &permissions, store).await
            }
            Context::PermissionFork {
                first_min_permission,
                first_source,
                second_min_permission,
                second_source,
            } => {
                let first = BatchPermission {
                    min_permission: first_min_permission,
                    source: first_source,
                };
                let second = BatchPermission {
                    min_permission: second_min_permission,
                    source: second_source,
                };
                validate_permission_batch(user, &[first, second], store).await
            }
        }
    }
}

pub struct TokenHandler {
    store: Arc<Store>,
}

impl TokenHandler {
    pub fn new(store: Arc<Store>) -> Self {
        Self { store }
    }

    ///ToDo: Rust Doc
    pub fn sign_user_token(
        store: &Store,
        is_service_account: bool,
        token_idx: u16,
        user_id: &Ulid, // User or ServiceAccount
        scope: Option<String>,
        expires_at: Option<u64>,
    ) -> Result<String, ArunaError> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let (kid, encoding_key) = store.get_encoding_key()?;
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

        encode(&header, &claims, &encoding_key).map_err(|e| {
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

        let (issuer_type, issuer_name, decoding_key, audiences) = self
            .store
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

        let claims = Self::get_validate_claims(token, header.alg, &decoding_key, &audiences)?;

        match issuer_type {
            IssuerType::OIDC => self.validate_oidc_token(&claims),
            IssuerType::SERVER => self.extract_token_info(&claims, None),
            IssuerType::DATAPROXY => self.extract_token_info(
                &claims,
                Some(Ulid::from_str(&issuer_name).map_err(|_| {
                    tracing::error!("Invalid token id provided");
                    ArunaError::Unauthorized
                })?),
            ),
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
    fn extract_token_info(
        &self,
        subject: &ArunaTokenClaims,
        impersonated: Option<Ulid>,
    ) -> Result<Requester, ArunaError> {
        let user_id = Ulid::from_string(&subject.sub).map_err(|_| {
            tracing::error!("Invalid token id provided");
            ArunaError::Unauthorized
        })?;

        let Some((is_service_account, token_idx)) = subject.info else {
            tracing::error!("No token info provided");
            return Err(ArunaError::Unauthorized);
        };
        match is_service_account {
            0u8 => {
                // False
                self.store.ensure_token_exists(&user_id, token_idx)?;

                Ok(Requester::User {
                    user_id,
                    auth_method: AuthMethod::Aruna(token_idx),
                    impersonated_by: impersonated,
                })
            }
            1u8 => {
                // True
                self.store.ensure_token_exists(&user_id, token_idx)?;
                let group_id = self.store.get_group_from_sa(&user_id)?;

                Ok(Requester::ServiceAccount {
                    service_account_id: user_id,
                    token_id: token_idx,
                    group_id,
                    impersonated_by: impersonated,
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
        let oidc_mapping = (claims.sub.clone(), claims.iss.clone());

        // Fetch user from oidc provider
        Ok(self.store.get_user_by_oidc(oidc_mapping).map_err(|e| {
            tracing::error!("{e}");
            ArunaError::Unauthorized
        })?)
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

impl Request for AddOidcProviderRequest {
    type Response = AddOidcProviderResponse;
    fn get_context(&self) -> Context {
        Context::GlobalAdmin
    }

    async fn run_request(
        self,
        requester: Option<Requester>,
        controller: &Controller,
    ) -> Result<Self::Response, ArunaError> {
        let request_tx = AddOidcProviderRequestTx {
            req: self,
            requester: Some(requester.ok_or_else(|| ArunaError::Unauthorized)?), // This MUST be some and is only None for internal startup use
        };
        let response = controller.transaction(Ulid::new().0, &request_tx).await?;

        Ok(bincode::deserialize(&response)?)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddOidcProviderRequestTx {
    pub req: AddOidcProviderRequest,
    pub requester: Option<Requester>,
}

#[typetag::serde]
#[async_trait::async_trait]
impl WriteRequest for AddOidcProviderRequestTx {
    async fn execute(
        &self,
        associated_event_id: u128,
        controller: &Controller,
    ) -> Result<SerializedResponse, crate::error::ArunaError> {
        if let Some(requester) = &self.requester {
            controller.authorize(requester, &self.req).await?;
        }

        let store = controller.get_store();
        let keys = IssuerKey::fetch_jwks(&self.req.issuer_endpoint).await?;
        let issuer_name = self.req.issuer_name.clone();
        let issuer_endpoint = self.req.issuer_endpoint.clone();
        let audiences = self.req.audiences.clone();

        Ok(tokio::task::spawn_blocking(move || {
            let mut wtxn = store.write_txn()?;

            store.add_issuer(&mut wtxn, issuer_name, issuer_endpoint, audiences, keys)?;

            // TODO: Register event?
            wtxn.commit(associated_event_id, &[], &[])?;

            Ok::<_, ArunaError>(bincode::serialize(&AddOidcProviderResponse {})?)
        })
        .await
        .map_err(|e| ArunaError::ServerError(e.to_string()))??)
    }
}

async fn validate_user_only(user: Requester, store: Arc<Store>) -> Result<(), ArunaError> {
    match &user {
        Requester::User { auth_method, .. } => match auth_method {
            AuthMethod::Aruna(..) => {
                let user = user.clone();
                tokio::task::spawn_blocking(move || {
                    let txn = store.read_txn()?;
                    let token = store.get_token(&user, &txn, &store.get_graph())?;

                    match token.scope {
                        Scope::Personal => Ok(()),
                        _ => Err(ArunaError::Forbidden("Invalid scope".to_string())),
                    }
                })
                .await
                .map_err(|_| {
                    tracing::error!("Error joining thread");
                    ArunaError::Unauthorized
                })??;
                Ok(())
            }
            AuthMethod::Oidc { .. } => Ok(()),
        },
        _ => {
            tracing::error!("ServiceAccount not allowed");
            Err(ArunaError::Forbidden(
                "SerivceAccounts are not allowed".to_string(),
            ))
        }
    }
}

async fn validate_subscriber_of(
    user: Requester,
    subscriber_id: Ulid,
    store: Arc<Store>,
) -> Result<(), ArunaError> {
    let user_id = user
        .get_id()
        .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;
    tokio::task::spawn_blocking(move || {
        let txn = store.read_txn()?;
        let result = store.get_subscribers(&txn)?;

        let Some(subscriber) = result
            .iter()
            .find(|subscriber| subscriber.id == subscriber_id)
        else {
            return Err(ArunaError::Forbidden("No owner found".to_string()));
        };
        if subscriber.owner != user_id {
            Err(ArunaError::Forbidden("Not owner".to_string()))
        } else {
            Ok(())
        }
    })
    .await
    .map_err(|_| {
        tracing::error!("Error joining thread");
        ArunaError::Unauthorized
    })??;
    Ok(())
}

async fn validate_permission_batch(
    user: Requester,
    permissions: &[BatchPermission],
    store: Arc<Store>,
) -> Result<(), ArunaError> {
    let user_id = user
        .get_id()
        .ok_or_else(|| ArunaError::Forbidden("Unregistered".to_string()))?;
    for permission in permissions {
        let source = permission.source.clone();
        let store = store.clone();
        let perm = tokio::task::spawn_blocking(move || store.get_permissions(&source, &user_id))
            .await
            .map_err(|_| {
                tracing::error!("Error joining thread");
                ArunaError::Unauthorized
            })??;
        if perm >= permission.min_permission {
            continue;
        } else {
            tracing::error!("Insufficient permission");
            return Err(ArunaError::Forbidden("Permission denied".to_string()));
        }
    }
    Ok(())
}
