use super::{
    controller::{Controller, Get},
    request::Request,
    transaction::Requester,
};
use crate::{
    context::Context,
    error::ArunaError,
    models::{ArunaTokenClaims, Audience, IssuerType},
};
use base64::{engine::general_purpose, Engine};
use chrono::Utc;
use jsonwebtoken::{decode_header, encode, Algorithm, DecodingKey, Header};
use ulid::Ulid;

#[async_trait::async_trait]
pub trait Auth: Send + Sync {
    async fn authorize_token<'a, R: Request>(
        &self,
        token: Option<String>,
        request: &'a R,
    ) -> Result<Option<Requester>, ArunaError>;
    async fn authorize<'a, R: Request>(
        &self,
        user: &Requester,
        request: &'a R,
    ) -> Result<(), ArunaError>;
}

#[async_trait::async_trait]
impl Auth for Controller {
    async fn authorize_token<'a, R: Request>(
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

        let token_id = self.process_token(&token).await?;
        let requester = self.get_requester_from_aruna_token(token_id).await?;

        self.authorize(&requester, request).await?;

        Ok(Some(requester))
    }

    async fn authorize<'a, R: Request>(
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
                let perm = self
                    .store
                    .read()
                    .await
                    .graph
                    .get_permissions(&source, &user.get_id())?;

                if perm >= min_permission {
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
                let first_perm = self
                    .store
                    .read()
                    .await
                    .graph
                    .get_permissions(&first_source, &user.get_id())?;
                let second_perm = self
                    .store
                    .read()
                    .await
                    .graph
                    .get_permissions(&second_source, &user.get_id())?;
                if first_perm >= first_min_permission && second_perm >= second_min_permission {
                    Ok(())
                } else {
                    tracing::error!("Insufficient permission");
                    return Err(ArunaError::Forbidden("Permission denied".to_string()));
                }
            }
        }
    }
}

impl Controller {
    ///ToDo: Rust Doc
    async fn _get_current_pubkey_serial(&self) -> u32 {
        // Gets the signing key info -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().await;
        signing_key.0
    }

    ///ToDo: Rust Doc
    pub async fn sign_user_token(
        &self,
        token_id: &Ulid,
        expires_at: Option<u64>,
    ) -> Result<String, ArunaError> {
        // Gets the signing key -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_info.read().await;

        let claims = ArunaTokenClaims {
            iss: "aruna".to_string(),
            sub: token_id.to_string(),
            exp: if let Some(expiration) = expires_at {
                expiration
            } else {
                // Add 10 years to token lifetime if  expiry unspecified
                (Utc::now().timestamp() as u64) + 315360000
            },
            aud: Some(Audience::String("aruna".to_string())),
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        encode(&header, &claims, &signing_key.1).map_err(|e| {
            tracing::error!("User token signing failed: {:?}", e);
            ArunaError::ServerError("Error creating token".to_string())
        })
    }

    async fn process_token(&self, token: &str) -> Result<Ulid, ArunaError> {
        let split = token.split('.').nth(1).ok_or_else(|| {
            tracing::error!("Invalid token");
            ArunaError::Unauthorized
        })?;
        let decoded = general_purpose::STANDARD_NO_PAD
            .decode(split)
            .map_err(|e| {
                tracing::error!("Error b64 decoding token: {:?}", e);
                ArunaError::Unauthorized
            })?;
        let claims: ArunaTokenClaims = serde_json::from_slice(&decoded).map_err(|e| {
            tracing::error!("Error deserializing token: {:?}", e);
            ArunaError::Unauthorized
        })?;

        let issuer = self
            .get_issuer(claims.iss.to_string())
            .await
            .map_err(|_| ArunaError::Unauthorized)?;

        let claims = self
            .get_claims(token, issuer.issuer_name, issuer.audiences)
            .await?;

        match issuer.issuer_type {
            IssuerType::OIDC => self.validate_oidc_token(&claims).await,
            IssuerType::ARUNA => self.token_exists(&claims.sub).await,
        }
    }

    async fn get_claims(
        &self,
        token: &str,
        issuer_name: String,
        issuer_audiences: Option<Vec<String>>,
    ) -> Result<ArunaTokenClaims, ArunaError> {
        let kid = decode_header(token)
            .map_err(|e| {
                tracing::error!(?e, "Error decoding token header");
                ArunaError::Unauthorized
            })?
            .kid
            .ok_or_else(|| {
                tracing::error!("No kid specified in token");
                ArunaError::Unauthorized
            })?;

        match self.get_issuer_key(issuer_name, kid).await {
            Some(decoding_key) => {
                Self::get_validate_claims(token, &decoding_key, &issuer_audiences)
            }
            None => {
                tracing::error!("No matching key found");
                Err(ArunaError::Unauthorized)
            }
        }
    }

    fn get_validate_claims(
        token: &str,
        decoding_key: &DecodingKey,
        audiences: &Option<Vec<String>>,
    ) -> Result<ArunaTokenClaims, ArunaError> {
        let header = decode_header(token).map_err(|e| {
            tracing::error!(?e, "Error decoding token header");
            ArunaError::Unauthorized
        })?;
        let alg = header.alg;
        let mut validation = jsonwebtoken::Validation::new(alg);
        if let Some(aud) = audiences {
            validation.set_audience(aud)
        };
        let tokendata = jsonwebtoken::decode::<ArunaTokenClaims>(token, decoding_key, &validation)
            .map_err(|e| {
                tracing::error!(?e, "Error decoding token header");
                ArunaError::Unauthorized
            })?;
        Ok(tokendata.claims)
    }

    ///ToDo: Rust Doc
    async fn token_exists(&self, subject: &str) -> Result<Ulid, ArunaError> {
        // Fetch user from cache
        let token_id = Ulid::from_string(subject).map_err(|_| {
            tracing::error!("Invalid token id provided");
            ArunaError::Unauthorized
        })?;

        if self.get(token_id).await?.is_some() {
            Ok(token_id)
        } else {
            tracing::error!("Token id not found");
            Err(ArunaError::Unauthorized)
        }
    }

    ///ToDo: Rust Doc
    async fn validate_oidc_token(&self, claims: &ArunaTokenClaims) -> Result<Ulid, ArunaError> {
        let _oidc_mapping = (claims.iss.clone(), claims.sub.clone());
        // Fetch user from oidc provider
        //self.controller.get_user_by_oidc(oidc_mapping)
        todo!()
    }
}
