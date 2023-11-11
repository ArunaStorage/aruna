use super::{
    structs::{Context, ContextVariant},
    token_handler::{Action, OIDCError, TokenHandler},
};
use crate::{
    caching::cache::Cache,
    database::{dsls::user_dsl::OIDCMapping, enums::DbPermissionLevel},
};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use std::sync::Arc;

pub struct PermissionHandler {
    cache: Arc<Cache>,
    pub token_handler: Arc<TokenHandler>,
}

impl PermissionHandler {
    pub fn new(cache: Arc<Cache>, token_handler: Arc<TokenHandler>) -> Self {
        Self {
            cache: cache.clone(),
            token_handler, //Arc::new(TokenHandler::new(cache, realm_info.to_string())),
        }
    }

    pub async fn check_permissions_verbose(
        &self,
        token: &str,
        ctxs: Vec<Context>,
    ) -> Result<(DieselUlid, Option<DieselUlid>, bool), tonic::Status> {
        // What are the cases?
        // 1. User Aruna token       --> (user_id, token_id)
        // 2. User OIDC token        --> (user_id, None)
        // 3. Endpoint signed token  --> (user_id, ?)
        // 4. Endpoint notifications --> (endpoint_id, None)
        // let (main_id, token, personal, permissions, is_proxy, proxy_intent) = tonic_auth!(
        //     self.token_handler.process_token(token).await,
        //     "Unauthorized"
        // );
        let (main_id, token, personal, permissions, is_proxy, proxy_intent) =
            match self.token_handler.process_token(token).await {
                Ok(results) => results,
                Err(err) => {
                    return match err.downcast_ref::<OIDCError>() {
                        Some(_) => Err(tonic::Status::unauthenticated("Not registered")),
                        None => Err(tonic::Status::unauthenticated("Unauthorized")),
                    }
                }
            };

        // Individual permission checking if token is signed from Dataproxy
        if is_proxy {
            return if let Some(intent) = proxy_intent {
                if intent.action == Action::Impersonate {
                    //Case 1: Impersonate
                    //  - Check if provided contexts are proxy/self/resource only
                    for ctx in &ctxs {
                        dbg!(&ctx);
                        match ctx.variant {
                            ContextVariant::SelfUser
                            | ContextVariant::GlobalProxy
                            | ContextVariant::Resource(_) => {}
                            _ => return Err(tonic::Status::invalid_argument(
                                "Only resource functionality allowed for Dataproxy signed tokens",
                            )),
                        }
                    }
                } else if intent.action == Action::FetchInfo {
                    //Case 2: FetchInfo
                    //  - Only get functions -> DbPermissionLevel::READ in contexts
                    for ctx in &ctxs {
                        dbg!(&ctx);
                        match ctx.variant {
                            ContextVariant::SelfUser
                            | ContextVariant::Registered
                            | ContextVariant::GlobalProxy => {}
                            ContextVariant::Resource((_, perm))
                            | ContextVariant::User((_, perm)) => {
                                if perm > DbPermissionLevel::READ {
                                    return Err(tonic::Status::permission_denied(
                                        "Only get functions allowed",
                                    ));
                                }
                            }
                            _ => {
                                return Err(tonic::Status::permission_denied(
                                    "Only get functions allowed",
                                ))
                            }
                        }
                    }
                    //unimplemented!("Permission check for Dataproxy notification fetch not yet implemented")
                }

                if self.cache.check_proxy_ctxs(&intent.target, &ctxs) {
                    Ok((main_id, token, true))
                } else {
                    Err(tonic::Status::unauthenticated(
                        "Invalid proxy authentication",
                    ))
                }
            } else {
                Err(tonic::Status::internal("Missing intent action"))
            };
        }

        // Check permissions for standard ArunaServer user token
        if self
            .cache
            .check_permissions_with_contexts(&ctxs, &permissions, personal, &main_id)
        {
            Ok((main_id, token, false))
        } else {
            Err(tonic::Status::unauthenticated("Invalid permissions"))
        }
    }

    ///ToDo: Rust Doc
    pub async fn check_permissions(
        &self,
        token: &str,
        ctxs: Vec<Context>,
    ) -> Result<DieselUlid, tonic::Status> {
        let (user_id, _, _) = self.check_permissions_verbose(token, ctxs).await?;
        Ok(user_id)
    }

    pub async fn check_unregistered_oidc(&self, token: &str) -> Result<OIDCMapping> {
        Ok(self.token_handler.process_oidc_token(token).await?.sub)
    }
}
