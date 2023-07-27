use super::{structs::Context, token_handler::TokenHandler};
use crate::{caching::cache::Cache, database::enums::DbPermissionLevel};
use diesel_ulid::DieselUlid;
use std::sync::Arc;

pub struct PermissionHandler {
    cache: Arc<Cache>,
    token_handler: Arc<TokenHandler>,
}

impl PermissionHandler {
    pub fn new(cache: Arc<Cache>, realm_info: &str) -> Self {
        Self {
            cache: cache.clone(),
            token_handler: Arc::new(TokenHandler::new(cache, realm_info.to_string())),
        }
    }

    pub async fn check_permissions(
        &self,
        token: &str,
        mut ctxs: Vec<Context>,
    ) -> Result<Option<DieselUlid>, tonic::Status> {
        let (mut main_id, associated_id, permissions, is_proxy) = tonic_auth!(
            self.token_handler.process_token(token).await,
            "Unauthorized"
        );

        if is_proxy {
            match associated_id {
                Some(id) => {
                    ctxs.push(Context::user_ctx(id, DbPermissionLevel::READ));
                    if !self.cache.check_proxy_ctxs(&main_id, &ctxs) {
                        return Err(tonic::Status::unauthenticated(
                            "Invalid proxy authentication",
                        ));
                    }
                    main_id = id;
                }
                None => {
                    if self.cache.check_proxy_ctxs(&main_id, &ctxs) {
                        return Ok(None);
                    } else {
                        return Err(tonic::Status::unauthenticated(
                            "Invalid proxy authentication",
                        ));
                    }
                }
            }
        }

        Ok(Some(main_id))
    }
}
