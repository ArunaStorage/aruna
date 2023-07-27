use super::{structs::Context, token_handler::TokenHandler};
use crate::caching::cache::Cache;
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
        _ctxs: Vec<Context>,
    ) -> Result<Option<DieselUlid>, tonic::Status> {
        let (user_id, permissions, is_proxy) = tonic_auth!(
            self.token_handler.process_token(token).await,
            "Unauthorized"
        );

        Ok(None)
    }
}
