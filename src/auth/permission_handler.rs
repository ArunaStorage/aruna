use super::{structs::Context, token_handler::TokenHandler};
use crate::caching::cache::Cache;
use anyhow::Result;
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

    pub fn check_permissions(&self, token: &str, ctxs: Vec<Context>) -> Result<Option<DieselUlid>> {
        Ok(None)
    }
}
