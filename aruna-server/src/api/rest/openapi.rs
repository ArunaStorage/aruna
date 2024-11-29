use super::api_paths::*;
use crate::{
    models::{models::Permission, requests::Direction},
    transactions::controller::Controller,
};
use std::sync::Arc;
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    components(schemas(Permission, Direction))
)]
pub struct ArunaApi;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if let Some(components) = openapi.components.as_mut() {
            components.add_security_scheme(
                "auth",
                SecurityScheme::ApiKey(ApiKey::Header(ApiKeyValue::with_description(
                    "Authorization",
                    "Prefixed with Bearer",
                ))),
            )
        }
    }
}

pub fn router(store: Arc<Controller>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(create_resource))
        .routes(routes!(update_resource_name))
        .routes(routes!(update_resource_title))
        .routes(routes!(create_resource_batch))
        .routes(routes!(create_project))
        .routes(routes!(get_resource))
        .routes(routes!(create_realm))
        .routes(routes!(get_realm))
        .routes(routes!(create_group))
        .routes(routes!(get_group))
        .routes(routes!(add_group))
        .routes(routes!(register_user))
        .routes(routes!(create_token))
        .routes(routes!(search))
        .routes(routes!(get_user_realms))
        .routes(routes!(get_user_groups))
        .routes(routes!(get_stats))
        .routes(routes!(get_realm_components))
        .routes(routes!(get_relations))
        .routes(routes!(get_group_users))
        .routes(routes!(get_realm_groups))
        .routes(routes!(get_relation_infos))
        .routes(routes!(get_user))
        .routes(routes!(request_group_access_realm))
        .routes(routes!(request_user_access_group))
        .routes(routes!(get_events))
        .routes(routes!(create_relation))
        .routes(routes!(create_relation_variant))
        .routes(routes!(create_component))
        .routes(routes!(add_user))
        .routes(routes!(add_component_to_realm))
        .routes(routes!(register_data))
        .with_state(store)
}
