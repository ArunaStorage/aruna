use super::api_paths::*;
use crate::models::models::*;
use crate::models::requests::*;
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};

#[derive(OpenApi)]
#[openapi(
    modifiers(&SecurityAddon),
    paths(
        create_resource,
        create_project,
        get_resource,
        create_realm,
        get_realm,
        create_group,
        get_group,
        add_group,
        register_user,
    ),
    components(schemas(
        CreateResourceRequest,
        CreateResourceResponse,
        CreateProjectRequest,
        CreateProjectResponse,
        GetResourceRequest,
        GetResourceResponse,
        CreateRealmRequest,
        CreateRealmResponse,
        CreateGroupRequest,
        CreateGroupResponse,
        GetGroupRequest,
        GetGroupResponse,
        AddGroupRequest,
        AddGroupResponse,
        RegisterUserRequest,
        RegisterUserResponse,
        User,
        Author,
        Relation,
        Resource,
        KeyValue,
        Endpoint,
        Hash,
        Realm,
        Group,
        VisibilityClass,
        ResourceVariant,
    ))
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
