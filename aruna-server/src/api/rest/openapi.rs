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
        create_resource_batch,
        create_project,
        get_resource,
        create_realm,
        get_realm,
        create_group,
        get_group,
        add_group,
        register_user,
        create_token,
        search,
        get_user_realms,
        get_user_groups,
        get_stats,
        get_realm_components,
        get_relations,
        get_group_users,
        get_realm_groups,
        get_relation_infos,
        get_user,
    ),
    components(schemas(
        CreateResourceBatchRequest,
        CreateResourceBatchResponse,
        CreateResourceRequest,
        CreateResourceResponse,
        CreateProjectRequest,
        CreateProjectResponse,
        GetResourcesRequest,
        GetResourcesResponse,
        CreateRealmRequest,
        CreateRealmResponse,
        CreateGroupRequest,
        CreateGroupResponse,
        GetGroupRequest,
        GetGroupResponse,
        AddGroupRequest,
        AddGroupResponse,
        AddUserRequest,
        AddUserResponse,
        RegisterUserRequest,
        RegisterUserResponse,
        CreateTokenRequest,
        CreateTokenResponse,
        SearchRequest,
        SearchResponse,
        GetRealmsFromUserRequest,
        GetRealmsFromUserResponse,
        GetGroupsFromUserRequest,
        GetGroupsFromUserResponse,
        GetStatsRequest,
        GetStatsResponse,
        GetRealmComponentsRequest,
        GetRealmComponentsResponse,
        GetRelationsRequest,
        GetRelationsResponse,
        GetUsersFromGroupRequest,
        GetUsersFromGroupResponse,
        GetGroupsFromRealmRequest,
        GetGroupsFromRealmResponse,
        GetRelationInfosRequest,
        GetRelationInfosResponse,
        GetUserRequest,
        GetUserResponse,
        Component,
        Token,
        User,
        Author,
        Relation,
        Resource,
        KeyValue,
        Hash,
        Realm,
        Group,
        VisibilityClass,
        ResourceVariant,
        BatchResource,
        Permission,
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
