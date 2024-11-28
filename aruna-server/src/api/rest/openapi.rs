use std::sync::Arc;

use super::api_paths::*;
use crate::models::models::*;
use crate::models::requests::*;
use crate::transactions::controller::Controller;
use utoipa::{
    openapi::security::{ApiKey, ApiKeyValue, SecurityScheme},
    Modify, OpenApi,
};
use utoipa_axum::router::OpenApiRouter;
use utoipa_axum::routes;

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
        request_group_access_realm,
        request_user_access_group,
        get_events,
        create_relation,
        create_relation_variant,
        create_component,
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
        CreateRelationRequest,
        CreateRelationResponse,
        CreateRelationVariantRequest,
        CreateRelationVariantResponse,
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
        CreateComponentRequest,
        CreateComponentResponse,
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
        GetEventsRequest,
        GetEventsResponse,
        GroupAccessRealmRequest,
        GroupAccessRealmResponse,
        UserAccessGroupRequest,
        UserAccessGroupResponse
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

pub fn router(store: Arc<Controller>) -> OpenApiRouter {
    OpenApiRouter::new()
        .routes(routes!(
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
            request_group_access_realm,
            request_user_access_group,
            get_events,
            create_relation,
            create_relation_variant,
            create_component
        ))
        .with_state(store)
}
