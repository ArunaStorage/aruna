use crate::models::requests::*;
use crate::{error::ArunaError, transactions::controller::Controller};
use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::IntoResponse,
    Json,
};
use std::sync::Arc;

use super::utils::{extract_token, into_axum_response};

/// Create a new resource
#[utoipa::path(
    post,
    path = "/api/v3/resource",
    request_body = CreateResourceRequest,
    responses(
        (status = 200, body = CreateResourceResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_resource(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/api/v3/resource/batch",
    request_body = CreateResourceBatchRequest,
    responses(
        (status = 200, body = CreateResourceResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_resource_batch(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceBatchRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Create a new resource
#[utoipa::path(
    post,
    path = "/api/v3/resource/project",
    request_body = CreateProjectRequest,
    responses(
        (status = 200, body = CreateProjectResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_project(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateProjectRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}


/// Create a new relation
#[utoipa::path(
    post,
    path = "/api/v3/resource/relation",
    request_body = CreateRelationRequest,
    responses(
        (status = 200, body = CreateRelationResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_relation(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}


/// Create a new relation variant
#[utoipa::path(
    post,
    path = "/api/v3/relation",
    request_body = CreateRelationVariantRequest,
    responses(
        (status = 200, body = CreateRelationVariantResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_relation_variant(
    State(state): State<Arc<Controller>>,
    headers: HeaderMap,
    Json(request): Json<CreateResourceRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&headers)).await)
}

/// Get  resource
#[utoipa::path(
    get,
    path = "/api/v3/resources",
    params(
        GetResourcesRequest,
    ),
    responses(
        (status = 200, body = GetResourcesResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn get_resource(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetResourcesRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Create a new realm
#[utoipa::path(
    post,
    path = "/api/v3/realm",
    request_body = CreateRealmRequest,
    responses(
        (status = 200, body = CreateRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_realm(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateRealmRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get realm
#[utoipa::path(
    get,
    path = "/api/v3/realm",
    params(
        GetRealmRequest,
    ),
    responses(
        (status = 200, body = GetRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn get_realm(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetRealmRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Add group
#[utoipa::path(
    post,
    path = "/api/v3/realm/group",
    request_body = AddGroupRequest,
    responses(
        (status = 200, body = GetRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn add_group(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<AddGroupRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Create a new  
#[utoipa::path(
    post,
    path = "/api/v3/group",
    request_body = CreateGroupRequest,
    responses(
        (status = 200, body = CreateGroupResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn create_group(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateGroupRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get realm
#[utoipa::path(
    get,
    path = "/api/v3/group",
    params(
        GetGroupRequest,
    ),
    responses(
        (status = 200, body = GetGroupResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_group(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetGroupRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Register a new user
#[utoipa::path(
    post,
    path = "/api/v3/user",
    request_body = RegisterUserRequest,
    responses(
        (status = 200, body = RegisterUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn register_user(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<RegisterUserRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Add user to group
#[utoipa::path(
    post,
    path = "/api/v3/group/user",
    request_body = AddUserRequest,
    responses(
        (status = 200, body = AddUserResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn add_user(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<AddUserRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Create a token
#[utoipa::path(
    post,
    path = "/api/v3/token",
    request_body = CreateTokenRequest,
    responses(
        (status = 200, body = CreateTokenResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn create_token(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<CreateTokenRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Search for resources
#[utoipa::path(
    get,
    path = "/api/v3/search",
    params(
        SearchRequest,
    ),
    responses(
        (status = 200, body = SearchResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn search(
    State(state): State<Arc<Controller>>,
    Query(request): Query<SearchRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get all realms from a user
#[utoipa::path(
    get,
    path = "/api/v3/user/realms",
    responses(
        (status = 200, body = GetRealmsFromUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_user_realms(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetRealmsFromUserRequest {}, extract_token(&header))
            .await,
    )
}

/// Get all groups from a user
#[utoipa::path(
    get,
    path = "/api/v3/user/groups",
    responses(
        (status = 200, body = GetGroupsFromUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_user_groups(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetGroupsFromUserRequest {}, extract_token(&header))
            .await,
    )
}

/// Get global server stats
#[utoipa::path(
    get,
    path = "/api/v3/stats",
    responses(
        (status = 200, body = GetStatsResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_stats(
    State(_state): State<Arc<Controller>>,
    _header: HeaderMap,
) -> impl IntoResponse {
    // TODO: Remove dummy data and impl stats collection
    // todo!();
    //into_axum_response(state.request(GetRealmsFromUserRequest{}, extract_token(&header)).await)
    Json(GetStatsResponse {
        resources: 1023,
        projects: 5,
        users: 12,
        storage: 12312930192,
        realms: 3,
    })
}

/// Get components of a realm (server, dataproxies, etc)
#[utoipa::path(
    get,
    path = "/api/v3/realm/components",
    params(
        GetRealmComponentsRequest,
    ),
    responses(
        (status = 200, body = GetRealmComponentsRequest),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_realm_components(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetRealmComponentsRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get relations of a resource
#[utoipa::path(
    get,
    path = "/api/v3/resource/relations",
    params(
        GetRelationsRequest,
    ),
    responses(
        (status = 200, body = GetRelationsResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_relations(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetRelationsRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get users from group
#[utoipa::path(
    get,
    path = "/api/v3/group/users",
    params(
        GetUsersFromGroupRequest,
    ),
    responses(
        (status = 200, body = GetUsersFromGroupResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_group_users(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetUsersFromGroupRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get groups from realm
#[utoipa::path(
    get,
    path = "/api/v3/realm/groups",
    params(
        GetGroupsFromRealmRequest,
    ),
    responses(
        (status = 200, body = GetGroupsFromRealmResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_realm_groups(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetGroupsFromRealmRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Get relation info
#[utoipa::path(
    get,
    path = "/api/v3/info/relation",
    responses(
        (status = 200, body = GetRelationInfosResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_relation_infos(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetRelationInfosRequest {}, extract_token(&header))
            .await,
    )
}

/// Get relation info
#[utoipa::path(
    get,
    path = "/api/v3/user",
    responses(
        (status = 200, body = GetUserResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_user(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(
        state
            .request(GetUserRequest {}, extract_token(&header))
            .await,
    )
}

/// Get events information
#[utoipa::path(
    get,
    path = "/api/v3/events",
    params(
        GetEventsRequest,
    ),
    responses(
        (status = 200, body = GetEventsResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn get_events(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetEventsRequest>,
    header: HeaderMap,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Request group join realm
#[utoipa::path(
    post,
    path = "/api/v3/realm/access",
    request_body = GroupAccessRealmRequest,
    responses(
        (status = 200, body = GroupAccessRealmResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn request_group_access_realm(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<GroupAccessRealmRequest>,
) -> impl IntoResponse {
    todo!();
    // into_axum_response(state.request(request, extract_token(&header)).await)
}

/// Request user join group
#[utoipa::path(
    post,
    path = "/api/v3/group/join",
    request_body = UserAccessGroupRequest,
    responses(
        (status = 200, body = UserAccessGroupResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn request_user_access_group(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<UserAccessGroupRequest>,
) -> impl IntoResponse {
    todo!()
    // into_axum_response(state.request(request, extract_token(&header)).await)
}
