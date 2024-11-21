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

/// Get  resource
#[utoipa::path(
    get,
    path = "/api/v3/resource",
    params(
        GetResourceRequest,
    ),
    responses(
        (status = 200, body = GetResourceResponse),
        ArunaError,
    ),
    security(
        ("auth" = [])
    ),
)]
pub async fn get_resource(
    State(state): State<Arc<Controller>>,
    Query(request): Query<GetResourceRequest>,
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


/// Create a rule
#[utoipa::path(
    post,
    path = "/api/v3/rule",
    request_body = AddRuleRequest, 
    responses(
        (status = 200, body = AddRuleResponse),
        ArunaError,
    ),
    security(
        (), // <-- make optional authentication
        ("auth" = [])
    ),
)]
pub async fn add_rule(
    State(state): State<Arc<Controller>>,
    header: HeaderMap,
    Json(request): Json<AddRuleRequest>,
) -> impl IntoResponse {
    into_axum_response(state.request(request, extract_token(&header)).await)
}
