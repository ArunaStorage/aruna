use std::{net::SocketAddr, sync::Arc};

use crate::{error::ArunaError, transactions::controller::Controller};
use axum::{
    response::Redirect,
    routing::{get, post},
    Router,
};
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use super::{api_paths, openapi};

pub struct RestServer {}

impl RestServer {
    pub async fn run(handler: Arc<Controller>, rest_port: u16) -> Result<(), ArunaError> {
        let swagger = SwaggerUi::new("/swagger-ui")
            .url("/api-docs/openapi.json", openapi::ArunaApi::openapi());

        let socket_address = SocketAddr::from(([0, 0, 0, 0], rest_port));
        let listener = tokio::net::TcpListener::bind(socket_address).await.unwrap();

        let app = Router::new()
            .merge(swagger)
            .route("/", get(|| async { Redirect::permanent("/swagger-ui") }))
            .route("/api/v3/resource", post(api_paths::create_resource))
            .route("/api/v3/resource/project", post(api_paths::create_project))
            .route("/api/v3/resource", get(api_paths::get_resource))
            .route("/api/v3/realm", post(api_paths::create_realm))
            .route("/api/v3/realm", get(api_paths::get_realm))
            .route("/api/v3/realm/group", post(api_paths::add_group))
            .route("/api/v3/group", post(api_paths::create_group))
            .route("/api/v3/group", get(api_paths::get_group))
            .with_state(handler)
            .layer(
                TraceLayer::new_for_http()
                    .on_response(())
                    .on_body_chunk(())
                    .on_eos(()),
            );
        axum::serve(listener, app.into_make_service())
            .await
            .map_err(|e| ArunaError::ServerError(e.to_string()))?;

        Ok(())
    }
}
