use crate::auth::Authorizer;
use crate::caching::cache::Cache;
use crate::database::connection::Database;
use aruna_rust_api::api::storage::services::v2::relations_service_server::RelationsService;
use aruna_rust_api::api::storage::services::v2::GetHierachyRequest;
use aruna_rust_api::api::storage::services::v2::GetHierachyResponse;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use aruna_rust_api::api::storage::services::v2::ModifyRelationsResponse;
use std::sync::Arc;
use std::sync::Mutex;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(RelationsServiceImpl);

#[tonic::async_trait]
impl RelationsService for RelationsServiceImpl {
    async fn modify_relations(
        &self,
        request: Request<ModifyRelationsRequest>,
    ) -> Result<Response<ModifyRelationsResponse>> {
        let inner_request = request.into_inner();

        todo!()
    }
    async fn get_hierachy(
        &self,
        request: Request<GetHierachyRequest>,
    ) -> Result<Response<GetHierachyResponse>> {
        todo!()
    }
}
