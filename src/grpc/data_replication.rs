use crate::auth::permission_handler::PermissionHandler;
use crate::caching::cache::Cache;
use crate::middlelayer::db_handler::DatabaseHandler;
use aruna_rust_api::api::storage::services::v2::{
    data_replication_service_server::DataReplicationService, DeleteReplicationRequest,
    DeleteReplicationResponse, GetReplicationStatusRequest, GetReplicationStatusResponse,
    PartialReplicateDataRequest, PartialReplicateDataResponse, ReplicateProjectDataRequest,
    ReplicateProjectDataResponse, UpdateReplicationStatusRequest, UpdateReplicationStatusResponse,
};
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(DataReplicationServiceImpl);

#[tonic::async_trait]
impl DataReplicationService for DataReplicationServiceImpl {
    async fn replicate_project_data(
        &self,
        _request: Request<ReplicateProjectDataRequest>,
    ) -> Result<Response<ReplicateProjectDataResponse>> {
        todo!()
    }
    async fn partial_replicate_data(
        &self,
        _request: Request<PartialReplicateDataRequest>,
    ) -> Result<Response<PartialReplicateDataResponse>> {
        todo!()
    }
    async fn get_replication_status(
        &self,
        _request: Request<GetReplicationStatusRequest>,
    ) -> Result<Response<GetReplicationStatusResponse>> {
        todo!()
    }
    async fn update_replication_status(
        &self,
        _request: Request<UpdateReplicationStatusRequest>,
    ) -> Result<Response<UpdateReplicationStatusResponse>> {
        todo!()
    }
    async fn delete_replication(
        &self,
        _request: Request<DeleteReplicationRequest>,
    ) -> Result<Response<DeleteReplicationResponse>> {
        todo!()
    }
}
