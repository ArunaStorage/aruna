use std::sync::Arc;

use tonic::transport::Server;

use crate::api::aruna::api::storage::services::v1::endpoint_service_server::EndpointServiceServer;
use crate::api::aruna::api::storage::services::v1::object_group_service_server::ObjectGroupServiceServer;
use crate::api::aruna::api::storage::services::v1::object_service_server::ObjectServiceServer;
use crate::api::aruna::api::storage::services::v1::project_service_server::ProjectServiceServer;
use crate::api::aruna::api::storage::services::v1::user_service_server::UserServiceServer;
use crate::config::ArunaServerConfig;
use crate::server::services::authz::Authz;
use crate::server::services::endpoint::EndpointServiceImpl;
use crate::server::services::objectgroup::ObjectGroupServiceImpl;
use crate::server::services::project::ProjectServiceImpl;
use crate::server::services::user::UserServiceImpl;
use crate::{
    api::aruna::api::storage::services::v1::collection_service_server::CollectionServiceServer,
    database::connection::Database,
};

use super::services::collection::CollectionServiceImpl;
use super::services::object::ObjectServiceImpl;

pub struct ServiceServer {}

impl ServiceServer {
    pub async fn run(&self) {
        // Read config relative to binary
        let config = ArunaServerConfig::new();

        // Connects to database
        let db = Database::new(&config.config.database_url);
        let db_ref = Arc::new(db);

        // Initialize instance default data proxy endpoint
        let default_endpoint = db_ref
            .init_default_endpoint(config.config.default_endpoint)
            .unwrap();

        // Upstart server
        let addr = "0.0.0.0:50051".parse().unwrap();
        let authz = Arc::new(Authz::new(db_ref.clone()).await);

        let endpoint_service =
            EndpointServiceImpl::new(db_ref.clone(), authz.clone(), default_endpoint.clone()).await;
        let project_service = ProjectServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let user_service = UserServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let collection_service = CollectionServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let object_service =
            ObjectServiceImpl::new(db_ref.clone(), authz.clone(), default_endpoint.clone()).await;
        let object_group_service = ObjectGroupServiceImpl::new(db_ref.clone(), authz.clone()).await;

        log::info!("ArunaServer listening on {}", addr);

        Server::builder()
            .add_service(EndpointServiceServer::new(endpoint_service))
            .add_service(UserServiceServer::new(user_service))
            .add_service(ProjectServiceServer::new(project_service))
            .add_service(CollectionServiceServer::new(collection_service))
            .add_service(ObjectServiceServer::new(object_service))
            .add_service(ObjectGroupServiceServer::new(object_group_service))
            .serve(addr)
            .await
            .unwrap();
    }
}
