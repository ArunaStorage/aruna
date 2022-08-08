use std::env;
use std::sync::{Arc};

use tonic::transport::Server;

use crate::{
    api::aruna::api::storage::services::v1::collection_service_server::CollectionServiceServer,
    database::connection::Database,
};
use crate::api::aruna::api::storage::internal::v1::internal_proxy_service_client::InternalProxyServiceClient;
use crate::api::aruna::api::storage::services::v1::object_service_server::ObjectServiceServer;

use super::services::object::ObjectServiceImpl;
use super::services::collection::CollectionServiceImpl;

pub struct ServiceServer {}

impl ServiceServer {
    pub async fn run(&self) {
        // ToDo: Implement config handling from YAML config file

        // Connects to database
        let db = Database::new();
        let db_ref = Arc::new(db);

        // Connects to data proxy
        let data_proxy_url = env::var("DATA_PROXY_URL").expect("DATA_PROXY_URL must be set");
        let data_proxy = InternalProxyServiceClient::connect(data_proxy_url.to_string()).await.unwrap(); //ToDo: Replace unwrap() with retry strategy

        // Upstart server
        let addr = "[::1]:50051".parse().unwrap();
        let collection_service = CollectionServiceImpl::new(db_ref.clone()).await;
        let object_service = ObjectServiceImpl::new(db_ref.clone(), data_proxy.clone()).await;

        println!("GreeterServer listening on {}", addr);

        Server::builder()
            .add_service(CollectionServiceServer::new(collection_service))
            .add_service(ObjectServiceServer::new(object_service))
            .serve(addr)
            .await
            .unwrap();
    }
}
