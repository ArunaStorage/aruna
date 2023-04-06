use std::env;
use std::sync::Arc;

use crate::config::ArunaServerConfig;
use crate::database::connection::Database;
use crate::database::cron::{Scheduler, Task};
use crate::server::mail_client::MailClient;
use crate::server::services::authz::Authz;
use crate::server::services::endpoint::EndpointServiceImpl;
use crate::server::services::info::{ResourceInfoServiceImpl, StorageInfoServiceImpl};
use crate::server::services::internal_authorize::InternalAuthorizeServiceImpl;
use crate::server::services::internal_notifications::InternalEventServiceImpl;
use crate::server::services::internal_proxy_notifier::InternalProxyNotifierServiceImpl;
use crate::server::services::objectgroup::ObjectGroupServiceImpl;
use crate::server::services::project::ProjectServiceImpl;
use crate::server::services::service_account::ServiceAccountServiceImpl;
use crate::server::services::user::UserServiceImpl;
use aruna_rust_api::api::internal::v1::internal_authorize_service_server::InternalAuthorizeServiceServer;
use aruna_rust_api::api::internal::v1::internal_event_service_server::InternalEventServiceServer;
use aruna_rust_api::api::internal::v1::internal_proxy_notifier_service_server::InternalProxyNotifierServiceServer;
use aruna_rust_api::api::storage::services::v1::collection_service_server::CollectionServiceServer;
use aruna_rust_api::api::storage::services::v1::endpoint_service_server::EndpointServiceServer;
use aruna_rust_api::api::storage::services::v1::object_group_service_server::ObjectGroupServiceServer;
use aruna_rust_api::api::storage::services::v1::object_service_server::ObjectServiceServer;
use aruna_rust_api::api::storage::services::v1::project_service_server::ProjectServiceServer;
use aruna_rust_api::api::storage::services::v1::resource_info_service_server::ResourceInfoServiceServer;
use aruna_rust_api::api::storage::services::v1::service_account_service_server::ServiceAccountServiceServer;
use aruna_rust_api::api::storage::services::v1::storage_info_service_server::StorageInfoServiceServer;
use aruna_rust_api::api::storage::services::v1::user_service_server::UserServiceServer;
use tonic::transport::Server;

use super::services::collection::CollectionServiceImpl;
use super::services::object::ObjectServiceImpl;

pub struct ServiceServer {}

impl ServiceServer {
    pub async fn run(&self) {
        // Read config relative to binary
        let config = ArunaServerConfig::new();

        // Connects to database
        let db = Database::new(&config.clone().config.database_url);
        let db_ref = Arc::new(db);

        let dev_env = match env::var("ARUNA_DEV_ENV") {
            Ok(var) => var.to_ascii_uppercase() == "TRUE",
            _ => false,
        };

        let mailclient = if !dev_env {
            match MailClient::new() {
                Ok(mc) => Some(mc),
                Err(e) => {
                    log::error!("Failed to initialize mail_client, err: {e}");
                    None
                }
            }
        } else {
            None
        };

        // Initialize instance default data proxy endpoint
        let default_endpoint = db_ref
            .init_default_endpoint(config.clone().config.default_endpoint)
            .unwrap();

        let mut cron_scheduler = Scheduler::new();
        cron_scheduler.add(Task::new(
            |db| {
                let res = db.update_collection_views();
                if res.is_err() {
                    log::info!(
                        "Update of cron: materialized collection view failed, with: {:#?}",
                        res
                    )
                }
            },
            "collection_views",
            300,
            db_ref.clone(),
        ));
        cron_scheduler.add(Task::new(
            |db| {
                let res = db.update_object_group_views();
                if res.is_err() {
                    log::info!(
                        "Update of cron: materialized object_group view failed, with: {:#?}",
                        res
                    )
                }
            },
            "object_views",
            300,
            db_ref.clone(),
        ));

        tokio::spawn(async move {
            cron_scheduler.run().await;
        });

        // Upstart server
        let addr = "0.0.0.0:50051".parse().unwrap();
        let authz = Arc::new(Authz::new(db_ref.clone(), config.clone()).await);

        let endpoint_service =
            EndpointServiceImpl::new(db_ref.clone(), authz.clone(), default_endpoint.clone()).await;
        let project_service = ProjectServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let user_service = UserServiceImpl::new(db_ref.clone(), authz.clone(), mailclient).await;
        let collection_service = CollectionServiceImpl::new(db_ref.clone(), authz.clone()).await;
        let object_service =
            ObjectServiceImpl::new(db_ref.clone(), authz.clone(), default_endpoint.clone()).await;
        let object_group_service = ObjectGroupServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let resource_info_service =
            ResourceInfoServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let storage_info_service =
            StorageInfoServiceImpl::new(db_ref.clone(), authz.clone(), config.config.loc_version)
                .await;

        let service_account_service =
            ServiceAccountServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let internal_event_service =
            InternalEventServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let internal_authorize_service =
            InternalAuthorizeServiceImpl::new(db_ref.clone(), authz.clone()).await;

        let internal_proxy_notifier_service =
            InternalProxyNotifierServiceImpl::new(db_ref.clone(), authz.clone()).await;

        log::info!("ArunaServer (external) listening on {}", addr);

        let main_server = tokio::spawn(async move {
            Server::builder()
                .add_service(EndpointServiceServer::new(endpoint_service))
                .add_service(UserServiceServer::new(user_service))
                .add_service(ProjectServiceServer::new(project_service))
                .add_service(CollectionServiceServer::new(collection_service))
                .add_service(ObjectServiceServer::new(object_service))
                .add_service(ObjectGroupServiceServer::new(object_group_service))
                .add_service(ResourceInfoServiceServer::new(resource_info_service))
                .add_service(StorageInfoServiceServer::new(storage_info_service))
                .add_service(ServiceAccountServiceServer::new(service_account_service))
                .serve(addr)
                .await
        });

        let other_addr = "0.0.0.0:50052".parse().unwrap();
        log::info!("ArunaServer (internal) listening on {}", other_addr);
        let internal_server = tokio::spawn(async move {
            Server::builder()
                .add_service(InternalProxyNotifierServiceServer::new(
                    internal_proxy_notifier_service,
                ))
                .add_service(InternalEventServiceServer::new(internal_event_service))
                .add_service(InternalAuthorizeServiceServer::new(
                    internal_authorize_service,
                ))
                .serve(other_addr)
                .await
        });

        match tokio::try_join!(main_server, internal_server) {
            Ok(_) => {}
            Err(err) => {
                log::error!("Task failed with {}.", err);
            }
        }
    }
}
