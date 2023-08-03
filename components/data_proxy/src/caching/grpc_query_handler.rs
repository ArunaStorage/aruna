use anyhow::Result;
use aruna_rust_api::api::{
    notification::services::v2::event_notification_service_client::{
        self, EventNotificationServiceClient,
    },
    storage::services::v2::{
        collection_service_client::{self, CollectionServiceClient},
        dataset_service_client::{self, DatasetServiceClient},
        endpoint_service_client::{self, EndpointServiceClient},
        object_service_client::{self, ObjectServiceClient},
        project_service_client::{self, ProjectServiceClient},
        storage_status_service_client::{self, StorageStatusServiceClient},
        user_service_client::{self, UserServiceClient},
    },
};
use tonic::transport::{Channel, ClientTlsConfig};

pub struct GrpcQueryHandler {
    project_service: ProjectServiceClient<Channel>,
    collection_service: CollectionServiceClient<Channel>,
    dataset_service: DatasetServiceClient<Channel>,
    object_service: ObjectServiceClient<Channel>,
    user_service: UserServiceClient<Channel>,
    endpoint_service: EndpointServiceClient<Channel>,
    storage_status_service: StorageStatusServiceClient<Channel>,
    event_notification_service: EventNotificationServiceClient<Channel>,
}

impl GrpcQueryHandler {
    #[allow(dead_code)]
    pub async fn new(server: impl Into<String>) -> Result<Self> {
        let tls_config = ClientTlsConfig::new();
        let endpoint = Channel::from_shared(server.into())?.tls_config(tls_config)?;
        let channel = endpoint.connect().await?;

        let project_service = project_service_client::ProjectServiceClient::new(channel.clone());

        let collection_service =
            collection_service_client::CollectionServiceClient::new(channel.clone());

        let dataset_service = dataset_service_client::DatasetServiceClient::new(channel.clone());

        let object_service = object_service_client::ObjectServiceClient::new(channel.clone());

        let user_service = user_service_client::UserServiceClient::new(channel.clone());

        let endpoint_service = endpoint_service_client::EndpointServiceClient::new(channel.clone());

        let storage_status_service =
            storage_status_service_client::StorageStatusServiceClient::new(channel.clone());

        let event_notification_service =
            event_notification_service_client::EventNotificationServiceClient::new(channel);

        Ok(GrpcQueryHandler {
            project_service,
            collection_service,
            dataset_service,
            object_service,
            user_service,
            endpoint_service,
            storage_status_service,
            event_notification_service,
        })
    }
}
