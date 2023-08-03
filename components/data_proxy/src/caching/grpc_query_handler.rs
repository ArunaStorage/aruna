use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::Collection;
use aruna_rust_api::api::storage::models::v2::Dataset;
use aruna_rust_api::api::storage::models::v2::Object;
use aruna_rust_api::api::storage::models::v2::Project;
use aruna_rust_api::api::storage::models::v2::User;
use aruna_rust_api::api::storage::services::v2::FullSyncEndpointRequest;
use aruna_rust_api::api::storage::services::v2::GetCollectionRequest;
use aruna_rust_api::api::storage::services::v2::GetDatasetRequest;
use aruna_rust_api::api::storage::services::v2::GetObjectRequest;
use aruna_rust_api::api::storage::services::v2::GetProjectRequest;
use aruna_rust_api::api::storage::services::v2::GetPubkeysRequest;
use aruna_rust_api::api::storage::services::v2::GetUserRedactedRequest;
use aruna_rust_api::api::storage::services::v2::Pubkey;
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
use diesel_ulid::DieselUlid;
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;

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

impl GrpcQueryHandler {
    async fn get_user(&self, id: DieselUlid, checksum: String) -> Result<User> {
        let user = self
            .user_service
            .clone()
            .get_user_redacted(Request::new(GetUserRedactedRequest {
                user_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .user
            .ok_or(anyhow!("Unknown user"))?;

        //let actual_checksum = checksum_user(&user)?;

        // if actual_checksum == checksum {
        //     bail!("Invalid checksum")
        // }

        Ok(user)
    }
    async fn get_pubkeys(&self) -> Result<Vec<Pubkey>> {
        Ok(self
            .storage_status_service
            .clone()
            .get_pubkeys(Request::new(GetPubkeysRequest {}))
            .await?
            .into_inner()
            .pubkeys)
    }
    async fn get_project(&self, id: &DieselUlid, checksum: String) -> Result<Project> {
        self.project_service
            .clone()
            .get_project(Request::new(GetProjectRequest {
                project_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .project
            .ok_or(anyhow!("unknown project"))
    }

    async fn get_collection(&self, id: &DieselUlid, checksum: String) -> Result<Collection> {
        self.collection_service
            .clone()
            .get_collection(Request::new(GetCollectionRequest {
                collection_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .collection
            .ok_or(anyhow!("unknown collection"))
    }

    async fn get_dataset(&self, id: &DieselUlid, checksum: String) -> Result<Dataset> {
        self.dataset_service
            .clone()
            .get_dataset(Request::new(GetDatasetRequest {
                dataset_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .dataset
            .ok_or(anyhow!("unknown dataset"))
    }

    async fn get_object(&self, id: &DieselUlid, checksum: String) -> Result<Object> {
        self.object_service
            .clone()
            .get_object(Request::new(GetObjectRequest {
                object_id: id.to_string(),
            }))
            .await?
            .into_inner()
            .object
            .ok_or(anyhow!("unknown object"))
    }

    // async fn full_sync(&self) -> Result<FullSyncData> {
    //     let result = self
    //         .endpoint_service
    //         .clone()
    //         .full_sync_endpoint(Request::new(FullSyncEndpointRequest {}))
    //         .await?
    //         .into_inner()
    //         .url;
    //     Ok(reqwest::get(result).await?.json::<FullSyncData>().await?)
    // }
}
