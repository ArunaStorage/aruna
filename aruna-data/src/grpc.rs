use aruna_rust_api::v3::aruna::api::v3::{
    auth_service_client::AuthServiceClient, events_service_client::EventsServiceClient,
    resource_service_client::ResourceServiceClient,
};
use tonic::transport::{Channel, ClientTlsConfig};

use crate::{error::ProxyError, CONFIG};


#[derive(Clone)]
pub struct ServerClient {
    resource_service: ResourceServiceClient<Channel>,
    auth_service: AuthServiceClient<Channel>,
    event_service: EventsServiceClient<Channel>,
}

impl ServerClient {
    pub async fn new() -> Result<Self, ProxyError> {
        let endpoint = match &CONFIG.proxy.aruna_url {
            Some(url) if url.starts_with("https://") => {
                Channel::from_shared("https://grpc.dev.aruna-storage.org")?
                    .tls_config(ClientTlsConfig::default())?
            }
            Some(url) if url.starts_with("http://") => {
                Channel::from_shared("http://grpc.dev.aruna-storage.org")?
            }
            _ => {
                return Err(ProxyError::InvalidConfig(
                    "aruna_url is not set or invalid".to_string(),
                ))
            }
        };

        let channel = endpoint.connect().await?;

        let resource_service = ResourceServiceClient::new(channel.clone());
        let auth_service = AuthServiceClient::new(channel.clone());
        let event_service = EventsServiceClient::new(channel);

        Ok(Self {
            resource_service,
            auth_service,
            event_service,
        })
    }
}
