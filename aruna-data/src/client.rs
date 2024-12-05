use std::sync::Arc;

use aruna_server::models::{
    models::ResourceVariant,
    requests::{CreateProjectRequest, CreateResourceRequest, CreateResourceResponse, RegisterDataRequest},
};
use ulid::Ulid;

use crate::{error::ProxyError, lmdbstore::LmdbStore, CONFIG};

#[derive(Clone)]
pub struct ServerClient {
    store: Arc<LmdbStore>,
    self_token: String,
}

impl ServerClient {
    pub async fn new(store: Arc<LmdbStore>, self_token: String) -> Result<Self, ProxyError> {
        // &CONFIG.proxy.aruna_url
        Ok(Self { store, self_token })
    }

    pub async fn get_events(&self) -> Result<(), ProxyError> {
        reqwest::Client::new()
            .get(format!(
                "{}/api/v3/info/events?subscriber_id={}",
                CONFIG
                    .proxy
                    .aruna_url
                    .as_ref()
                    .unwrap_or(&"http://localhost:8080".to_string()),
                "subscriber_id"
            ))
            .header("Authorization", format!("Bearer {}", self.self_token))
            .send()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .error_for_status()
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .json::<serde_json::Value>()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?;

        Ok(())
    }


    pub async fn create_project(
        &self,
        name: &str,
        token: &str,
    ) -> Result<Ulid, ProxyError> {
        let result = reqwest::Client::new()
            .post(format!(
                "{}/api/v3/resources/projects",
                CONFIG
                    .proxy
                    .aruna_url
                    .as_ref()
                    .unwrap_or(&"http://localhost:8080".to_string()),
            ))
            .json(&CreateProjectRequest {
                name: name.to_string(),
                ..Default::default()
            })
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .error_for_status()
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .json::<CreateResourceResponse>()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?;
        Ok(result.resource.id)
    }

    pub async fn create_object(
        &self,
        name: &str,
        variant: ResourceVariant,
        parent: Ulid,
        token: &str,
    ) -> Result<Ulid, ProxyError> {
        let result = reqwest::Client::new()
            .post(format!(
                "{}/api/v3/resources",
                CONFIG
                    .proxy
                    .aruna_url
                    .as_ref()
                    .unwrap_or(&"http://localhost:8080".to_string()),
            ))
            .json(&CreateResourceRequest {
                name: name.to_string(),
                variant: variant,
                parent_id: parent,
                ..Default::default()
            })
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .error_for_status()
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .json::<CreateResourceResponse>()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?;
        Ok(result.resource.id)
    }

    pub async fn add_data(
        &self,
        id: Ulid,
        req: RegisterDataRequest,
        token: &str,
    ) -> Result<(), ProxyError> {
        reqwest::Client::new()
            .post(format!(
                "{}/api/v3/resources/{id}/data",
                CONFIG
                    .proxy
                    .aruna_url
                    .as_ref()
                    .unwrap_or(&"http://localhost:8080".to_string()),
            ))
            .json(&req)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| ProxyError::RequestError(e.to_string()))?
            .error_for_status()
            .map_err(|e| ProxyError::RequestError(e.to_string()))?;
        Ok(())
    }
}
