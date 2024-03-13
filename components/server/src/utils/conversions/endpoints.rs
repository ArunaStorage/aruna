use crate::database::dsls::endpoint_dsl::{Endpoint as DBEndpoint, HostConfig, HostConfigs};
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::{Endpoint, EndpointHostConfig};

impl TryFrom<Vec<EndpointHostConfig>> for HostConfigs {
    type Error = anyhow::Error;
    fn try_from(config: Vec<EndpointHostConfig>) -> Result<Self> {
        let res: Result<Vec<HostConfig>> = config
            .into_iter()
            .map(|c| -> Result<HostConfig> {
                Ok(HostConfig {
                    url: c.url,
                    is_primary: c.is_primary,
                    ssl: c.ssl,
                    public: c.public,
                    feature: c.host_variant.try_into()?,
                })
            })
            .collect();
        Ok(HostConfigs(res?))
    }
}

impl From<DBEndpoint> for Endpoint {
    fn from(ep: DBEndpoint) -> Self {
        Endpoint {
            id: ep.id.to_string(),
            ep_variant: ep.endpoint_variant.into(),
            name: ep.name,
            is_public: ep.is_public,
            status: ep.status.into(),
            host_configs: ep.host_config.0.into(),
        }
    }
}
impl From<HostConfigs> for Vec<EndpointHostConfig> {
    fn from(config: HostConfigs) -> Self {
        config
            .0
            .into_iter()
            .map(|c| EndpointHostConfig {
                url: c.url,
                is_primary: c.is_primary,
                ssl: c.ssl,
                public: c.public,
                host_variant: c.feature.into(),
            })
            .collect()
    }
}
