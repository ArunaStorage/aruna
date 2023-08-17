use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::pub_key_dsl::PubKey;
use crate::database::enums::EndpointStatus;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::{
    get_endpoint_request::Endpoint as GetEndpoint, CreateEndpointRequest, DeleteEndpointRequest,
    GetEndpointRequest,
};
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use std::str::FromStr;

pub struct CreateEP(pub CreateEndpointRequest);
pub struct DeleteEP(pub DeleteEndpointRequest);
pub struct GetEP(pub GetEndpointRequest);

impl CreateEP {
    pub fn build_endpoint(&self) -> Result<(Endpoint, PubKey)> {
        let id = DieselUlid::generate();
        let endpoint = Endpoint {
            id,
            name: self.0.name.clone(),
            endpoint_variant: self.0.ep_variant.try_into()?,
            host_config: Json(self.0.host_configs.clone().try_into()?),
            documentation_object: None,
            is_public: self.0.is_public,
            status: EndpointStatus::INITIALIZING,
        };
        let pubkey = PubKey {
            id: 0,
            proxy: Some(id),
            pubkey: self.0.pubkey.clone(),
        };
        Ok((endpoint, pubkey))
    }
}

pub enum GetBy {
    NAME(String),
    ID(DieselUlid),
}
impl GetEP {
    pub fn get_query(&self) -> Result<GetBy> {
        let query = match self {
            GetEP(req) => match &req.endpoint {
                None => return Err(anyhow!("No endpoint name or id provided")),
                Some(ep) => match ep {
                    GetEndpoint::EndpointName(name) => GetBy::NAME(name.clone()),
                    GetEndpoint::EndpointId(id) => GetBy::ID(DieselUlid::from_str(id)?),
                },
            },
        };
        Ok(query)
    }
}

impl DeleteEP {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.endpoint_id)?)
    }
}
