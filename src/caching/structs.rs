use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::pub_key_dsl::PubKey;
use crate::database::dsls::user_dsl::User;
use ahash::RandomState;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_rust_api::api::storage::models::v2::GenericResource;
use aruna_rust_api::api::storage::models::v2::Pubkey as APIPubkey;
use aruna_rust_api::api::storage::models::v2::User as APIUser;
use aruna_rust_api::api::storage::services::v2::full_sync_endpoint_response;
use aruna_rust_api::api::storage::services::v2::FullSyncEndpointResponse;
use dashmap::mapref::multiple::RefMulti;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;

#[derive(Clone)]
pub enum PubKeyEnum {
    DataProxy((String, DecodingKey, DieselUlid)), // DataProxy((Raw Key String, DecodingKey, Endpoint ID))
    Server((String, DecodingKey)), // Server((Key String, DecodingKey)) + ArunaServer ID ?
}

impl PubKeyEnum {
    pub fn get_key_string(&self) -> String {
        match self {
            PubKeyEnum::DataProxy((k, _, _)) => k.to_string(),
            PubKeyEnum::Server((k, _)) => k.to_string(),
        }
    }

    pub fn get_name(&self) -> String {
        match self {
            PubKeyEnum::DataProxy((_, _, n)) => n.to_string(),
            PubKeyEnum::Server((_, _)) => "".to_string(),
        }
    }
}

impl TryFrom<PubKey> for PubKeyEnum {
    type Error = anyhow::Error;
    fn try_from(pk: PubKey) -> Result<Self> {
        let public_pem = format!(
            "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
            &pk.pubkey
        );
        let decoding_key = DecodingKey::from_ed_pem(public_pem.as_bytes())?;

        Ok(match pk.proxy {
            Some(proxy) => PubKeyEnum::DataProxy((pk.pubkey.to_string(), decoding_key, proxy)),
            None => PubKeyEnum::Server((pk.pubkey.to_string(), decoding_key)),
        })
    }
}

impl ObjectWithRelations {
    pub fn get_children(&self) -> Vec<DieselUlid> {
        self.outbound_belongs_to
            .0
            .iter()
            .map(|x| *x.key())
            .collect::<Vec<_>>()
    }

    pub fn get_parents(&self) -> Vec<DieselUlid> {
        self.inbound_belongs_to
            .0
            .iter()
            .map(|x| *x.key())
            .collect::<Vec<_>>()
    }
}

pub struct ProxyCacheIterator<'a> {
    resource_iter: Box<
        dyn Iterator<Item = RefMulti<'a, DieselUlid, ObjectWithRelations, RandomState>>
            + 'a
            + Send
            + Sync,
    >,
    user_iter:
        Box<(dyn Iterator<Item = RefMulti<'a, DieselUlid, User, RandomState>> + 'a + Send + Sync)>,
    pub_key_iter:
        Box<(dyn Iterator<Item = RefMulti<'a, i16, PubKeyEnum, RandomState>> + 'a + Send + Sync)>,
    endpoint_id: DieselUlid,
}

impl<'a> ProxyCacheIterator<'a> {
    pub fn new(
        resource_iter: Box<
            (dyn Iterator<Item = RefMulti<'a, DieselUlid, ObjectWithRelations, RandomState>>
                 + 'a
                 + Send
                 + Sync),
        >,
        user_iter: Box<
            (dyn Iterator<Item = RefMulti<'a, DieselUlid, User, RandomState>> + 'a + Send + Sync),
        >,
        pub_key_iter: Box<
            (dyn Iterator<Item = RefMulti<'a, i16, PubKeyEnum, RandomState>> + 'a + Send + Sync),
        >,
        endpoint_id: DieselUlid,
    ) -> ProxyCacheIterator<'a> {
        ProxyCacheIterator {
            resource_iter,
            user_iter,
            pub_key_iter,
            endpoint_id,
        }
    }
}

impl<'a> Iterator for ProxyCacheIterator<'a> {
    type Item = GrpcProxyInfos;
    fn next(&mut self) -> Option<Self::Item> {
        for res in self.resource_iter.by_ref() {
            let res = res.value();
            if res.object.endpoints.0.contains_key(&self.endpoint_id) {
                return Some(GrpcProxyInfos::Resource(res.clone().into()));
            }
        }
        for res in self.user_iter.by_ref() {
            let res = res.value();
            if res
                .attributes
                .0
                .trusted_endpoints
                .contains_key(&self.endpoint_id)
            {
                return Some(GrpcProxyInfos::User(res.clone().into()));
            }
        }
        if let Some(pk) = self.pub_key_iter.next() {
            return Some(GrpcProxyInfos::PubKey(APIPubkey {
                id: *pk.key() as i32,
                key: pk.value().get_key_string(),
                location: pk.value().get_name(),
            }));
        }
        None
    }
}

#[derive(Clone)]
pub enum GrpcProxyInfos {
    Resource(generic_resource::Resource),
    User(APIUser),
    PubKey(APIPubkey),
}

impl From<GrpcProxyInfos> for FullSyncEndpointResponse {
    fn from(value: GrpcProxyInfos) -> Self {
        match value {
            GrpcProxyInfos::Resource(r) => FullSyncEndpointResponse {
                target: Some(full_sync_endpoint_response::Target::GenericResource(
                    GenericResource { resource: Some(r) },
                )),
            },
            GrpcProxyInfos::User(u) => FullSyncEndpointResponse {
                target: Some(full_sync_endpoint_response::Target::User(u)),
            },
            GrpcProxyInfos::PubKey(p) => FullSyncEndpointResponse {
                target: Some(full_sync_endpoint_response::Target::Pubkey(p)),
            },
        }
    }
}
