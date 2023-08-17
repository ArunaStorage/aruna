use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::pub_key_dsl::PubKey;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;

#[derive(Clone)]
pub enum PubKeyEnum {
    DataProxy((String, DecodingKey, DieselUlid)), // DataProxy((Raw Key String, DecodingKey, Endpoint ID))
    Server((String, DecodingKey)), // Server((Key String, DecodingKey)) + ArunaServer ID ?
}

impl TryFrom<PubKey> for PubKeyEnum {
    type Error = anyhow::Error;
    fn try_from(pk: PubKey) -> Result<Self> {
        Ok(match pk.proxy {
            Some(proxy) => PubKeyEnum::DataProxy((
                pk.pubkey.to_string(),
                DecodingKey::from_ed_components(&pk.pubkey)?,
                proxy,
            )),
            None => PubKeyEnum::Server((
                pk.pubkey.to_string(),
                DecodingKey::from_ed_components(&pk.pubkey)?,
            )),
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
