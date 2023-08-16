use crate::database::dsls::object_dsl::ObjectWithRelations;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;

#[derive(Clone)]
pub enum PubKey {
    DataProxy((String, DecodingKey, DieselUlid)), // DataProxy((Raw Key String, DecodingKey, Endpoint ID))
    Server((String, DecodingKey)), // Server((Key String, DecodingKey)) + ArunaServer ID ?
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
