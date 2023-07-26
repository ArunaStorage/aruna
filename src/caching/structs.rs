use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;

use crate::database::dsls::{
    internal_relation_dsl::INTERNAL_RELATION_VARIANT_BELONGS_TO, object_dsl::ObjectWithRelations,
};

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub enum PubKey {
    DataProxy(DecodingKey),
    Server(DecodingKey),
}

#[derive(Debug, PartialEq, PartialOrd, Eq, Ord, Hash, Clone)]
pub struct ResourceInfo {
    pub resource: ObjectWithRelations,
    pub endpoints: Vec<DieselUlid>,
}

impl ObjectWithRelations {
    pub fn get_children(&self) -> Vec<DieselUlid> {
        self.outbound
            .0
            .iter()
            .filter(|x| x.variant == INTERNAL_RELATION_VARIANT_BELONGS_TO)
            .map(|x| x.target)
            .collect::<Vec<_>>()
    }

    pub fn get_parents(&self) -> Vec<DieselUlid> {
        self.inbound
            .0
            .iter()
            .filter(|x| x.variant == INTERNAL_RELATION_VARIANT_BELONGS_TO)
            .map(|x| x.origin)
            .collect::<Vec<_>>()
    }
}
