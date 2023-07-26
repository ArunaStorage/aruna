use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;

use crate::database::dsls::object_dsl::ObjectWithRelations;

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
