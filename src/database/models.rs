use uuid;

use super::schema::collection_labels;
use super::schema::collections;
use super::schema::labels;

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct Collection {
    pub id: uuid::Uuid,
    pub name: String,
    pub description: String,
}

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct Label {
    pub id: uuid::Uuid,
    pub key: String,
    pub value: String,
}

#[derive(Queryable, Insertable, Associations, Debug)]
#[belongs_to(Collection)]
#[belongs_to(Label)]
pub struct CollectionLabel {
    pub collection_id: uuid::Uuid,
    pub label_id: uuid::Uuid,
}
