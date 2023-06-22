use crate::database::schema::bundles;

#[derive(Queryable, Insertable, Identifiable, Debug, Clone, Selectable)]
#[diesel(table_name = bundles)]
pub struct Bundle {
    pub id: diesel_ulid::DieselUlid,
    pub bundle_id: String,
    pub object_id: diesel_ulid::DieselUlid,
    pub endpoint_id: diesel_ulid::DieselUlid,
    pub collection_id: diesel_ulid::DieselUlid,
}
