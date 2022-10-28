use crate::database::schema::*;
use uuid;

#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(table_name = collection_stats)]
pub struct CollectionStat {
    pub id: uuid::Uuid,
    pub object_count: i64,
    pub object_group_count: i64,
    pub size: i64,
    pub last_updated: chrono::NaiveDateTime,
}

#[derive(Queryable, Identifiable, Debug, Clone)]
#[diesel(table_name = object_group_stats)]
pub struct ObjectGroupStat {
    pub id: uuid::Uuid,
    pub object_count: i64,
    pub size: i64,
    pub last_updated: chrono::NaiveDateTime,
}
