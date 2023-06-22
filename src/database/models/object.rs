use super::auth::*;
use super::enums::*;
use super::traits::IsKeyValue;
use super::traits::ToDbKeyValue;
use crate::database::models::collection::Collection;
use crate::database::schema::*;
use crate::error::ArunaError;
use aruna_rust_api::api::storage::models::v1::EndpointHostConfig;
use diesel::deserialize;
use diesel::deserialize::FromSql;
use diesel::pg::sql_types::Jsonb;
use diesel::pg::Pg;
use diesel::pg::PgValue;
use diesel::serialize;
use diesel::serialize::Output;
use diesel::serialize::ToSql;

#[derive(Queryable, Insertable, Identifiable, Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Source {
    pub id: diesel_ulid::DieselUlid,
    pub link: String,
    pub source_type: SourceType,
}

#[derive(
    AsChangeset,
    Associations,
    Queryable,
    Insertable,
    Identifiable,
    Debug,
    Selectable,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
)]
#[diesel(belongs_to(User, foreign_key = created_by))]
#[diesel(belongs_to(Source))]
#[diesel(belongs_to(Object, foreign_key = origin_id))]
#[diesel(table_name = objects)]
pub struct Object {
    pub id: diesel_ulid::DieselUlid,
    pub shared_revision_id: diesel_ulid::DieselUlid,
    pub revision_number: i64,
    pub filename: String,
    pub created_at: chrono::NaiveDateTime,
    pub created_by: diesel_ulid::DieselUlid,
    pub content_len: i64,
    pub object_status: ObjectStatus,
    pub dataclass: Dataclass,
    pub source_id: Option<diesel_ulid::DieselUlid>,
    pub origin_id: diesel_ulid::DieselUlid,
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Default, Clone, AsExpression, PartialEq, PartialOrd,
)]
#[diesel(sql_type = Jsonb)]
pub enum DataProxyFeature {
    #[default]
    PROXY,
    INTERNAL,
    BUNDLER,
}

#[derive(
    serde::Serialize, serde::Deserialize, Debug, Default, Clone, AsExpression, PartialEq, PartialOrd,
)]
#[diesel(sql_type = Jsonb)]
pub struct HostConfig {
    pub url: String,
    pub is_primary: bool,
    pub ssl: bool,
    pub public: bool,
    pub feature: DataProxyFeature,
}

impl FromSql<Jsonb, Pg> for HostConfig {
    fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
        let value = <serde_json::Value as FromSql<Jsonb, Pg>>::from_sql(value)?;
        Ok(serde_json::from_value(value)?)
    }
}

impl ToSql<Jsonb, Pg> for HostConfig {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let mut out = Output::reborrow(out);
        let value = serde_json::to_value(self)?;
        <serde_json::Value as ToSql<Jsonb, Pg>>::to_sql(&value, &mut out)
    }
}

#[derive(FromSqlRow, AsExpression, serde::Serialize, serde::Deserialize, Debug, Default, Clone)]
#[diesel(sql_type = Jsonb)]
pub struct HostConfigs {
    pub configs: Vec<HostConfig>,
}

impl FromSql<Jsonb, Pg> for HostConfigs {
    fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
        let value = <serde_json::Value as FromSql<Jsonb, Pg>>::from_sql(value)?;
        Ok(serde_json::from_value(value)?)
    }
}

impl ToSql<Jsonb, Pg> for HostConfigs {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let mut out = Output::reborrow(out);
        let value = serde_json::to_value(self)?;
        <serde_json::Value as ToSql<Jsonb, Pg>>::to_sql(&value, &mut out)
    }
}

#[derive(AsChangeset, Queryable, Insertable, Identifiable, Debug, Clone)]
pub struct Endpoint {
    pub id: diesel_ulid::DieselUlid,
    pub endpoint_type: EndpointType,
    pub name: String,
    pub documentation_path: Option<String>,
    pub is_public: bool,
    pub status: EndpointStatus,
    pub is_bundler: bool,
    pub host_config: HostConfigs,
}

impl Endpoint {
    pub fn get_primary_url(&self, feature: DataProxyFeature) -> Result<(String, bool), ArunaError> {
        for hconf in self.host_config.configs.iter() {
            if hconf.is_primary && hconf.feature == feature {
                return Ok((hconf.url.to_string(), hconf.ssl));
            }
        }
        Err(ArunaError::InvalidRequest(
            "Unable to find proxy url".to_string(),
        ))
    }
}

impl TryFrom<i32> for DataProxyFeature {
    type Error = ArunaError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(DataProxyFeature::PROXY),
            2 => Ok(DataProxyFeature::INTERNAL),
            3 => Ok(DataProxyFeature::BUNDLER),
            _ => Err(ArunaError::InvalidRequest(
                "Invalid dataproxy feature".to_string(),
            )),
        }
    }
}

impl From<EndpointHostConfig> for HostConfig {
    fn from(value: EndpointHostConfig) -> Self {
        HostConfig {
            url: value.url,
            is_primary: value.is_primary,
            ssl: value.ssl,
            public: value.public,
            feature: value
                .host_type
                .try_into()
                .unwrap_or_else(|_| DataProxyFeature::PROXY),
        }
    }
}

#[derive(AsChangeset, Associations, Queryable, Insertable, Identifiable, Debug)]
#[diesel(belongs_to(Endpoint))]
#[diesel(belongs_to(Object))]
pub struct ObjectLocation {
    pub id: diesel_ulid::DieselUlid,
    pub bucket: String,
    pub path: String,
    pub endpoint_id: diesel_ulid::DieselUlid,
    pub object_id: diesel_ulid::DieselUlid,
    pub is_primary: bool,
    pub is_encrypted: bool,
    pub is_compressed: bool,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Selectable, Clone, Debug)]
#[diesel(belongs_to(Endpoint))]
#[diesel(belongs_to(Object))]
pub struct EncryptionKey {
    pub id: diesel_ulid::DieselUlid,
    pub hash: Option<String>,
    pub object_id: diesel_ulid::DieselUlid,
    pub endpoint_id: diesel_ulid::DieselUlid,
    pub is_temporary: bool,
    pub encryption_key: String,
}

#[derive(
    Associations, Queryable, Insertable, Identifiable, Debug, Clone, PartialEq, Eq, PartialOrd, Ord,
)]
#[diesel(belongs_to(Object))]
#[diesel(table_name = hashes)]
pub struct Hash {
    pub id: diesel_ulid::DieselUlid,
    pub hash: String,
    pub object_id: diesel_ulid::DieselUlid,
    pub hash_type: HashType,
}

#[derive(Associations, Queryable, Insertable, Identifiable, Selectable, Debug, Clone)]
#[diesel(table_name = object_key_value)]
#[diesel(belongs_to(Object))]
pub struct ObjectKeyValue {
    pub id: diesel_ulid::DieselUlid,
    pub object_id: diesel_ulid::DieselUlid,
    pub key: String,
    pub value: String,
    pub key_value_type: KeyValueType,
}

impl IsKeyValue for ObjectKeyValue {
    fn get_key(&self) -> &str {
        &self.key
    }

    fn get_value(&self) -> &str {
        &self.value
    }

    fn get_associated_uuid(&self) -> &diesel_ulid::DieselUlid {
        &self.object_id
    }

    fn get_type(&self) -> &KeyValueType {
        &self.key_value_type
    }
}

impl ToDbKeyValue for ObjectKeyValue {
    fn new_kv<ObjectKeyValue>(
        key: &str,
        value: &str,
        belongs_to: diesel_ulid::DieselUlid,
        kv_type: KeyValueType,
    ) -> Self {
        Self {
            id: diesel_ulid::DieselUlid::generate(),
            object_id: belongs_to,
            key: key.to_string(),
            value: value.to_string(),
            key_value_type: kv_type,
        }
    }
}

#[derive(
    Associations,
    Queryable,
    Insertable,
    Identifiable,
    Selectable,
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    AsChangeset,
)]
#[diesel(table_name = relations)]
#[diesel(belongs_to(Object))]
#[diesel(belongs_to(Project))]
#[diesel(belongs_to(Collection))]
pub struct Relation {
    pub id: diesel_ulid::DieselUlid,
    pub object_id: diesel_ulid::DieselUlid,
    pub path: String,
    pub project_id: diesel_ulid::DieselUlid,
    pub project_name: String,
    pub collection_id: diesel_ulid::DieselUlid,
    pub collection_path: String,
    pub shared_revision_id: diesel_ulid::DieselUlid,
    pub path_active: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_is_key_value_test() {
        let test_kv_label_oid = diesel_ulid::DieselUlid::generate();

        let test_kv_label = ObjectKeyValue::new_kv::<ObjectKeyValue>(
            "test_key",
            "test_value",
            test_kv_label_oid,
            KeyValueType::LABEL,
        );

        assert_eq!(*test_kv_label.get_associated_uuid(), test_kv_label_oid);
        assert_eq!(test_kv_label.get_key(), "test_key".to_string());
        assert_eq!(test_kv_label.get_value(), "test_value".to_string());
        assert_eq!(*test_kv_label.get_type(), KeyValueType::LABEL);

        let test_kv_hook_oid = diesel_ulid::DieselUlid::generate();

        let test_kv_hook = ObjectKeyValue::new_kv::<ObjectKeyValue>(
            "test_key_hook",
            "test_value_hook",
            test_kv_hook_oid,
            KeyValueType::HOOK,
        );

        assert_eq!(*test_kv_hook.get_associated_uuid(), test_kv_hook_oid);
        assert_eq!(test_kv_hook.get_key(), "test_key_hook".to_string());
        assert_eq!(test_kv_hook.get_value(), "test_value_hook".to_string());
        assert_eq!(*test_kv_hook.get_type(), KeyValueType::HOOK);
    }
}
