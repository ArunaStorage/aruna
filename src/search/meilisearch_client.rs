use std::{fmt::Display, str::FromStr};

use aruna_rust_api::api::storage::models::v2::{
    generic_resource::Resource, Collection, Dataset, KeyValue as ApiKeyValue,
    KeyValueVariant as ApiKeyValueVariant, Object, Project, Status as ApiStatus,
};
use diesel_ulid::DieselUlid;
use meilisearch_sdk::{task_info::TaskInfo, Client};
use prost_wkt_types::Timestamp;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::database::{
    dsls::object_dsl::{KeyValue, KeyValueVariant, Object as DbObject},
    enums::{DataClass, ObjectStatus, ObjectType},
};

// Enum for the different index variants (multi-index search?)
#[derive(Serialize)]
pub enum MeilisearchIndexes {
    PROJECT,
    COLLECTION,
    DATASET,
    OBJECT,
}
// Implement display to get static index names
impl Display for MeilisearchIndexes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MeilisearchIndexes::PROJECT => write!(f, "projects"),
            MeilisearchIndexes::COLLECTION => write!(f, "collections"),
            MeilisearchIndexes::DATASET => write!(f, "datasets"),
            MeilisearchIndexes::OBJECT => write!(f, "objects"),
        }
    }
}

// Struct for generalized object data used for the search index
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectDocument {
    pub id: diesel_ulid::DieselUlid,
    pub resource_type: ObjectType,
    pub resource_status: ObjectStatus,
    pub name: String,
    pub description: String,
    pub size: i64,             // Yay or nay?
    pub labels: Vec<KeyValue>, // Without specific internal labels
    pub dataclass: DataClass,
    pub created_at: i64, // Converted to UNIX timestamp for filtering/sorting
    pub created_by: DieselUlid, // Should be the user name or something like that
}

// Coversion from database model Object into ObjectDocument
impl From<DbObject> for ObjectDocument {
    fn from(db_object: DbObject) -> Self {
        // Remove internal/unimportant labels
        let filtered_labels = db_object
            .key_values
            .0
             .0
            .into_iter()
            .filter(|kv| !kv.key.starts_with("app.aruna-storage"))
            .collect::<Vec<_>>();

        ObjectDocument {
            id: db_object.id,
            resource_type: db_object.object_type,
            resource_status: db_object.object_status,
            name: db_object.name,
            description: db_object.description,
            size: db_object.content_len,
            labels: filtered_labels,
            dataclass: db_object.data_class,
            created_at: db_object.created_at.unwrap_or_default().timestamp(),
            created_by: db_object.created_by,
        }
    }
}

// Conversion from ObjectDocument into generic API resource.
impl From<ObjectDocument> for Resource {
    fn from(val: ObjectDocument) -> Self {
        match val.resource_type {
            ObjectType::PROJECT => Resource::Project(val.into()),
            ObjectType::COLLECTION => Resource::Collection(val.into()),
            ObjectType::DATASET => Resource::Dataset(val.into()),
            ObjectType::OBJECT => Resource::Object(val.into()),
        }
    }
}
// Conversion from generic API resource into ObjectDocument.
impl TryFrom<Resource> for ObjectDocument {
    type Error = anyhow::Error;

    fn try_from(generic_resource: Resource) -> Result<Self, Self::Error> {
        match generic_resource {
            Resource::Project(project) => Ok(ObjectDocument::try_from(project)?),
            Resource::Collection(collection) => Ok(ObjectDocument::try_from(collection)?),
            Resource::Dataset(dataset) => Ok(ObjectDocument::try_from(dataset)?),
            Resource::Object(object) => Ok(ObjectDocument::try_from(object)?),
        }
    }
}

// Conversion from ObjectDocument into API Project
impl From<ObjectDocument> for Project {
    fn from(object_document: ObjectDocument) -> Self {
        Project {
            id: object_document.id.to_string(),
            name: object_document.name,
            description: object_document.description,
            key_values: convert_labels_to_proto(object_document.labels),
            relations: vec![],
            stats: None,
            data_class: object_document.dataclass as i32,
            created_at: Some(Timestamp {
                seconds: object_document.created_at,
                nanos: 0,
            }),
            created_by: object_document.created_by.to_string(),
            status: Into::<ApiStatus>::into(object_document.resource_status) as i32,
            dynamic: false, // Meaningful information?
        }
    }
}
// Conversion from API Project into ObjectDocument
impl TryFrom<Project> for ObjectDocument {
    type Error = anyhow::Error;

    fn try_from(project: Project) -> Result<Self, Self::Error> {
        // Build and return ObjectDocument
        Ok(ObjectDocument {
            id: DieselUlid::from_str(&project.id)?,
            resource_type: ObjectType::PROJECT,
            resource_status: ObjectStatus::try_from(project.status)?,
            name: project.name,
            description: project.description,
            size: if let Some(stats) = project.stats {
                stats.size
            } else {
                0
            },
            labels: convert_proto_to_key_value(project.key_values)?,
            dataclass: DataClass::try_from(project.data_class)?,
            created_at: project.created_at.unwrap_or_default().seconds,
            created_by: DieselUlid::from_str(&project.created_by)?,
        })
    }
}

// Conversion from ObjectDocument into API Collection
impl From<ObjectDocument> for Collection {
    fn from(object_document: ObjectDocument) -> Collection {
        Collection {
            id: object_document.id.to_string(),
            name: object_document.name,
            description: object_document.description,
            key_values: convert_labels_to_proto(object_document.labels),
            relations: vec![],
            stats: None,
            data_class: object_document.dataclass as i32,
            created_at: Some(Timestamp {
                seconds: object_document.created_at,
                nanos: 0,
            }),
            created_by: object_document.created_by.to_string(),
            status: Into::<ApiStatus>::into(object_document.resource_status) as i32,
            dynamic: false, // Needed information?
        }
    }
}
// Conversion from API Collection into ObjectDocument
impl TryFrom<Collection> for ObjectDocument {
    type Error = anyhow::Error;

    fn try_from(collection: Collection) -> Result<Self, Self::Error> {
        // Build and return ObjectDocument
        Ok(ObjectDocument {
            id: DieselUlid::from_str(&collection.id)?,
            resource_type: ObjectType::COLLECTION,
            resource_status: ObjectStatus::try_from(collection.status)?,
            name: collection.name,
            description: collection.description,
            size: if let Some(stats) = collection.stats {
                stats.size
            } else {
                0
            },
            labels: convert_proto_to_key_value(collection.key_values)?,
            dataclass: DataClass::try_from(collection.data_class)?,
            created_at: collection.created_at.unwrap_or_default().seconds,
            created_by: DieselUlid::from_str(&collection.created_by)?,
        })
    }
}

// Conversion from ObjectDocument into API Dataset
impl From<ObjectDocument> for Dataset {
    fn from(object_document: ObjectDocument) -> Dataset {
        Dataset {
            id: object_document.id.to_string(),
            name: object_document.name,
            description: object_document.description,
            key_values: convert_labels_to_proto(object_document.labels),
            relations: vec![],
            stats: None,
            data_class: object_document.dataclass as i32,
            created_at: Some(Timestamp {
                seconds: object_document.created_at,
                nanos: 0,
            }),
            created_by: object_document.created_by.to_string(),
            status: Into::<ApiStatus>::into(object_document.resource_status) as i32,
            dynamic: false, // Needed information?
        }
    }
}
// Conversion from API Dataset into ObjectDocument
impl TryFrom<Dataset> for ObjectDocument {
    type Error = anyhow::Error;

    fn try_from(dataset: Dataset) -> Result<Self, Self::Error> {
        // Build and return ObjectDocument
        Ok(ObjectDocument {
            id: DieselUlid::from_str(&dataset.id)?,
            resource_type: ObjectType::DATASET,
            resource_status: ObjectStatus::try_from(dataset.status)?,
            name: dataset.name,
            description: dataset.description,
            size: if let Some(stats) = dataset.stats {
                stats.size
            } else {
                0
            },
            labels: convert_proto_to_key_value(dataset.key_values)?,
            dataclass: DataClass::try_from(dataset.data_class)?,
            created_at: dataset.created_at.unwrap_or_default().seconds,
            created_by: DieselUlid::from_str(&dataset.created_by)?,
        })
    }
}

// Conversion from ObjectDocument into API Object
impl From<ObjectDocument> for Object {
    fn from(object_document: ObjectDocument) -> Object {
        Object {
            id: object_document.id.to_string(),
            name: object_document.name,
            description: object_document.description,
            key_values: convert_labels_to_proto(object_document.labels),
            relations: vec![],
            content_len: object_document.size,
            data_class: object_document.dataclass as i32,
            created_at: Some(Timestamp {
                seconds: object_document.created_at,
                nanos: 0,
            }),
            created_by: object_document.created_by.to_string(),
            status: Into::<ApiStatus>::into(object_document.resource_status) as i32,
            dynamic: false, // Needed information?
            hashes: vec![],
        }
    }
}
// Conversion from API Object into ObjectDocument
impl TryFrom<Object> for ObjectDocument {
    type Error = anyhow::Error;

    fn try_from(object: Object) -> Result<Self, Self::Error> {
        // Build and return ObjectDocument
        Ok(ObjectDocument {
            id: DieselUlid::from_str(&object.id)?,
            resource_type: ObjectType::OBJECT,
            resource_status: ObjectStatus::try_from(object.status)?,
            name: object.name,
            description: object.description,
            size: object.content_len,
            labels: convert_proto_to_key_value(object.key_values)?,
            dataclass: DataClass::try_from(object.data_class)?,
            created_at: object.created_at.unwrap_or_default().seconds,
            created_by: DieselUlid::from_str(&object.created_by)?,
        })
    }
}

impl From<KeyValueVariant> for ApiKeyValueVariant {
    fn from(val: KeyValueVariant) -> Self {
        match val {
            KeyValueVariant::HOOK => ApiKeyValueVariant::Hook,
            KeyValueVariant::LABEL => ApiKeyValueVariant::Label,
            KeyValueVariant::STATIC_LABEL => ApiKeyValueVariant::StaticLabel,
        }
    }
}

fn convert_proto_to_key_value(key_values: Vec<ApiKeyValue>) -> anyhow::Result<Vec<KeyValue>> {
    let whatev: Result<Vec<_>, _> = key_values
        .into_iter()
        .map(|kv| KeyValue::try_from(&kv))
        .collect();

    match whatev {
        Ok(some_vec) => Ok(some_vec),
        Err(err) => Err(anyhow::anyhow!(err.to_string())),
    }
}

fn convert_labels_to_proto(labels: Vec<KeyValue>) -> Vec<ApiKeyValue> {
    labels
        .into_iter()
        .map(|l| ApiKeyValue {
            key: l.key,
            value: l.value,
            variant: Into::<ApiKeyValueVariant>::into(l.variant) as i32,
        })
        .collect()
}

#[derive(Clone)]
pub struct MeilisearchClient {
    _server_url: String,
    _api_key: Option<String>,
    pub client: Client,
}

impl MeilisearchClient {
    ///ToDo: Rust Doc
    pub fn new(
        meilisearch_instance_url: &str,
        meilisearch_instance_api_key: Option<&str>,
    ) -> anyhow::Result<Self> {
        let meilisearch_client =
            Client::new(meilisearch_instance_url, meilisearch_instance_api_key);

        Ok(MeilisearchClient {
            _server_url: meilisearch_instance_url.to_string(),
            _api_key: meilisearch_instance_api_key.map(|api_key| api_key.to_string()),
            client: meilisearch_client,
        })
    }

    ///ToDo: Rust Doc
    pub async fn create_index(
        &self,
        index_name: &str,
        primary_key: Option<&str>, // Has to be unique index document attribute, so most likely 'id'
    ) -> anyhow::Result<TaskInfo> {
        Ok(self.client.create_index(index_name, primary_key).await?)
    }

    ///ToDo: Rust Doc
    pub async fn list_index<T: 'static + DeserializeOwned>(
        &self,
        index_name: &str,
    ) -> anyhow::Result<Vec<T>> {
        let result = self
            .client
            .index(index_name)
            .search()
            .execute::<T>()
            .await?
            .hits;

        // Collect result hits in vector
        let document_objects = result.into_iter().map(|hit| hit.result).collect();

        Ok(document_objects)
    }

    ///ToDo: Rust Doc
    pub async fn add_or_update_stuff<S: Serialize>(
        &self,
        stuff: &[S], // Slice of ... whatever is in the index
        stuff_type: MeilisearchIndexes,
    ) -> anyhow::Result<TaskInfo> {
        // Extract index name of provided enum variant
        let index_name = stuff_type.to_string();

        // Add or update documents in index
        Ok(self
            .client
            .index(index_name)
            .add_or_update(stuff, Some("id"))
            .await?)
    }

    ///ToDo: Rust Doc
    pub async fn delete_stuff<S: Serialize + Display + std::fmt::Debug>(
        &self,
        stuff: &[S], // Slice of ... whatever is in the index
        stuff_type: MeilisearchIndexes,
    ) -> anyhow::Result<TaskInfo> {
        // Extract index name of enum variant
        let index_name = stuff_type.to_string();

        // Delete documents to search
        Ok(self
            .client
            .index(index_name)
            .delete_documents(stuff)
            .await?)
    }

    ///ToDo: Rust Doc
    pub async fn query_generic_stuff<T: 'static + DeserializeOwned>(
        &self,
        index_name: &str,
        query_phrase: &str,
        query_filter: &str,
        query_limit: usize,
        query_offset: usize,
    ) -> anyhow::Result<(Vec<T>, i32)> {
        // Query specific index
        let result = self
            .client
            .index(index_name)
            .search()
            .with_query(query_phrase)
            .with_limit(query_limit)
            .with_filter(query_filter)
            .with_offset(query_offset)
            .execute::<T>()
            .await?;

        // Extract estimated hits attribute from result
        let estimated_hits = match &result.estimated_total_hits {
            Some(estimate) => *estimate as i32,
            None => {
                log::warn!("No estimated hit count received");
                -1
            }
        };

        // Collect result hits in vector
        let document_objects = result.hits.into_iter().map(|hit| hit.result).collect();

        Ok((document_objects, estimated_hits))
    }
}
