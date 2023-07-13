use crate::database::{
    crud::{CrudDb, PrimaryKey},
    enums::{DataClass, ObjectStatus, ObjectType},
};

use anyhow::anyhow;
use anyhow::Result;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::Json;
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum KeyValueVariant {
    HOOK,
    LABEL,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct KeyValue {
    pub key: String,
    pub value: String,
    pub variant: KeyValueVariant,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct KeyValues(pub Vec<KeyValue>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub enum RelationVariant {
    URL,
    IDENTIFIER,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct ExternalRelation {
    pub name: String,
    pub identifier: String,
    pub variant: RelationVariant,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq)]
pub struct ExternalRelations(pub Vec<ExternalRelation>);

#[derive(FromRow, Debug)]
pub struct Object {
    pub id: DieselUlid,
    pub shared_id: DieselUlid,
    pub revision_number: i32,
    pub path: String,
    pub created_at: Option<NaiveDateTime>,
    pub created_by: DieselUlid,
    pub content_len: i64,
    pub key_values: Json<KeyValues>,
    pub object_status: ObjectStatus,
    pub data_class: DataClass,
    pub object_type: ObjectType,
    pub external_relations: Json<ExternalRelations>,
    pub hashes: Vec<String>,
}

#[async_trait::async_trait]
impl CrudDb for Object {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO objects (id, shared_id, revision_number, path, created_by, content_len, key_values, object_status, data_class, object_type, external_relations, hashes) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.shared_id,
                    &self.revision_number,
                    &self.path,
                    &self.created_by,
                    &self.content_len,
                    &self.key_values,
                    &self.object_status,
                    &self.data_class,
                    &self.object_type,
                    &self.external_relations,
                    &self.hashes,
                ],
            )
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM objects WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| Object::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM objects";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(Object::from_row).collect::<Vec<_>>())
    }

    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM objects WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}

impl Object {
    pub async fn add_key_value(id: &DieselUlid, client: &Client, kv: KeyValue) -> Result<()> {
        let query = "UPDATE objects
        SET key_values = key_values || $1::jsonb
        WHERE id = $2;";

        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&Json(kv), id]).await?;
        Ok(())
    }

    pub async fn remove_key_value(&self, client: &Client, kv: KeyValue) -> Result<()> {
        let element: i32 = self
            .key_values
            .0
             .0
            .iter()
            .position(|e| *e == kv)
            .ok_or_else(|| anyhow!("Unable to find key_value"))? as i32;

        let query = "UPDATE objects
        SET key_values = key_values - $1::INTEGER
        WHERE id = $2;";

        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&element, &self.id]).await?;
        Ok(())
    }

    pub async fn add_external_relations(
        id: &DieselUlid,
        client: &Client,
        rel: ExternalRelation,
    ) -> Result<()> {
        let query = "UPDATE objects
        SET external_relations = external_relations || $1::jsonb
        WHERE id = $2;";

        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&Json(rel), id]).await?;
        Ok(())
    }

    pub async fn remove_external_relation(
        &self,
        client: &Client,
        rel: ExternalRelation,
    ) -> Result<()> {
        let element: i32 = self
            .external_relations
            .0
             .0
            .iter()
            .position(|e| *e == rel)
            .ok_or_else(|| anyhow!("Unable to find key_value"))? as i32;

        let query = "UPDATE objects
        SET external_relations = external_relations - $1::INTEGER
        WHERE id = $2;";

        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&element, &self.id]).await?;
        Ok(())
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        match (&self.created_at, other.created_at) {
            (Some(_), None) => {
                self.id == other.id
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self.external_relations == other.external_relations
                    && self.hashes == other.hashes
            }
            (Some(_), Some(_)) => {
                self.id == other.id
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.created_at == other.created_at
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self.external_relations == other.external_relations
                    && self.hashes == other.hashes
            }
            (None, Some(_)) => {
                self.id == other.id
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self.external_relations == other.external_relations
                    && self.hashes == other.hashes
            }
            (None, None) => {
                self.id == other.id
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self.external_relations == other.external_relations
                    && self.hashes == other.hashes
            }
        }
    }
}
impl Eq for Object {}
