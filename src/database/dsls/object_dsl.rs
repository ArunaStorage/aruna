use crate::database::{
    crud::{CrudDb, PrimaryKey},
    enums::{DataClass, ObjectStatus, ObjectType},
};

use crate::database::dsls::internal_relation_dsl::InternalRelation;
use anyhow::anyhow;
use anyhow::Result;
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio_postgres::Client;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
#[allow(non_camel_case_types)]
pub enum KeyValueVariant {
    HOOK,
    LABEL,
    STATIC_LABEL,
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
pub enum DefinedVariant {
    URL,
    IDENTIFIER,
    CUSTOM,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct ExternalRelation {
    pub identifier: String,
    pub defined_variant: DefinedVariant,
    pub custom_variant: Option<String>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq)]
pub struct ExternalRelations(pub Vec<ExternalRelation>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq)]
pub struct Hashes(pub Vec<Hash>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq)]
pub struct Hash {
    pub alg: Algorithm,
    pub hash: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, PartialOrd, Eq)]
pub enum Algorithm {
    MD5,
    SHA256,
}

#[derive(FromRow, FromSql, Debug, Clone)]
pub struct Object {
    pub id: DieselUlid,
    pub shared_id: DieselUlid,
    pub revision_number: i32,
    pub name: String,
    pub description: String,
    pub created_at: Option<NaiveDateTime>,
    pub created_by: DieselUlid,
    pub content_len: i64,
    pub count: i32,
    pub key_values: Json<KeyValues>,
    pub object_status: ObjectStatus,
    pub data_class: DataClass,
    pub object_type: ObjectType,
    pub external_relations: Json<ExternalRelations>,
    pub hashes: Json<Hashes>,
}

#[derive(FromRow, Debug, FromSql, Clone)]
pub struct ObjectWithRelations {
    pub object: Object,
    pub inbound: Json<Inbound>,
    pub outbound: Json<Outbound>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct Inbound(pub Vec<InternalRelation>);
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct Outbound(pub Vec<InternalRelation>);
#[derive(Serialize, Deserialize, FromRow, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct InternalRelationWithULIDsAsStrings {
    pub id: String,
    pub origin_pid: String,
    pub origin_type: ObjectType,
    pub type_name: String,
    pub target_pid: String,
    pub target_type: ObjectType,
    pub is_persistent: bool,
}
#[async_trait::async_trait]
impl CrudDb for Object {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO objects (id, shared_id, revision_number, name, description, created_by, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.shared_id,
                    &self.revision_number,
                    &self.name,
                    &self.description,
                    &self.created_by,
                    &self.content_len,
                    &self.count,
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

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "UPDATE objects 
            SET object_status = 'DELETED'
            WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
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
    pub async fn finish_object_staging(
        id: &DieselUlid,
        client: &Client,
        hashes: Option<Hashes>,
        content_len: i64,
        object_status: ObjectStatus,
    ) -> Result<()> {
        match hashes {
            Some(h) => {
                let query_some = "UPDATE objects 
            SET hashes = $1, content_len = $2, object_status = $3
            WHERE id = $4;";
                let prepared = client.prepare(query_some).await?;
                client
                    .execute(&prepared, &[&Json(h), &content_len, &object_status, id])
                    .await?
            }
            None => {
                let query_none = "UPDATE objects 
            SET content_len = $1, object_status = $2
            WHERE id = $3;";
                let prepared = client.prepare(query_none).await?;
                client
                    .execute(&prepared, &[&content_len, &object_status, id])
                    .await?
            }
        };
        Ok(())
    }
    pub async fn get_latest_object_by_dynamic_id(
        id: &DieselUlid,
        client: &Client,
    ) -> Result<Object> {
        let query = "SELECT * FROM objects WHERE shared_id = $1, revision_number = (SELECT MAX (revision_number) FROM objects WHERE shared_id = $1);";
        let prepared = client.prepare(query).await?;
        let object: Object = client
            .query_one(&prepared, &[&id])
            .await
            .map(|e| Object::from_row(&e))?;
        Ok(object)
    }
    pub async fn get_all_revisions(id: &DieselUlid, client: &Client) -> Result<Vec<Object>> {
        let query = "SELECT * FROM objects WHERE shared_id = $1";
        let prepared = client.prepare(query).await?;
        let object: Vec<Object> = client
            .query(&prepared, &[&id])
            .await?
            .iter()
            .map(Object::from_row)
            .collect();
        Ok(object)
    }
    pub async fn get_object_with_relations(
        id: &DieselUlid,
        client: &Client,
    ) -> Result<ObjectWithRelations> {
        let query = "SELECT o.*,
            JSON_AGG(ir1.*) FILTER (WHERE ir1.target_pid = o.id) inbound,
            JSON_AGG(ir1.*) FILTER (WHERE ir1.origin_pid = o.id) outbound
            FROM objects o
            LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
            WHERE o.id = $1
            GROUP BY o.id;";
        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[&id]).await;

        row.map(|e| -> Result<ObjectWithRelations> {
            //let inbound: Json<Inbound> = ;
            let inbound = Json(Inbound(
                e.get::<usize, Json<Vec<InternalRelationWithULIDsAsStrings>>>(15)
                    .0
                    .into_iter()
                    .map(|i| -> Result<InternalRelation> {
                        Ok(InternalRelation {
                            id: DieselUlid::from(uuid::Uuid::parse_str(&i.id)?),
                            origin_pid: DieselUlid::from(uuid::Uuid::parse_str(&i.origin_pid)?),
                            origin_type: i.origin_type,
                            type_name: i.type_name,
                            target_pid: DieselUlid::from(uuid::Uuid::parse_str(&i.target_pid)?),
                            target_type: i.target_type,
                            is_persistent: i.is_persistent,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ));
            let outbound = Json(Outbound(
                e.get::<usize, Json<Vec<InternalRelationWithULIDsAsStrings>>>(16)
                    .0
                    .into_iter()
                    .map(|i| -> Result<InternalRelation> {
                        Ok(InternalRelation {
                            id: DieselUlid::from(uuid::Uuid::parse_str(&i.id)?),
                            origin_pid: DieselUlid::from(uuid::Uuid::parse_str(&i.origin_pid)?),
                            origin_type: i.origin_type,
                            type_name: i.type_name,
                            target_pid: DieselUlid::from(uuid::Uuid::parse_str(&i.target_pid)?),
                            target_type: i.target_type,
                            is_persistent: i.is_persistent,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            ));
            Ok(ObjectWithRelations {
                object: Object::from_row(&e),
                inbound,
                outbound,
            })
        })?
    }
    pub async fn update(&self, client: &Client) -> Result<()> {
        let query = "UPDATE objects 
        SET description = $2, key_values = $3, data_class = $4)
        WHERE id = $1 ;";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.description,
                    &self.key_values,
                    &self.data_class,
                ],
            )
            .await?;
        Ok(())
    }
    pub async fn update_name(id: DieselUlid, name: String, client: &Client) -> Result<()> {
        let query = "UPDATE objects 
        SET name = $2
        WHERE id = $1 ;";
        let prepared = client.prepare(query).await?;
        client.query(&prepared, &[&id, &name]).await?;
        Ok(())
    }
    pub async fn update_description(
        id: DieselUlid,
        description: String,
        client: &Client,
    ) -> Result<()> {
        let query = "UPDATE objects 
        SET description = $2
        WHERE id = $1 ;";
        let prepared = client.prepare(query).await?;
        client.query(&prepared, &[&id, &description]).await?;
        Ok(())
    }
    pub async fn update_dataclass(
        id: DieselUlid,
        dataclass: DataClass,
        client: &Client,
    ) -> Result<()> {
        let query = "UPDATE objects 
        SET data_class = $2
        WHERE id = $1 ;";
        let prepared = client.prepare(query).await?;
        client.query(&prepared, &[&id, &dataclass]).await?;
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
impl PartialEq for ObjectWithRelations {
    fn eq(&self, other: &Self) -> bool {
        // Faster than comparing vecs
        let self_inbound_set: HashSet<_> = self.inbound.0 .0.iter().cloned().collect();
        let other_inbound_set: HashSet<_> = other.inbound.0 .0.iter().cloned().collect();
        let self_outbound_set: HashSet<_> = self.outbound.0 .0.iter().cloned().collect();
        let other_outbound_set: HashSet<_> = other.outbound.0 .0.iter().cloned().collect();
        self.object == other.object
            && self_inbound_set
                .iter()
                .all(|item| other_inbound_set.contains(item))
            && self_outbound_set
                .iter()
                .all(|item| other_outbound_set.contains(item))
    }
}
impl Eq for ObjectWithRelations {}
