use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::{
    crud::{CrudDb, PrimaryKey},
    enums::{DataClass, ObjectStatus, ObjectType},
};
use crate::utils::database_utils::create_multi_query;
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use chrono::NaiveDateTime;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use futures::pin_mut;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, Json, ToSql, Type};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::{Client, CopyInSink};

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

#[derive(Serialize, Deserialize, Debug, Clone, Hash, PartialEq, Eq, PartialOrd)]
pub enum DefinedVariant {
    URL,
    IDENTIFIER,
    CUSTOM,
}

#[derive(Serialize, Deserialize, Hash, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct ExternalRelation {
    pub identifier: String,
    pub defined_variant: DefinedVariant,
    pub custom_variant: Option<String>,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ExternalRelations(pub DashMap<String, ExternalRelation, RandomState>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Hashes(pub Vec<Hash>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Hash {
    pub alg: Algorithm,
    pub hash: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Algorithm {
    MD5,
    SHA256,
}

#[derive(FromRow, FromSql, Debug, Clone, ToSql)]
pub struct Object {
    pub id: DieselUlid,
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
    pub dynamic: bool,
    pub endpoints: Json<DashMap<DieselUlid, bool, RandomState>>,
}

#[derive(FromRow, Debug, FromSql, Clone)]
pub struct ObjectWithRelations {
    #[from_row(flatten)]
    pub object: Object,
    pub inbound: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
    pub inbound_belongs_to: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
    pub outbound: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
    pub outbound_belongs_to: Json<DashMap<DieselUlid, InternalRelation, RandomState>>,
}

#[async_trait::async_trait]
impl CrudDb for Object {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO objects (id, revision_number, name, description, created_by, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes, dynamic, endpoints) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
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
                    &self.dynamic,
                    &self.endpoints,
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
        rel: Vec<ExternalRelation>,
    ) -> Result<()> {
        let query_one =
            "UPDATE objects SET external_relations = external_relations || $1::jsonb WHERE id = $2";
        let dash_map: DashMap<String, ExternalRelation, RandomState> =
            DashMap::from_iter(rel.into_iter().map(|r| (r.identifier.clone(), r)));
        let query_two = Json(ExternalRelations(dash_map));
        let prepared = client.prepare(query_one).await?;
        client.execute(&prepared, &[&query_two, &id]).await?;
        Ok(())
    }

    pub async fn remove_external_relation(
        id: &DieselUlid,
        client: &Client,
        rel: Vec<ExternalRelation>,
    ) -> Result<()> {
        let keys: Vec<String> = rel.into_iter().map(|e| e.identifier).collect();
        let query =
            "UPDATE objects SET external_relations = external_relations - $1::text[] WHERE id = $2;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&keys, id]).await?;
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

    pub async fn get_object_with_relations(
        id: &DieselUlid,
        client: &Client,
    ) -> Result<ObjectWithRelations> {
        let query = "SELECT o.*,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
        FROM objects o
        LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
        WHERE o.id = $1
        GROUP BY o.id;";
        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[&id]).await?;
        Ok(ObjectWithRelations::from_row(&row))
    }

    pub async fn get_objects_with_relations(
        ids: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        let query_one = "SELECT o.*,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
        COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
        COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
        FROM objects o
        LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
        WHERE o.id IN ";
        let query_three = " GROUP BY o.id;";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_two = create_multi_query(&inserts);
        let query = format!("{query_one}{query_two}{query_three}");
        let prepared = client.prepare(&query).await?;
        let objects = client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(ObjectWithRelations::from_row)
            .collect();
        Ok(objects)
    }
    pub async fn update(&self, client: &Client) -> Result<()> {
        let query = "UPDATE objects 
        SET description = $2, key_values = $3, data_class = $4
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
    pub async fn set_deleted(ids: &Vec<DieselUlid>, client: &Client) -> Result<()> {
        let query_one = "UPDATE objects 
            SET object_status = 'DELETED'
            WHERE id IN ";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_two = create_multi_query(&inserts);
        let query = format!("{query_one}{query_two};");
        let prepared = client.prepare(&query).await?;
        client.execute(&prepared, &inserts).await?;
        Ok(())
    }
    pub fn get_cloned_persistent(&self, new_id: DieselUlid) -> Self {
        let object = self.clone();
        Object {
            id: new_id,
            revision_number: object.revision_number,
            name: object.name,
            description: object.description,
            created_at: object.created_at,
            created_by: object.created_by,
            content_len: object.content_len,
            count: object.count,
            key_values: object.key_values,
            object_status: object.object_status,
            data_class: object.data_class,
            object_type: object.object_type,
            external_relations: object.external_relations,
            hashes: object.hashes,
            dynamic: false,
            endpoints: object.endpoints,
        }
    }
    pub async fn archive(
        ids: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<Vec<ObjectWithRelations>> {
        let query_one = " WITH o AS 
            (UPDATE objects 
            SET dynamic=false 
            WHERE objects.id IN ";
        //$id
        let query_three =  " RETURNING *)
        SELECT o.*,
            COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') inbound,
            COALESCE(JSON_OBJECT_AGG(ir1.origin_pid, ir1.*) FILTER (WHERE ir1.target_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') inbound_belongs_to,
            COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND NOT ir1.relation_name = 'BELONGS_TO'), '{}') outbound,
            COALESCE(JSON_OBJECT_AGG(ir1.target_pid, ir1.*) FILTER (WHERE ir1.origin_pid = o.id AND ir1.relation_name = 'BELONGS_TO'), '{}') outbound_belongs_to
            FROM objects o
            LEFT OUTER JOIN internal_relations ir1 ON o.id IN (ir1.target_pid, ir1.origin_pid)
            WHERE o.id IN ";
        let query_five = " GROUP BY o.id;";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_insert = create_multi_query(&inserts);
        let query = format!("{query_one}{query_insert}{query_three}{query_insert}{query_five}");
        let prepared = client.prepare(&query).await?;
        let result: Vec<ObjectWithRelations> = client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(ObjectWithRelations::from_row)
            .collect();
        Ok(result)
    }
    pub async fn get_objects(ids: &Vec<DieselUlid>, client: &Client) -> Result<Vec<Object>> {
        let query_one = "SELECT * FROM objects WHERE objects.id IN ";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_insert = create_multi_query(&inserts);
        let query = format!("{query_one}{query_insert};");
        let prepared = client.prepare(&query).await?;
        Ok(client
            .query(&prepared, &inserts)
            .await?
            .iter()
            .map(Object::from_row)
            .collect())
    }
    pub async fn batch_create(objects: &Vec<Object>, client: &Client) -> Result<()> {
        //let query = "INSERT INTO objects
        //    (id, revision_number, name, description, created_by, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes, dynamic, endpoints)
        //    VALUES $1;";
        let query = "COPY objects \
        (id, revision_number, name, description, created_by, content_len, count, key_values, object_status, data_class, object_type, external_relations, hashes, dynamic, endpoints)
        FROM STDIN BINARY";
        let sink: CopyInSink<_> = client.copy_in(query).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::UUID,
                Type::INT4,
                Type::VARCHAR,
                Type::VARCHAR,
                Type::UUID,
                Type::INT8,
                Type::INT4,
                Type::JSONB,
                ObjectStatus::get_type(),
                DataClass::get_type(),
                ObjectType::get_type(),
                Type::JSONB,
                Type::JSONB,
                Type::BOOL,
                Type::JSONB,
            ],
        );
        pin_mut!(writer);
        for object in objects {
            writer
                .as_mut()
                .write(&[
                    &object.id,
                    &object.revision_number,
                    &object.name,
                    &object.description,
                    &object.created_by,
                    &object.content_len,
                    &object.count,
                    &object.key_values,
                    &object.object_status,
                    &object.data_class,
                    &object.object_type,
                    &object.external_relations,
                    &object.hashes,
                    &object.dynamic,
                    &object.endpoints,
                ])
                .await?;
        }
        writer.finish().await?;
        Ok(())
    }
}

impl PartialEq for Object {
    fn eq(&self, other: &Self) -> bool {
        let self_external_relation: HashSet<_> = self
            .external_relations
            .0
             .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let other_external_relation: HashSet<_> = other
            .external_relations
            .0
             .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        match (&self.created_at, other.created_at) {
            (Some(_), None) | (None, Some(_)) | (None, None) => {
                self.id == other.id
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self_external_relation
                        .iter()
                        .all(|i| other_external_relation.contains(i))
                    && self.hashes == other.hashes
                    && self.dynamic == other.dynamic
            }
            (Some(_), Some(_)) => {
                self.id == other.id
                    && self.created_at == other.created_at
                    && self.revision_number == other.revision_number
                    && self.created_by == other.created_by
                    && self.content_len == other.content_len
                    && self.key_values == other.key_values
                    && self.object_status == other.object_status
                    && self.data_class == other.data_class
                    && self.object_type == other.object_type
                    && self_external_relation
                        .iter()
                        .all(|i| other_external_relation.contains(i))
                    && self.hashes == other.hashes
                    && self.dynamic == other.dynamic
            }
        }
    }
}
impl Eq for Object {}
impl PartialEq for ObjectWithRelations {
    fn eq(&self, other: &Self) -> bool {
        // Faster than comparing vecs
        let self_inbound_set: HashSet<_> =
            self.inbound.0.iter().map(|r| r.value().clone()).collect();
        let other_inbound_set: HashSet<_> =
            other.inbound.0.iter().map(|r| r.value().clone()).collect();
        let self_inbound_belongs_to_set: HashSet<_> = self
            .inbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let other_inbound_belongs_to_set: HashSet<_> = other
            .inbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let self_outbound_set: HashSet<_> =
            self.outbound.0.iter().map(|r| r.value().clone()).collect();
        let other_outbound_set: HashSet<_> =
            other.outbound.0.iter().map(|r| r.value().clone()).collect();
        let self_outbound_belongs_to_set: HashSet<_> = self
            .outbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        let other_outbound_belongs_to_set: HashSet<_> = other
            .outbound_belongs_to
            .0
            .iter()
            .map(|r| r.value().clone())
            .collect();
        self.object == other.object
            && self_inbound_set
                .iter()
                .all(|item| other_inbound_set.contains(item))
            && self_outbound_set
                .iter()
                .all(|item| other_outbound_set.contains(item))
            && self_inbound_belongs_to_set
                .iter()
                .all(|item| other_inbound_belongs_to_set.contains(item))
            && self_outbound_belongs_to_set
                .iter()
                .all(|item| other_outbound_belongs_to_set.contains(item))
    }
}
impl Eq for ObjectWithRelations {}

impl ObjectWithRelations {
    pub fn random_object_to(id: &DieselUlid, to: &DieselUlid) -> Self {
        Self {
            object: Object {
                id: *id,
                created_at: None,
                revision_number: 0,
                created_by: DieselUlid::generate(),
                content_len: 0,
                key_values: Json(KeyValues(vec![])),
                object_status: ObjectStatus::AVAILABLE,
                data_class: DataClass::PUBLIC,
                object_type: ObjectType::OBJECT,
                external_relations: Json(ExternalRelations(DashMap::default())),
                hashes: Json(Hashes(vec![])),
                dynamic: false,
                name: "a_name".to_string(),
                description: "a_name".to_string(),
                count: 0,
                endpoints: Json(DashMap::default()),
            },
            inbound: Json(DashMap::default()),
            inbound_belongs_to: Json(DashMap::default()),
            outbound: Json(DashMap::default()),
            outbound_belongs_to: Json(DashMap::from_iter([(*to, InternalRelation::default())])),
        }
    }
}
