use crate::database::crud::{CrudDb, PrimaryKey};
use crate::utils::database_utils::create_multi_query;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use futures::pin_mut;
use postgres_from_row::FromRow;
use postgres_types::{FromSql, ToSql, Type};
use serde::{Deserialize, Serialize};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::{Client, CopyInSink};

use super::super::enums::ObjectType;

#[derive(
    Serialize,
    Deserialize,
    Hash,
    ToSql,
    FromRow,
    Debug,
    FromSql,
    Clone,
    PartialEq,
    Eq,
    PartialOrd,
    Default,
)]
pub struct InternalRelation {
    pub id: DieselUlid,
    pub origin_pid: DieselUlid,
    pub origin_type: ObjectType,
    pub relation_name: String,
    pub target_pid: DieselUlid,
    pub target_type: ObjectType,
}

pub const INTERNAL_RELATION_VARIANT_BELONGS_TO: &str = "BELONGS_TO";
pub const INTERNAL_RELATION_VARIANT_ORIGIN: &str = "ORIGIN";
pub const INTERNAL_RELATION_VARIANT_VERSION: &str = "VERSION";
pub const INTERNAL_RELATION_VARIANT_METADATA: &str = "METADATA";
pub const INTERNAL_RELATION_VARIANT_POLICY: &str = "POLICY";

#[async_trait::async_trait]
impl CrudDb for InternalRelation {
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO internal_relations (id, origin_pid, origin_type, relation_name, target_pid, target_type) VALUES (
            $1, $2, $3, $4, $5, $6
        );";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.origin_pid,
                    &self.origin_type,
                    &self.relation_name,
                    &self.target_pid,
                    &self.target_type,
                ],
            )
            .await?;
        Ok(())
    }
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        // This needs to be dynamic for (origin/target)/(did/pid)
        let query = "SELECT * FROM internal_relations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| InternalRelation::from_row(&e)))
    }
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM internal_relations";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(InternalRelation::from_row)
            .collect::<Vec<_>>())
    }

    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM internal_relations WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl InternalRelation {
    /// Updates relations to latest origin_pid
    pub async fn update_to(old: DieselUlid, new: DieselUlid, client: &Client) -> Result<()> {
        let query = "UPDATE internal_relations 
            SET origin_pid = $2 
            WHERE origin_pid = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&old, &new]).await?;
        Ok(())
    }
    /// Updates relations to latest target_pid
    pub async fn update_from(old: DieselUlid, new: DieselUlid, client: &Client) -> Result<()> {
        let query = "UPDATE internal_relations 
            SET target_pid = $2 
            WHERE target_pid = $1;";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&old, &new]).await?;
        Ok(())
    }
    pub async fn batch_create(relations: &Vec<InternalRelation>, client: &Client) -> Result<()> {
        let query = "COPY internal_relations (id, origin_pid, origin_type, relation_name, target_pid, target_type)\
        FROM STDIN BINARY;";
        let sink: CopyInSink<_> = client.copy_in(query).await?;
        let writer = BinaryCopyInWriter::new(
            sink,
            &[
                Type::UUID,
                Type::UUID,
                ObjectType::get_type(),
                Type::VARCHAR,
                Type::UUID,
                ObjectType::get_type(),
            ],
        );
        pin_mut!(writer);
        for relation in relations {
            writer
                .as_mut()
                .write(&[
                    &relation.id,
                    &relation.origin_pid,
                    &relation.origin_type,
                    &relation.relation_name,
                    &relation.target_pid,
                    &relation.target_type,
                ])
                .await?;
        }
        writer.finish().await?;
        Ok(())
    }

    // Gets all outbound relations for pid
    pub async fn get_all_by_id(id: &DieselUlid, client: &Client) -> Result<Vec<InternalRelation>> {
        let query = "SELECT * FROM internal_relations 
            WHERE origin_pid = $1 OR target_pid = $1;";

        let prepared = client.prepare(query).await?;
        let relations = client
            .query(&prepared, &[id])
            .await?
            .iter()
            .map(InternalRelation::from_row)
            .collect();
        Ok(relations)
    }

    pub fn clone_relation(&self, replace: &DieselUlid) -> Self {
        InternalRelation {
            id: DieselUlid::generate(),
            origin_pid: *replace,
            origin_type: self.origin_type,
            relation_name: self.relation_name.clone(),
            target_pid: self.target_pid,
            target_type: self.target_type,
        }
    }
    pub async fn batch_delete(ids: &Vec<DieselUlid>, client: &Client) -> Result<()> {
        let query_one = "DELETE FROM internal_relations WHERE id IN ";
        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in ids {
            inserts.push(id);
        }
        let query_two = create_multi_query(&inserts);
        let query = format!("{query_one}{query_two};");
        dbg!(&query);
        dbg!(&inserts);
        let prepared = client.prepare(&query).await?;
        client.execute(&prepared, &inserts).await?;
        Ok(())
    }
}
