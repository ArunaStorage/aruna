use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::{Json, ToSql};
use serde::{Deserialize, Serialize};
use tokio_postgres::Client;

use crate::{
    database::{
        crud::{CrudDb, PrimaryKey},
        enums::{NotificationReferenceType, PersistentNotificationVariant},
    },
    utils::database_utils::create_multi_query,
};

#[derive(FromRow, Debug, PartialEq, Eq)]
pub struct PersistentNotification {
    pub id: DieselUlid,
    pub user_id: DieselUlid,
    pub notification_variant: PersistentNotificationVariant,
    pub message: String,
    pub refs: Json<NotificationReferences>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NotificationReferences(pub Vec<NotificationReference>);

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct NotificationReference {
    pub reference_type: NotificationReferenceType,
    pub reference_name: String,
    pub reference_value: String,
}

#[async_trait::async_trait]
impl CrudDb for PersistentNotification {
    //ToDo: Rust Doc
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO persistent_notifications 
          (id, user_id, notification_variant, message, refs) 
        VALUES 
          ($1, $2, $3, $4, $5);";
        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.user_id,
                    &self.notification_variant,
                    &self.message,
                    &self.refs,
                ],
            )
            .await?;

        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM persistent_notifications WHERE id = $1;";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&&id])
            .await?
            .map(|e| PersistentNotification::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM persistent_notifications;";
        let prepared = client.prepare(query).await?;

        let rows = client.query(&prepared, &[]).await?;

        Ok(rows
            .iter()
            .map(PersistentNotification::from_row)
            .collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM persistent_notifications WHERE id = $1";
        let prepared = client.prepare(query).await?;

        client.execute(&prepared, &[&self.id]).await?;

        Ok(())
    }
}

impl PersistentNotification {
    //ToDo: Rust Doc
    pub async fn get_user_notifications(
        user_id: &DieselUlid,
        client: &Client,
    ) -> Result<Vec<PersistentNotification>> {
        let query = "SELECT * FROM persistent_notifications WHERE user_id = $1;";
        let prepared = client.prepare(query).await?;

        let rows = client.query(&prepared, &[&user_id]).await?;

        Ok(rows
            .iter()
            .map(PersistentNotification::from_row)
            .collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    pub async fn acknowledge_user_notifications(
        notification_ids: &Vec<DieselUlid>,
        client: &Client,
    ) -> Result<()> {
        let base_query = "DELETE FROM persistent_notifications WHERE id IN ";

        let mut inserts = Vec::<&(dyn ToSql + Sync)>::new();
        for id in notification_ids {
            inserts.push(id);
        }

        let query_extension = create_multi_query(&inserts);
        let final_query = format!("{base_query}{query_extension};");
        let prepared = client.prepare(&final_query).await?;

        client.execute(&prepared, &inserts).await?;

        Ok(())
    }
}
