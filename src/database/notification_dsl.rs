use anyhow::Result;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use tokio_postgres::Client;

use super::{
    crud::{CrudDb, PrimaryKey},
    enums::ObjectType,
};

#[derive(FromRow, Debug)]
pub struct NotificationStreamConsumer {
    pub id: DieselUlid,
    pub subject: String,
    pub resource_id: DieselUlid,
    pub resource_type: ObjectType,
    pub notify_on_sub_resources: bool,
    //ToDo: - Persist resource action? (For filtering?)
    //      - Persist full consumer config?
}

#[async_trait::async_trait]
impl CrudDb for NotificationStreamConsumer {
    //ToDo: Rust Doc
    async fn create(&self, client: &Client) -> Result<()> {
        let query = "INSERT INTO notification_stream_consumers 
          (id, subject, resource_id, resource_type, notify_on_sub_resources) 
        VALUES 
          ($1, $2, $3, $4, $5);";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.subject,
                    &self.resource_id,
                    &self.resource_type,
                    &self.notify_on_sub_resources,
                ],
            )
            .await?;
        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM notification_stream_consumers WHERE id = $1";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| NotificationStreamConsumer::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM notification_stream_consumers";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows
            .iter()
            .map(NotificationStreamConsumer::from_row)
            .collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, id: impl PrimaryKey, client: &Client) -> Result<()> {
        let query = "DELETE FROM notification_stream_consumers WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&id]).await?;
        Ok(())
    }
}
