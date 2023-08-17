use crate::database::crud::CrudDb;
use crate::database::dsls::endpoint_dsl::Endpoint;
use crate::database::dsls::pub_key_dsl::PubKey;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::endpoints_request_types::{CreateEP, DeleteEP, GetBy, GetEP};
use anyhow::{anyhow, Result};
use tokio_postgres::GenericClient;

impl DatabaseHandler {
    pub async fn create_endpoint(&self, request: CreateEP) -> Result<(Endpoint, PubKey)> {
        let mut client = self.database.get_client().await?;
        let idx = PubKey::get_max_id(&client).await?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let (mut endpoint, mut pubkey) = request.build_endpoint()?;
        pubkey.id = idx + 1;
        endpoint.create(transaction_client).await?;
        pubkey.create(transaction_client).await?;
        transaction.commit().await?;
        Ok((endpoint, pubkey))
    }
    pub async fn get_endpoint(&self, request: GetEP) -> Result<Endpoint> {
        let client = self.database.get_client().await?;
        let endpoint = match request.get_query()? {
            GetBy::ID(id) => Endpoint::get(id, client.client()).await?,
            GetBy::NAME(name) => Endpoint::get_by_name(name, client.client()).await?,
        };
        endpoint.ok_or_else(|| anyhow!("No endpoint found"))
    }
    pub async fn get_endpoints(&self) -> Result<Vec<Endpoint>> {
        let client = self.database.get_client().await?;
        let endpoints = Endpoint::all(client.client()).await?;
        Ok(endpoints)
    }
    pub async fn delete_endpoint(&self, request: DeleteEP) -> Result<()> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        Endpoint::delete_by_id(&id, client.client()).await?;
        Ok(())
    }
}
