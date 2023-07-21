use anyhow::Result;
use aruna_cache::query::QueryHandler;
use aruna_rust_api::api::storage::models::v2::{Collection, Dataset, Object, Project, User};
use aruna_rust_api::api::storage::services::v2::Pubkey as APIPubkey;
use async_trait::async_trait;
use diesel_ulid::DieselUlid;

pub struct DBQueryHandler {}

#[async_trait]
impl QueryHandler for DBQueryHandler {
    async fn get_user(&self, id: DieselUlid, checksum: String) -> Result<User> {
        todo!()
    }
    async fn get_pubkeys(&self) -> Result<Vec<APIPubkey>> {
        todo!()
    }
    async fn get_project(&self, id: DieselUlid, checksum: String) -> Result<Project> {
        todo!()
    }
    async fn get_collection(&self, id: DieselUlid, checksum: String) -> Result<Collection> {
        todo!()
    }
    async fn get_dataset(&self, id: DieselUlid, checksum: String) -> Result<Dataset> {
        todo!()
    }
    async fn get_object(&self, id: DieselUlid, checksum: String) -> Result<Object> {
        todo!()
    }
}
