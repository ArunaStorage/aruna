use anyhow::Result;
use aruna_cache::query::{FullSyncData, QueryHandler};
use aruna_cache::structs::Resource;
use aruna_rust_api::api::storage::models::v2::{generic_resource, User};
use aruna_rust_api::api::storage::services::v2::Pubkey as APIPubkey;
use async_trait::async_trait;
use diesel_ulid::DieselUlid;

pub struct DBQueryHandler {}

#[async_trait]
impl QueryHandler for DBQueryHandler {
    async fn get_user(&self, _id: DieselUlid, _checksum: String) -> Result<User> {
        todo!()
    }
    async fn get_pubkeys(&self) -> Result<Vec<APIPubkey>> {
        todo!()
    }
    async fn get_resource(
        &self,
        _res: &Resource,
        _checksum: String,
    ) -> Result<generic_resource::Resource> {
        todo!()
    }
    async fn full_sync(&self) -> Result<FullSyncData> {
        todo!()
    }
}
