use crate::database::crud::CrudDb;
use crate::database::dsls::license_dsl::License;
use crate::middlelayer::db_handler::DatabaseHandler;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::CreateLicenseRequest;

impl DatabaseHandler {
    pub async fn create_license(&self, request: CreateLicenseRequest) -> Result<String> {
        let client = self.database.get_client().await?;
        let mut license: License = request.into();
        license.create(&client).await?;
        Ok(license.tag)
    }
    pub async fn get_license(&self, tag: String) -> Result<License> {
        let client = self.database.get_client().await?;
        let license = License::get(tag, &client)
            .await?
            .ok_or_else(|| anyhow!("No license found"))?;
        Ok(license)
    }
    pub async fn list_licenses(&self) -> Result<Vec<License>> {
        let client = self.database.get_client().await?;
        let licenses = License::all(&client).await?;
        Ok(licenses)
    }
}
