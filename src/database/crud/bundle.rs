use std::str::FromStr;

use crate::{
    database::{connection::Database, models::object::Endpoint},
    error::ArunaError,
};
use aruna_rust_api::api::{
    bundler::services::v1::{CreateBundleRequest, CreateBundleResponse},
    internal::v1::{GetBundlesRequest, GetBundlesResponse},
};
use diesel::{Connection, QueryDsl, RunQueryDsl};
use diesel_ulid::DieselUlid;

impl Database {
    pub fn create_bundle(
        &self,
        request: CreateBundleRequest,
    ) -> Result<(CreateBundleResponse, String), ArunaError> {
        use crate::database::schema::endpoints::dsl::*;
        let endpoint_id: Option<DieselUlid> = if request.endpoint_id.is_empty() {
            None
        } else {
            Some(DieselUlid::from_str(&request.endpoint_id)?)
        };

        self.pg_connection
            .get()?
            .transaction::<(CreateBundleResponse, String), ArunaError, _>(|conn| {
                let ep: Endpoint = endpoints.filter().first::<Endpoint>(conn)?;

                // Get all object ids
                // Validate that they are in collection
                // Get Endpoint specific
                todo!()
            })
    }

    pub fn delete_bundle(
        &self,
        request: CreateBundleRequest,
    ) -> Result<(CreateBundleResponse, String), ArunaError> {
        todo!()
    }

    pub fn get_all_bundles(
        &self,
        request: GetBundlesRequest,
    ) -> Result<GetBundlesResponse, ArunaError> {
        todo!()
    }
}
