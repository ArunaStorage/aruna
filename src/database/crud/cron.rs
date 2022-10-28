use crate::{database::connection::Database, error::ArunaError};
use diesel::connection::SimpleConnection;

impl Database {
    pub fn update_collection_views(&self) -> Result<(), ArunaError> {
        // Update materialized views

        self.pg_connection
            .get()?
            .batch_execute("REFRESH MATERIALIZED VIEW collection_stats")?;
        Ok(())
    }

    pub fn update_object_group_views(&self) -> Result<(), ArunaError> {
        // Update materialized views
        self.pg_connection
            .get()?
            .batch_execute("REFRESH MATERIALIZED VIEW object_group_stats")?;
        Ok(())
    }
}
