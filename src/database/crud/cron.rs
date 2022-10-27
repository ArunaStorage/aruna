use diesel::{sql_query, RunQueryDsl};

use crate::{database::connection::Database, error::ArunaError};

impl Database {
    pub fn update_collection_views(&self) -> Result<(), ArunaError> {
        // Update materialized views
        let mut conn = self.pg_connection.get()?;
        sql_query("REFRESH MATERIALIZED VIEW collection_stats").execute(&mut conn)?;
        Ok(())
    }

    pub fn update_object_group_views(&self) -> Result<(), ArunaError> {
        // Update materialized views
        let mut conn = self.pg_connection.get()?;
        sql_query("REFRESH MATERIALIZED VIEW object_group_stats").execute(&mut conn)?;
        Ok(())
    }
}
