use diesel::{sql_query, Connection, RunQueryDsl};

use crate::{database::connection::Database, error::ArunaError};

impl Database {
    pub fn update_collection_views(&self) -> Result<(), ArunaError> {
        // Update materialized views
        self
            .pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                sql_query("REFRESH MATERIALIZED VIEW collection_stats").execute(conn)?;
                Ok(())
            })
    }

    pub fn update_object_group_views(&self) -> Result<(), ArunaError> {
        // Update materialized views
        self
            .pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                sql_query("REFRESH MATERIALIZED VIEW object_group_stats").execute(conn)?;
                Ok(())
            })
    }
}
