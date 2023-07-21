use crate::database::connection::Database;
use std::sync::Arc;

pub struct DatabaseHandler {
    pub database: Arc<Database>,
}
