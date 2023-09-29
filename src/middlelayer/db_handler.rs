use crate::{
    caching::cache::Cache, database::connection::Database,
    notification::natsio_handler::NatsIoHandler,
};
use std::sync::Arc;

pub struct DatabaseHandler {
    pub database: Arc<Database>,
    pub natsio_handler: Arc<NatsIoHandler>,
    pub cache: Arc<Cache>,
}
