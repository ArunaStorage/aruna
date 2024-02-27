use crate::{
    caching::cache::Cache, database::connection::Database, hooks::hook_handler::HookMessage,
    notification::natsio_handler::NatsIoHandler,
};
use async_channel::Sender;
use std::sync::Arc;

pub struct DatabaseHandler {
    pub database: Arc<Database>,
    pub natsio_handler: Arc<NatsIoHandler>,
    pub cache: Arc<Cache>,
    pub hook_sender: Sender<HookMessage>,
}
