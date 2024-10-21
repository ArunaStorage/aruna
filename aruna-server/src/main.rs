use aruna_synevi::{config_from_env, start_server};
use tracing_subscriber::EnvFilter;

mod api;
mod context;
mod error;
mod graph;
mod macros;
mod models;
mod requests;
mod storage;

#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("tower_http=debug".parse().unwrap())
        .add_directive("aruna_synevi=trace".parse().unwrap())
        .add_directive("synevi_core=trace".parse().unwrap());

    tracing_subscriber::fmt().with_env_filter(filter).init();
    
    let config = config_from_env();

    start_server(config, None).await.unwrap()
}
