use aruna_server::{config_from_env, start_server};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_server=trace".parse().unwrap())
        .add_directive("tower_http=debug".parse().unwrap())
        .add_directive("aruna_synevi=debug".parse().unwrap())
        .add_directive("synevi_core=trace".parse().unwrap());

    tracing_subscriber::fmt().with_env_filter(filter).init();

    let config = config_from_env();

    start_server(config, None).await.unwrap()
}
