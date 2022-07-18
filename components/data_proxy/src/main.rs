mod data_middleware;
mod data_server;
mod signing;
mod storage_backend;

#[tokio::main]
async fn main() {
    data_server::server::serve().await.unwrap();
}
