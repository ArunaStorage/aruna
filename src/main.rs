extern crate aruna_server;

#[tokio::main]
async fn main() {
    let server = aruna_server::server::grpc_server::ServiceServer {};
    server.run().await;
}
