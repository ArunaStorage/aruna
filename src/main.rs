#[macro_use]
extern crate diesel;

extern crate aruna_server;

#[tokio::main]
async fn main() {
    let server = aruna_server::server::server::ServiceServer {};
    server.run().await;
}
