mod api;
#[macro_use]
extern crate diesel;

mod database;
mod s3;
mod server;

#[tokio::main]
async fn main() {
    let server = server::server::ServiceServer {};
    server.run().await;
}
