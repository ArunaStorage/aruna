//mod api;
#[macro_use]
extern crate diesel;

mod api;
mod database;
mod server;

#[tokio::main]
async fn main() {
    let server = server::server::ServiceServer {};
    server.run().await;
}
