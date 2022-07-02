use database::connection::Database;

mod api;
#[macro_use]
extern crate diesel;

mod database;

fn main() {
    let db = Database::new();
}
