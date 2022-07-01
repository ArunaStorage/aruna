use database::connection::Database;

mod api;
mod database;

fn main() {
    let db = Database::new();
}
