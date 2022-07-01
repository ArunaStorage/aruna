use std::env;

use diesel::{Connection, PgConnection};
use dotenv::dotenv;

pub struct Database {
    pg_connection: PgConnection,
}

impl Database {
    pub fn new() -> Self {
        let connection = Database::establish_connection();
        let database = Database {
            pg_connection: connection,
        };

        return database;
    }

    fn establish_connection() -> PgConnection {
        dotenv().ok();

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        PgConnection::establish(&database_url)
            .expect(&format!("Error connecting to {}", database_url))
    }
}
