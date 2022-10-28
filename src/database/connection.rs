use diesel::r2d2::{self, ConnectionManager, Pool};
use diesel::PgConnection;
use dotenv::dotenv;

pub struct Database {
    pub pg_connection: Pool<ConnectionManager<PgConnection>>,
}

impl Database {
    pub fn new(database_url: &str) -> Self {
        let connection = Database::establish_connection(database_url);

        Database {
            pg_connection: connection,
        }
    }

    fn establish_connection(database_url: &str) -> Pool<ConnectionManager<PgConnection>> {
        dotenv().ok();

        //let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let manager = ConnectionManager::<PgConnection>::new(database_url);

        r2d2::Pool::builder()
            .build(manager)
            .expect("Failed to create database connection pool")
    }
}
