//use std::env;
//use diesel::associations::HasTable;
//use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager, Pool};
//use diesel::result::Error;
use diesel::PgConnection;
use dotenv::dotenv;

//use aruna_rust_api::api::storage::models::v1::Collection;
//use aruna_rust_api::api::storage::services::v1::CreateNewCollectionRequest;
//use crate::database::models::{self, CollectionLabel, Label};
//use crate::database::util;

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
