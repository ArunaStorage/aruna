//use std::env;
//use diesel::associations::HasTable;
//use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager, Pool};
//use diesel::result::Error;
use diesel::PgConnection;
use dotenv::dotenv;

//use crate::api::aruna::api::storage::models::v1::Collection;
//use crate::api::aruna::api::storage::services::v1::CreateNewCollectionRequest;
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

// impl Database {
//     pub fn create_collection(&self, create_request: CreateNewCollectionRequest) -> uuid::Uuid {
//         use crate::database::schema::::dsl::*;
//         use crate::database::schema::collections::dsl::*;
//         use crate::database::schema::labels::dsl::*;

//         let collection_uuid = uuid::Uuid::new_v4();

//         let db_labels = util::to_db_labels(create_request.labels);

//         let db_collection = models::Collection {
//             id: collection_uuid.clone(),
//             name: create_request.name,
//             description: create_request.description,
//         };

//         let db_collection_labels: Vec<CollectionLabel> = db_labels
//             .iter()
//             .map(|label| CollectionLabel {
//                 collection_id: collection_uuid.clone(),
//                 label_id: label.id,
//             })
//             .collect();

//         self.pg_connection
//             .get()
//             .unwrap()
//             .transaction::<_, Error, _>(|conn| {
//                 db_labels.insert_into(labels).execute(conn)?;
//                 db_collection.insert_into(collections).execute(conn)?;
//                 db_collection_labels
//                     .insert_into(collection_labels)
//                     .execute(conn)?;
//                 Ok(())
//             })
//             .unwrap();

//         return collection_uuid;
//     }

//     pub fn get_collection(&self, req_collection_id: uuid::Uuid) -> Collection {
//         use crate::database::schema;
//         use crate::database::schema::collection_labels::dsl::*;
//         use crate::database::schema::collections::dsl::*;
//         use crate::database::schema::labels::dsl::*;

//         let read_values = self
//             .pg_connection
//             .get()
//             .unwrap()
//             .transaction::<(Vec<Label>, models::Collection), Error, _>(|conn| {
//                 let read_collection_label: Vec<Label> = collections::table()
//                     .inner_join(
//                         collection_labels::table()
//                             .inner_join(labels.on(label_id.eq(schema::labels::id)))
//                             .on(collection_id.eq(schema::collections::dsl::id)),
//                     )
//                     .filter(schema::collections::id.eq(req_collection_id))
//                     .select(labels::all_columns())
//                     .load::<Label>(conn)
//                     .unwrap();

//                 let read_collection = collections::table()
//                     .filter(schema::collections::id.eq(req_collection_id))
//                     .first::<models::Collection>(conn)
//                     .unwrap();
//                 Ok((read_collection_label, read_collection))
//             })
//             .unwrap();

//         let (model_labels, model_collection) = read_values;
//         let proto_labels = util::to_proto_labels(model_labels);

//         let proto_collection = Collection {
//             name: model_collection.name,
//             description: model_collection.description,
//             id: model_collection.id.to_string(),
//             labels: proto_labels,
//             ..Default::default()
//         };

//         return proto_collection;
//     }
// }

// #[cfg(test)]
// mod tests {
//     use crate::api::sciobjsdb::api::storage::{
//         models::v1::KeyValue, services::v1::CreateCollectionRequest,
//     };

//     use super::Database;

//     #[test]
//     fn collection_test() {
//         let database = Database::new();
//         let request = CreateCollectionRequest {
//             name: "testname".to_string(),
//             description: "a  test collection".to_string(),
//             labels: vec![
//                 KeyValue {
//                     key: "key1".to_string(),
//                     value: "value1".to_string(),
//                 },
//                 KeyValue {
//                     key: "key2".to_string(),
//                     value: "value2".to_string(),
//                 },
//             ],
//         };

//         let id = database.create_collection(request);
//         let collection = database.get_collection(id);
//         assert_eq!(collection.labels.len(), 2)
//     }
// }
