use std::env;

use diesel::associations::HasTable;
use diesel::prelude::*;
use diesel::r2d2::{self, ConnectionManager, Pool};
use diesel::result::Error;
use diesel::PgConnection;
use dotenv::dotenv;

use crate::api::sciobjsdb::api::storage::models::v1::Collection;
use crate::api::sciobjsdb::api::storage::models::v1::KeyValue;
use crate::database::models::{self, Label};

pub struct Database {
    pg_connection: Pool<ConnectionManager<PgConnection>>,
}

impl Database {
    pub fn new() -> Self {
        let connection = Database::establish_connection();
        let database = Database {
            pg_connection: connection,
        };

        return database;
    }

    fn establish_connection() -> Pool<ConnectionManager<PgConnection>> {
        dotenv().ok();

        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let manager = ConnectionManager::<PgConnection>::new(&database_url);
        let pool = r2d2::Pool::builder()
            .build(manager)
            .expect("Failed to create database connection pool");

        return pool;
    }
}

impl Database {
    pub fn create_collection(&self) -> uuid::Uuid {
        use crate::database::schema::collection_labels::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::labels::dsl::*;

        let collection_uuid = uuid::Uuid::new_v4();

        let collection = models::Collection {
            id: collection_uuid.clone(),
            name: "test".to_string(),
            description: "a test collection".to_string(),
        };

        let mut labels_list = vec![];
        labels_list.push(models::Label {
            id: uuid::Uuid::new_v4(),
            key: "key1".to_string(),
            value: "value1".to_string(),
        });

        labels_list.push(models::Label {
            id: uuid::Uuid::new_v4(),
            key: "key2".to_string(),
            value: "value2".to_string(),
        });

        let mut collection_labels_list = vec![];
        collection_labels_list.push(models::CollectionLabel {
            collection_id: collection.id,
            label_id: labels_list[0].id,
        });

        collection_labels_list.push(models::CollectionLabel {
            collection_id: collection.id,
            label_id: labels_list[1].id,
        });

        self.pg_connection
            .get()
            .unwrap()
            .transaction::<_, Error, _>(|conn| {
                labels_list.insert_into(labels).execute(conn)?;
                collection.insert_into(collections).execute(conn)?;
                collection_labels_list
                    .insert_into(collection_labels)
                    .execute(conn)?;
                Ok(())
            })
            .unwrap();

        return collection_uuid;
    }

    pub fn get_collection(&self, req_collection_id: uuid::Uuid) -> Collection {
        use crate::database::schema;
        use crate::database::schema::collection_labels::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::labels::dsl::*;

        let read_values = self
            .pg_connection
            .get()
            .unwrap()
            .transaction::<(Vec<Label>, models::Collection), Error, _>(|conn| {
                let read_collection_label: Vec<Label> = collections::table()
                    .inner_join(
                        collection_labels::table()
                            .inner_join(labels.on(label_id.eq(schema::labels::id)))
                            .on(collection_id.eq(schema::collections::dsl::id)),
                    )
                    .filter(schema::collections::id.eq(req_collection_id))
                    .select(labels::all_columns())
                    .load::<Label>(self.pg_connection.get().as_mut().unwrap())
                    .unwrap();

                let read_collection = collections::table()
                    .filter(schema::collections::id.eq(req_collection_id))
                    .first::<models::Collection>(self.pg_connection.get().as_mut().unwrap())
                    .unwrap();
                Ok((read_collection_label, read_collection))
            })
            .unwrap();

        let (model_labels, model_collection) = read_values;
        let proto_labels = model_labels
            .into_iter()
            .map(|model_label| KeyValue {
                key: model_label.key,
                value: model_label.value,
            })
            .collect();

        let proto_collection = Collection {
            name: model_collection.name,
            description: model_collection.description,
            id: model_collection.id.to_string(),
            labels: proto_labels,
            ..Default::default()
        };

        return proto_collection;
    }
}

#[cfg(test)]
mod tests {
    use super::Database;

    #[test]
    fn collection_test() {
        let database = Database::new();
        let id = database.create_collection();
        let collection = database.get_collection(id);
        assert_eq!(collection.labels.len(), 2)
    }
}
