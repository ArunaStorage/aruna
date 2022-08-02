use diesel::associations::HasTable;
use diesel::prelude::*;
use diesel::Connection;

use crate::database::models::auth::User;
use crate::database::{
    connection::Database,
    models::{self, auth::ApiToken},
};

impl Database {
    pub fn get_api_token(
        &self,
        token_id: uuid::Uuid,
    ) -> Result<ApiToken, Box<dyn std::error::Error>> {
        use crate::database::schema;
        use crate::database::schema::api_tokens::dsl::*;
        //use crate::database::schema::users::dsl::*;
        use diesel::result::Error;

        let token_values = self
            .pg_connection
            .get()?
            .transaction::<models::auth::ApiToken, Error, _>(|conn| {
                let user_token = api_tokens
                    .filter(id.eq(token_id))
                    .first::<ApiToken>(conn)
                    .unwrap();
                Ok(user_token)
            })?;

        // let test = self
        //     .pg_connection
        //     .get()?
        //     .transaction::<models::auth::User, Error, _>(|conn| {
        //         Ok(users.filter(id.eq(token_id)).first::<User>(conn).unwrap())
        //     })?;

        Ok(token_values)

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
        //                 Ok((read_collection_label, read_collection)
        // self.pg_connection
        //     .get()
        //     .unwrap()
        //     .transaction::<_, Error, _>(|conn| {
        //         db_labels.insert_into(labels).execute(conn)?;
        //         db_collection.insert_into(collections).execute(conn)?;
        //         db_collection_labels
        //             .insert_into(collection_labels)
        //             .execute(conn)?;
        //         Ok(())
        //     })
        //     .unwrap();

        // return collection_uuid;
    }
}
