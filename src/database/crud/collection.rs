use super::utils::*;
use crate::api::aruna::api::storage::models::v1::CollectionOverview;
use crate::api::aruna::api::storage::services::v1::{
    get_collection_by_id_response::Collection as GetCollectionResponseEnum,
    CreateNewCollectionRequest, CreateNewCollectionResponse, GetCollectionByIdRequest,
    GetCollectionByIdResponse,
};
use crate::database::connection::Database;
use crate::database::models;
use crate::error::ArunaError;
use chrono::Local;
use diesel::insert_into;
use diesel::prelude::*;

impl Database {
    pub fn create_new_collection(
        &self,
        request: CreateNewCollectionRequest,
        creator: uuid::Uuid,
    ) -> Result<CreateNewCollectionResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use diesel::result::Error;

        let collection_uuid = uuid::Uuid::new_v4();

        let shared_version_uuid = uuid::Uuid::new_v4();

        let key_values = to_collection_key_values(request.labels, request.hooks, collection_uuid);

        let db_collection = models::collection::Collection {
            id: collection_uuid,
            shared_version_id: shared_version_uuid,
            name: request.name,
            description: request.description,
            created_by: creator,
            created_at: Local::now().naive_local(),
            version_id: None,
            dataclass: None,
            project_id: uuid::Uuid::parse_str(&request.project_id)?,
        };

        self.pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                // Get the API token, if this errors -> no corresponding database token object could be found
                insert_into(collection_key_value)
                    .values(key_values)
                    .execute(conn)?;

                insert_into(collections)
                    .values(db_collection)
                    .execute(conn)?;
                Ok(())
            })?;

        return Ok(CreateNewCollectionResponse {
            id: collection_uuid.to_string(),
        });
    }

    fn get_collection_by_id(
        &self,
        request: GetCollectionByIdRequest,
    ) -> Result<GetCollectionByIdResponse, ArunaError> {
        use crate::database::schema::collection_key_value::dsl as ckv;
        use crate::database::schema::collections::dsl as col;
        use crate::database::schema::required_labels::dsl as rlbl;
        use diesel::prelude::*;
        use diesel::result::Error;

        let format = request.format;

        let collection_id = uuid::Uuid::parse_str(&request.id)?;

        let collection = self
            .pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                let collection_info = col::collections
                    .filter(col::id.eq(collection_id))
                    .first::<models::collection::Collection>(conn)?;

                let collection_key_values = ckv::collection_key_value
                    .filter(ckv::collection_id.eq(collection_id))
                    .load::<models::collection::CollectionKeyValue>(conn)?;

                let label_ontology = rlbl::required_labels
                    .filter(rlbl::collection_id.eq(collection_id))
                    .load::<models::collection::RequiredLabel>(conn)?;
                todo!();
                // let retformat = match format {
                //     0 | 1 => {
                //         let test = "";
                //         GetCollectionResponseEnum::CollectionOverview(CollectionOverview {
                //             id: collection_info.id,
                //             name: collection_info.name,
                //             description: collection_info.description,
                //             labels: ,
                //             hooks: todo!(),
                //             label_ontology: todo!(),
                //             created: todo!(),
                //             authorization: todo!(),
                //             stats: ,
                //             is_public: todo!(),
                //             version: todo!(),
                //         })
                //     }
                //     2 => GetCollectionResponseEnum::CollectionWithId(_),
                //     3 => GetCollectionResponseEnum::CollectionFull(_),
                // };

                // Ok(collection_info)
            })?;

        todo!()
    }
}
