use crate::database::connection::Database;

impl Database {
    fn new_collection() -> () {
        todo!()
    }
}

// impl Create for CollectionHandler<'_> {
//     type Input = CreateNewCollectionRequest;
//     type Output = uuid::Uuid;

//     let my_collection = models::collection{};

//     fn create(&self, request: CreateNewCollectionRequest) -> uuid::Uuid {
//         self.database
//             .pg_connection
//             .get()
//             .unwrap()
//             .transaction::<_, Error, _>(|conn| {
//                 db_collection.insert_into(collections).execute(conn)?;
//                 Ok(())
//             })
//             .unwrap();

//         uuid::Uuid::new_v4()
//     }
// }
