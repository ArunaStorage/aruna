use aruna_server::auth::permission_handler::PermissionHandler;
use aruna_server::auth::token_handler::TokenHandler;
use aruna_server::caching::cache::Cache;
use aruna_server::database::connection::Database;
use aruna_server::grpc::projects::ProjectServiceImpl;
use aruna_server::middlelayer::db_handler::DatabaseHandler;
use aruna_server::notification::natsio_handler::NatsIoHandler;
use aruna_server::search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes};
use std::sync::Arc;

use super::test_utils::DEFAULT_ENDPOINT_ULID;

#[allow(dead_code)]
pub async fn init_database() -> Arc<Database> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init database connection
    let db = Database::new(
        dotenvy::var("DATABASE_HOST").unwrap(),
        dotenvy::var("DATABASE_PORT")
            .unwrap()
            .parse::<u16>()
            .unwrap(),
        dotenvy::var("DATABASE_DB").unwrap(),
        dotenvy::var("DATABASE_USER").unwrap(),
    )
    .unwrap();

    db.initialize_db().await.unwrap();

    Arc::new(db)
}

#[allow(dead_code)]
pub async fn init_cache(db: Arc<Database>, sync: bool) -> Arc<Cache> {
    // Init cache
    let cache = Arc::new(Cache::default());

    // Sync cache on demand
    if sync {
        cache.sync_cache(db.clone()).await.unwrap();
    }

    // Return cache
    cache
}

#[allow(dead_code)]
pub async fn init_nats_client() -> Arc<NatsIoHandler> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init NatsIoHandler
    let client = async_nats::connect(dotenvy::var("NATS_HOST").unwrap())
        .await
        .unwrap();

    let natsio_handler = NatsIoHandler::new(client, dotenvy::var("REPLY_SECRET").unwrap(), None)
        .await
        .unwrap();

    Arc::new(natsio_handler)
}

#[allow(dead_code)]
pub async fn init_search_client() -> Arc<MeilisearchClient> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init MeilisearchClient
    let meilisearch_client = MeilisearchClient::new(
        &dotenvy::var("MEILISEARCH_HOST").unwrap(),
        Some(&dotenvy::var("MEILISEARCH_API_KEY").unwrap()),
    )
    .unwrap();

    // Create index if not exists on startup
    meilisearch_client
        .get_or_create_index(&MeilisearchIndexes::OBJECT.to_string(), Some("id"))
        .await
        .unwrap();

    Arc::new(meilisearch_client)
}

#[allow(dead_code)]
pub async fn init_database_handler_middlelayer() -> DatabaseHandler {
    // Init DatabaseHandler
    DatabaseHandler {
        database: init_database().await,
        natsio_handler: init_nats_client().await,
    }
}

#[allow(dead_code)]
pub async fn init_database_handler(
    db_conn: Arc<Database>,
    nats_handler: Arc<NatsIoHandler>,
) -> Arc<DatabaseHandler> {
    // Init DatabaseHandler
    Arc::new(DatabaseHandler {
        database: db_conn,
        natsio_handler: nats_handler,
    })
}

#[allow(dead_code)]
pub async fn init_permission_handler(
    db: Arc<Database>,
    cache: Arc<Cache>,
) -> Arc<PermissionHandler> {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init TokenHandler
    let token_handler = TokenHandler::new(
        cache.clone(),
        db.clone(),
        dotenvy::var("OAUTH_REALMINFO").unwrap(),
        dotenvy::var("ENCODING_KEY").unwrap(),
        dotenvy::var("DECODING_KEY").unwrap(),
    )
    .await
    .unwrap();

    let token_handler_arc = Arc::new(token_handler);

    // Init PermissionHandler
    Arc::new(PermissionHandler::new(
        cache.clone(),
        token_handler_arc.clone(),
    ))
}

#[allow(dead_code)]
pub async fn init_project_service() -> ProjectServiceImpl {
    // Load env
    dotenvy::from_filename(".env").unwrap();

    // Init database connection
    let db_conn = init_database().await;

    // Init Cache
    let cache = init_cache(db_conn.clone(), true).await;

    // Init TokenHandler
    let token_handler = Arc::new(
        TokenHandler::new(
            cache.clone(),
            db_conn.clone(),
            dotenvy::var("OAUTH_REALMINFO").unwrap(),
            dotenvy::var("ENCODING_KEY").unwrap(),
            dotenvy::var("DECODING_KEY").unwrap(),
        )
        .await
        .unwrap(),
    );

    // Init PermissionHandler
    let perm_handler = Arc::new(PermissionHandler::new(cache.clone(), token_handler.clone()));

    // Init MeilisearchClient
    let search_client = init_search_client().await;

    // Init NatsIoHandler
    let nats_client = init_nats_client().await;

    // Init DatabaseHandler
    let database_handler = init_database_handler(db_conn.clone(), nats_client.clone()).await;

    // Init project service
    ProjectServiceImpl::new(
        database_handler,
        perm_handler,
        cache,
        search_client,
        DEFAULT_ENDPOINT_ULID.to_string(),
    )
    .await
}
