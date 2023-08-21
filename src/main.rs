use std::{str::FromStr, sync::Arc};

use anyhow::Result;
use aruna_rust_api::api::{
    notification::services::v2::event_notification_service_server::EventNotificationServiceServer,
    storage::services::v2::{
        collection_service_server::CollectionServiceServer,
        dataset_service_server::DatasetServiceServer,
        endpoint_service_server::EndpointServiceServer, object_service_server::ObjectServiceServer,
        project_service_server::ProjectServiceServer,
        relations_service_server::RelationsServiceServer,
        search_service_server::SearchServiceServer,
        storage_status_service_server::StorageStatusServiceServer,
        user_service_server::UserServiceServer,
    },
};
use aruna_server::{
    auth::{permission_handler::PermissionHandler, token_handler::TokenHandler},
    caching::{cache::Cache, notifications_handler::NotificationHandler},
    database::{self, crud::CrudDb, dsls::endpoint_dsl::Endpoint},
    grpc::{
        collections::CollectionServiceImpl, datasets::DatasetServiceImpl,
        endpoints::EndpointServiceImpl, info::StorageStatusServiceImpl,
        notification::NotificationServiceImpl, object::ObjectServiceImpl,
        projects::ProjectServiceImpl, relations::RelationsServiceImpl, search::SearchServiceImpl,
        users::UserServiceImpl,
    },
    middlelayer::db_handler::DatabaseHandler,
    notification::natsio_handler::NatsIoHandler,
    search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes},
    utils::mailclient::MailClient,
};
use diesel_ulid::DieselUlid;
use simple_logger::SimpleLogger;
use tonic::transport::Server;

#[tokio::main]
pub async fn main() -> Result<()> {
    // Init logger
    SimpleLogger::new()
        .with_level(log::LevelFilter::Debug)
        .env()
        .init()
        .unwrap();

    // Load env
    dotenvy::from_filename(".env")?;

    // Init database connection
    let db = database::connection::Database::new(
        dotenvy::var("DATABASE_HOST")?,
        dotenvy::var("DATABASE_PORT")?.parse::<u16>()?,
        dotenvy::var("DATABASE_DB")?,
        dotenvy::var("DATABASE_USER")?,
    )?;
    db.initialize_db().await?;
    let db_arc = Arc::new(db);

    // Init cache
    let cache = Cache::new();
    let cache_arc = Arc::new(cache);
    cache_arc.sync_cache(db_arc.clone()).await?;

    // Init TokenHandler
    let token_handler = TokenHandler::new(
        cache_arc.clone(),
        db_arc.clone(),
        dotenvy::var("OAUTH_REALMINFO")?,
        dotenvy::var("ENCODING_KEY")?,
        dotenvy::var("DECODING_KEY")?,
    )
    .await?;
    let token_handler_arc = Arc::new(token_handler);

    // Init PermissionHandler
    let authorizer = PermissionHandler::new(cache_arc.clone(), token_handler_arc.clone());
    let auth_arc = Arc::new(authorizer);

    // Init NatsIoHandler
    let client = async_nats::connect(dotenvy::var("NATS_HOST")?).await?;
    let natsio_handler = NatsIoHandler::new(client, dotenvy::var("REPLY_SECRET")?, None)
        .await
        .map_err(|_| anyhow::anyhow!("NatsIoHandler init failed"))?;
    let natsio_arc = Arc::new(natsio_handler);

    // Init DatabaseHandler
    let database_handler = DatabaseHandler {
        database: db_arc.clone(),
        natsio_handler: natsio_arc.clone(),
    };
    let db_handler_arc = Arc::new(database_handler);
    dbg!("Bin hier!");

    // NotificationHandler
    let _ = NotificationHandler::new(db_arc.clone(), cache_arc.clone(), natsio_arc.clone()).await?;

    // MeilisearchClient
    let meilisearch_client = MeilisearchClient::new(
        &dotenvy::var("MEILISEARCH_HOST")?,
        Some(&dotenvy::var("MEILISEARCH_API_KEY")?),
    )?;
    let meilisearch_arc = Arc::new(meilisearch_client);

    // Create index if not exists on startup
    meilisearch_arc
        .get_or_create_index(&MeilisearchIndexes::OBJECT.to_string(), Some("id"))
        .await?;

    // init MailClient
    let _: Option<MailClient> = if !dotenvy::var("ARUNA_DEV_ENV")?.parse::<bool>()? {
        Some(MailClient::new()?)
    } else {
        None
    };

    let default_endpoint = dotenvy::var("DEFAULT_DATAPROXY_ULID")?;

    // Init server builder
    let mut builder = Server::builder().add_service(EndpointServiceServer::new(
        EndpointServiceImpl::new(
            db_handler_arc.clone(),
            auth_arc.clone(),
            cache_arc.clone(),
            default_endpoint.to_string(),
        )
        .await,
    ));

    // Check default endpoint -> Only endpoint service available
    let client = db_arc.get_client().await?;

    dbg!(&default_endpoint.is_empty());

    if !&default_endpoint.is_empty()
        && Endpoint::get(DieselUlid::from_str(&default_endpoint)?, &client)
            .await?
            .is_some()
    {
        // Add other services
        builder = builder
            .add_service(UserServiceServer::new(
                UserServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    token_handler_arc.clone(),
                )
                .await,
            ))
            .add_service(ProjectServiceServer::new(
                ProjectServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(CollectionServiceServer::new(
                CollectionServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ))
            .add_service(DatasetServiceServer::new(
                DatasetServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ))
            .add_service(ObjectServiceServer::new(
                ObjectServiceImpl::new(db_handler_arc.clone(), auth_arc.clone(), cache_arc.clone())
                    .await,
            ))
            .add_service(RelationsServiceServer::new(
                RelationsServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ))
            .add_service(EventNotificationServiceServer::new(
                NotificationServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    natsio_arc.clone(),
                )
                .await,
            ))
            .add_service(SearchServiceServer::new(
                SearchServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                    meilisearch_arc.clone(),
                )
                .await,
            ))
            .add_service(StorageStatusServiceServer::new(
                StorageStatusServiceImpl::new(
                    db_handler_arc.clone(),
                    auth_arc.clone(),
                    cache_arc.clone(),
                )
                .await,
            ));
    }

    // Do it.
    let addr: std::net::SocketAddr = "0.0.0.0:50051".parse()?;
    log::info!("ArunaServer listening on {}", addr);
    builder.serve(addr).await?;

    // Cron scheduler?

    Ok(())
}
