use std::sync::Arc;

use anyhow::Result;
use aruna_rust_api::api::{
    notification::services::v2::event_notification_service_server::EventNotificationServiceServer,
    storage::services::v2::{
        collection_service_server::CollectionServiceServer,
        dataset_service_server::DatasetServiceServer,
        endpoint_service_server::EndpointServiceServer, object_service_server::ObjectServiceServer,
        project_service_server::ProjectServiceServer,
        relations_service_server::RelationsServiceServer,
        search_service_server::SearchServiceServer, user_service_server::UserServiceServer,
    },
};
use aruna_server::{
    auth::{permission_handler::PermissionHandler, token_handler::TokenHandler},
    caching::{cache::Cache, notifications_handler::NotificationHandler},
    database::{self, dsls::endpoint_dsl::Endpoint},
    grpc::{
        collections::CollectionServiceImpl, datasets::DatasetServiceImpl,
        endpoints::EndpointServiceImpl, notification::NotificationServiceImpl,
        object::ObjectServiceImpl, projects::ProjectServiceImpl, relations::RelationsServiceImpl,
        search::SearchServiceImpl, users::UserServiceImpl,
    },
    middlelayer::db_handler::DatabaseHandler,
    notification::natsio_handler::NatsIoHandler,
    search::meilisearch_client::MeilisearchClient,
    utils::mailclient::MailClient,
};
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

    // Database
    let db = database::connection::Database::new(
        dotenvy::var("DATABASE_HOST")?,
        dotenvy::var("DATABASE_PORT")?.parse::<u16>()?,
        dotenvy::var("DATABASE_DB")?,
        dotenvy::var("DATABASE_USER")?,
    )?;
    db.initialize_db().await?;
    let db_arc = Arc::new(db);

    log::info!("Database init");

    // Cache
    let cache = Cache::new();
    let cache_arc = Arc::new(cache);

    // Token Handler
    let token_handler = TokenHandler::new(
        cache_arc.clone(),
        db_arc.clone(),
        dotenvy::var("OAUTH_REALMINFO")?,
        dotenvy::var("ENCODING_KEY")?,
        dotenvy::var("DECODING_KEY")?,
    )
    .await?;
    let token_handler_arc = Arc::new(token_handler);

    // Permission Handler
    let authorizer = PermissionHandler::new(cache_arc.clone(), token_handler_arc.clone());
    let auth_arc = Arc::new(authorizer);

    // MailClient
    let mailclient: Option<MailClient> = if !dotenvy::var("ARUNA_DEV_ENV")?.parse::<bool>()? {
        Some(MailClient::new()?)
    } else {
        None
    };

    // NatsIoHandler
    let client = async_nats::connect(dotenvy::var("NATS_HOST")?).await?;
    let natsio_handler = NatsIoHandler::new(client, dotenvy::var("REPLY_SECRET")?, None)
        .await
        .map_err(|_| anyhow::anyhow!(""))?;

    let natsio_arc = Arc::new(natsio_handler);

    // Database Handler
    let database_handler = DatabaseHandler {
        database: db_arc.clone(),
        natsio_handler: natsio_arc.clone(),
    };
    let db_handler_arc = Arc::new(database_handler);

    // Notification Handler
    let _ = NotificationHandler::new(db_arc.clone(), cache_arc.clone(), natsio_arc.clone()).await?;

    // MeilisearchClient
    let meilisearch_client = MeilisearchClient::new(
        &dotenvy::var("MEILISEARCH_HOST")?,
        Some(&dotenvy::var("MEILISEARCH_API_KEY")?),
    )?;
    let meilisearch_arc = Arc::new(meilisearch_client);

    // Init server builder
    let mut builder = Server::builder().add_service(EndpointServiceServer::new(
        EndpointServiceImpl::new(db_handler_arc.clone(), auth_arc.clone(), cache_arc.clone()).await,
    ));

    // Check default endpoint -> Only endpoint service available
    let client = db_arc.get_client().await?;
    if let Some(_) = Endpoint::get_default(&client).await? {
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
            ));
    }

    // Do it.
    builder.serve("0.0.0.0:50052".parse()?).await?;

    // Cron scheduler

    Ok(())
}
