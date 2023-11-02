use std::{str::FromStr, sync::Arc};

use anyhow::{anyhow, bail};
use aruna_rust_api::api::storage::models::v2::User as ApiUser;
use aruna_rust_api::api::{
    notification::services::v2::{
        announcement_event::EventVariant as AnnEventVariant, event_message::MessageVariant,
        AnnouncementEvent, EventVariant, ResourceEvent, UserEvent,
    },
    storage::models::v2::generic_resource,
};
use async_nats::jetstream::consumer::DeliverPolicy;
use chrono::Utc;
use diesel_ulid::DieselUlid;
use futures::StreamExt;
use jsonwebtoken::DecodingKey;
use time::OffsetDateTime;

use crate::database::dsls::pub_key_dsl::PubKey;
use crate::search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes, ObjectDocument};
use crate::{
    database::{
        connection::Database,
        crud::CrudDb,
        dsls::{object_dsl::Object, user_dsl::User},
    },
    notification::natsio_handler::NatsIoHandler,
    utils::grpc_utils::{checksum_resource, checksum_user},
};

use super::cache::Cache;
use super::structs::PubKeyEnum;

pub struct NotificationHandler {}

impl NotificationHandler {
    ///ToDo: Rust Doc
    pub async fn new(
        database: Arc<Database>,
        cache: Arc<Cache>,
        natsio_handler: Arc<NatsIoHandler>,
        search_client: Arc<MeilisearchClient>,
    ) -> anyhow::Result<Self> {
        // Create standard pull consumer to fetch all notifications
        /*
        let (some1, _) = natsio_handler
            .create_event_consumer(EventType::All, DeliverPolicy::All)
            .await?;
        */

        // Create ephemeral pull consumer which fetches only messages from the time of startup
        let pull_consumer = natsio_handler
            .create_internal_consumer(
                DieselUlid::generate(),
                "AOS.>".to_string(), // All event notifications
                DeliverPolicy::ByStartTime {
                    start_time: tonic_invalid!(
                        OffsetDateTime::from_unix_timestamp(Utc::now().timestamp()),
                        "Incorrect timestamp format"
                    ),
                },
                true,
            )
            .await?;

        //Todo: Persist consumer for ArunaServer instances?
        //let pull_consumer = natsio_handler.get_pull_consumer(some1.to_string()).await?;

        // Async move the consumer listening
        let mut messages = pull_consumer.messages().await?;
        let cache_clone = cache.clone();
        let database_clone = database.clone();
        tokio::spawn(async move {
            loop {
                if let Some(Ok(nats_message)) = messages.next().await {
                    log_received!(&nats_message);

                    // Deserialize messages in gRPC definitions
                    let msg_variant = match serde_json::from_slice(
                        nats_message.message.payload.to_vec().as_slice(),
                    ) {
                        Ok(variant) => variant,
                        Err(err) => {
                            return Err::<MessageVariant, anyhow::Error>(anyhow::anyhow!(err))
                        }
                    };

                    // Update cache depending on message variant
                    match NotificationHandler::update_server_cache(
                        msg_variant,
                        cache_clone.clone(),
                        database_clone.clone(),
                        search_client.clone(),
                    )
                    .await
                    {
                        Ok(_) => log::info!("NotificationHandler cache update successful"),
                        Err(err) => log::error!("NotificationHandler cache update failed: {err}"),
                    }

                    // Acknowlege received message in every case becaue the only way cache update can fail is through the database query.
                    //   We have to trust that messages will only be sent if all database operations have been
                    //   successful in advance and the database has a consistent status in relation to the message being sent.
                    //   This means that in case of an error, the message does not represent the current state of the database,
                    //   but is faulty or the database itself is not accessible.
                    match &nats_message.reply {
                        Some(reply_subject) => {
                            match natsio_handler.acknowledge_raw(reply_subject).await {
                                Ok(_) => log::info!(
                                    "NotificationHandler message acknowledgement successful"
                                ),
                                Err(err) => log::info!(
                                    "NotificationHandler message acknowledgement failed: {err}"
                                ),
                            }
                        }
                        None => log::error!("Nats message "),
                    };
                }
            }
        });

        // Return ... something
        Ok(NotificationHandler {})
    }

    ///ToDo: Rust Doc
    async fn update_server_cache(
        message: MessageVariant,
        cache: Arc<Cache>,
        database: Arc<Database>,
        search_client: Arc<MeilisearchClient>,
    ) -> anyhow::Result<()> {
        match message {
            MessageVariant::ResourceEvent(event) => {
                process_resource_event(event, cache, database, search_client).await?
            }
            MessageVariant::UserEvent(event) => {
                process_user_event(event, cache, database).await?;
            }

            MessageVariant::AnnouncementEvent(event) => {
                process_announcement_event(event, cache, database).await?;
            }
        }

        Ok(())
    }
}

/* -------------- Helper -------------- */
async fn process_resource_event(
    resource_event: ResourceEvent,
    cache: Arc<Cache>,
    database: Arc<Database>,
    search_client: Arc<MeilisearchClient>,
) -> anyhow::Result<()> {
    if let Some(resource) = resource_event.resource {
        // Process cache
        if let Ok(variant) = EventVariant::try_from(resource_event.event_variant) {
            // Extract resource id to validate format
            let resource_ulid = DieselUlid::from_str(&resource.resource_id)?;

            // Update resource cache for all resources affected by the received notification
            for res_ulid in match variant {
                EventVariant::Unspecified => bail!("Unspecified event variant not allowed"),
                EventVariant::Created
                | EventVariant::Available
                | EventVariant::Updated
                | EventVariant::Deleted => vec![resource_ulid],
                EventVariant::Snapshotted => {
                    let client = database.get_client().await?;
                    let mut ids = vec![resource_ulid];
                    ids.append(
                        &mut Object::fetch_subresources_by_id(&resource_ulid, &client).await?,
                    );
                    ids
                }
            } {
                if let Some(object_plus) = cache.get_object(&res_ulid) {
                    // Convert to proto and compare checksum
                    let proto_resource: generic_resource::Resource =
                        object_plus.clone().try_into()?;
                    let proto_checksum = checksum_resource(proto_resource.clone())?;

                    if proto_checksum != resource.checksum {
                        // Update updated object in cache and search index
                        cache.upsert_object(&res_ulid, object_plus);

                        // Update resource search index
                        search_client
                            .add_or_update_stuff(
                                &[ObjectDocument::try_from(proto_resource)?],
                                MeilisearchIndexes::OBJECT,
                            )
                            .await?;
                    }
                } else {
                    // Fetch object with relations from database and put into cache
                    let client = database.get_client().await?;
                    let object_plus = Object::get_object_with_relations(&res_ulid, &client).await?;

                    // Update resource search index
                    search_client
                        .add_or_update_stuff(
                            &[ObjectDocument::from(object_plus.object.clone())],
                            MeilisearchIndexes::OBJECT,
                        )
                        .await?;

                    // Add to cache
                    cache.object_cache.insert(res_ulid, object_plus);
                }
            }
        } else {
            // Return error if variant is None
            bail!("Resource event variant missing")
        }
    } else {
        // Return error
        bail!("Resource event resource missing")
    }

    Ok(())
}

async fn process_user_event(
    user_event: UserEvent,
    cache: Arc<Cache>,
    database: Arc<Database>,
) -> anyhow::Result<()> {
    // Extract user id
    let user_ulid = DieselUlid::from_str(&user_event.user_id)?;

    // Process cache
    if let Ok(variant) = EventVariant::try_from(user_event.event_variant) {
        match variant {
            EventVariant::Unspecified => bail!("Unspecified user event variant not allowed"),
            EventVariant::Created | EventVariant::Updated | EventVariant::Available => {
                // Check if user already exists
                if let Some(user) = cache.get_user(&user_ulid) {
                    // Convert to proto and compare checksum
                    let proto_user = ApiUser::from(user.clone());
                    let proto_checksum = checksum_user(&proto_user)?;

                    if proto_checksum != user_event.checksum {
                        cache.update_user(&user_ulid, user);
                    }
                } else {
                    // Fetch user from database and add to cache
                    let client = database.get_client().await?;
                    if let Some(user) = User::get(user_ulid, &client).await? {
                        cache.add_user(user_ulid, user)
                    } else {
                        bail!("User does not exist")
                    }
                }
            }
            EventVariant::Deleted => cache.remove_user(&user_ulid),
            _ => {}
        }
    } else {
        // Return error if variant is None
        bail!("User event variant missing")
    }

    Ok(())
}

async fn process_announcement_event(
    announcement_event: AnnouncementEvent,
    cache: Arc<Cache>,
    database: Arc<Database>,
) -> anyhow::Result<()> {
    if let Some(variant) = announcement_event.event_variant {
        match variant {
            AnnEventVariant::NewDataProxyId(id) => {
                //Note: Endpoint cache currently not implemented
                log::info!("Received NewDataProxyId announcement for: {id}")
            }
            AnnEventVariant::RemoveDataProxyId(id) => {
                let endpoint_id = DieselUlid::from_str(&id)?;
                // Remove endpoint pubkeys from cache
                cache.remove_endpoint_pubkeys(&endpoint_id);
                // Remove endpoint from all users in cache
                cache.remove_endpoint_from_users(&endpoint_id);
                // Remove endpoint from all resources
                cache.remove_endpoint_from_users(&endpoint_id);
                //Note: No need to remove pubkeys from database here
                //      as they are cascade deleted with the endpoint
            }
            AnnEventVariant::UpdateDataProxyId(id) => {
                //Note: Endpoint cache currently not implemented
                log::info!("Received UpdateDataProxyId announcement for: {id}");
            }
            AnnEventVariant::NewPubkey(serial) => {
                // Fetch pubkey from database
                let client = database.get_client().await?;
                let pubkey = PubKey::get(serial, &client)
                    .await?
                    .ok_or_else(|| anyhow!("Could not find pub key"))?;
                let pub_pem = format!(
                    "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                    pubkey.pubkey
                );
                let decoding_key = DecodingKey::from_ed_pem(pub_pem.as_bytes())?;

                // Insert pubkey in cache
                let cache_pubkey = match pubkey.proxy {
                    Some(endpoint_id) => {
                        PubKeyEnum::DataProxy((pubkey.pubkey, decoding_key, endpoint_id))
                    }
                    None => PubKeyEnum::Server((pubkey.pubkey, decoding_key)),
                };

                cache.add_pubkey(pubkey.id as i32, cache_pubkey);
            }
            AnnEventVariant::RemovePubkey(serial) => cache.remove_pubkey(&serial),
            AnnEventVariant::Downtime(info) => {
                //ToDo: Implement downtime/shutdown preparation
                //  - Set instance status to something like `HealthStatus::Shutdown`
                //  - Do not accept any new requests
                log::info!("Received Downtime announcement: {:?}", info)
            }
            AnnEventVariant::Version(version) => {
                log::info!("Received Version announcement: {:?}", version)
            }
        }
    } else {
        // Return error if variant is None
        bail!("Announcement event variant missing")
    }

    Ok(())
}
