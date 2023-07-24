use aruna_server::database::{crud::CrudDb, notification_dsl::StreamConsumer};
use async_nats::jetstream::consumer::Config;
use diesel_ulid::DieselUlid;

mod init_db;

#[tokio::test]
async fn create_stream_consumer() {
    // Init database connection
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define stream consumer
    let stream_consumer = StreamConsumer {
        id: DieselUlid::generate(),
        user_id: None,
        config: postgres_types::Json(Config {
            name: Some("some_consumer".to_string()),
            durable_name: Some("some_consumer".to_string()),
            filter_subject: "some_subject".to_string(),
            ..Default::default()
        }),
    };

    // Persist stream consumer in database
    stream_consumer.create(&client).await.unwrap();

    // Validate stream consumer creation
    if let Some(created_consumer) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        assert_eq! {created_consumer.id, stream_consumer.id};
        assert_eq! {created_consumer.user_id, stream_consumer.user_id};
        assert_eq! {created_consumer.config, stream_consumer.config};
    } else {
        panic!("StreamConsumer should exist.")
    }
}

#[tokio::test]
async fn get_stream_consumer() {
    // Init database connection
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define stream consumer
    let stream_consumer = StreamConsumer {
        id: DieselUlid::generate(),
        user_id: None,
        config: postgres_types::Json(Config {
            name: Some("some_consumer".to_string()),
            durable_name: Some("some_consumer".to_string()),
            filter_subject: "some_subject".to_string(),
            ..Default::default()
        }),
    };

    // Persist stream consumer in database
    stream_consumer.create(&client).await.unwrap();

    // Fetch stream consumer from database and validate equality
    if let Some(get_consumer) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        assert_eq! {get_consumer, stream_consumer}
    } else {
        panic!("StreamConsumer should exist.")
    }
}

#[tokio::test]
async fn delete_stream_consumer() {
    // Init database connection
    let db = crate::init_db::init_db().await;
    let client = db.get_client().await.unwrap();

    // Define stream consumer
    let stream_consumer = StreamConsumer {
        id: DieselUlid::generate(),
        user_id: None,
        config: postgres_types::Json(Config {
            name: Some("some_consumer".to_string()),
            durable_name: Some("some_consumer".to_string()),
            filter_subject: "some_subject".to_string(),
            ..Default::default()
        }),
    };

    // Persist stream consumer in database
    stream_consumer.create(&client).await.unwrap();

    // Fetch stream consumer from database and validate equality
    if let Some(get_consumer) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        assert_eq! {get_consumer, stream_consumer}
    } else {
        panic!("StreamConsumer should exist.")
    }

    // Delete stream consumer
    stream_consumer.delete(&client).await.unwrap();

    // Try to fetch deleted stream consumer
    if let Some(_) = StreamConsumer::get(stream_consumer.id, &client)
        .await
        .unwrap()
    {
        panic!("StreamConsumer shouldn't exist.")
    }
}
