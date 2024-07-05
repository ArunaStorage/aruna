use aruna_rust_api::api::storage::services::v2::Announcement as ProtoAnnouncement;
use aruna_server::database::dsls::info_dsl::Announcement;
use diesel_ulid::DieselUlid;

use crate::common::init::init_database_handler_middlelayer;

#[tokio::test]
async fn get_announcement() {
    // Init middleware database handler
    let db_handler = init_database_handler_middlelayer().await;
    let client = db_handler.database.get_client().await.unwrap();

    // Create an announcement in the database
    let original = Announcement {
        id: DieselUlid::generate(),
        announcement_type: "RELEASE".to_string(),
        title: "Announcement Title".to_string(),
        teaser: "Announcement Teaser".to_string(),
        image_url: format!("https://announcement_image_url/dummy.webp"),
        content: "Announcement Content".to_string(),
        created_by: "The Aruna Team".to_string(),
        created_at: chrono::Utc::now().naive_local(),
        modified_by: "The Aruna Team".to_string(),
        modified_at: chrono::Utc::now().naive_local(),
    }
    .upsert(&client)
    .await
    .unwrap();

    // Error: get non-existent announcement
    assert!(db_handler.get_announcement(original.id).await.is_err());

    // Get the created announcement
    let get_ann = db_handler.get_announcement(original.id).await.unwrap();

    assert_eq!(ProtoAnnouncement::try_from(original).unwrap(), get_ann)
}

