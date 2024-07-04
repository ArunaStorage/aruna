use aruna_server::database::{crud::CrudDb, dsls::info_dsl::Announcement};
use diesel_ulid::DieselUlid;
use rand::seq::SliceRandom;
use tokio_postgres::GenericClient;

use crate::common::init;

#[tokio::test]
async fn create_test() {
    let db = init::init_database().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Invalid announcement type
    let mut ann = Announcement {
        id: DieselUlid::generate(),
        announcement_type: "INVALID".to_string(),
        title: "Announcement Title".to_string(),
        teaser: "Announcement Teaser".to_string(),
        image_url: format!("https://announcement_image_url/dummy.webp"),
        content: "Announcement Content".to_string(),
        created_by: "The Aruna Team".to_string(),
        created_at: chrono::Utc::now().naive_local(),
        modified_by: "The Aruna Team".to_string(),
        modified_at: chrono::Utc::now().naive_local(),
    };
    let res = ann.create(client).await;

    assert!(res.is_err());

    ann.announcement_type = "RELEASE".to_string();
    ann.create(client).await.unwrap();

    assert_eq!(
        ann,
        Announcement::get(ann.id, client).await.unwrap().unwrap()
    )
}

#[tokio::test]
async fn upsert_test() {
    let db = init::init_database().await;
    let client = db.get_client().await.unwrap();
    let client = client.client();

    // Invalid announcement type
    let mut ann = Announcement {
        id: DieselUlid::generate(),
        announcement_type: "ORGA".to_string(),
        title: "Original Announcement Title".to_string(),
        teaser: "Original Announcement Teaser".to_string(),
        image_url: format!("https://announcement_image_url/example.webp"),
        content: "Original Announcement Content".to_string(),
        created_by: "The Aruna Team".to_string(),
        created_at: chrono::Utc::now().naive_local(),
        modified_by: "The Aruna Team".to_string(),
        modified_at: chrono::Utc::now().naive_local(),
    };
    ann.create(client).await.unwrap();

    // Try to set invalid and valid announcement type 
    ann.announcement_type = "INVALID".to_string();
    assert!(ann.create(client).await.is_err());
    ann.announcement_type = "BLOG".to_string();

    let type_updated = ann.upsert(client).await.unwrap();

    assert_eq!(ann.id, type_updated.id);
    assert_eq!(&type_updated.announcement_type, "BLOG");

    // Update announcement title
    ann.title = "Updated Announcement Title".to_string();
    let title_updated = ann.upsert(client).await.unwrap();

    assert_eq!(ann.id, title_updated.id);
    assert_eq!(&title_updated.title, "Updated Announcement Title");

    // Update announcement teaser and content together
    ann.teaser = "Updated Announcement Teaser".to_string();
    ann.content = "Updated Announcement Content".to_string();

    let teaser_content_updated = ann.upsert(client).await.unwrap();

    assert_eq!(ann.id, teaser_content_updated.id);
    assert_eq!(&teaser_content_updated.teaser, "Updated Announcement Teaser");
    assert_eq!(&teaser_content_updated.content, "Updated Announcement Content");
}
