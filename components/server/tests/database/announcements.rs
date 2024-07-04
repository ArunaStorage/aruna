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

