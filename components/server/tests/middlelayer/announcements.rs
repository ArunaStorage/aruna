use aruna_rust_api::api::storage::{
    models::v2::PageRequest,
    services::v2::{Announcement as ProtoAnnouncement, GetAnnouncementsRequest},
};
use aruna_server::database::dsls::info_dsl::Announcement;
use diesel_ulid::DieselUlid;
use itertools::Itertools;

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

#[tokio::test]
async fn get_announcements() {
    // Init middleware database handler
    let db_handler = init_database_handler_middlelayer().await;
    let client = db_handler.database.get_client().await.unwrap();

    // Create some announcements
    let ann_futures = (0..5)
        .into_iter()
        .map(|_| async {
            Announcement {
                id: DieselUlid::generate(),
                announcement_type: "ORGA".to_string(),
                title: "Announcement Title".to_string(),
                teaser: "Announcement Teaser".to_string(),
                image_url: "https://announcement_image_url/{}.webp".to_string(),
                content: "Announcement Content {}".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: chrono::Utc::now().naive_local(),
                modified_by: "The Aruna Team".to_string(),
                modified_at: chrono::Utc::now().naive_local(),
            }
            .upsert(&client)
            .await
            .unwrap()
        })
        .collect_vec();
    let announcements = futures::future::join_all(ann_futures).await;

    let mut request = GetAnnouncementsRequest {
        announcement_ids: vec![],
        page: None,
    };

    // Get all announcements
    let all_announcements = db_handler.get_announcements(request.clone()).await.unwrap();
    assert!(all_announcements.len() >= announcements.len());

    for a in &announcements {
        assert!(all_announcements.contains(&ProtoAnnouncement::try_from(a.clone()).unwrap()))
    }

    // Get first, third and last of the created announcements by id
    request.announcement_ids = vec![
        announcements.first().unwrap().id.to_string(),
        announcements.get(2).unwrap().id.to_string(),
        announcements.last().unwrap().id.to_string(),
    ];
    let anns = db_handler.get_announcements(request.clone()).await.unwrap();
    assert_eq!(anns.len(), 3);

    // Get pages with two announcements
    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 2,
    };
    request.announcement_ids = vec![];
    request.page = Some(page.clone());

    let page_01 = db_handler.get_announcements(request.clone()).await.unwrap();
    assert_eq!(page_01.as_slice(), &all_announcements[..2]);

    page.start_after = page_01.last().unwrap().announcement_id.clone();
    request.page = Some(page);
    let page_02 = db_handler.get_announcements(request.clone()).await.unwrap();
    assert_eq!(page_02.as_slice(), &all_announcements[2..4]);
}

#[tokio::test]
async fn get_announcements_by_type() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;

    todo!()
}

#[tokio::test]
async fn set_announcements() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;

    todo!()
}
