use std::str::FromStr;

use aruna_rust_api::api::storage::{
    models::v2::{AnnouncementType, PageRequest},
    services::v2::{
        Announcement as ProtoAnnouncement, GetAnnouncementsByTypeRequest, GetAnnouncementsRequest,
        SetAnnouncementsRequest,
    },
};
use aruna_server::database::dsls::info_dsl::Announcement;
use diesel_ulid::DieselUlid;
use itertools::Itertools;

use crate::common::{init::init_database_handler_middlelayer, test_utils};

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
    let client = db_handler.database.get_client().await.unwrap();

    let mut types = vec![
        "ORGA".to_string(),
        "ORGA".to_string(),
        "RELEASE".to_string(),
        "RELEASE".to_string(),
    ];

    let mut announcements = vec![];
    while let Some(a_type) = test_utils::choose_and_remove(&mut types) {
        announcements.push(
            Announcement {
                id: DieselUlid::generate(),
                announcement_type: a_type.to_string(),
                title: format!("Announcement Title {}", announcements.len() + 1),
                teaser: format!("Announcement Teaser {}", announcements.len() + 1),
                image_url: format!(
                    "https://announcement_image_url/{}.webp",
                    announcements.len() + 1
                ),
                content: format!("Announcement Content {}", announcements.len() + 1),
                created_by: "The Aruna Team".to_string(),
                created_at: chrono::Utc::now().naive_local(),
                modified_by: "The Aruna Team".to_string(),
                modified_at: chrono::Utc::now().naive_local(),
            }
            .upsert(&client)
            .await
            .unwrap(),
        )
    }

    // Get announcements by type
    let mut request = GetAnnouncementsByTypeRequest {
        announcement_type: AnnouncementType::Orga as i32,
        page: None,
    };
    let typed_anns = db_handler
        .get_announcements_by_type(request.clone())
        .await
        .unwrap();

    assert!(typed_anns.len() >= 2);
    for ta in &typed_anns {
        assert_eq!(ta.announcement_type(), AnnouncementType::Orga)
    }

    // Get announcements by type paginated
    let mut page = PageRequest {
        start_after: "".to_string(),
        page_size: 1,
    };
    request.page = Some(page.clone());

    let page_01 = db_handler
        .get_announcements_by_type(request.clone())
        .await
        .unwrap();
    assert_eq!(page_01.len(), 1);
    assert_eq!(
        page_01.first().unwrap().announcement_type,
        AnnouncementType::Orga as i32
    );

    page.start_after = page_01.first().unwrap().announcement_id.clone();
    request.page = Some(page);
    let page_02 = db_handler
        .get_announcements_by_type(request.clone())
        .await
        .unwrap();
    assert_eq!(page_02.len(), 1);
    assert_eq!(
        page_02.first().unwrap().announcement_type,
        AnnouncementType::Orga as i32
    );
    assert_ne!(
        page_01.first().unwrap().announcement_id,
        page_02.first().unwrap().announcement_id
    );
}

#[tokio::test]
async fn set_announcements() {
    // Init
    let db_handler = init_database_handler_middlelayer().await;

    todo!()
}
