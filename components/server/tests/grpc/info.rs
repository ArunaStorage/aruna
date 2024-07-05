use aruna_rust_api::api::storage::services::v2::storage_status_service_server::StorageStatusService;
use aruna_rust_api::api::storage::{
    models::v2::AnnouncementType,
    services::v2::{Announcement as ProtoAnnouncement, SetAnnouncementsRequest},
};
use itertools::Itertools;
use tonic::Request;

use crate::common::{
    init::init_storage_status_service,
    test_utils::{self, add_token},
};

#[tokio::test]
async fn set_announcement() {
    // Init StorageStatusService
    let info_service = init_storage_status_service().await;

    let mut inner_request = SetAnnouncementsRequest {
        announcements_upsert: vec![
            ProtoAnnouncement {
                announcement_id: "".to_string(),
                announcement_type: AnnouncementType::Release as i32,
                title: "gRPC set_announcement(1)".to_string(),
                teaser: "Some teaser".to_string(),
                image_url: "".to_string(),
                content: "".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: None,
                modified_by: "The Aruna Team".to_string(),
                modified_at: None,
            },
            ProtoAnnouncement {
                announcement_id: "".to_string(),
                announcement_type: AnnouncementType::Blog as i32,
                title: "gRPC set_announcement(2)".to_string(),
                teaser: "Some teaser".to_string(),
                image_url: "".to_string(),
                content: "".to_string(),
                created_by: "The Aruna Team".to_string(),
                created_at: None,
                modified_by: "The Aruna Team".to_string(),
                modified_at: None,
            },
        ],
        announcements_delete: vec![],
    };

    // Set announcements without token
    let grpc_request = Request::new(inner_request.clone());
    assert!(info_service.set_announcements(grpc_request).await.is_err());

    // Set announcements with invalid token
    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::INVALID_OIDC_TOKEN,
    );
    assert!(info_service.set_announcements(grpc_request).await.is_err());

    // Set announcements without sufficient permissions
    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::USER1_OIDC_TOKEN,
    );
    assert!(info_service.set_announcements(grpc_request).await.is_err());

    // Set announcements
    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::ADMIN_OIDC_TOKEN,
    );
    let inserted_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    //TODO: Update announcements
    let mut update_announcement = inserted_announcements.first().unwrap().clone();
    update_announcement.announcement_type = AnnouncementType::Update as i32;
    update_announcement.content = "Lorem Ipsum Dolor".to_string();
    inner_request.announcements_upsert = vec![update_announcement];

    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::ADMIN_OIDC_TOKEN,
    );

    let updated_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;

    let original = inserted_announcements.first().unwrap();
    let updated = updated_announcements.first().unwrap();

    assert_eq!(original.announcement_id, updated.announcement_id);
    assert_ne!(original.announcement_type, updated.announcement_type);
    assert_ne!(original.content, updated.content);
    assert_ne!(original.modified_at, updated.modified_at);

    // Delete announcements
    let delete_ids = inserted_announcements
        .iter()
        .map(|a| a.announcement_id.clone())
        .collect_vec();
    inner_request.announcements_upsert = vec![];
    inner_request.announcements_delete = delete_ids;

    let grpc_request = add_token(
        Request::new(inner_request.clone()),
        test_utils::ADMIN_OIDC_TOKEN,
    );

    info_service.set_announcements(grpc_request).await.unwrap();
}

