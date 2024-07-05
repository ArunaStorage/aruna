use aruna_rust_api::api::storage::services::v2::storage_status_service_server::StorageStatusService;
use aruna_rust_api::api::storage::services::v2::GetAnnouncementRequest;
use aruna_rust_api::api::storage::{
    models::v2::AnnouncementType,
    services::v2::{Announcement as ProtoAnnouncement, SetAnnouncementsRequest},
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use tonic::Request;

use crate::common::test_utils::ADMIN_OIDC_TOKEN;
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

#[tokio::test]
async fn get_announcement() {
    // Init StorageStatusService
    let info_service = init_storage_status_service().await;

    let set_request = SetAnnouncementsRequest {
        announcements_upsert: vec![ProtoAnnouncement {
            announcement_id: "".to_string(),
            announcement_type: AnnouncementType::Blog as i32,
            title: "gRPC get_announcement(1)".to_string(),
            teaser: "Some teaser".to_string(),
            image_url: "".to_string(),
            content: "".to_string(),
            created_by: "The Aruna Team".to_string(),
            created_at: None,
            modified_by: "The Aruna Team".to_string(),
            modified_at: None,
        }],
        announcements_delete: vec![],
    };

    // Insert announcement
    let grpc_request = add_token(Request::new(set_request), test_utils::ADMIN_OIDC_TOKEN);
    let inserted_announcements = info_service
        .set_announcements(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcements;
    let inserted_announcement = inserted_announcements.first().unwrap();

    // Get non-existent announcement
    let mut get_request = GetAnnouncementRequest {
        announcement_id: DieselUlid::generate().to_string(),
    };
    let grpc_request = Request::new(get_request.clone());

    assert!(info_service.get_announcement(grpc_request).await.is_err());

    // Get announcement without token
    get_request.announcement_id = inserted_announcement.announcement_id.to_string();
    let grpc_request = Request::new(get_request.clone());

    let get_announcement = info_service
        .get_announcement(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcement
        .unwrap();

    assert_eq!(inserted_announcement, &get_announcement);

    // Get announcement with token
    let get_request = GetAnnouncementRequest {
        announcement_id: inserted_announcement.announcement_id.to_string(),
    };
    let grpc_request = add_token(Request::new(get_request.clone()), ADMIN_OIDC_TOKEN);

    let get_announcement = info_service
        .get_announcement(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .announcement
        .unwrap();

    assert_eq!(inserted_announcement, &get_announcement);
}

#[tokio::test]
async fn get_announcements() {
    // Init StorageStatusService
    let service = init_storage_status_service().await;

    //TODO: Insert announcements

    //TODO: Get all announcements

    //TODO: Get all announcements paginated

    //TODO: Get announcements by id

    //TODO: Get announcements by id paginated

    todo!();
}

#[tokio::test]
async fn get_announcements_by_type() {
    // Init StorageStatusService
    let service = init_storage_status_service().await;

    //TODO: Insert announcements

    //TODO: Get announcements by type

    //TODO: Get announcements by type paginated

    todo!();
}
