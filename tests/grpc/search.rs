use std::str::FromStr;

use aruna_rust_api::api::storage::services::v2::{
    search_service_server::SearchService, user_service_server::UserService,
    GetPersonalNotificationsRequest, PersonalNotificationVariant, ReferenceType, References,
    RequestResourceAccessRequest,
};
use aruna_server::database::enums::ObjectType;
use diesel_ulid::DieselUlid;

use crate::common::{
    init::init_service_block,
    test_utils::{
        add_token, fast_track_grpc_project_create, USER1_OIDC_TOKEN, USER1_ULID, USER2_OIDC_TOKEN,
        USER2_ULID,
    },
};

#[tokio::test]
async fn grpc_request_access() {
    // Init gRPC services
    let service_block = init_service_block().await;

    // Create random project
    let project =
        fast_track_grpc_project_create(&service_block.project_service, USER1_OIDC_TOKEN).await;

    let project_ulid = DieselUlid::from_str(&project.id).unwrap();
    let user2_ulid = DieselUlid::from_str(&USER2_ULID.to_string()).unwrap();

    // Request access of non-existing resource
    let mut inner_request = RequestResourceAccessRequest {
        resource_id: DieselUlid::generate().to_string(),
    };

    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    let response = service_block
        .search_service
        .request_resource_access(grpc_request)
        .await;

    assert!(response.is_err());

    // Request access of without token
    inner_request.resource_id = project_ulid.to_string();

    let grpc_request = tonic::Request::new(inner_request.clone());

    let response = service_block
        .search_service
        .request_resource_access(grpc_request)
        .await;

    assert!(response.is_err());

    // Request access of resource
    let grpc_request = add_token(tonic::Request::new(inner_request.clone()), USER2_OIDC_TOKEN);

    service_block
        .search_service
        .request_resource_access(grpc_request)
        .await
        .unwrap();

    // Validate personal notification of user1
    let grpc_request = add_token(
        tonic::Request::new(GetPersonalNotificationsRequest {
            user_id: USER1_ULID.to_string(),
        }),
        USER1_OIDC_TOKEN,
    );

    let notifications = service_block
        .user_service
        .get_personal_notifications(grpc_request)
        .await
        .unwrap()
        .into_inner()
        .notifications;

    let user2 = service_block.cache.get_user(&user2_ulid).unwrap();

    // Have to loop all notifications as id is unknown
    let mut validated = false;
    for notification in &notifications {
        if notification.message
            == format!(
                "{} ({}) requests access for {:?} ({})",
                user2.display_name,
                user2.id,
                ObjectType::PROJECT,
                project_ulid.to_string()
            )
        {
            assert_eq!(
                notification.variant,
                PersonalNotificationVariant::AccessRequested as i32
            );
            assert!(notification.refs.contains(&References {
                ref_type: ReferenceType::Resource as i32,
                ref_name: project.name.clone(),
                ref_value: project_ulid.to_string()
            }));
            assert!(notification.refs.contains(&References {
                ref_type: ReferenceType::User as i32,
                ref_name: "another-test-user".to_string(),
                ref_value: USER2_ULID.to_string()
            }));
            validated = true;
            break;
        }
    }
    if !validated {
        panic!("Revoked permission personal notification does not exist")
    }
}
