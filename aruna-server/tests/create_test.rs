pub mod common;

#[cfg(test)]
mod create_tests {
    use crate::common::{init_test, ADMIN_TOKEN};
    use aruna_rust_api::v3::aruna::api::v3::{
        CreateGroupRequest, CreateProjectRequest, CreateRealmRequest, CreateResourceRequest, Realm,
    };
    use aruna_server::models::requests::{
        BatchResource, CreateResourceBatchRequest, CreateResourceBatchResponse,
    };
    use ulid::Ulid;
    pub const OFFSET: u16 = 0;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_realm() {
        let mut clients = init_test(OFFSET).await;

        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };

        let response = clients
            .realm_client
            .create_realm(request.clone())
            .await
            .unwrap()
            .into_inner();

        let realm = response.realm.unwrap();

        assert_eq!(&realm.name, &request.name);
        assert_eq!(&realm.tag, &request.tag);
        assert_eq!(&realm.description, &request.description);

        let request = CreateRealmRequest {
            // Same tag
            tag: "test".to_string(),
            name: "SecondTestRealm".to_string(),
            description: String::new(),
        };

        // Tags must be unique
        assert!(clients
            .realm_client
            .create_realm(request.clone())
            .await
            .is_err())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_group() {
        let mut clients = init_test(OFFSET).await;

        let request = CreateGroupRequest {
            name: "TestGroup".to_string(),
            description: String::new(),
        };

        let response = clients
            .group_client
            .create_group(request.clone())
            .await
            .unwrap()
            .into_inner();

        let group = response.group.unwrap();

        assert_eq!(&group.name, &request.name);
        assert_eq!(&group.description, &request.description);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_project() {
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let response = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap();

        assert_eq!(response.name, request.name);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_resource() {
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap()
            .id;

        // Create resource
        let request = CreateResourceRequest {
            name: "TestResource".to_string(),
            parent_id,
            visibility: 1,
            variant: 2,
            ..Default::default()
        };
        let resource = clients
            .resource_client
            .create_resource(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap();

        assert_eq!(request.name, resource.name);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_resource_batch() {
        // Setup
        let mut clients = init_test(OFFSET).await;

        // Create realm
        let request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };
        let response = clients
            .realm_client
            .create_realm(request)
            .await
            .unwrap()
            .into_inner();
        let Realm { id: realm_id, .. } = response.realm.unwrap();

        // Create project
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id,
            visibility: 1,
            ..Default::default()
        };
        let parent_id = Ulid::from_string(
            &clients
                .resource_client
                .create_project(request.clone())
                .await
                .unwrap()
                .into_inner()
                .resource
                .unwrap()
                .id,
        )
        .unwrap();

        // Check if linking works
        let mut resources = Vec::new();
        for i in 0..1000 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("TestObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    ..Default::default()
                });
            } else {
                resources.push(BatchResource {
                    name: format!("TestObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::Idx(i - 1),
                    ..Default::default()
                });
            }
        }
        let request = CreateResourceBatchRequest { resources };

        //dbg!(&request);

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resource/batch", clients.rest_endpoint);

        let response: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resources.len(), 1000);

        // Check if linking fails when not correctly chaining parents
        let mut resources = Vec::new();
        for i in 0..1000 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("Test2ObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    ..Default::default()
                });
            } else {
                resources.push(BatchResource {
                    name: format!("Test2ObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::Idx(i + 1),
                    ..Default::default()
                });
            }
        }
        let request = CreateResourceBatchRequest { resources };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resource/batch", clients.rest_endpoint);

        assert!(client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .error_for_status()
            .is_err())
    }

    // #[tokio::test(flavor = "multi_thread")]
    // async fn create_user() {
    //     // Setup
    //     let clients = init_test(OFFSET).await;

    //     // Create realm
    //     let request = RegisterUserRequest {
    //         first_name: "user2".to_string(),
    //         last_name: "user2".to_string(),
    //         email: "user2@test.org".to_string(),
    //         identifier: String::new(),
    //     };

    //     let client = reqwest::Client::new();
    //     let url = format!("{}/api/v3/user", clients.rest_endpoint);

    //     let response: RegisterUserResponse = client
    //         .post(url)
    //         .header("Authorization", format!("Bearer {}", TEST_TOKEN))
    //         .json(&request)
    //         .send()
    //         .await
    //         .unwrap()
    //         .json()
    //         .await
    //         .unwrap();

    //     dbg!(&response);

    //     let request = CreateTokenRequest {
    //         user_id: response.user.id,
    //         name: "Second token".to_string(),
    //         expires_at: None,
    //     };

    //     let url = format!("{}/api/v3/token", clients.rest_endpoint);
    //     let response: CreateTokenResponse = client
    //         .post(url)
    //         .header("Authorization", format!("Bearer {}", TEST_TOKEN))
    //         .json(&request)
    //         .send()
    //         .await
    //         .unwrap()
    //         .json()
    //         .await
    //         .unwrap();

    //     dbg!(&response);
    // }
}
