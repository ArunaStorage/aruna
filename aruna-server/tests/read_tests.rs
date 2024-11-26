mod common;

#[cfg(test)]
mod read_tests {

    use crate::common::{init_test, SECOND_TOKEN, TEST_TOKEN};
    use aruna_rust_api::v3::aruna::api::v3::{
        AddGroupRequest, CreateProjectRequest, CreateRealmRequest, GetRealmRequest, Realm,
    };
    use aruna_server::models::requests::{
        BatchResource, CreateGroupRequest, CreateGroupResponse,
        CreateProjectRequest as ModelsCreateProject, CreateProjectResponse,
        CreateResourceBatchRequest, CreateResourceBatchResponse, GetRealmsFromUserRequest,
        GetRealmsFromUserResponse, SearchResponse,
    };
    use ulid::Ulid;
    pub const OFFSET: u16 = 100;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_realm() {
        let mut clients = init_test(OFFSET).await;

        let create_request = CreateRealmRequest {
            tag: "test".to_string(),
            name: "TestRealm".to_string(),
            description: String::new(),
        };

        let response = clients
            .realm_client
            .create_realm(create_request.clone())
            .await
            .unwrap()
            .into_inner();

        let id = response.realm.unwrap().id;

        let request = GetRealmRequest { id };

        let realm = clients
            .realm_client
            .get_realm(request)
            .await
            .unwrap()
            .into_inner()
            .realm
            .unwrap();

        assert_eq!(&realm.name, &create_request.name);
        assert_eq!(&realm.tag, &create_request.tag);
        assert_eq!(&realm.description, &create_request.description);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search() {
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
        for i in 0..50 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("TestObjectNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    ..Default::default()
                });
            } else {
                if i % 2 != 0 {
                    resources.push(BatchResource {
                        name: format!("TestObjectNo{i}"),
                        parent: aruna_server::models::requests::Parent::Idx(i - 1),
                        visibility: aruna_server::models::models::VisibilityClass::Public,
                        ..Default::default()
                    });
                } else {
                    resources.push(BatchResource {
                        name: format!("TestObjectNo{i}"),
                        parent: aruna_server::models::requests::Parent::Idx(i - 1),
                        visibility: aruna_server::models::models::VisibilityClass::Private,
                        ..Default::default()
                    });
                }
            }
        }
        let request = CreateResourceBatchRequest { resources };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resource/batch", clients.rest_endpoint);

        let _response: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", TEST_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let url = format!("{}/api/v3/search", clients.rest_endpoint);

        let response: SearchResponse = client
            .get(url.clone())
            .query(&[("query", "TestObject")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.expected_hits, 25);

        let response: SearchResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", TEST_TOKEN))
            .query(&[("query", "TestObject")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.expected_hits, 50);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_search_permissions() {
        //
        // Setup for search permissions
        //
        let mut clients = init_test(OFFSET).await;
        let client = reqwest::Client::new();

        // 1. Create realm
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

        // 2. Create project for user 1
        let request = CreateProjectRequest {
            name: "TestProject".to_string(),
            group_id: response.admin_group_id,
            realm_id: realm_id.clone(),
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

        // 3. Create resources for user 1
        let mut resources = Vec::new();
        for i in 0..5 {
            resources.push(BatchResource {
                name: format!("PUBLIC_TEST_U1_{i}"),
                parent: aruna_server::models::requests::Parent::ID(parent_id),
                visibility: aruna_server::models::models::VisibilityClass::Public,
                ..Default::default()
            });
        }
        for i in 5..10 {
            resources.push(BatchResource {
                name: format!("PRIVATE_TEST_U1_{i}"),
                parent: aruna_server::models::requests::Parent::ID(parent_id),
                visibility: aruna_server::models::models::VisibilityClass::Private,
                ..Default::default()
            });
        }
        let request = CreateResourceBatchRequest { resources };
        let url = format!("{}/api/v3/resource/batch", clients.rest_endpoint);
        let _batch_response1: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", TEST_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // 5.Create resources for user 2
        // 5.1. Create group
        let request = CreateGroupRequest {
            name: "SecondGroup".to_string(),
            description: String::new(),
        };
        let url = format!("{}/api/v3/group", clients.rest_endpoint);
        let response: CreateGroupResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", SECOND_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // 5.2. Add group to realm
        let request = AddGroupRequest {
            realm_id: realm_id.clone(),
            group_id: response.group.id.to_string(),
        };
        let _response = clients
            .realm_client
            .add_group(request)
            .await
            .unwrap()
            .into_inner();

        // 5.3 Create Project
        let request = ModelsCreateProject {
            name: "TestProjectNo2".to_string(),
            group_id: response.group.id,
            realm_id: Ulid::from_string(&realm_id).unwrap(),
            visibility: aruna_server::models::models::VisibilityClass::Public,
            ..Default::default()
        };
        let url = format!("{}/api/v3/resource/project", clients.rest_endpoint);
        let response: CreateProjectResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", SECOND_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        // 5.4. Create resources for user 1
        let mut resources = Vec::new();
        for i in 0..5 {
            resources.push(BatchResource {
                name: format!("PUBLIC_TEST_U2_{i}"),
                parent: aruna_server::models::requests::Parent::ID(response.resource.id),
                visibility: aruna_server::models::models::VisibilityClass::Public,
                ..Default::default()
            });
        }
        for i in 5..10 {
            resources.push(BatchResource {
                name: format!("PRIVATE_TEST_U2_{i}"),
                parent: aruna_server::models::requests::Parent::ID(response.resource.id),
                visibility: aruna_server::models::models::VisibilityClass::Private,
                ..Default::default()
            });
        }
        let request = CreateResourceBatchRequest { resources };
        let url = format!("{}/api/v3/resource/batch", clients.rest_endpoint);
        let _batch_response2: CreateResourceBatchResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", SECOND_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        //
        // Test search
        //
        let url = format!("{}/api/v3/search", clients.rest_endpoint);

        let response: SearchResponse = client
            .get(url.clone())
            .query(&[("query", "PUBLIC")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.expected_hits, 12);

        let response: SearchResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", TEST_TOKEN))
            .query(&[("query", "PRIVATE")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(&response.expected_hits, &5);
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => n.name.contains("U1"),
                _ => false,
            }
        }));
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => !n.name.contains("U2"),
                _ => false,
            }
        }));

        let url = format!("{}/api/v3/search", clients.rest_endpoint);
        let response: SearchResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", SECOND_TOKEN))
            .query(&[("query", "PRIVATE")])
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(&response.expected_hits, &5);
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => !n.name.contains("U1"),
                _ => false,
            }
        }));
        assert!(response.resources.iter().all(|r| {
            match r {
                aruna_server::models::models::GenericNode::Resource(n) => n.name.contains("U2"),
                _ => false,
            }
        }))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_user_realms() {
        let mut clients = init_test(OFFSET).await;

        let mut ids = Vec::new();
        for i in 0..5 {
            let response = clients
                .realm_client
                .create_realm(CreateRealmRequest {
                    tag: format!("test_user_realm{i}"),
                    name: format!("TestUserRealm{i}"),
                    description: String::new(),
                })
                .await
                .unwrap()
                .into_inner();
            ids.push(Ulid::from_string(&response.realm.unwrap().id).unwrap());
        }

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/user/realm", clients.rest_endpoint);

        let response: GetRealmsFromUserResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", TEST_TOKEN))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        for realm in response.realms {
            assert!(ids.contains(&realm.id))
        }
    }
}
