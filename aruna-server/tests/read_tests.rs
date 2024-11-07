mod common;

#[cfg(test)]
mod read_tests {

    use crate::common::{init_test, TEST_TOKEN};
    use aruna_rust_api::v3::aruna::api::v3::{
        CreateProjectRequest, CreateRealmRequest, GetRealmRequest, Realm as GrpcRealm,
    };
    use aruna_server::models::requests::{
        BatchResource, CreateResourceBatchRequest, CreateResourceBatchResponse, SearchResponse,
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
        let GrpcRealm { id: realm_id, .. } = response.realm.unwrap();

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

        let client = reqwest::Client::new();
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
}
