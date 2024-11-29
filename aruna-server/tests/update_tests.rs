mod common;

#[cfg(test)]
mod update_tests {

    use aruna_rust_api::v3::aruna::api::v3::{
        Author, CreateProjectRequest, CreateRealmRequest, CreateResourceRequest, KeyValue, Realm,
    };
    use aruna_server::models::requests::{
        UpdateResourceNameRequest, UpdateResourceNameResponse, UpdateResourceTitleRequest,
        UpdateResourceTitleResponse,
    };
    use ulid::Ulid;

    use crate::common::{init_test, ADMIN_TOKEN};
    pub const OFFSET: u16 = 200;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_updates() {
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
        let project_id = Ulid::from_string(&parent_id).unwrap();

        // Create resource
        let request = CreateResourceRequest {
            name: "TestResource".to_string(),
            parent_id,
            visibility: 1,
            variant: 2,
            title: "TitleTest".to_string(),
            description: "DescriptionTest".to_string(),
            labels: vec![
                KeyValue {
                    key: "test".to_string(),
                    value: "value".to_string(),
                    locked: false,
                },
                KeyValue {
                    key: "another_test".to_string(),
                    value: "another_value".to_string(),
                    locked: false,
                },
            ],
            identifiers: vec!["ORCID".to_string(), "WHATEVER".to_string()],
            authors: vec![
                Author {
                    id: "01JDVSKJPCMEBWZNG8PYKZEBFQ".to_string(),
                    first_name: "Jane".to_string(),
                    last_name: "Doe".to_string(),
                    email: "converge@test.org".to_string(),
                    orcid: "THIS_IS_AN_ORCID".to_string(),
                },
                Author {
                    id: "01JDVSKTFKR4J1DZQT11231R6Q".to_string(),
                    first_name: "John".to_string(),
                    last_name: "Doe".to_string(),
                    email: "test@test.org".to_string(),
                    orcid: "THIS_IS_ANOTHER_ORCID".to_string(),
                },
            ],
            license_tag: "CC0".to_string(),
        };
        let resource = clients
            .resource_client
            .create_resource(request.clone())
            .await
            .unwrap()
            .into_inner()
            .resource
            .unwrap();
        let resource_id = Ulid::from_string(&resource.id).unwrap();
        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/name", clients.rest_endpoint);
        let update_name = UpdateResourceNameRequest {
            id: resource_id,
            name: "ReleaseName".to_string(),
        };
        let response: UpdateResourceNameResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_name)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.name, update_name.name);

        let url = format!("{}/api/v3/resources/title", clients.rest_endpoint);
        let update_name = UpdateResourceTitleRequest {
            id: resource_id,
            title: "ReleaseTitle".to_string(),
        };
        let response: UpdateResourceTitleResponse = client
            .post(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&update_name)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(response.resource.title, update_name.title);
    }
}
