mod common;

#[cfg(test)]
mod delete_tests {

    use aruna_rust_api::v3::aruna::api::v3::{CreateProjectRequest, CreateRealmRequest, Realm};
    use aruna_server::models::{
        models::Resource,
        requests::{
            BatchResource, CreateResourceBatchRequest, CreateResourceBatchResponse, DeleteResponse,
            GetResourcesResponse,
        },
    };
    use ulid::Ulid;

    use crate::common::{init_test, ADMIN_TOKEN};
    pub const OFFSET: u16 = 300;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_delete() {
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

        // Create batch resources
        let mut resources = Vec::new();
        for i in 0..999 {
            if i == 0 {
                resources.push(BatchResource {
                    name: format!("TestFolderNo{i}"),
                    parent: aruna_server::models::requests::Parent::ID(parent_id),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            } else {
                resources.push(BatchResource {
                    name: format!("TestFolderNo{i}"),
                    parent: aruna_server::models::requests::Parent::Idx(i - 1),
                    variant: aruna_server::models::models::ResourceVariant::Folder,
                    ..Default::default()
                });
            }
        }
        resources.push(BatchResource {
            name: format!("TestObject"),
            parent: aruna_server::models::requests::Parent::ID(parent_id),
            variant: aruna_server::models::models::ResourceVariant::Object,
            ..Default::default()
        });
        let request = CreateResourceBatchRequest { resources };

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources/batch", clients.rest_endpoint);
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
        let all_ids = response
            .resources
            .iter()
            .map(|res| ("ids".to_string(), res.id.to_string()))
            .collect::<Vec<(String, String)>>();

        let url = format!(
            "{}/api/v3/resources/{}",
            clients.rest_endpoint,
            parent_id.to_string()
        );
        let _response: DeleteResponse = client
            .delete(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .json(&request)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        let client = reqwest::Client::new();
        let url = format!("{}/api/v3/resources", clients.rest_endpoint);
        let response: GetResourcesResponse = client
            .get(url)
            .header("Authorization", format!("Bearer {}", ADMIN_TOKEN))
            .query(&all_ids)
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();

        assert!(response.resources.iter().all(|res| res
            == &Resource {
                id: res.id,
                variant: res.variant.clone(),
                deleted: true,
                ..Default::default()
            }))
    }
}
