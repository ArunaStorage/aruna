pub mod common;

#[cfg(test)]
mod create_tests {

    use crate::common::init_test;
    use aruna_rust_api::v3::aruna::api::v3::{CreateGroupRequest, CreateProjectRequest, CreateRealmRequest, CreateResourceRequest, Realm};
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
            .. Default::default()
        };
        let response = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner().resource.unwrap();

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
            .. Default::default()
        };
        let parent_id = clients
            .resource_client
            .create_project(request.clone())
            .await
            .unwrap()
            .into_inner().resource.unwrap().id;

        // Create resource
        let request = CreateResourceRequest {
            name: "TestResource".to_string(),
            parent_id,
            visibility: 1,
            variant: 2, 
            .. Default::default()
        };
        let resource = clients
            .resource_client
            .create_resource(request.clone())
            .await
            .unwrap().into_inner().resource.unwrap();

        assert_eq!(request.name, resource.name);
         
    }
}
