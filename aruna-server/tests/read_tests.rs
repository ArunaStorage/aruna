mod common;

#[cfg(test)]
mod read_tests {

    use crate::common::init_test;
    use aruna_rust_api::v3::aruna::api::v3::{CreateRealmRequest, GetRealmRequest};
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

        let request = GetRealmRequest {
            id,
        };

        let realm = clients
            .realm_client
            .get_realm(request)
            .await
            .unwrap()
            .into_inner().realm.unwrap();

        assert_eq!(&realm.name, &create_request.name);
        assert_eq!(&realm.tag, &create_request.tag);
        assert_eq!(&realm.description, &create_request.description);
    }
}
