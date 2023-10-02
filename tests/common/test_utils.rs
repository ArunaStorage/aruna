use aruna_rust_api::api::storage::{
    models::v2::{Collection, DataClass as ApiDataClass, Dataset, PermissionLevel, Project},
    services::v2::{
        authorization_service_server::AuthorizationService,
        collection_service_server::CollectionService, create_collection_request,
        create_dataset_request, dataset_service_server::DatasetService,
        project_service_server::ProjectService, CreateAuthorizationRequest,
        CreateCollectionRequest, CreateDatasetRequest, CreateProjectRequest,
        DeleteAuthorizationRequest, GetCollectionRequest, GetDatasetRequest,
        UpdateAuthorizationRequest,
    },
};
use aruna_server::{
    database::{
        dsls::{
            internal_relation_dsl::InternalRelation,
            object_dsl::{ExternalRelations, Hashes, KeyValues, Object},
            user_dsl::{User, UserAttributes},
        },
        enums::{DataClass, DbPermissionLevel, ObjectMapping, ObjectStatus, ObjectType},
    },
    grpc::{
        authorization::AuthorizationServiceImpl, collections::CollectionServiceImpl,
        datasets::DatasetServiceImpl, projects::ProjectServiceImpl,
    },
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tonic::{
    metadata::{AsciiMetadataKey, AsciiMetadataValue},
    Request,
};

/* ----- Begin Testing Constants ---------- */
#[allow(dead_code)]
pub static ADMIN_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjM2Njc0Mzk0ODQsImlhdCI6MTY2NzQwMzQ4NCwiYXV0aF90aW1lIjoxNjY3NDAzNDg0LCJqdGkiOiI0YWVlYzYzNC02NmU4LTQ1OWMtOGZjZi1lMGJmNjY3MDZjMTUiLCJpc3MiOiJsb2NhbGhvc3QudGVzdCIsImF1ZCI6ImFydW5hIiwic3ViIjoiZGY1YjAyMDktNjBlMC00YTNiLTgwNmQtYmJmYzk5ZDllMTUyIiwidHlwIjoiSUQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhcnVuYS1hZG1pbiJ9.5hZh2lG6JSOCvMB1crX6miEaDLf6GTCVC3dcnfc2KMME4SY68DEMAZJzk8ag_aQba4ObDOq4-QpRl4vNW6HgA8yYsBI6pbCZorWvjWklwnfv0vDVmegVybSWu2LJONxZ4lMxip1zR4FT_nRUBIda_hq-SQHGuJI1n4NxVzQ67Rreo-i6TDyqHj_aCuNN9OQxwPZQisOuNbd7oACrkCzbbv37jHf46uDUQnHwqS3DCO60ywAbe28zh0YwjfUINIf_1HgNXkS7ZF1eDcZmohFu24Wo8G2Hb2bo_zp8vR2jatNkchRq__9hUcySHAcLuiPfl8OLsqx2WA7JMyX7OZStI9MIRC6yK9hHF81pwpd29cK47wdBer0FzQaNnuBw5BXjhk5YYz0RUs27kYHOUnQJHAhCWKbGyvDy0wDkOp5XrWvgxJrPbhDY0Fjmh-4nrHdd7ozqoVtRt8G1jsKZmv3y9w7VObURLQplWpLHwQ_vqvcG0_3DDSB90_HYrOnn93xNixMq0Gk0ZCrYe2QJN92njkhhND5KqWDfho6TF1OFok2hrnMGKtlKdeiB9qH2vC4y-OweOf1pB8OXk2_3QB9FDGeLNrLeTL9uY2XTtyyqRZGIekEr8MBCyhtgwOy7jG24MMwmcTKOroQNboFu-_S0kz4k77PVHSL5785IuLlRVSY";
#[allow(dead_code)]
pub static ADMIN_USER_ULID: &str = "01H819G3ZMK5DC9Q5PD18N9SXB";
#[allow(dead_code)]
pub static USER1_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjM2Njc0Mzk0ODQsImlhdCI6MTY2NzQwMzQ4NCwiYXV0aF90aW1lIjoxNjY3NDAzNDg0LCJqdGkiOiI2NzRkNDZiNy1kZGFkLTQ1ZmYtYjU4YS01MjFkYjk2M2QyNjYiLCJpc3MiOiJsb2NhbGhvc3QudGVzdCIsImF1ZCI6ImFydW5hIiwic3ViIjoiMzk4OTM3ODEtMzIwZS00ZGJmLWJlMzktYzA2ZDhiMjhlODk3IiwidHlwIjoiSUQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJyZWd1bGFyLXVzZXIifQ.xV0KkMTYe-k7iFEaz2ZW1b7VYX1HG_PB1O7yAZdafBheBJr9y9dSxI3pvoFN4pgRPF1zSPq7YLB6lwT6IRAE5sDxXorcSOc-FH2L5YCWKd5Dz1XdDNSDpYkN-9VRXevOpaQuw2_PnSVqJ922uDWz4zO5L3nNZ3dD69uR1-jaUlbjbzBf_9d-f5hXDjVI2XMLaGBBwD9EoGFZkucYKN8XgnrK6eyBUezX8W7U5vet5r-gNgDKPu17BPEaghURosa2JSIKDvfoCSOJmF_6IiDmfmiFp3wzCLldp1hZP0ve6NyzM3V80J0GiIOintvjBMGwUr8B6eFE2lm1M5vl7fsENBJF7mMYpSUBQYWqFShWujNIOJfsaHg_y9n0DUHNxJ1fYTInQDWOzky29VfXDr5yNGNtq9AOQiDGaUp4f0q-VjE1FC58a96tV2VhxYA_yTxxE-DzxZ-h_DvVTcZ9i_s9w4wVT2C2cCZrkvOH5cxdoXkGpJwwZRainUyDRJr2D5GpaMRPlIF4tUjV1H84x9aHpzvblscbgrKnsBwq2-PpIrRCezIt5XMXl5pjlQaI0sMe4sSOgK-Qls3ILhz33tslUypIn29D6iy5U7pYb-0n8KnPnufNWn4uWviHsHFwGgvZkDhvlwqGW8BxtWePU4xc3Fu7N2sdv1Vvmj3-cYSLcaA";
#[allow(dead_code)]
pub static USER1_ULID: &str = "01H8KWYY5MTAH1YZGPYVS7PQWD";
#[allow(dead_code)]
pub static USER2_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjMyNjc0Mzk0ODQsImlhdCI6MTY2NzQwMzQ4NCwiYXV0aF90aW1lIjoxNjY3NDAzNDg0LCJqdGkiOiI2YWVlNDg1ZC1jMzVlLTRmYWQtYTQ3Zi01YzVmOWNkMmYzMjAiLCJpc3MiOiJsb2NhbGhvc3QudGVzdCIsImF1ZCI6ImFydW5hIiwic3ViIjoiYzY3NWI5ZmUtNzI3NS00NmM2LWFmNWEtMjY0MTExNGI1Mzc1IiwidHlwIjoiSUQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJ1c2VyMyJ9.hSNxHV_IVjbYUa0V-Tj19-uZi1Sd5786G9S9t0SCwh_Vn5XCZFwv4_y-GfuOq4edtqBHStfIgD6PLEE1ZvD3BQUsU9wNRlAY6Me51vPKJvatHcbWR0Pf-j4BZJ3qLVTAsDDAV5_n-CR1d6I4ev_FCX7Xn7rXz9GhVMKrJ7apMEet4HizFexfA6EaCKaYvb3h-fpNgTD1p__qNquFvP8NjLu-EcPKc2XWWDaCEjTEIvS9iE2L-G173U2ewjrk2apU1rQfRfgd8BhP9Pj8eHb0zigxElkGMZhAqkwt8KWyuneOl_X6M3t_MKyiz_xlWY_3Q_ai-ZEYzxRc5xVdRffBtett8FxnFCNYwdFucIQaYexSTk1O3oXgVGekC4a_-GF8I51YjvjyvpBtdg26fmhdS7_ZHj3flM2mlfNi1byQRG3VYUn-gyyZJ0QCo5-q9CK8PWjqLSXaF1FqjlsnB5FqMKpRm1uT8K26ntFQQmrSotNfbQgFjnPDLbaGn9wC2Ld8hAtjEHNhl9eVvxVsa2E6Nyl_Xpt-r-m_J4DcH7Qcv0DiLoVzIhbO6OM_wDOlKsyQZ9FL7mXISisUKU-cPqyiiaI9hQ8f6sJm9iAy5xZQSoemKeEKxfsPfXviVpWYb_v9TdfbQjIJ3LR14ouPU056pSn2ofU3HBrUulnuVPLEP-8";
#[allow(dead_code)]
pub static USER2_ULID: &str = "01HBG7TQS8HTV3M2PKFKMMBSJ5";
#[allow(dead_code)]
pub static DEFAULT_ENDPOINT_ULID: &str = "01H81W0ZMB54YEP5711Q2BK46V";
/* ----- End Testing Constants ---------- */

#[allow(dead_code)]
pub fn new_user(object_ids: Vec<ObjectMapping<DieselUlid>>) -> User {
    User {
        id: DieselUlid::generate(),
        display_name: "test1".to_string(),
        external_id: None,
        email: "test2@test3".to_string(),
        attributes: Json(UserAttributes {
            global_admin: false,
            service_account: false,
            custom_attributes: Vec::new(),
            tokens: DashMap::default(),
            trusted_endpoints: DashMap::default(),
            permissions: DashMap::from_iter(object_ids.iter().map(|o| match o {
                ObjectMapping::PROJECT(id) => {
                    (*id, ObjectMapping::PROJECT(DbPermissionLevel::WRITE))
                }
                ObjectMapping::COLLECTION(id) => {
                    (*id, ObjectMapping::COLLECTION(DbPermissionLevel::WRITE))
                }
                ObjectMapping::DATASET(id) => {
                    (*id, ObjectMapping::DATASET(DbPermissionLevel::WRITE))
                }
                ObjectMapping::OBJECT(id) => (*id, ObjectMapping::OBJECT(DbPermissionLevel::WRITE)),
            })),
        }),
        active: true,
    }
}

#[allow(dead_code)]
pub fn new_object(user_id: DieselUlid, object_id: DieselUlid, object_type: ObjectType) -> Object {
    Object {
        id: object_id,
        revision_number: 0,
        name: object_id.to_string(),
        description: "b".to_string(),
        count: 1,
        created_at: None,
        content_len: 1337,
        created_by: user_id,
        key_values: Json(KeyValues(vec![])),
        object_status: ObjectStatus::AVAILABLE,
        data_class: DataClass::PUBLIC,
        object_type,
        external_relations: Json(ExternalRelations(DashMap::default())),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(DashMap::default()),
    }
}

#[allow(dead_code)]
pub fn new_internal_relation(origin: &Object, target: &Object) -> InternalRelation {
    InternalRelation {
        id: DieselUlid::generate(),
        origin_pid: origin.id,
        origin_type: origin.object_type,
        target_pid: target.id,
        target_type: target.object_type,
        relation_name: "BELONGS_TO".to_string(),
        target_name: target.name.to_string(),
    }
}

#[allow(dead_code)]
pub fn object_from_mapping(
    user_id: DieselUlid,
    object_mapping: ObjectMapping<DieselUlid>,
) -> Object {
    let (id, object_type) = match object_mapping {
        ObjectMapping::PROJECT(id) => (id, ObjectType::PROJECT),
        ObjectMapping::COLLECTION(id) => (id, ObjectType::COLLECTION),
        ObjectMapping::DATASET(id) => (id, ObjectType::DATASET),
        ObjectMapping::OBJECT(id) => (id, ObjectType::OBJECT),
    };
    Object {
        id,
        revision_number: 0,
        name: "a".to_string(),
        description: "b".to_string(),
        count: 1,
        created_at: None,
        content_len: 1337,
        created_by: user_id,
        key_values: Json(KeyValues(vec![])),
        object_status: ObjectStatus::AVAILABLE,
        data_class: DataClass::PRIVATE,
        object_type,
        external_relations: Json(ExternalRelations(DashMap::default())),
        hashes: Json(Hashes(Vec::new())),
        dynamic: false,
        endpoints: Json(DashMap::default()),
    }
}

#[allow(dead_code)]
pub fn add_token<T>(mut req: tonic::Request<T>, token: &str) -> tonic::Request<T> {
    let metadata = req.metadata_mut();
    metadata.append(
        AsciiMetadataKey::from_bytes(b"Authorization").unwrap(),
        AsciiMetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    req
}

#[allow(dead_code)]
pub fn rand_string(length: usize) -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/* ----- Resource create convenience functions ---------- */
#[allow(dead_code)]
pub async fn fast_track_grpc_get_collection(
    collection_service: &CollectionServiceImpl,
    token: &str,
    collection_id: &str,
) -> Collection {
    // Create request with token
    let get_request = add_token(
        Request::new(GetCollectionRequest {
            collection_id: collection_id.to_string(),
        }),
        token,
    );

    // Fetch collection vie gRPC service
    collection_service
        .get_collection(get_request)
        .await
        .unwrap()
        .into_inner()
        .collection
        .unwrap()
}

#[allow(dead_code)]
pub async fn fast_track_grpc_get_dataset(
    dataset_service: &DatasetServiceImpl,
    token: &str,
    datset_id: &str,
) -> Dataset {
    // Create request with token
    let get_request = add_token(
        Request::new(GetDatasetRequest {
            dataset_id: datset_id.to_string(),
        }),
        token,
    );

    // Fetch collection vie gRPC service
    dataset_service
        .get_dataset(get_request)
        .await
        .unwrap()
        .into_inner()
        .dataset
        .unwrap()
}

#[allow(dead_code)]
pub async fn fast_track_grpc_project_create(
    project_service: &ProjectServiceImpl,
    token: &str,
) -> Project {
    // Create request with token
    let project_name = rand_string(32);

    let create_request = CreateProjectRequest {
        name: project_name.to_string(),
        description: "".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: ApiDataClass::Private as i32,
        preferred_endpoint: "".to_string(),
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let create_response = project_service
        .create_project(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_project = create_response.project.unwrap();

    assert!(!proto_project.id.is_empty());
    assert_eq!(proto_project.name, project_name);

    proto_project
}

#[allow(dead_code)]
pub async fn fast_track_grpc_collection_create(
    collection_service: &CollectionServiceImpl,
    token: &str,
    parent: create_collection_request::Parent,
) -> Collection {
    // Create request with token
    let collection_name = rand_string(32);

    let create_request = CreateCollectionRequest {
        name: collection_name.to_string(),
        description: "".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: ApiDataClass::Private as i32,
        parent: Some(parent),
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let create_response = collection_service
        .create_collection(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_collection = create_response.collection.unwrap();

    assert!(!proto_collection.id.is_empty());
    assert_eq!(proto_collection.name, collection_name);

    proto_collection
}

#[allow(dead_code)]
pub async fn fast_track_grpc_dataset_create(
    dataset_service: &DatasetServiceImpl,
    token: &str,
    parent: create_dataset_request::Parent,
) -> Dataset {
    // Create request with token
    let dataset_name = rand_string(32);

    let create_request = CreateDatasetRequest {
        name: dataset_name.to_string(),
        description: "".to_string(),
        key_values: vec![],
        external_relations: vec![],
        data_class: ApiDataClass::Private as i32,
        parent: Some(parent),
    };

    let grpc_request = add_token(Request::new(create_request), token);

    // Create project via gRPC service
    let create_response = dataset_service
        .create_dataset(grpc_request)
        .await
        .unwrap()
        .into_inner();

    let proto_dataset = create_response.dataset.unwrap();

    assert!(!proto_dataset.id.is_empty());
    assert_eq!(proto_dataset.name, dataset_name);

    proto_dataset
}

#[allow(dead_code)]
pub async fn fast_track_grpc_permission_add(
    auth_service: &AuthorizationServiceImpl,
    token: &str,
    user_ulid: &DieselUlid,
    resource_ulid: &DieselUlid,
    permission_level: DbPermissionLevel,
) {
    // Create request with token
    let inner_request = CreateAuthorizationRequest {
        resource_id: resource_ulid.to_string(),
        user_id: user_ulid.to_string(),
        permission_level: PermissionLevel::from(permission_level) as i32,
    };
    let grpc_request = add_token(Request::new(inner_request), token);

    // Add permission to user
    auth_service
        .create_authorization(grpc_request)
        .await
        .unwrap();
}

#[allow(dead_code)]
pub async fn fast_track_grpc_permission_update(
    auth_service: &AuthorizationServiceImpl,
    token: &str,
    user_ulid: &DieselUlid,
    resource_ulid: &DieselUlid,
    permission_level: DbPermissionLevel,
) {
    // Create request with token
    let inner_request = UpdateAuthorizationRequest {
        resource_id: resource_ulid.to_string(),
        user_id: user_ulid.to_string(),
        permission_level: PermissionLevel::from(permission_level) as i32,
    };
    let grpc_request = add_token(Request::new(inner_request), token);

    // Add permission to user
    auth_service
        .update_authorization(grpc_request)
        .await
        .unwrap();
}

#[allow(dead_code)]
pub async fn fast_track_grpc_permission_delete(
    auth_service: &AuthorizationServiceImpl,
    token: &str,
    user_ulid: &DieselUlid,
    resource_ulid: &DieselUlid,
) {
    // Create request with token
    let inner_request = DeleteAuthorizationRequest {
        resource_id: resource_ulid.to_string(),
        user_id: user_ulid.to_string(),
    };
    let grpc_request = add_token(Request::new(inner_request), token);

    // Add permission to user
    auth_service
        .delete_authorization(grpc_request)
        .await
        .unwrap();
}
