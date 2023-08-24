use aruna_server::database::{
    dsls::{
        internal_relation_dsl::InternalRelation,
        object_dsl::{ExternalRelations, Hashes, KeyValues, Object},
        user_dsl::{User, UserAttributes},
    },
    enums::{DataClass, DbPermissionLevel, ObjectMapping, ObjectStatus, ObjectType},
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};

/* ----- Begin Testing Constants ---------- */
#[allow(dead_code)]
pub static ADMIN_OIDC_TOKEN: &str = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJleHAiOjM2Njc0Mzk0ODQsImlhdCI6MTY2NzQwMzQ4NCwiYXV0aF90aW1lIjoxNjY3NDAzNDg0LCJqdGkiOiI0YWVlYzYzNC02NmU4LTQ1OWMtOGZjZi1lMGJmNjY3MDZjMTUiLCJpc3MiOiJsb2NhbGhvc3QudGVzdCIsImF1ZCI6ImFydW5hIiwic3ViIjoiZGY1YjAyMDktNjBlMC00YTNiLTgwNmQtYmJmYzk5ZDllMTUyIiwidHlwIjoiSUQiLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJhcnVuYS1hZG1pbiJ9.5hZh2lG6JSOCvMB1crX6miEaDLf6GTCVC3dcnfc2KMME4SY68DEMAZJzk8ag_aQba4ObDOq4-QpRl4vNW6HgA8yYsBI6pbCZorWvjWklwnfv0vDVmegVybSWu2LJONxZ4lMxip1zR4FT_nRUBIda_hq-SQHGuJI1n4NxVzQ67Rreo-i6TDyqHj_aCuNN9OQxwPZQisOuNbd7oACrkCzbbv37jHf46uDUQnHwqS3DCO60ywAbe28zh0YwjfUINIf_1HgNXkS7ZF1eDcZmohFu24Wo8G2Hb2bo_zp8vR2jatNkchRq__9hUcySHAcLuiPfl8OLsqx2WA7JMyX7OZStI9MIRC6yK9hHF81pwpd29cK47wdBer0FzQaNnuBw5BXjhk5YYz0RUs27kYHOUnQJHAhCWKbGyvDy0wDkOp5XrWvgxJrPbhDY0Fjmh-4nrHdd7ozqoVtRt8G1jsKZmv3y9w7VObURLQplWpLHwQ_vqvcG0_3DDSB90_HYrOnn93xNixMq0Gk0ZCrYe2QJN92njkhhND5KqWDfho6TF1OFok2hrnMGKtlKdeiB9qH2vC4y-OweOf1pB8OXk2_3QB9FDGeLNrLeTL9uY2XTtyyqRZGIekEr8MBCyhtgwOy7jG24MMwmcTKOroQNboFu-_S0kz4k77PVHSL5785IuLlRVSY";
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
