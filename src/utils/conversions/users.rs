use std::str::FromStr;
use crate::database::{
    dsls::user_dsl::{
        APIToken, CustomAttributes as DBCustomAttributes,
        DataProxyAttribute as DBDataProxyAttribute, User as DBUser,
        UserAttributes as DBUserAttributes,
    },
    enums::{DbPermissionLevel, ObjectMapping},
};
use aruna_rust_api::api::storage::{
    models::v2::{
        permission::ResourceId, CustomAttribute, DataProxyAttribute, OidcMapping, Permission,
        PermissionLevel, Token, User, UserAttributes,
    },
    services::v2::ServiceAccount,
};
use diesel_ulid::DieselUlid;

// Conversion from database model user token to proto user
impl From<DBUser> for User {
    fn from(db_user: DBUser) -> Self {
        // Return proto user
        User {
            id: db_user.id.to_string(),
            display_name: db_user.display_name,
            active: db_user.active,
            email: db_user.email,
            attributes: Some(db_user.attributes.0.into()),
            first_name: db_user.first_name,
            last_name: db_user.last_name,
        }
    }
}
// Conversion from database permission to proto permission
pub fn convert_permission_to_proto(
    resource_id: DieselUlid,
    resource_mapping: ObjectMapping<DbPermissionLevel>,
) -> Permission {
    match resource_mapping {
        ObjectMapping::PROJECT(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::ProjectId(resource_id.to_string())),
        },
        ObjectMapping::COLLECTION(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::CollectionId(resource_id.to_string())),
        },
        ObjectMapping::DATASET(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::DatasetId(resource_id.to_string())),
        },
        ObjectMapping::OBJECT(perm) => Permission {
            permission_level: PermissionLevel::from(perm) as i32,
            resource_id: Some(ResourceId::ObjectId(resource_id.to_string())),
        },
    }
}

// Conversion from database model token to proto token
pub fn convert_token_to_proto(token_id: &DieselUlid, db_token: APIToken) -> Token {
    Token {
        id: token_id.to_string(),
        name: db_token.name,
        created_at: Some(db_token.created_at.into()),
        expires_at: Some(db_token.expires_at.into()),
        permission: db_token.object_id.map(|id| Permission {
            permission_level: Into::<PermissionLevel>::into(db_token.user_rights) as i32,
            resource_id: Some(ResourceId::from(id)),
        }),
    }
}

impl From<DBUserAttributes> for UserAttributes {
    fn from(attr: DBUserAttributes) -> Self {
        let tokens: Vec<Token> = attr
            .tokens
            .into_iter()
            .map(|t| Token {
                id: t.0.to_string(),
                name: t.1.name,
                created_at: Some(t.1.created_at.into()),
                expires_at: Some(t.1.expires_at.into()),
                permission: Some(Permission {
                    permission_level: t.1.user_rights.into(),
                    resource_id: t.1.object_id.map(|resource| match resource {
                        ObjectMapping::PROJECT(id) => ResourceId::ProjectId(id.to_string()),
                        ObjectMapping::COLLECTION(id) => ResourceId::CollectionId(id.to_string()),
                        ObjectMapping::DATASET(id) => ResourceId::DatasetId(id.to_string()),
                        ObjectMapping::OBJECT(id) => ResourceId::ObjectId(id.to_string()),
                    }),
                }),
            })
            .collect();

        let personal_permissions: Vec<Permission> = attr
            .permissions
            .into_iter()
            .map(|(id, perm)| convert_permission_to_proto(id, perm))
            .collect();
        UserAttributes {
            global_admin: attr.global_admin,
            service_account: attr.service_account,
            tokens,
            custom_attributes: attr
                .custom_attributes
                .into_iter()
                .map(|c| c.into())
                .collect(),
            personal_permissions,
            trusted_endpoints: attr
                .trusted_endpoints
                .iter()
                .map(|e| e.key().to_string())
                .collect(),
            external_ids: attr
                .external_ids
                .iter()
                .map(|a| OidcMapping {
                    external_id: a.external_id.to_string(),
                    oidc_url: a.oidc_name.to_string(),
                })
                .collect(),
            pubkey: attr.pubkey,
            data_proxy_attributes: attr
                .data_proxy_attribute
                .into_iter()
                .flat_map(|(_, a)| {
                    a.into_iter()
                        .map(|a| a.into())
                        .collect::<Vec<DataProxyAttribute>>()
                })
                .collect(),
        }
    }
}

impl From<DBDataProxyAttribute> for DataProxyAttribute {
    fn from(value: DBDataProxyAttribute) -> Self {
        DataProxyAttribute {
            attribute_name: value.attribute_name,
            attribute_value: value.attribute_value,
            signature: value.signature,
            proxy_id: value.proxy_id.to_string(),
        }
    }
}

impl From<DBCustomAttributes> for CustomAttribute {
    fn from(attr: DBCustomAttributes) -> Self {
        CustomAttribute {
            attribute_name: attr.attribute_name,
            attribute_value: attr.attribute_value,
        }
    }
}
impl DBUser {
    pub fn into_redacted(self) -> User {
        let mut user: User = self.into();
        user.email = String::new();
        user.display_name = String::new();
        if let Some(attr) = user.attributes.as_mut() {
            attr.external_ids = Vec::new();
        }
        user
    }
}

pub fn as_api_token(id: DieselUlid, token: APIToken) -> Token {
    Token {
        id: id.to_string(),
        name: token.name,
        created_at: Some(token.created_at.into()),
        expires_at: Some(token.expires_at.into()),
        permission: Some(Permission {
            permission_level: token.user_rights.into(),
            resource_id: token.object_id.map(|resource| match resource {
                ObjectMapping::PROJECT(id) => ResourceId::ProjectId(id.to_string()),
                ObjectMapping::COLLECTION(id) => ResourceId::CollectionId(id.to_string()),
                ObjectMapping::DATASET(id) => ResourceId::DatasetId(id.to_string()),
                ObjectMapping::OBJECT(id) => ResourceId::ObjectId(id.to_string()),
            }),
        }),
    }
}

impl TryFrom<DBUser> for ServiceAccount {
    type Error = tonic::Status;
    fn try_from(user: DBUser) -> Result<Self, tonic::Status> {
        if user.attributes.0.service_account {
            if user.attributes.0.permissions.len() > 1 {
                // THIS SHOULD NOT HAPPEN!
                Err(tonic::Status::invalid_argument(
                    "Service account has more than one permission",
                ))
            } else {
                let permissions = user.attributes.0.permissions.iter().next().ok_or_else(|| {
                    tonic::Status::internal("No permissions found for service_account")
                })?;
                let (id, perm) = permissions.pair();
                let permission_level = perm.into_inner().into();
                let resource_id = Some(match perm {
                    ObjectMapping::PROJECT(_) => ResourceId::ProjectId(id.to_string()),
                    ObjectMapping::COLLECTION(_) => ResourceId::CollectionId(id.to_string()),
                    ObjectMapping::DATASET(_) => ResourceId::DatasetId(id.to_string()),
                    ObjectMapping::OBJECT(_) => ResourceId::ObjectId(id.to_string()),
                });
                Ok(ServiceAccount {
                    svc_account_id: user.id.to_string(),
                    name: user.display_name.to_string(),
                    permission: Some(Permission {
                        permission_level,
                        resource_id,
                    }),
                })
            }
        } else {
            Err(tonic::Status::invalid_argument(
                "User is not a service_account",
            ))
        }
    }
}

impl TryFrom<DataProxyAttribute> for DBDataProxyAttribute {
    type Error = anyhow::Error;
    fn try_from(value: DataProxyAttribute) -> anyhow::Result<Self> {
        Ok(DBDataProxyAttribute {
            attribute_name: value.attribute_name,
            attribute_value: value.attribute_value,
            signature: value.signature,
            proxy_id: DieselUlid::from_str(&value.proxy_id)?,
        })

    }
}