use crate::database::enums::{DbPermissionLevel, ObjectMapping};
use ahash::RandomState;
use anyhow::{anyhow, Result};
use chrono::NaiveDateTime;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_from_row::FromRow;
use postgres_types::Json;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt::Display};
use tokio_postgres::Client;

use super::{
    super::crud::{CrudDb, PrimaryKey},
    Empty,
};

#[derive(Debug, FromRow, Clone)]
pub struct User {
    pub id: DieselUlid,
    pub display_name: String,
    pub first_name: String,
    pub last_name: String,
    pub email: String,
    pub attributes: Json<UserAttributes>,
    pub active: bool,
}

#[derive(Serialize, Deserialize, Clone, FromRow, Debug, Eq, PartialEq, PartialOrd)]
pub struct APIToken {
    pub pub_key: i32,
    pub name: String,
    pub created_at: NaiveDateTime,
    pub expires_at: NaiveDateTime,
    pub object_id: Option<ObjectMapping<DieselUlid>>,
    pub user_rights: DbPermissionLevel,
}

#[derive(Serialize, Deserialize, Clone, FromRow, Debug, Eq, PartialEq, PartialOrd)]
pub struct OIDCMapping {
    pub external_id: String,
    pub oidc_name: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UserAttributes {
    pub global_admin: bool,
    pub service_account: bool,
    pub tokens: DashMap<DieselUlid, APIToken, RandomState>,
    pub trusted_endpoints: DashMap<DieselUlid, Empty, RandomState>,
    pub custom_attributes: Vec<CustomAttributes>,
    pub permissions: DashMap<DieselUlid, ObjectMapping<DbPermissionLevel>, RandomState>,
    pub external_ids: Vec<OIDCMapping>,
    pub pubkey: String,
    pub data_proxy_attribute: DashMap<DieselUlid, DataProxyAttribute>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct CustomAttributes {
    pub attribute_name: String,
    pub attribute_value: String,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, PartialOrd)]
pub struct DataProxyAttribute {
    pub attribute_name: String,
    pub attribute_value: String,
    pub signature: String,
    pub proxy_id: DieselUlid,
}

#[async_trait::async_trait]
impl CrudDb for User {
    //ToDo: Rust Doc
    async fn create(&mut self, client: &Client) -> Result<()> {
        let query = "INSERT INTO users 
          (id, display_name, first_name, last_name, email, attributes, active) 
        VALUES 
          ($1, $2, $3, $4, $5, $6, $7);";

        let prepared = client.prepare(query).await?;

        client
            .query(
                &prepared,
                &[
                    &self.id,
                    &self.display_name,
                    &self.first_name,
                    &self.last_name,
                    &self.email,
                    &self.attributes,
                    &self.active,
                ],
            )
            .await?;
        Ok(())
    }

    //ToDo: Rust Doc
    async fn get(id: impl PrimaryKey, client: &Client) -> Result<Option<Self>> {
        let query = "SELECT * FROM users WHERE id = $1";
        let prepared = client.prepare(query).await?;

        Ok(client
            .query_opt(&prepared, &[&id])
            .await?
            .map(|e| User::from_row(&e)))
    }

    //ToDo: Rust Doc
    async fn all(client: &Client) -> Result<Vec<Self>> {
        let query = "SELECT * FROM users";
        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[]).await?;
        Ok(rows.iter().map(User::from_row).collect::<Vec<_>>())
    }

    //ToDo: Rust Doc
    async fn delete(&self, client: &Client) -> Result<()> {
        let query = "DELETE FROM users WHERE id = $1";
        let prepared = client.prepare(query).await?;
        client.execute(&prepared, &[&self.id]).await?;
        Ok(())
    }
}

impl User {
    //ToDo: Rust Doc
    pub async fn update_display_name(
        client: &Client,
        user_id: &DieselUlid,
        display_name: impl Into<String>,
    ) -> Result<User> {
        let query = "UPDATE users
            SET display_name = $1
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&display_name.into(), user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn update_email(
        client: &Client,
        user_id: &DieselUlid,
        email: impl Into<String>,
    ) -> Result<User> {
        let query = "UPDATE users
            SET email = $1
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&email.into(), user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn set_user_attributes(
        client: &Client,
        user_id: &DieselUlid,
        user_attributes: Json<UserAttributes>,
    ) -> Result<User> {
        let query = "UPDATE users
            SET attributes = $1
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&user_attributes, user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn set_user_global_admin(
        client: &Client,
        user_id: &DieselUlid,
        is_admin: bool,
    ) -> Result<User> {
        let query = "UPDATE users
            SET attributes = JSONB_SET(attributes, '{global_admin}', to_jsonb($1::bool))
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[&is_admin, user_id]).await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn set_user_service_account(
        client: &Client,
        user_id: &DieselUlid,
        is_service_account: bool,
    ) -> Result<User> {
        let query = "UPDATE users
            SET attributes = JSONB_SET(attributes, '{service_account}', to_jsonb($1::bool))
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&is_service_account, user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn add_user_permission(
        client: &Client,
        user_id: &DieselUlid,
        user_perm: HashMap<DieselUlid, ObjectMapping<DbPermissionLevel>, RandomState>,
    ) -> Result<User> {
        let query = "UPDATE users
            SET attributes = jsonb_set(attributes, '{permissions}', attributes->'permissions' || $1::jsonb, true) 
            WHERE id = $2 
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&Json(user_perm), user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    pub async fn remove_user_permission(
        client: &Client,
        user_id: &DieselUlid,
        resource_id: &DieselUlid,
    ) -> Result<User> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, '{permissions}', (attributes->'permissions') - $1::TEXT) 
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&resource_id.to_string(), user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    pub async fn remove_all_user_permissions(
        client: &Client,
        user_id: &DieselUlid,
    ) -> Result<User> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, '{permissions}', '{}') 
            WHERE id = $1
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[user_id]).await?;

        Ok(User::from_row(&row))
    }

    pub async fn update_user_permission(
        client: &Client,
        user_id: &DieselUlid,
        resource_id: &DieselUlid,
        user_perm: ObjectMapping<DbPermissionLevel>,
    ) -> Result<User> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, ARRAY['permissions', $1::TEXT], $2::jsonb, true) 
            WHERE id = $3
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(
                &prepared,
                &[&resource_id.to_string(), &Json(user_perm), &user_id],
            )
            .await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn add_user_token(
        client: &Client,
        user_id: &DieselUlid,
        token: HashMap<DieselUlid, &APIToken, RandomState>,
    ) -> Result<User> {
        let query = "UPDATE users
            SET attributes = jsonb_set(attributes, '{tokens}', attributes->'tokens' || $1::jsonb, true) 
            WHERE id = $2 
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&Json(token), user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    pub async fn remove_user_token(
        client: &Client,
        user_id: &DieselUlid,
        token_id: &DieselUlid,
    ) -> Result<User> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, '{tokens}', (attributes->'tokens') - $1::TEXT) 
            WHERE id = $2
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&token_id.to_string(), user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    pub async fn remove_all_tokens(client: &Client, user_id: &DieselUlid) -> Result<User> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, '{tokens}', '{}') 
            WHERE id = $1
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[user_id]).await?;

        Ok(User::from_row(&row))
    }

    pub async fn deactivate_user(client: &Client, user_id: &DieselUlid) -> Result<User> {
        let query = "UPDATE users
            SET active = false 
            WHERE id = $1
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[user_id]).await?;
        Ok(User::from_row(&row))
    }

    pub async fn activate_user(client: &Client, user_id: &DieselUlid) -> Result<User> {
        let query = "UPDATE users
            SET active = true 
            WHERE id = $1
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client.query_one(&prepared, &[user_id]).await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Rust Doc
    pub async fn add_trusted_endpoint(
        client: &Client,
        user_id: &DieselUlid,
        endpoint_id: &DieselUlid,
    ) -> Result<User> {
        let query = "UPDATE users
            SET attributes = jsonb_set(attributes, '{trusted_endpoints}', attributes->'trusted_endpoints' || $1::jsonb, true) 
            WHERE id = $2 
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let map: DashMap<DieselUlid, Empty, RandomState> =
            DashMap::from_iter([(endpoint_id.to_owned(), Empty {})]);
        let row = client.query_one(&prepared, &[&Json(map), user_id]).await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Docs
    pub async fn remove_trusted_endpoint(
        client: &Client,
        user_id: &DieselUlid,
        endpoint_id: &DieselUlid,
    ) -> Result<User> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, '{trusted_endpoints}', (attributes->'trusted_endpoints') - $1::TEXT) 
            WHERE id = $2 
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let row = client
            .query_one(&prepared, &[&endpoint_id.to_string(), &user_id])
            .await?;

        Ok(User::from_row(&row))
    }

    //ToDo: Docs
    pub async fn remove_endpoint_from_users(
        client: &Client,
        endpoint_id: &DieselUlid,
    ) -> Result<Vec<User>> {
        let query = "UPDATE users 
            SET attributes = jsonb_set(attributes, '{trusted_endpoints}', (attributes->'trusted_endpoints') - $1::TEXT) 
            RETURNING *;";

        let prepared = client.prepare(query).await?;
        let rows = client.query(&prepared, &[&endpoint_id.to_string()]).await?;

        Ok(rows.iter().map(User::from_row).collect::<Vec<_>>())
    }
}

impl Display for UserAttributes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_string(&self)
                .map_err(|_| anyhow!("Invalid json"))
                .unwrap_or_default()
        )
    }
}

impl PartialEq for UserAttributes {
    fn eq(&self, other: &Self) -> bool {
        self.to_string() == other.to_string()
    }
}

impl PartialEq for User {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.display_name == other.display_name
            && self.email == other.email
            && self.attributes.0.to_string() == other.attributes.0.to_string()
            && self.active == other.active
    }
}

impl Eq for User {}
