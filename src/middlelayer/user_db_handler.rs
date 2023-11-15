use std::collections::HashMap;
use std::str::FromStr;

use crate::database::crud::CrudDb;
use crate::database::dsls::persistent_notification_dsl::{
    NotificationReference, NotificationReferences, PersistentNotification,
};
use crate::database::dsls::user_dsl::{OIDCMapping, User, UserAttributes};
use crate::database::enums::{
    DbPermissionLevel, NotificationReferenceType, ObjectMapping, PersistentNotificationVariant,
};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::user_request_types::{
    ActivateUser, DeactivateUser, RegisterUser, UpdateUserEmail, UpdateUserName,
};
use anyhow::{anyhow, bail, Result};
use aruna_rust_api::api::notification::services::v2::EventVariant;
use aruna_rust_api::api::storage::services::v2::PersonalNotification;
use diesel_ulid::DieselUlid;
use postgres_types::Json;
use tokio_postgres::GenericClient;

impl DatabaseHandler {
    pub async fn register_user(
        &self,
        request: RegisterUser,
        external_id: OIDCMapping,
    ) -> Result<(DieselUlid, User)> {
        let client = self.database.get_client().await?;
        let user_id = DieselUlid::generate();
        let new_attributes = UserAttributes {
            global_admin: false,
            service_account: false,
            tokens: Default::default(),
            trusted_endpoints: Default::default(),
            custom_attributes: vec![],
            permissions: Default::default(),
            external_ids: vec![external_id],
        };
        let mut user = User {
            id: user_id,
            display_name: request.get_display_name(),
            email: request.get_email(),
            attributes: Json(new_attributes),
            active: false,
        };
        user.create(client.client()).await?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Created)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok((user_id, user))
    }

    pub async fn deactivate_user(&self, request: DeactivateUser) -> Result<(DieselUlid, User)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        User::deactivate_user(&client, &id).await?;
        let user = User::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok((id, user))
    }

    pub async fn activate_user(&self, request: ActivateUser) -> Result<(DieselUlid, User)> {
        let client = self.database.get_client().await?;
        let id = request.get_id()?;
        User::activate_user(&client, &id).await?;
        let user = User::get(id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok((id, user))
    }
    pub async fn update_display_name(
        &self,
        request: UpdateUserName,
        user_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let name = request.get_name();
        User::update_display_name(&client, &user_id, name).await?;
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(user)
    }
    pub async fn update_email(
        &self,
        request: UpdateUserEmail,
        user_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let email = request.get_email();
        User::update_email(&client, &user_id, email).await?;
        let user = User::get(user_id, &client)
            .await?
            .ok_or_else(|| anyhow!("User not found"))?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(user)
    }

    pub async fn add_endpoint_to_user(
        &self,
        user_id: DieselUlid,
        endpoint_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let user = User::add_trusted_endpoint(&client, &user_id, &endpoint_id).await?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(user)
    }

    pub async fn add_permission_to_user(
        &self,
        user_id: DieselUlid,
        resource_id: DieselUlid,
        resource_name: &str,
        perm_level: ObjectMapping<DbPermissionLevel>,
        persistent_notification: bool,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Update user permissions
        let user = User::add_user_permission(
            &client,
            &user_id,
            HashMap::from_iter([(resource_id, perm_level)]),
        )
        .await?;

        // Create personal/persistent notification (if needed)
        if persistent_notification {
            let mut p_notification = PersistentNotification {
                id: DieselUlid::generate(),
                user_id,
                notification_variant: PersistentNotificationVariant::PERMISSION_GRANTED,
                message: format!("Permission granted for {} ({})", resource_name, resource_id),
                refs: Json(NotificationReferences(vec![NotificationReference {
                    reference_type: NotificationReferenceType::Resource,
                    reference_name: resource_name.to_string(),
                    reference_value: resource_id.to_string(),
                }])),
            };
            p_notification.create(&client).await?;
        }

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(user)
    }

    pub async fn remove_permission_from_user(
        &self,
        user_id: DieselUlid,
        resource_id: DieselUlid,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Fetch resource to validate it exists
        let resource = if let Some(resource) = self.cache.get_object(&resource_id) {
            resource
        } else {
            bail!("Object does not exist");
        };

        // Remove permission for specific resource from user
        let user = User::remove_user_permission(&client, &user_id, &resource_id).await?;

        // Create personal/persistent notification (no transaction needed)
        let mut p_notification = PersistentNotification {
            id: DieselUlid::generate(),
            user_id,
            notification_variant: PersistentNotificationVariant::PERMISSION_REVOKED,
            message: format!(
                "Permission revoked for {} ({})",
                resource.object.name, resource_id
            ),
            refs: Json(NotificationReferences(vec![NotificationReference {
                reference_type: NotificationReferenceType::Resource,
                reference_name: resource.object.name,
                reference_value: resource.object.id.to_string(),
            }])),
        };
        p_notification.create(&client).await?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(user)
    }

    pub async fn update_permission_from_user(
        &self,
        user_id: DieselUlid,
        resource_id: DieselUlid,
        permission: ObjectMapping<DbPermissionLevel>,
    ) -> Result<User> {
        let client = self.database.get_client().await?;

        // Remove permission for specific resource from user
        let user =
            User::update_user_permission(&client, &user_id, &resource_id, permission).await?;

        // Try to emit user updated notification(s)
        if let Err(err) = self
            .natsio_handler
            .register_user_event(&user, EventVariant::Updated)
            .await
        {
            // Log error (rollback transaction and return)
            log::error!("{}", err);
            //transaction.rollback().await?;
            return Err(anyhow::anyhow!("Notification emission failed"));
        }

        Ok(user)
    }

    //ToDo: Rust Doc
    pub async fn get_persistent_notifications(
        &self,
        user_id: DieselUlid,
    ) -> Result<Vec<PersonalNotification>> {
        let client = self.database.get_client().await?;

        // Fetch notifications from database
        let db_notifications =
            PersistentNotification::get_user_notifications(&user_id, &client).await?;

        // Convert to proto and return
        let proto_notifications: Vec<PersonalNotification> =
            db_notifications.into_iter().map(|m| m.into()).collect();

        Ok(proto_notifications)
    }

    //ToDo: Rust Doc
    pub async fn acknowledge_persistent_notifications(
        &self,
        notification_ids: Vec<String>,
    ) -> Result<()> {
        let client = self.database.get_client().await?;

        // Convert provided id strings to DieselUlids
        let result: Result<Vec<_>, _> = notification_ids
            .into_iter()
            .map(|id| DieselUlid::from_str(&id))
            .collect();

        let notification_ulids = tonic_invalid!(result, "Invalid notification ids provided");

        // Acknowledge notification (delete from persistent notifications table)
        PersistentNotification::acknowledge_user_notifications(&notification_ulids, &client)
            .await?;

        Ok(())
    }

    pub async fn add_oidc_provider(
        &self,
        user_id: DieselUlid,
        mapping: &OIDCMapping,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let user = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?;

        let mut new_attributes = user.attributes.0.clone();
        new_attributes.external_ids.push(mapping.clone());
        let user = User::set_user_attributes(&client, &user_id, Json(new_attributes)).await?;
        self.cache.update_user(&user_id, user.clone());
        Ok(user)
    }

    pub async fn remove_oidc_provider(
        &self,
        user_id: DieselUlid,
        provider_name: &str,
    ) -> Result<User> {
        let client = self.database.get_client().await?;
        let user = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?;

        let mut new_attributes = user.attributes.0.clone();
        if user.attributes.0.external_ids.len() == 1 {
            bail!("Cannot remove last external id");
        }
        new_attributes.external_ids.retain(|e| {
            if e.oidc_name != provider_name {
                true
            } else {
                false
            }
        });
        let user = User::set_user_attributes(&client, &user_id, Json(new_attributes)).await?;
        self.cache.update_user(&user_id, user.clone());
        Ok(user)
    }

    //ToDo: Rust Doc
    pub async fn request_resource_access(
        &self,
        request_user_ulid: DieselUlid,
        resource_ulid: DieselUlid,
    ) -> Result<()> {
        let client = self.database.get_client().await?;

        // Fetch resource and requesting user to validate they exist
        let resource = if let Some(resource) = self.cache.get_object(&resource_ulid) {
            resource
        } else {
            bail!("Object does not exist");
        };

        let request_user = if let Some(cache_user) = self.cache.get_user(&request_user_ulid) {
            cache_user
        } else {
            bail!("Requesting user does not exist");
        };

        // Create personal/persistent notification
        let mut p_notification = PersistentNotification {
            id: DieselUlid::generate(),
            user_id: resource.object.created_by,
            notification_variant: PersistentNotificationVariant::ACCESS_REQUESTED,
            message: format!(
                "{} ({}) requests access for {:?} {} ({})",
                request_user.display_name,
                request_user.id,
                resource.object.object_type,
                resource.object.name,
                resource_ulid
            ),
            refs: Json(NotificationReferences(vec![
                NotificationReference {
                    reference_type: NotificationReferenceType::User,
                    reference_name: request_user.display_name,
                    reference_value: request_user.id.to_string(),
                },
                NotificationReference {
                    reference_type: NotificationReferenceType::Resource,
                    reference_name: resource.object.name,
                    reference_value: resource.object.id.to_string(),
                },
            ])),
        };
        p_notification.create(&client).await?;

        Ok(())
    }
}
