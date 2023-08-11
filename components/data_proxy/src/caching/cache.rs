use super::{grpc_query_handler::GrpcQueryHandler, transforms::ExtractAccessKeyPermissions};
use crate::{
    database::{database::Database, persistence::WithGenericBytes},
    structs::{Object, ObjectLocation, PubKey, User},
};
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use aruna_rust_api::api::storage::services::v2::Pubkey;
use dashmap::{DashMap, DashSet};
use diesel_ulid::DieselUlid;
use s3s::auth::SecretKey;
use std::{
    str::FromStr,
    sync::{Arc, RwLock},
};

pub struct Cache {
    // Map with AccessKey as key and User as value
    pub users: DashMap<String, User, RandomState>,
    // HashMap that contains user_id <-> Vec<access_key> pairs
    pub user_access_keys: DashMap<DieselUlid, Vec<String>, RandomState>,
    // Map with ObjectId as key and Object as value
    pub resources: DashMap<DieselUlid, (Object, Option<ObjectLocation>), RandomState>,
    // Maps with path as key and set of ObjectIds as value
    pub paths: DashMap<String, DashSet<DieselUlid>, RandomState>,
    // Persistence layer
    pub persistence: Option<Arc<Database>>,
    pub notifications: Option<GrpcQueryHandler>,
}

impl Cache {
    pub async fn new(
        notifications_url: Option<impl Into<String>>,
        with_persistence: bool,
    ) -> Result<Arc<RwLock<Self>>> {
        let persistence = if with_persistence {
            None
        } else {
            Some(Arc::new(Database::new()?))
        };
        let cache = Arc::new(RwLock::new(Cache {
            users: DashMap::default(),
            user_access_keys: DashMap::default(),
            resources: DashMap::default(),
            paths: DashMap::default(),
            persistence,
            notifications: None,
        }));
        let notifications = match notifications_url {
            Some(s) => Some(GrpcQueryHandler::new(s, cache.clone()).await?),
            None => None,
        };
        cache.write().unwrap().notifications = notifications;
        Ok(cache)
    }

    /// Requests a secret key from the cache
    pub fn get_secret(&self, access_key: &str) -> Result<SecretKey> {
        Ok(SecretKey::from(
            self.users
                .get(access_key)
                .ok_or_else(|| anyhow!("User not found"))?
                .secret
                .as_ref(),
        ))
    }

    pub async fn set_pubkeys(&self, pks: Vec<PubKey>) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            PubKey::delete_all(&persistence.get_client().await?).await?;
            for pk in pks {
                pk.upsert(&persistence.get_client().await?).await?;
            }
        }

        Ok(())
    }

    pub async fn upsert_user(&self, user: GrpcUser) -> Result<()> {
        let user_id = DieselUlid::from_str(&user.id)?;
        let mut access_ids = Vec::new();
        for (key, perm) in user.extract_access_key_permissions()?.into_iter() {
            let user_access = User {
                access_key: key.clone(),
                user_id,
                secret: self
                    .get_secret(&key)
                    .map(|k| k.expose().to_string())
                    .unwrap_or_default(),
                permissions: perm,
            };
            if let Some(persistence) = &self.persistence {
                user_access.upsert(&persistence.get_client().await?).await?;
            }
            self.users.insert(key.clone(), user_access);
            access_ids.push(key);
        }
        self.user_access_keys.insert(user_id, access_ids);
        Ok(())
    }

    pub async fn remove_user(&self, user_id: DieselUlid) -> Result<()> {
        let keys = self
            .user_access_keys
            .remove(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?
            .1;

        for key in keys {
            let user = self.users.remove(&key);
            if let Some(persistence) = &self.persistence {
                if let Some((_, user)) = user {
                    User::delete(&user.user_id.to_string(), &persistence.get_client().await?)
                        .await?;
                }
            }
        }
        Ok(())
    }

    pub async fn upsert_object(
        &self,
        object: Object,
        location: Option<ObjectLocation>,
    ) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            object.upsert(&persistence.get_client().await?).await?;
            if let Some(l) = &location {
                l.upsert(&persistence.get_client().await?).await?;
            }
        }
        self.paths.insert(
            object.name.to_string(),
            DashSet::from_iter(object.clone().children.into_iter()),
        );
        self.resources.insert(object.id, (object, location));
        Ok(())
    }

    pub async fn delete_object(&self, id: DieselUlid) -> Result<()> {
        if let Some(persistence) = &self.persistence {
            Object::delete(&id, &persistence.get_client().await?).await?;
            ObjectLocation::delete(&id, &persistence.get_client().await?).await?;
        }
        let old = self.resources.remove(&id);
        if let Some((_, (obj, _))) = old {
            self.paths.remove(&obj.name);
        };
        Ok(())
    }

    pub fn is_user(&self, user_id: DieselUlid) -> bool {
        self.user_access_keys.get(&user_id).is_some()
    }

    pub fn is_resource(&self, resource_id: DieselUlid) -> bool {
        self.resources.get(&resource_id).is_some()
    }
}
