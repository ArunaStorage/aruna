use super::{
    auth::AuthHandler, grpc_query_handler::GrpcQueryHandler,
    transforms::ExtractAccessKeyPermissions,
};
use crate::{
    database::{database::Database, persistence::WithGenericBytes},
    structs::{Object, ObjectLocation, ObjectType, PubKey, User},
};
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use s3s::{auth::SecretKey, path::S3Path};
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub enum ResourceString {
    Project(String),
    Collection(String, String),
    Dataset(String, Option<String>, String),
    Object(String, Option<String>, Option<String>, String),
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct ResourceStrings(pub Vec<ResourceString>);

impl TryFrom<&S3Path> for ResourceStrings {
    type Error = anyhow::Error;
    fn try_from(value: &S3Path) -> Result<Self> {
        if let Some((b, k)) = value.as_object() {
            let mut results = Vec::new();

            let pathvec = k.split('/').collect::<Vec<&str>>();
            match pathvec.len() {
                0 => {
                    results.push(ResourceString::Project(b.to_string()));
                }
                1 => {
                    results.push(ResourceString::Collection(
                        b.to_string(),
                        pathvec[0].to_string(),
                    ));
                    results.push(ResourceString::Dataset(
                        b.to_string(),
                        None,
                        pathvec[0].to_string(),
                    ));
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        None,
                        pathvec[0].to_string(),
                    ));
                }
                2 => {
                    results.push(ResourceString::Dataset(
                        b.to_string(),
                        Some(pathvec[0].to_string()),
                        pathvec[1].to_string(),
                    ));
                    results.push(ResourceString::Object(
                        b.to_string(),
                        Some(pathvec[0].to_string()),
                        None,
                        pathvec[1].to_string(),
                    ));
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        Some(pathvec[0].to_string()),
                        pathvec[1].to_string(),
                    ));
                }
                3 => {
                    results.push(ResourceString::Object(
                        b.to_string(),
                        Some(pathvec[0].to_string()),
                        Some(pathvec[1].to_string()),
                        pathvec[2].to_string(),
                    ));
                }
                _ => {
                    results.push(ResourceString::Object(
                        b.to_string(),
                        None,
                        None,
                        k.to_string(),
                    ));
                }
            }
            return Ok(ResourceStrings(results));
        } else {
            return Err(anyhow!("Invalid path"));
        }
    }
}

#[derive(Debug, Clone, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub enum ResourceIds {
    Project(DieselUlid),
    Collection(DieselUlid, DieselUlid),
    Dataset(DieselUlid, Option<DieselUlid>, DieselUlid),
    Object(
        DieselUlid,
        Option<DieselUlid>,
        Option<DieselUlid>,
        DieselUlid,
    ),
}

impl PartialEq<DieselUlid> for ResourceIds {
    fn eq(&self, other: &DieselUlid) -> bool {
        match self {
            ResourceIds::Project(id) => id == other,
            ResourceIds::Collection(_, id) => id == other,
            ResourceIds::Dataset(_, _, id) => id == other,
            ResourceIds::Object(_, _, _, id) => id == other,
        }
    }
}

impl ResourceIds {
    pub fn get_id(&self) -> DieselUlid {
        match self {
            ResourceIds::Project(id) => id.clone(),
            ResourceIds::Collection(_, id) => id.clone(),
            ResourceIds::Dataset(_, _, id) => id.clone(),
            ResourceIds::Object(_, _, _, id) => id.clone(),
        }
    }

    pub fn check_if_in(&self, id: DieselUlid) -> bool {
        match self {
            ResourceIds::Project(pid) => pid == &id,
            ResourceIds::Collection(pid, cid) => pid == &id || cid == &id,
            ResourceIds::Dataset(pid, cid, did) => {
                pid == &id || cid.unwrap_or_default() == id || did == &id
            }
            ResourceIds::Object(pid, cid, did, oid) => {
                pid == &id
                    || cid.unwrap_or_default() == id
                    || did.unwrap_or_default() == id
                    || oid == &id
            }
        }
    }
}

pub struct Cache {
    // Map with AccessKey as key and User as value
    pub users: DashMap<String, User, RandomState>,
    // HashMap that contains user_id <-> Vec<access_key> pairs
    pub user_access_keys: DashMap<DieselUlid, Vec<String>, RandomState>,
    // Map with ObjectId as key and Object as value
    pub resources: DashMap<DieselUlid, (Object, Option<ObjectLocation>), RandomState>,
    // Maps with bucket / key as key and set of all ObjectIds as value
    pub paths: DashMap<ResourceString, ResourceIds, RandomState>,
    // Pubkeys
    pub pubkeys: DashMap<i32, (PubKey, DecodingKey), RandomState>,
    // Persistence layer
    pub persistence: RwLock<Option<Database>>,
    pub aruna_client: RwLock<Option<Arc<GrpcQueryHandler>>>,
    pub auth: RwLock<Option<AuthHandler>>,
}

impl Cache {
    pub async fn new(
        notifications_url: Option<impl Into<String>>,
        with_persistence: bool,
        self_id: DieselUlid,
    ) -> Result<Arc<Self>> {
        let persistence = if with_persistence {
            RwLock::new(None)
        } else {
            RwLock::new(Some(Database::new().await?))
        };
        let cache = Arc::new(Cache {
            users: DashMap::default(),
            user_access_keys: DashMap::default(),
            resources: DashMap::default(),
            paths: DashMap::default(),
            pubkeys: DashMap::default(),
            persistence,
            aruna_client: RwLock::new(None),
            auth: RwLock::new(None),
        });
        if let Some(url) = notifications_url {
            let notication_handler: Arc<GrpcQueryHandler> =
                Arc::new(GrpcQueryHandler::new(url, cache.clone(), self_id.to_string()).await?);

            let notifications_handler_clone = notication_handler.clone();
            tokio::spawn(async move {
                notifications_handler_clone
                    .clone()
                    .create_notifications_channel()
                    .await
            });

            cache.set_notifications(notication_handler).await
        };

        let auth_handler = AuthHandler::new(cache.clone(), self_id);
        cache.set_auth(auth_handler).await;
        Ok(cache)
    }

    pub async fn set_notifications(&self, notifications: Arc<GrpcQueryHandler>) {
        let mut guard = self.aruna_client.write().await;
        *guard = Some(notifications);
    }

    pub async fn set_auth(&self, auth: AuthHandler) {
        let mut guard = self.auth.write().await;
        *guard = Some(auth);
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

    /// Requests a secret key from the cache
    pub async fn create_secret(
        &self,
        user: GrpcUser,
        access_key: Option<String>,
    ) -> Result<(String, String)> {
        let new_secret = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();

        let access_key = access_key.unwrap_or_else(|| user.id.to_string());

        let perm = user
            .extract_access_key_permissions()?
            .iter()
            .find(|e| e.0 == access_key.as_str())
            .ok_or_else(|| anyhow!("Access key not found"))?
            .1
            .clone();

        let user_access = User {
            access_key: access_key.to_string(),
            user_id: DieselUlid::from_str(&user.id)?,
            secret: new_secret.clone(),
            permissions: perm,
        };
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            user_access.upsert(&persistence.get_client().await?).await?;
        }

        self.users.insert(access_key.to_string(), user_access);

        Ok((access_key, new_secret))
    }

    pub async fn set_pubkeys(&self, pks: Vec<PubKey>) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            PubKey::delete_all(&persistence.get_client().await?).await?;
            for pk in pks.iter() {
                pk.upsert(&persistence.get_client().await?).await?;
            }
        }
        self.pubkeys.clear();
        for pk in pks.into_iter() {
            let dec_key = DecodingKey::from_ed_pem(
                format!(
                    "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
                    pk.key
                )
                .as_bytes(),
            )?;
            self.pubkeys.insert(pk.id, (pk.clone(), dec_key));
        }
        Ok(())
    }

    pub fn get_res_by_res_string(&self, res: ResourceString) -> Option<ResourceIds> {
        self.paths.get(&res).map(|e| e.value().clone())
    }

    pub fn get_name_trees(
        &self,
        resource_id: &str,
        variant: ObjectType,
    ) -> Result<Vec<(ResourceString, ResourceIds)>> {
        // FIXME: This is really inefficient, but should work in a first iteration
        let resource_id = DieselUlid::from_str(resource_id)?;
        let (initial_res, _) = self
            .resources
            .get(&resource_id)
            .ok_or_else(|| anyhow!("Resource not found"))?
            .clone();
        match variant {
            ObjectType::PROJECT => {
                return Ok(vec![(
                    ResourceString::Project(initial_res.name),
                    ResourceIds::Project(initial_res.id),
                )])
            }
            ObjectType::COLLECTION => {
                let mut res = Vec::new();
                for elem in self.resources.iter() {
                    if initial_res.children.contains(elem.key()) {
                        let other1 = self
                            .resources
                            .get(elem.key())
                            .ok_or_else(|| anyhow!("Resource not found"))?
                            .0
                            .clone();
                        res.push((
                            ResourceString::Collection(
                                other1.name.to_string(),
                                initial_res.name.clone(),
                            ),
                            ResourceIds::Collection(other1.id, initial_res.id),
                        ));
                    }
                }
                return Ok(res);
            }
            ObjectType::DATASET => {
                let mut res = Vec::new();
                for elem in self.resources.iter() {
                    if initial_res.children.contains(elem.key()) {
                        let other1 = self
                            .resources
                            .get(elem.key())
                            .ok_or_else(|| anyhow!("Resource not found"))?
                            .0
                            .clone();
                        if elem.value().0.object_type == ObjectType::PROJECT {
                            res.push((
                                ResourceString::Dataset(
                                    other1.name.to_string(),
                                    None,
                                    initial_res.name.clone(),
                                ),
                                ResourceIds::Dataset(other1.id, None, initial_res.id),
                            ));
                        } else {
                            for elem2 in self.resources.iter() {
                                if elem.value().0.children.contains(elem2.key())
                                    && elem2.value().0.object_type == ObjectType::COLLECTION
                                {
                                    let other2 = self
                                        .resources
                                        .get(elem2.key())
                                        .ok_or_else(|| anyhow!("Resource not found"))?
                                        .0
                                        .clone();
                                    res.push((
                                        ResourceString::Dataset(
                                            other2.name.to_string(),
                                            Some(other1.name.to_string()),
                                            initial_res.name.clone(),
                                        ),
                                        ResourceIds::Dataset(
                                            other2.id,
                                            Some(other1.id),
                                            initial_res.id,
                                        ),
                                    ));
                                }
                            }
                        }
                    }
                }
                return Ok(res);
            }
            ObjectType::OBJECT => {
                let mut res = Vec::new();
                for elem in self.resources.iter() {
                    if initial_res.children.contains(elem.key()) {
                        let other1 = self
                            .resources
                            .get(elem.key())
                            .ok_or_else(|| anyhow!("Resource not found"))?
                            .0
                            .clone();
                        if elem.value().0.object_type == ObjectType::PROJECT {
                            res.push((
                                ResourceString::Object(
                                    other1.name.to_string(),
                                    None,
                                    None,
                                    initial_res.name.clone(),
                                ),
                                ResourceIds::Object(other1.id, None, None, initial_res.id),
                            ));
                        } else if elem.value().0.object_type == ObjectType::COLLECTION {
                            for elem2 in self.resources.iter() {
                                if elem.value().0.children.contains(elem2.key())
                                    && elem2.value().0.object_type == ObjectType::COLLECTION
                                {
                                    let other2 = self
                                        .resources
                                        .get(elem2.key())
                                        .ok_or_else(|| anyhow!("Resource not found"))?
                                        .0
                                        .clone();
                                    res.push((
                                        ResourceString::Object(
                                            other2.name.to_string(),
                                            Some(other1.name.to_string()),
                                            None,
                                            initial_res.name.clone(),
                                        ),
                                        ResourceIds::Object(
                                            other2.id,
                                            Some(other1.id),
                                            None,
                                            initial_res.id,
                                        ),
                                    ));
                                }
                            }
                        } else {
                            for elem2 in self.resources.iter() {
                                if elem.value().0.children.contains(elem2.key()) {
                                    for elem3 in self.resources.iter() {
                                        if elem2.value().0.children.contains(elem3.key()) {
                                            let other2 = self
                                                .resources
                                                .get(elem2.key())
                                                .ok_or_else(|| anyhow!("Resource not found"))?
                                                .0
                                                .clone();
                                            let other3 = self
                                                .resources
                                                .get(elem3.key())
                                                .ok_or_else(|| anyhow!("Resource not found"))?
                                                .0
                                                .clone();
                                            res.push((
                                                ResourceString::Object(
                                                    other3.name.to_string(),
                                                    Some(other2.name.to_string()),
                                                    Some(other1.name.to_string()),
                                                    initial_res.name.clone(),
                                                ),
                                                ResourceIds::Object(
                                                    other3.id,
                                                    Some(other2.id),
                                                    Some(other1.id),
                                                    initial_res.id,
                                                ),
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                return Ok(res);
            }
        }
    }

    pub fn get_pubkey(&self, kid: i32) -> Result<(PubKey, DecodingKey)> {
        Ok(self
            .pubkeys
            .get(&kid)
            .ok_or_else(|| anyhow!("Pubkey not found"))?
            .clone())
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
            if let Some(persistence) = self.persistence.read().await.as_ref() {
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
            if let Some(persistence) = self.persistence.read().await.as_ref() {
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
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            object.upsert(&persistence.get_client().await?).await?;
            if let Some(l) = &location {
                l.upsert(&persistence.get_client().await?).await?;
            }
        }
        let object_id = object.id.clone();
        let obj_type = object.object_type.clone();
        self.resources.insert(object.id, (object, location));
        self.paths.retain(|_, v| v != &object_id);
        let tree = self.get_name_trees(&object_id.to_string(), obj_type)?;
        for (e, v) in tree {
            self.paths.insert(e, v);
        }
        Ok(())
    }

    pub async fn delete_object(&self, id: DieselUlid) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            Object::delete(&id, &persistence.get_client().await?).await?;
            ObjectLocation::delete(&id, &persistence.get_client().await?).await?;
        }
        self.resources.remove(&id);
        self.paths.retain(|_, v| v != &id);
        Ok(())
    }

    pub fn get_user_by_key(&self, access_key: &str) -> Option<User> {
        self.users.get(access_key).map(|e| e.value().clone())
    }

    pub fn is_user(&self, user_id: DieselUlid) -> bool {
        self.user_access_keys.get(&user_id).is_some()
    }

    pub fn is_resource(&self, resource_id: DieselUlid) -> bool {
        self.resources.get(&resource_id).is_some()
    }
}
