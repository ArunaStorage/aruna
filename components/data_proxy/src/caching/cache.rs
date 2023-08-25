use super::{
    auth::AuthHandler, grpc_query_handler::GrpcQueryHandler,
    transforms::ExtractAccessKeyPermissions,
};
use crate::structs::TypedRelation;
use crate::{
    database::{database::Database, persistence::WithGenericBytes},
    structs::{Object, ObjectLocation, ObjectType, PubKey, ResourceIds, ResourceString, User},
};
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use s3s::auth::SecretKey;
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;

pub struct Cache {
    // Map with AccessKey as key and User as value
    pub users: DashMap<String, User, RandomState>,
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
        encoding_key: String,
        encoding_key_serial: i32,
    ) -> Result<Arc<Self>> {
        let cache = Arc::new(Cache {
            users: DashMap::default(),
            resources: DashMap::default(),
            paths: DashMap::default(),
            pubkeys: DashMap::default(),
            persistence: RwLock::new(None),
            aruna_client: RwLock::new(None),
            auth: RwLock::new(None),
        });
        let auth_handler =
            AuthHandler::new(cache.clone(), self_id, encoding_key, encoding_key_serial);
        cache.set_auth(auth_handler).await;
        if let Some(url) = notifications_url {
            let notication_handler: Arc<GrpcQueryHandler> =
                Arc::new(GrpcQueryHandler::new(url, cache.clone(), self_id.to_string()).await?);

            let notifications_handler_clone = notication_handler.clone();
            tokio::spawn(async move {
                notifications_handler_clone
                    .clone()
                    .create_notifications_channel()
                    .await
                    .unwrap()
            });

            cache.set_notifications(notication_handler).await
        };

        if with_persistence {
            let persistence = Database::new().await?;
            cache.set_persistence(persistence).await?;
        }

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

    pub async fn set_persistence(&self, persistence: Database) -> Result<()> {
        let persistence = self.sync_with_persistence(persistence).await?;
        let mut guard = self.persistence.write().await;
        *guard = Some(persistence);
        Ok(())
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

    pub async fn sync_with_persistence(&self, database: Database) -> Result<Database> {
        let client = database.get_client().await?;
        for user in User::get_all(&client).await? {
            self.users.insert(user.access_key.clone(), user);
        }

        self.set_pubkeys(PubKey::get_all(&client).await?).await?;

        for object in Object::get_all(&client).await? {
            let location = ObjectLocation::get(&object.id, &client).await?;
            self.resources
                .insert(object.id, (object.clone(), Some(location.clone())));
            let tree = self.get_name_trees(&object.id.to_string(), object.object_type)?;
            for (e, v) in tree {
                self.paths.insert(e, v);
            }
        }

        Ok(database)
    }

    /// Requests a secret key from the cache
    pub async fn create_or_get_secret(
        &self,
        user: GrpcUser,
        access_key: Option<String>,
    ) -> Result<(String, String)> {
        let access_key = access_key.unwrap_or_else(|| user.id.to_string());

        match self
            .users
            .get(&access_key)
            .map(|e| e.value().secret.clone())
        {
            Some(secret) => {
                if !secret.is_empty() {
                    return Ok((access_key, secret));
                }
            }
            None => {}
        }

        let new_secret = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();

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
                    "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                    pk.key
                )
                .as_bytes(),
            )?;
            self.pubkeys.insert(pk.id.into(), (pk.clone(), dec_key));
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
            ObjectType::Project => Ok(vec![(
                ResourceString::Project(initial_res.name),
                ResourceIds::Project(initial_res.id),
            )]),
            ObjectType::Collection => {
                let mut res = Vec::new();
                for parent in initial_res
                    .parents
                    .ok_or_else(|| anyhow!("Collection has no parents"))?
                {
                    match parent {
                        TypedRelation::Project(parent_id) => {
                            let parent_full = self
                                .resources
                                .get(&parent_id)
                                .ok_or_else(|| anyhow!("Parent for collection not found"))?
                                .0
                                .clone();
                            res.push((
                                ResourceString::Collection(
                                    parent_full.name.to_string(),
                                    initial_res.name.clone(),
                                ),
                                ResourceIds::Collection(parent_id, initial_res.id),
                            ))
                        }
                        _ => {
                            return Err(anyhow!(
                                "Collections cant have parents other than projects"
                            ))
                        }
                    }
                }
                Ok(res)
            }
            ObjectType::Dataset => {
                let mut res = Vec::new();
                for parent in initial_res
                    .parents
                    .ok_or_else(|| anyhow!("No parents found for dataset"))?
                {
                    match parent {
                        TypedRelation::Project(parent_id) => {
                            let parent_full = self
                                .resources
                                .get(&parent_id)
                                .ok_or_else(|| anyhow!("Parent for dataset not found"))?
                                .0
                                .clone();
                            res.push((
                                ResourceString::Dataset(
                                    parent_full.name.to_string(),
                                    None,
                                    initial_res.name.clone(),
                                ),
                                ResourceIds::Dataset(parent_id, None, initial_res.id),
                            ))
                        }
                        TypedRelation::Collection(parent_id) => {
                            let parent_full = self
                                .resources
                                .get(&parent_id)
                                .ok_or_else(|| anyhow!("No parent found"))?
                                .0
                                .clone();
                            for grand_parent in parent_full
                                .parents
                                .ok_or_else(|| anyhow!("No parent found"))?
                            {
                                match grand_parent {
                                    TypedRelation::Project(grand_parent_id) => {
                                        let grand_parent_full = self
                                            .resources
                                            .get(&grand_parent_id)
                                            .ok_or_else(|| {
                                                anyhow!("Parent for collection not found")
                                            })?
                                            .0
                                            .clone();
                                        res.push((
                                            ResourceString::Dataset(
                                                grand_parent_full.name.to_string(),
                                                Some(parent_full.name.to_string()),
                                                initial_res.name.clone(),
                                            ),
                                            ResourceIds::Dataset(
                                                grand_parent_id,
                                                Some(parent_id),
                                                initial_res.id,
                                            ),
                                        ))
                                    }
                                    _ => {
                                        return Err(anyhow!(
                                            "Collections cant have parents other than projects"
                                        ))
                                    }
                                }
                            }
                        }
                        _ => {
                            return Err(anyhow!(
                                "Datasets cannot have parents other than projects and collections"
                            ))
                        }
                    }
                }
                Ok(res)
            }
            ObjectType::Object => {
                let mut res = Vec::new();
                for parent in initial_res
                    .parents
                    .ok_or_else(|| anyhow!("No parents found for object"))?
                {
                    match parent {
                        TypedRelation::Project(parent_id) => {
                            let full_parent = self
                                .resources
                                .get(&parent_id)
                                .ok_or_else(|| anyhow!("Parent not found"))?
                                .0
                                .clone();
                            res.push((
                                ResourceString::Object(
                                    full_parent.name.to_string(),
                                    None,
                                    None,
                                    initial_res.name.clone(),
                                ),
                                ResourceIds::Object(parent_id, None, None, initial_res.id),
                            ))
                        }
                        TypedRelation::Collection(parent_id) => {
                            let parent_full = self
                                .resources
                                .get(&parent_id)
                                .ok_or_else(|| anyhow!("No parent found"))?
                                .0
                                .clone();
                            for grand_parent in parent_full
                                .parents
                                .ok_or_else(|| anyhow!("No parent found"))?
                            {
                                match grand_parent {
                                    TypedRelation::Project(grand_parent_id) => {
                                        let grand_parent_full = self
                                            .resources
                                            .get(&grand_parent_id)
                                            .ok_or_else(|| {
                                                anyhow!("Parent for collection not found")
                                            })?
                                            .0
                                            .clone();
                                        res.push((
                                            ResourceString::Object(
                                                grand_parent_full.name.to_string(),
                                                Some(parent_full.name.to_string()),
                                                None,
                                                initial_res.name.clone(),
                                            ),
                                            ResourceIds::Object(
                                                grand_parent_id,
                                                Some(parent_id),
                                                None,
                                                initial_res.id,
                                            ),
                                        ))
                                    }
                                    _ => {
                                        return Err(anyhow!(
                                            "Collections cant have parents other than projects"
                                        ))
                                    }
                                }
                            }
                        }
                        TypedRelation::Dataset(parent_id) => {
                            let parent_full = self
                                .resources
                                .get(&parent_id)
                                .ok_or_else(|| anyhow!("No parent found"))?
                                .0
                                .clone();
                            for grand_parent in parent_full
                                .parents
                                .ok_or_else(|| anyhow!("Parent not found"))?
                            {
                                match grand_parent {
                                    TypedRelation::Project(project_parent_id) => {
                                        let project_parent_full = self
                                            .resources
                                            .get(&project_parent_id)
                                            .ok_or_else(|| anyhow!("Parent for dataset not found"))?
                                            .0
                                            .clone();
                                        res.push((
                                            ResourceString::Object(
                                                project_parent_full.name.to_string(),
                                                None,
                                                Some(parent_full.name.to_string()),
                                                initial_res.name.clone(),
                                            ),
                                            ResourceIds::Object(
                                                project_parent_id,
                                                None,
                                                Some(parent_id),
                                                initial_res.id,
                                            ),
                                        ))
                                    }
                                    TypedRelation::Collection(collection_parent_id) => {
                                        let collection_parent_full = self
                                            .resources
                                            .get(&collection_parent_id)
                                            .ok_or_else(|| anyhow!("Parent for dataset not found"))?
                                            .0
                                            .clone();
                                        for project_parent in
                                            collection_parent_full.parents.ok_or_else(|| {
                                                anyhow!("No parents found for collection")
                                            })?
                                        {
                                            match project_parent {
                                                TypedRelation::Project(project_parent_id) => {
                                                    let project_parent_full = self.resources.get(&project_parent_id).ok_or_else(|| anyhow!("Parent for dataset not found"))?.0.clone();
                                                    res.push((
                                                        ResourceString::Object(
                                                            project_parent_full.name.to_string(),
                                                            Some(collection_parent_full.name.to_string()),
                                                            Some(parent_full.name.to_string()),
                                                            initial_res.name.clone(),
                                                        ),
                                                        ResourceIds::Object(
                                                            project_parent_id,
                                                            Some(collection_parent_id),
                                                            Some(parent_id),
                                                            initial_res.id,
                                                        )
                                                        ))
                                                },
                                                _ => return Err(anyhow!("Collections cannot have parents other than projects"))

                                            }
                                        }
                                    }
                                    _ => {
                                        return Err(anyhow!(
                                            "Datasets can only have collection or project children"
                                        ))
                                    }
                                }
                            }
                        }
                        _ => return Err(anyhow!("Objects cannot have object parents")),
                    }
                }
                Ok(res)
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
        Ok(())
    }

    pub async fn remove_user(&self, user_id: DieselUlid) -> Result<()> {
        let keys = self
            .users
            .iter()
            .filter_map(|k| {
                if k.value().user_id == user_id {
                    Some(k.key().clone())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

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
        let object_id = object.id;
        let obj_type = object.object_type.clone();

        let location = if let Some(o) = self.resources.get(&object.id) {
            if location.is_none() {
                o.value().1.clone()
            } else {
                location
            }
        } else {
            location
        };

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
}
