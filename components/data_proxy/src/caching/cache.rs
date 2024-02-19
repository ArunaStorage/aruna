use super::{auth::AuthHandler, grpc_query_handler::GrpcQueryHandler};
use crate::caching::grpc_query_handler::sort_objects;
use crate::data_backends::storage_backend::StorageBackend;
use crate::replication::replication_handler::ReplicationMessage;
use crate::structs::{AccessKeyPermissions, TypedRelation, User};
use crate::trace_err;
use crate::{
    database::{database::Database, persistence::WithGenericBytes},
    structs::{Object, ObjectLocation, ObjectType, PubKey},
};
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use async_channel::Sender;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use digest::KeyInit;
use jsonwebtoken::DecodingKey;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use s3s::auth::SecretKey;
use std::collections::{HashMap, VecDeque};
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio_postgres::GenericClient;
use tracing::{debug, error, info_span, trace, Instrument};

pub struct Cache {
    // Map DieselUlid as key and (User, Vec<String>) as value -> Vec<String> is a list of registered access keys -> access_keys
    users: DashMap<DieselUlid, Arc<RwLock<(User, Vec<String>)>>, RandomState>,
    // Permissions
    access_keys: DashMap<String, AccessKeyPermissions, RandomState>,
    // Map with ObjectId as key and Object as value
    resources: DashMap<
        DieselUlid,
        (Arc<RwLock<Object>>, Arc<RwLock<Option<ObjectLocation>>>),
        RandomState,
    >,
    bundles: DashMap<DieselUlid, Vec<DieselUlid>>,
    // Maps with path / key as key and set of all ObjectIds as value
    paths: SkipMap<String, DieselUlid>,
    // Pubkeys
    pubkeys: DashMap<i32, (PubKey, DecodingKey), RandomState>,
    // Persistence layer
    persistence: RwLock<Option<Database>>,
    pub(crate) aruna_client: RwLock<Option<Arc<GrpcQueryHandler>>>,
    pub(crate) auth: RwLock<Option<AuthHandler>>,
    pub(crate) sender: Sender<ReplicationMessage>,
    backend: Option<Arc<Box<dyn StorageBackend>>>,
}

impl Cache {
    #[tracing::instrument(
        level = "debug",
        skip(
            notifications_url,
            with_persistence,
            self_id,
            encoding_key,
            encoding_key_serial,
            backend
        )
    )]
    pub async fn new(
        notifications_url: Option<impl Into<String>>,
        with_persistence: bool,
        self_id: DieselUlid,
        encoding_key: String,
        encoding_key_serial: i32,
        sender: Sender<ReplicationMessage>,
        backend: Option<Arc<Box<dyn StorageBackend>>>,
    ) -> Result<Arc<Self>> {
        // Initialize cache
        let cache = Arc::new(Cache {
            users: DashMap::default(),
            access_keys: DashMap::default(),
            resources: DashMap::default(),
            bundles: DashMap::default(),
            paths: SkipMap::new(),
            pubkeys: DashMap::default(),
            persistence: RwLock::new(None),
            aruna_client: RwLock::new(None),
            auth: RwLock::new(None),
            sender,
            backend,
        });

        // Initialize auth handler
        let auth_handler =
            AuthHandler::new(cache.clone(), self_id, encoding_key, encoding_key_serial);

        // Set auth handler in cache
        cache.set_auth(auth_handler).await;

        // Set database conn in cache
        if with_persistence {
            let persistence = Database::new().await?;
            cache.set_persistence(persistence).await?;
        }

        // Fully sync cache (and database if persistent DataProxy)
        if let Some(url) = notifications_url {
            let notication_handler: Arc<GrpcQueryHandler> = Arc::new(trace_err!(
                GrpcQueryHandler::new(url, cache.clone(), self_id.to_string()).await
            )?);

            let notifications_handler_clone = notication_handler.clone();
            tokio::spawn(
                async move {
                    notifications_handler_clone
                        .clone()
                        .create_notifications_channel()
                        .await
                        .unwrap()
                }
                .instrument(info_span!("create_notifications_channel")),
            );

            cache.set_notifications(notication_handler).await;
            debug!("initialized notification handler");
        };

        Ok(cache)
    }

    #[tracing::instrument(level = "trace", skip(self, notifications))]
    async fn set_notifications(&self, notifications: Arc<GrpcQueryHandler>) {
        let mut guard = self.aruna_client.write().await;
        *guard = Some(notifications);
    }

    #[tracing::instrument(level = "trace", skip(self, auth))]
    async fn set_auth(&self, auth: AuthHandler) {
        let mut guard = self.auth.write().await;
        *guard = Some(auth);
    }

    #[tracing::instrument(level = "trace", skip(self, persistence))]
    async fn set_persistence(&self, persistence: Database) -> Result<()> {
        let persistence = self.sync_with_persistence(persistence).await?;
        let mut guard = self.persistence.write().await;
        *guard = Some(persistence);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, access_key))]
    /// Requests a secret key from the cache
    pub async fn get_secret(&self, access_key: &str) -> Result<SecretKey> {
        let secret = self
            .access_keys
            .get(access_key)
            .ok_or_else(|| anyhow!("User not found"))?
            .value()
            .secret
            .clone();
        if secret.is_empty() {
            error!("secret is empty");
            Err(anyhow!("Secret is empty"))
        } else {
            Ok(SecretKey::from(secret))
        }
    }

    #[tracing::instrument(level = "trace", skip(self, database))]
    pub async fn sync_with_persistence(&self, database: Database) -> Result<Database> {
        let client = database.get_client().await?;

        let access_keys = AccessKeyPermissions::get_all(&client).await?;
        let user_keys = access_keys.iter().fold(HashMap::new(), |mut map, elem| {
            map.entry(elem.user_id)
                .or_insert(Vec::new())
                .push(elem.access_key.clone());
            map
        });
        for key in access_keys.into_iter() {
            self.access_keys.insert(key.access_key.to_string(), key);
        }
        for user in User::get_all(&client).await? {
            let keys = user_keys.get(&user.user_id).cloned().unwrap_or_default();
            self.users
                .insert(user.user_id.clone(), Arc::new(RwLock::new((user, keys))));
        }
        debug!("synced users");

        self.sync_pubkeys(PubKey::get_all(&client).await?).await?;
        debug!("synced pubkeys");

        // Sort objects from database before sync
        let mut database_objects = Object::get_all(&client).await?;
        sort_objects(&mut database_objects);

        let mut prefixes: HashMap<DieselUlid, Vec<String>> = HashMap::new();

        for object in database_objects {
            let location = ObjectLocation::get_opt(&object.id, &client).await?;
            self.resources.insert(
                object.id,
                (
                    Arc::new(RwLock::new(object.clone())),
                    Arc::new(RwLock::new(location)),
                ),
            );
            if let Some(parents) = object.parents {
                for parent in parents {
                    let new_prefix = prefixes
                        .get(&parent.get_id())
                        .ok_or_else(|| anyhow!("Expected parent for non root object"))?
                        .iter()
                        .map(|e| {
                            let path = format!("{}/{}", e, object.name);
                            self.paths.insert(path.clone(), object.id);
                            path
                        })
                        .collect::<Vec<_>>();
                    prefixes.insert(object.id, new_prefix);
                }
            } else {
                prefixes.insert(object.id, vec![object.name.clone()]);
                self.paths.insert(object.name.clone(), object.id);
            }
        }
        debug!("synced objects");
        Ok(database)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    /// Requests a secret key from the cache
    pub async fn create_or_update_secret(
        &self,
        access_key: &str,
        user_id: &DieselUlid,
    ) -> Result<(String, String)> {
        let user = self
            .users
            .get(&user_id)
            .ok_or_else(|| anyhow!("User not found"))?
            .clone();
        let permissions = if user_id.to_string().as_str() == access_key {
            let mut user_info = user.write().await;
            user_info.1.push(access_key.to_string());
            user_info.0.personal_permissions.clone()
        } else {
            let mut user_info = user.write().await;
            let token = user_info
                .0
                .tokens
                .get(&DieselUlid::from_str(access_key)?)
                .cloned()
                .ok_or_else(|| anyhow!("Access key not found"))?;
            user_info.1.push(access_key.to_string());
            token
        };
        let new_secret = thread_rng()
            .sample_iter(&Alphanumeric)
            .take(30)
            .map(char::from)
            .collect::<String>();

        let new_access_key = AccessKeyPermissions {
            access_key: access_key.to_string(),
            user_id: *user_id,
            secret: new_secret.clone(),
            permissions: permissions,
        };

        if let Some(pers) = self.persistence.read().await.as_ref() {
            new_access_key
                .upsert(pers.get_client().await?.client())
                .await?;
        }
        self.access_keys
            .insert(access_key.to_string(), new_access_key);
        Ok((access_key.to_string(), new_secret))
    }

    #[tracing::instrument(level = "trace", skip(self, pks))]
    pub async fn sync_pubkeys(&self, pks: Vec<PubKey>) -> Result<()> {
        for pk in pks.into_iter() {
            trace!(pk = ?pk, "syncing pubkey");
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

    #[tracing::instrument(level = "trace", skip(self, pks))]
    pub async fn set_pubkeys(&self, pks: Vec<PubKey>) -> Result<()> {
        trace!(num_pks = pks.len(), "overwriting pks in persistence");
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            PubKey::delete_all(persistence.get_client().await?.client()).await?;
            for pk in pks.iter() {
                pk.upsert(persistence.get_client().await?.client()).await?;
            }
        }
        trace!("clearing pks in cache");
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
        trace!("updated pks in cache");
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_pubkey(&self, pk: PubKey) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            pk.upsert(persistence.get_client().await?.client()).await?;
        }
        let dec_key = DecodingKey::from_ed_pem(
            format!(
                "-----BEGIN PUBLIC KEY-----{}-----END PUBLIC KEY-----",
                pk.key
            )
            .as_bytes(),
        )?;
        self.pubkeys.insert(pk.id.into(), (pk, dec_key));
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, res))]
    pub fn get_resource_by_name(&self, res: &str) -> Option<DieselUlid> {
        self.paths.get(res)
    }

    #[tracing::instrument(level = "trace", skip(self, res))]
    pub fn get_full_resource_by_name(&self, res: &str) -> Option<Object> {
        let id = self.paths.get(&res).value().clone();
        self.resources.get(&id).map(|e| e.value().0.read().clone())
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_prefixes(&self, resource_id: &DieselUlid) -> Vec<(DieselUlid, String)> {
        let mut prefixes = VecDeque::from([(resource_id.clone(), "".to_string())]);
        let final_result = Vec::new();
        while let Some((id, name)) = prefixes.pop_front() {
            if let Some(parents) = self.get_parents(&id).await {
                for (parent_name, parent_id) in parents {
                    prefixes.push_back((parent_id, format!("{}/{}", parent_name, name)));
                }
            }
        }
        final_result
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_suffixes(&self, resource_id: &DieselUlid) -> Vec<(DieselUlid, String)> {
        let mut prefixes = VecDeque::from([(resource_id.clone(), "".to_string())]);
        let final_result = Vec::new();
        while let Some((id, name)) = prefixes.pop_front() {
            if let Some(children) = self.get_children(&id).await {
                for (child_name, child_id) in children {
                    prefixes.push_back((child_id, format!("{}/{}", name, child_name)));
                }
            }
        }
        final_result
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_name_trees(
        &self,
        resource_id: &DieselUlid,
        resource_name: String,
        new_name: Option<String>,
    ) -> (Vec<String>, Option<Vec<String>>) {
        let prefixes = self.get_prefixes(resource_id).await;
        let suffixes = self.get_suffixes(resource_id).await;
        let mut final_paths = Vec::new();
        let mut new_paths = Vec::new();
        for pre in prefixes.iter() {
            for suf in suffixes.iter() {
                final_paths.push(format!("{}/{}/{}", pre.1, resource_name, suf.1));
                if let Some(new_name) = &new_name {
                    new_paths.push(format!("{}/{}/{}", pre.1, new_name, suf.1));
                }
            }
        }
        (
            final_paths,
            if new_name.is_some() {
                Some(new_paths)
            } else {
                None
            },
        )
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_parents(&self, resource_id: &DieselUlid) -> Option<Vec<(String, DieselUlid)>> {
        let resource = self.resources.get(resource_id)?;
        let resource = resource.value().0.read().await;
        if let Some(parents) = &resource.parents {
            let mut collected_parents = Vec::new();
            for parent in parents {
                match parent {
                    TypedRelation::Project(id)
                    | TypedRelation::Collection(id)
                    | TypedRelation::Dataset(id)
                    | TypedRelation::Object(id) => {
                        let parent = self.resources.get(id)?;
                        let parent = parent.value().0.read().await;
                        collected_parents.push((parent.name.clone(), parent.id));
                    }
                }
            }
            return Some(collected_parents);
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_children(
        &self,
        resource_id: &DieselUlid,
    ) -> Option<Vec<(String, DieselUlid)>> {
        let resource = self.resources.get(resource_id)?;
        let resource = resource.value().0.read().await;
        if let Some(children) = &resource.children {
            let mut collected_children = Vec::new();
            for child in children {
                match child {
                    TypedRelation::Project(id)
                    | TypedRelation::Collection(id)
                    | TypedRelation::Dataset(id)
                    | TypedRelation::Object(id) => {
                        let child = self.resources.get(id)?;
                        let child = child.value().0.read().await;
                        collected_children.push((child.name.clone(), child.id));
                    }
                }
            }
            return Some(collected_children);
        }
        None
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_pubkey(&self, kid: i32) -> Result<(PubKey, DecodingKey)> {
        Ok(trace_err!(self
            .pubkeys
            .get(&kid)
            .ok_or_else(|| anyhow!("Pubkey not found")))?
        .clone())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn upsert_user(self: Arc<Cache>, user: GrpcUser) -> Result<()> {
        let user_id = trace_err!(DieselUlid::from_str(&user.id))?;
        let proxy_user = User::try_from(user)?;

        let (to_update, to_delete) = if let Some(user) = self.users.get(&user_id) {
            let mut user = user.value().write().await;
            let comparison = proxy_user.compare_permissions(&user.0);
            let new_keys = user
                .1
                .iter()
                .filter(|k| !comparison.1.contains(k))
                .cloned()
                .collect::<Vec<_>>();
            *user = (proxy_user.clone(), new_keys);
            comparison
        } else {
            self.users.insert(
                user_id,
                Arc::new(RwLock::new((proxy_user.clone(), Vec::new()))),
            );
            (vec![], vec![])
        };
        for key in to_delete {
            self.access_keys.remove(&key);
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                AccessKeyPermissions::delete(&key, persistence.get_client().await?.client())
                    .await?;
            }
        }
        for key in to_update {
            let mut access_key = self
                .access_keys
                .get(&key.0)
                .ok_or_else(|| anyhow!("Access key not found"))?
                .value()
                .clone();
            access_key.permissions = key.1;
            self.access_keys.insert(key.0, access_key.clone());
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                access_key
                    .upsert(persistence.get_client().await?.client())
                    .await?;
            }
        }
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            proxy_user
                .upsert(persistence.get_client().await?.client())
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn remove_user(&self, user_id: DieselUlid) -> Result<()> {
        if let Some((u, v)) = self.users.remove(&user_id) {
            for key in v.read().await.1.iter() {
                self.access_keys.remove(key.as_str());
                if let Some(persistence) = self.persistence.read().await.as_ref() {
                    AccessKeyPermissions::delete(key, persistence.get_client().await?.client())
                        .await?;
                }
            }
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                User::delete(&u, persistence.get_client().await?.client()).await?;
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object, location))]
    pub async fn upsert_object(
        &self,
        object: Object,
        location: Option<ObjectLocation>,
    ) -> Result<()> {
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            let mut client = persistence.get_client().await?;
            let transaction = client.transaction().await?;
            let transaction_client = transaction.client();

            object.upsert(transaction_client).await?;

            if let Some(l) = &location {
                l.upsert(transaction_client).await?;
            }
            transaction.commit().await?;
        }
        let old_name = if let Some(o) = self.resources.get(&object.id) {
            let (obj, loc) = o.value();
            let mut object = obj.write().await;
            let old_name = object.name.clone();
            *object = object.clone();
            if let Some(location) = location {
                *loc.write().await = Some(location);
            }
            old_name
        } else {
            self.resources.insert(
                object.id,
                (
                    Arc::new(RwLock::new(object.clone())),
                    Arc::new(RwLock::new(location)),
                ),
            );
            object.name.to_string()
        };

        if old_name != object.name && object.object_type != ObjectType::Bundle {
            self.update_object_name(object.id, old_name, object.name)
                .await?;
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, object_id, new_name))]
    pub async fn update_object_name(
        &self,
        object_id: DieselUlid,
        old_name: String,
        new_name: String,
    ) -> Result<()> {
        let (old_paths, new_paths) = self
            .get_name_trees(&object_id, old_name, Some(new_name))
            .await;
        for (old, new) in old_paths.into_iter().zip(new_paths.unwrap_or_default()) {
            let old_opt_entry = self.paths.remove(&old);
            if let Some(old_entry) = old_opt_entry {
                self.paths.insert(new, old_entry.value().clone());
            }
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn delete_object(&self, id: DieselUlid) -> Result<()> {
        // Remove object and location from database
        if let Some(persistence) = self.persistence.read().await.as_ref() {
            let mut client = persistence.get_client().await?;
            let transaction = client.transaction().await?;
            let transaction_client = transaction.client();

            ObjectLocation::delete(&id, transaction_client).await?;
            Object::delete(&id, transaction_client).await?;

            transaction.commit().await?;
        }

        // Remove data from storage backend
        if let Some(s3_backend) = &self.backend {
            if let Some(resource) = self.resources.get(&id) {
                let (_, loc) = resource.value();
                if let Some(location) = loc.read().await.as_ref() {
                    s3_backend.delete_object(location.clone()).await?;
                }
            }
        }
        // Remove object and location from cache
        let old = self
            .resources
            .remove(&id)
            .ok_or_else(|| anyhow!("Resource not found"))?;
        for p in self
            .get_name_trees(&id, old.1 .0.read().await.name.clone(), None)
            .await
            .0
        {
            self.paths.remove(&p);
        }
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_key_perms(&self, access_key: &str) -> Option<AccessKeyPermissions> {
        let result = self.access_keys.get(access_key).map(|e| e.value().clone());
        trace!(?result);
        result
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_resource(
        &self,
        resource_id: &DieselUlid,
    ) -> Result<(Object, Option<ObjectLocation>)> {
        let resource = trace_err!(self
            .resources
            .get(resource_id)
            .ok_or_else(|| anyhow!("Resource not found")))?;
        Ok(resource.clone())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_path(&self, path: &str) -> Option<DieselUlid> {
        self.paths.get(path).map(|e| e.value().clone())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_stripped_path_range(&self, prefix: &str, skip: &str) -> Vec<(String, DieselUlid)> {
        self.paths
            .range(format!("{prefix}/{skip}")..=format!("{prefix}~"))
            .map(|e| {
                (
                    e.key().strip_prefix(prefix).unwrap_or_default().to_string(),
                    e.value().clone(),
                )
            })
            .collect()
    }
}
