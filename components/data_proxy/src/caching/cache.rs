use super::{
    auth::AuthHandler, grpc_query_handler::GrpcQueryHandler,
    transforms::ExtractAccessKeyPermissions,
};
use crate::caching::grpc_query_handler::sort_objects;
use crate::data_backends::storage_backend::StorageBackend;
use crate::replication::replication_handler::ReplicationMessage;
use crate::structs::{DbPermissionLevel, TypedRelation};
use crate::trace_err;
use crate::{
    database::{database::Database, persistence::WithGenericBytes},
    structs::{Object, ObjectLocation, ObjectType, PubKey, User},
};
use ahash::RandomState;
use anyhow::Result;
use anyhow::{anyhow, bail};
use aruna_rust_api::api::storage::models::v2::User as GrpcUser;
use async_channel::Sender;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use jsonwebtoken::DecodingKey;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use s3s::auth::SecretKey;
use std::collections::{HashMap, VecDeque};
use std::{str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tokio_postgres::GenericClient;
use tracing::{debug, error, info_span, trace, Instrument};

pub struct Cache {
    // Map with AccessKey as key and User as value
    pub users: DashMap<String, Arc<RwLock<User>>, RandomState>,
    // Map with ObjectId as key and Object as value
    pub resources: DashMap<
        DieselUlid,
        (Arc<RwLock<Object>>, Arc<RwLock<Option<ObjectLocation>>>),
        RandomState,
    >,
    // Maps with path / key as key and set of all ObjectIds as value
    pub paths: SkipMap<String, DieselUlid>,
    // Pubkeys
    pub pubkeys: DashMap<i32, (PubKey, DecodingKey), RandomState>,
    // Persistence layer
    pub persistence: RwLock<Option<Database>>,
    pub aruna_client: RwLock<Option<Arc<GrpcQueryHandler>>>,
    pub auth: RwLock<Option<AuthHandler>>,
    pub sender: Sender<ReplicationMessage>,
    pub backend: Option<Arc<Box<dyn StorageBackend>>>,
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
            resources: DashMap::default(),
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
    pub async fn set_notifications(&self, notifications: Arc<GrpcQueryHandler>) {
        let mut guard = self.aruna_client.write().await;
        *guard = Some(notifications);
    }

    #[tracing::instrument(level = "trace", skip(self, auth))]
    pub async fn set_auth(&self, auth: AuthHandler) {
        let mut guard = self.auth.write().await;
        *guard = Some(auth);
    }

    #[tracing::instrument(level = "trace", skip(self, persistence))]
    pub async fn set_persistence(&self, persistence: Database) -> Result<()> {
        let persistence = self.sync_with_persistence(persistence).await?;
        let mut guard = self.persistence.write().await;
        *guard = Some(persistence);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self, access_key))]
    /// Requests a secret key from the cache
    pub async fn get_secret(&self, access_key: &str) -> Result<SecretKey> {
        let secret = trace_err!(self
            .users
            .get(access_key)
            .ok_or_else(|| anyhow!("User not found")))?
        .value()
        .read()
        .await
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
        for user in User::get_all(&client).await? {
            self.users
                .insert(user.access_key.clone(), Arc::new(RwLock::new(user)));
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
    pub async fn create_or_get_secret(
        &self,
        user: GrpcUser,
        access_key: Option<String>,
    ) -> Result<(String, String)> {
        let access_key = access_key.unwrap_or_else(|| user.id.to_string());
        trace!(access_key = ?access_key);
        let perm = trace_err!(user
            .extract_access_key_permissions()?
            .iter()
            .find(|e| e.0 == access_key.as_str())
            .ok_or_else(|| anyhow!("Access key not found")))?
        .1
        .clone();

        if let Some(k) = self.users.get(access_key.as_str()) {
            let user = k.value().clone();
            let user_ref = user.read().await;
            return Ok((user_ref.access_key.clone(), user_ref.secret.clone()));
        } else {
            let new_secret = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(30)
                .map(char::from)
                .collect::<String>();

            let user = Arc::new(User {
                access_key: access_key.to_string(),
                user_id: DieselUlid::from_str(&user.id)?,
                secret: new_secret.clone(),
                admin: false,
                permissions: perm,
            });

            cache.users.insert(access_key.clone(), user.clone());

            if let Some(persistence) = cache.persistence.read().await.as_ref() {
                user.upsert(persistence.get_client().await?.client())
                    .await?;
            }

            return Ok((access_key, new_secret));
        }
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
    #[tracing::instrument(level = "trace", skip(self, res))]
    pub fn get_full_resource_by_name(
        &self,
        res: ResourceString,
    ) -> Option<(Object, Option<ObjectLocation>)> {
        self.paths
            .get(&res)
            .and_then(|e| {
                let id = e.value().clone();
                self.resources.get(&id.get_id()).map(|e| e.value().clone())
            })
            .clone()
    }

    #[tracing::instrument(level = "trace", skip(self, res))]
    pub fn get_resource_by_name(&self, res: String) -> Option<DieselUlid> {
        self.paths.get(&res)
    }

    #[tracing::instrument(level = "trace", skip(self, resource_id))]
    pub async fn get_parent_names(
        &self,
        resource_id: &DieselUlid,
    ) -> Option<Vec<(String, DieselUlid)>> {
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
    pub async fn get_name_trees(
        &self,
        resource_id: DieselUlid,
    ) -> Result<Vec<(String, DieselUlid)>> {
        let resource = self
            .resources
            .get(&resource_id)
            .ok_or_else(|| anyhow!("Resource not found"))?
            .value()
            .0
            .read()
            .await;

        // BackTrace to root projects
        let mut queue = VecDeque::from([(resource_id, resource.name.clone())]);
        let mut prefixes = Vec::new();

        loop {
            if let Some(front) = queue.pop_front() {
                match self.get_parent_names(&front.0).await {
                    Some(parents) => {
                        parents.into_iter().for_each(|e| {
                            let mut prefix = e.0;
                            prefix.push_str("/");
                            prefix.push_str(&front.1);
                            queue.push_back((e.1, prefix));
                        });
                    }
                    None => {
                        prefixes.push((front.1, front.0));
                    }
                }
            } else {
                break;
            }
        }
        // Backtrace down to leafs
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

        tokio::spawn(
            async move {
                for (key, perm) in user.extract_access_key_permissions()?.into_iter() {
                    loop {
                        if let Some(e) = self.users.try_entry(key.to_string()) {
                            let user = {
                                let mut mut_entry = trace_err!(e.or_try_insert_with(|| {
                                    Ok::<_, anyhow::Error>(User {
                                        access_key: key.to_string(),
                                        user_id,
                                        secret: "".to_string(),
                                        admin: false,
                                        permissions: HashMap::default(),
                                    })
                                }))?;
                                mut_entry.value_mut().permissions = perm.clone();

                                mut_entry.clone()
                            };
                            if let Some(persistence) = self.persistence.read().await.as_ref() {
                                user.upsert(persistence.get_client().await?.client())
                                    .await?;
                            }
                            break;
                        }
                    }
                }
                Ok::<(), anyhow::Error>(())
            }
            .instrument(info_span!("upsert_user")),
        )
        .await??;
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn add_permission_to_access_key(
        &self,
        access_key: &str,
        permission: (DieselUlid, DbPermissionLevel),
    ) -> Result<()> {
        if let Some(mut user) = self.users.get_mut(access_key) {
            user.value_mut()
                .permissions
                .insert(permission.0, permission.1);

            if let Some(persistence) = self.persistence.read().await.as_ref() {
                user.value()
                    .upsert(persistence.get_client().await?.client())
                    .await?;
            }

            Ok(())
        } else {
            error!("user not found");
            Err(anyhow!("User not found"))
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub async fn _remove_permission_from_access_key(
        &self,
        access_key: &str,
        permission: &DieselUlid,
    ) -> Result<()> {
        if let Some(mut user) = self.users.get_mut(access_key) {
            user.value_mut().permissions.remove(permission);

            if let Some(persistence) = self.persistence.read().await.as_ref() {
                user.value()
                    .upsert(persistence.get_client().await?.client())
                    .await?;
            }

            Ok(())
        } else {
            error!("user not found");
            Err(anyhow!("User not found"))
        }
    }

    #[tracing::instrument(level = "trace", skip(self))]
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

        trace!("gathered user keys");

        for key in keys {
            let user = self.users.remove(&key);
            if let Some(persistence) = self.persistence.read().await.as_ref() {
                if let Some((_, user)) = user {
                    User::delete(
                        &user.user_id.to_string(),
                        persistence.get_client().await?.client(),
                    )
                    .await?;
                }
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

        self.resources.insert(object.id, (object.clone(), location));
        trace!("inserted into cache");
        self.paths.retain(|_, v| v != &object_id);
        trace!("inserted paths");

        if object.object_type != ObjectType::Bundle {
            trace!("Getting name trees because full_sync");
            let tree = self.get_name_trees(&object_id.to_string(), obj_type)?;
            for (e, v) in tree {
                self.paths.insert(e, v);
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
                if let Some(location) = loc {
                    s3_backend.delete_object(location.clone()).await?;
                }
            }
        }

        // Remove object and location from cache
        self.resources.remove(&id);
        self.paths.retain(|_, v| v != &id);
        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_user_by_key(&self, access_key: &str) -> Option<User> {
        let result = self.users.get(access_key).map(|e| e.value().clone());
        trace!(?result);
        result
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_resource_ids_from_id(
        &self,
        id: DieselUlid,
    ) -> Result<(Vec<ResourceIds>, TypedRelation)> {
        let mut result = Vec::new();
        let rel;

        if let Some(resource) = self.resources.get(&id) {
            let (object, _) = resource.value();

            match object.object_type {
                ObjectType::Bundle => {
                    error!("Bundles are not allowed in this context");
                    bail!("Bundles are not allowed in this context")
                }
                ObjectType::Project => {
                    rel = TypedRelation::Project(object.id);
                    result.push(ResourceIds::Project(object.id))
                }
                ObjectType::Collection => {
                    rel = TypedRelation::Collection(object.id);
                    for parent in trace_err!(object
                        .parents
                        .as_ref()
                        .ok_or_else(|| anyhow!("Invalid collection, missing parent")))?
                    {
                        match parent {
                            TypedRelation::Project(parent_id) => {
                                result.push(ResourceIds::Collection(*parent_id, object.id))
                            }
                            _ => {
                                error!("Invalid collection, parent is not a project");
                                bail!("Invalid collection, parent is not a project")
                            }
                        }
                    }
                }
                ObjectType::Dataset => {
                    rel = TypedRelation::Dataset(object.id);
                    for parent in trace_err!(object
                        .parents
                        .as_ref()
                        .ok_or_else(|| anyhow!("Invalid dataset, missing parent")))?
                    {
                        match parent {
                            TypedRelation::Project(parent_id) => {
                                result.push(ResourceIds::Dataset(*parent_id, None, object.id))
                            }
                            TypedRelation::Collection(parent_id) => {
                                if let Some(resource) = self.resources.get(parent_id) {
                                    let (collection_object, _) = resource.value();

                                    for parent_parent in trace_err!(collection_object
                                        .parents
                                        .as_ref()
                                        .ok_or_else(|| anyhow!("Invalid dataset, missing parent")))?
                                    {
                                        match parent_parent {
                                            TypedRelation::Project(project_id) => {
                                                result.push(ResourceIds::Dataset(
                                                    *project_id,
                                                    Some(*parent_id),
                                                    object.id,
                                                ))
                                            }
                                            _ => {
                                                error!(
                                                    "Invalid dataset, parent is not a collection"
                                                );
                                                bail!("Invalid dataset, parent is not a collection")
                                            }
                                        }
                                    }
                                } else {
                                    error!("Invalid dataset, parent is not a project");
                                    bail!("Invalid dataset, parent is not a project")
                                }
                            }
                            _ => {
                                error!("Invalid dataset, parent is not a project");
                                bail!("Invalid dataset, parent is not a project")
                            }
                        }
                    }
                }
                ObjectType::Object => {
                    rel = TypedRelation::Object(object.id);
                    for parent in trace_err!(object
                        .parents
                        .as_ref()
                        .ok_or_else(|| anyhow!("Invalid dataset, missing parent")))?
                    {
                        match parent {
                            TypedRelation::Project(parent_id) => {
                                result.push(ResourceIds::Object(*parent_id, None, None, object.id))
                            }
                            TypedRelation::Collection(parent_id) => {
                                if let Some(resource) = self.resources.get(parent_id) {
                                    let (collection_object, _) = resource.value();

                                    for parent_parent in trace_err!(collection_object
                                        .parents
                                        .as_ref()
                                        .ok_or_else(|| anyhow!("Invalid dataset, missing parent")))?
                                    {
                                        match parent_parent {
                                            TypedRelation::Project(project_id) => {
                                                result.push(ResourceIds::Object(
                                                    *project_id,
                                                    Some(*parent_id),
                                                    None,
                                                    object.id,
                                                ))
                                            }
                                            _ => {
                                                error!(
                                                    "Invalid dataset, parent is not a collection"
                                                );
                                                bail!("Invalid dataset, parent is not a collection")
                                            }
                                        }
                                    }
                                } else {
                                    error!("Invalid dataset, parent is not a project");
                                    bail!("Invalid dataset, parent is not a project")
                                }
                            }
                            TypedRelation::Dataset(parent_id) => {
                                if let Some(resource) = self.resources.get(parent_id) {
                                    let (dataset_object, _) = resource.value();

                                    for parent_parent in trace_err!(dataset_object
                                        .parents
                                        .as_ref()
                                        .ok_or_else(|| anyhow!("Invalid dataset, missing parent")))?
                                    {
                                        match parent_parent {
                                            TypedRelation::Project(project_id) => {
                                                result.push(ResourceIds::Object(
                                                    *project_id,
                                                    None,
                                                    Some(*parent_id),
                                                    object.id,
                                                ))
                                            }

                                            TypedRelation::Collection(collection_id) => {
                                                if let Some(resource) =
                                                    self.resources.get(collection_id)
                                                {
                                                    let (collection_object, _) = resource.value();

                                                    for parent_parent in
                                                        trace_err!(collection_object
                                                            .parents
                                                            .as_ref()
                                                            .ok_or_else(|| {
                                                                anyhow!(
                                                                "Invalid dataset, missing parent"
                                                            )
                                                            }))?
                                                    {
                                                        match parent_parent {
                                                            TypedRelation::Project(project_id) => {
                                                                result.push(ResourceIds::Object(
                                                                    *project_id,
                                                                    Some(*collection_id),
                                                                    Some(*parent_id),
                                                                    object.id,
                                                                ))
                                                            }
                                                            _ => {
                                                                error!("Invalid dataset, parent is not a collection");
                                                                bail!("Invalid dataset, parent is not a collection")
                                                            }
                                                        }
                                                    }
                                                } else {
                                                    error!(
                                                        "Invalid dataset, parent is not a project"
                                                    );
                                                    bail!("Invalid dataset, parent is not a collection")
                                                }
                                            }
                                            _ => {
                                                error!("Invalid dataset, parent is not a project");
                                                bail!("Invalid dataset, parent is not a collection")
                                            }
                                        }
                                    }
                                } else {
                                    error!("Invalid dataset, parent is not a project");
                                    bail!("Invalid dataset, parent is not a project")
                                }
                            }
                            _ => {
                                error!("Invalid dataset, parent is not a project");
                                bail!("Invalid dataset, parent is not a project")
                            }
                        }
                    }
                }
            }
        } else {
            return Err(anyhow!("Resource not found"));
        }
        trace!(?result, ?rel);
        Ok((result, rel))
    }

    #[tracing::instrument(level = "trace", skip(self))]
    pub fn get_path_levels(
        &self,
        resource_id: DieselUlid,
    ) -> Result<Vec<(String, Option<ObjectLocation>)>> {
        let init = trace_err!(self
            .resources
            .get(&resource_id)
            .ok_or_else(|| anyhow!("Resource not found")))?;

        let mut queue = VecDeque::with_capacity(10_000);

        if init.0.object_type == ObjectType::Bundle {
            for x in trace_err!(init
                .0
                .children
                .as_ref()
                .ok_or_else(|| anyhow!("No children found")))?
            {
                queue.push_back(("".to_string(), x.get_id()));
            }
        } else {
            queue.push_back(("".to_string(), init.0.id));
        };

        let mut finished = Vec::with_capacity(10_000);

        while let Some((mut name, id)) = queue.pop_front() {
            let resource = self
                .resources
                .get(&id)
                .ok_or_else(|| anyhow!("Resource not found"))?;

            if resource.0.object_type == ObjectType::Object {
                name = format!("{}/{}", name, resource.0.name);
                name = name.trim_matches('/').to_string();
                finished.push((name, resource.1.clone()));
            } else if let Some(child) = resource.0.children.as_ref() {
                if child.is_empty() {
                    name = format!("{}/{}", name, resource.0.name);
                    name = name.trim_matches('/').to_string();
                    finished.push((name, resource.1.clone()));
                } else {
                    for x in child {
                        name = name.trim_matches('/').to_string();
                        queue.push_back((format!("{}/{}", name, resource.0.name), x.get_id()));
                    }
                }
            } else {
                name = format!("{}/{}", name, resource.0.name);
                name = name.trim_matches('/').to_string();
                finished.push((name, resource.1.clone()));
            }
        }
        trace!(?finished);
        Ok(finished)
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
}
