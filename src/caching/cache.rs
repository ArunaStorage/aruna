use super::structs::ProxyCacheIterator;
use super::structs::PubKeyEnum;
use crate::auth::structs::Context;
use crate::auth::structs::ContextVariant;
use crate::database::connection::Database;
use crate::database::crud::CrudDb;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::internal_relation_dsl::INTERNAL_RELATION_VARIANT_BELONGS_TO;
use crate::database::dsls::object_dsl::get_all_objects_with_relations;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::pub_key_dsl::PubKey as DbPubkey;
use crate::database::dsls::user_dsl::User;
use crate::database::enums::DbPermissionLevel;
use crate::database::enums::ObjectMapping;
use crate::database::enums::ObjectStatus;
use crate::database::enums::ObjectType;
use crate::utils::cache_utils::{
    get_collection_children, get_dataset_relations, get_project_children,
};
use ahash::HashMap;
use ahash::HashSet;
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::PermissionLevel;
use aruna_rust_api::api::storage::models::v2::User as APIUser;
use aruna_rust_api::api::storage::services::v2::get_hierarchy_response::Graph;
use aruna_rust_api::api::storage::services::v2::UserPermission;
use itertools::Itertools;
use std::collections::VecDeque;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use dashmap::DashMap;
use diesel_ulid::DieselUlid;

pub struct Cache {
    pub object_cache: DashMap<DieselUlid, ObjectWithRelations, RandomState>,
    pub user_cache: DashMap<DieselUlid, User, RandomState>,
    pub pubkeys: DashMap<i32, PubKeyEnum, RandomState>,
    lock: AtomicBool,
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

impl Cache {
    pub fn new() -> Self {
        Self {
            object_cache: DashMap::default(),
            user_cache: DashMap::default(),
            pubkeys: DashMap::default(),
            lock: AtomicBool::new(false),
        }
    }

    pub async fn sync_cache(&self, db: Arc<Database>) -> Result<()> {
        self.lock.store(true, std::sync::atomic::Ordering::Relaxed);
        self.object_cache.clear();
        self.user_cache.clear();
        self.pubkeys.clear();
        let client = db.get_client().await?;

        let all_objects = get_all_objects_with_relations(&client).await?;
        for obj in all_objects {
            self.object_cache.insert(obj.object.id, obj);
        }

        let users = User::all(&client).await?;
        for user in users {
            self.user_cache.insert(user.id, user);
        }

        let pubkeys = DbPubkey::all(&client).await?;
        for pubkey in pubkeys {
            self.pubkeys
                .insert(pubkey.id as i32, PubKeyEnum::try_from(pubkey)?);
        }

        self.lock.store(false, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    pub fn check_lock(&self) {
        while self.lock.load(std::sync::atomic::Ordering::Relaxed) {
            std::hint::spin_loop()
        }
    }

    pub fn get_object(&self, id: &DieselUlid) -> Option<ObjectWithRelations> {
        self.check_lock();
        self.object_cache.get(id).map(|x| x.value().clone())
    }

    pub fn get_user(&self, id: &DieselUlid) -> Option<User> {
        self.check_lock();
        self.user_cache.get(id).map(|x| x.value().clone())
    }

    pub fn get_pubkey(&self, serial: i32) -> Option<PubKeyEnum> {
        self.check_lock();
        self.pubkeys.get(&serial).map(|x| x.value().clone())
    }

    pub fn get_pubkey_serial(&self, raw_pubkey: &str) -> Option<i32> {
        self.check_lock();
        for entry in &self.pubkeys {
            match entry.value() {
                PubKeyEnum::DataProxy((raw_key, _, _)) | PubKeyEnum::Server((raw_key, _)) => {
                    if raw_pubkey == raw_key {
                        return Some(*entry.key());
                    }
                }
            }
        }

        None
    }

    pub fn upsert_object(&self, id: &DieselUlid, object: ObjectWithRelations) {
        self.check_lock();
        if let Some(mut x) = self.object_cache.get_mut(id) {
            *x.value_mut() = object;
        } else {
            self.object_cache.insert(object.object.id, object);
        }
    }
    pub fn update_relations(&self, relations: Vec<InternalRelation>) {
        self.check_lock();

        let zip = relations
            .iter()
            .map(|ir| (ir.origin_pid, ir.target_pid, ir));
        for (origin_id, target_id, relation) in zip {
            if let Some(mut origin) = self.object_cache.get_mut(&origin_id) {
                match relation.relation_name.as_ref() {
                    INTERNAL_RELATION_VARIANT_BELONGS_TO => origin
                        .outbound_belongs_to
                        .0
                        .insert(target_id, relation.clone()),
                    _ => origin.outbound.0.insert(target_id, relation.clone()),
                };
                let clone = origin.clone();
                *origin.value_mut() = clone;
            }
            if let Some(mut target) = self.object_cache.get_mut(&target_id) {
                match relation.relation_name.as_ref() {
                    INTERNAL_RELATION_VARIANT_BELONGS_TO => target
                        .inbound_belongs_to
                        .0
                        .insert(target_id, relation.clone()),
                    _ => target.inbound.0.insert(origin_id, relation.clone()),
                };
                let clone = target.clone();
                *target.value_mut() = clone;
            }
        }
    }

    pub fn update_user(&self, id: &DieselUlid, user: User) {
        self.check_lock();
        if let Some(mut x) = self.user_cache.get_mut(id) {
            *x.value_mut() = user;
        }
    }

    pub fn add_object(&self, rel: ObjectWithRelations) {
        self.check_lock();
        self.object_cache.insert(rel.object.id, rel);
    }

    pub fn remove_object(&self, id: &DieselUlid) {
        self.check_lock();
        if let Some(mut x) = self.object_cache.get_mut(id) {
            x.value_mut().object.object_status = ObjectStatus::DELETED;
        }
    }

    pub fn add_user(&self, id: DieselUlid, user: User) {
        self.check_lock();
        self.user_cache.insert(id, user);
    }

    pub fn add_pubkey(&self, id: i32, key: PubKeyEnum) {
        self.check_lock();
        self.pubkeys.insert(id, key);
    }

    pub fn remove_pubkey(&self, id: &i32) {
        self.check_lock();
        self.pubkeys.remove(id);
    }

    pub fn remove_user(&self, id: &DieselUlid) {
        self.check_lock();
        self.user_cache.remove(id);
    }

    pub fn get_user_by_oidc(&self, external_id: &str) -> Result<User> {
        self.check_lock();
        self.user_cache
            .iter()
            .find(|x| x.value().external_id == Some(external_id.to_string()))
            .map(|x| x.value().clone())
            .ok_or_else(|| anyhow!("User not found"))
    }

    pub async fn get_all_users(&self) -> Vec<APIUser> {
        self.check_lock();
        Vec::from_iter(self.user_cache.iter().map(|u| u.clone().into()))
    }

    pub async fn get_all_users_proto(&self) -> Vec<APIUser> {
        self.check_lock();
        Vec::from_iter(self.user_cache.iter().map(|u| u.clone().into()))
    }

    pub async fn get_all_deactivated(&self) -> Vec<APIUser> {
        self.check_lock();
        Vec::from_iter(self.user_cache.iter().filter_map(|u| {
            if u.active {
                Some(u.clone().into())
            } else {
                None
            }
        }))
    }

    pub fn remove_endpoint_pubkeys(&self, endpoint_id: &DieselUlid) {
        self.check_lock();
        for entry in &self.pubkeys {
            if let PubKeyEnum::DataProxy((_, _, pubkey_endpoint)) = entry.value() {
                if endpoint_id == pubkey_endpoint {
                    self.pubkeys.remove(entry.key());
                }
            }
        }
    }

    pub fn remove_endpoint_from_resources(&self, endpoint_id: &DieselUlid) {
        self.check_lock();
        for entry in &self.object_cache {
            entry.object.endpoints.0.remove(endpoint_id);
        }
    }

    pub fn remove_endpoint_from_users(&self, endpoint_id: &DieselUlid) {
        self.check_lock();
        self.user_cache.iter().for_each(|entry| {
            let val = entry.value();
            val.attributes.0.trusted_endpoints.remove(endpoint_id);
        });
    }

    pub fn get_hierarchy(&self, id: &DieselUlid) -> Result<Graph> {
        self.check_lock();
        let init = self
            .object_cache
            .get(id)
            .map(|x| x.value().clone())
            .ok_or_else(|| anyhow!("No resource found"))?;
        let resource = match init.object.object_type {
            ObjectType::PROJECT => Graph::Project(get_project_children(&init, self)),
            ObjectType::COLLECTION => Graph::Collection(get_collection_children(&init, self)),
            ObjectType::DATASET => Graph::Dataset(get_dataset_relations(&init)),
            ObjectType::OBJECT => return Err(anyhow!("Objects do not have a hierarchy")),
        };

        Ok(resource)
    }

    pub fn get_resource_permissions(
        &self,
        resource_id: DieselUlid,
        recursive: bool,
    ) -> Result<HashMap<DieselUlid, Vec<UserPermission>>> {
        // Lock cache
        self.check_lock();

        // Fetch all resources included in request
        let mut resources = vec![resource_id];
        if recursive {
            resources.append(&mut self.get_subresources(&resource_id)?)
        }

        // Collect all permissions for the resources from user cache
        let mut resource_perms: HashMap<DieselUlid, Vec<UserPermission>> = HashMap::default();

        // Loop over resource ids
        for resource_id in resources {
            // Loop over users
            for entry in &self.user_cache {
                // Loop over users permissions
                for perm in entry.value().get_permissions(None)? {
                    if perm.0 == resource_id {
                        if let Some(permissions) = resource_perms.get_mut(&resource_id) {
                            permissions.push(UserPermission {
                                user_id: entry.value().id.to_string(),
                                user_name: entry.value().display_name.to_string(),
                                permission_level: PermissionLevel::from(perm.1) as i32,
                            });
                        } else {
                            resource_perms.insert(
                                resource_id,
                                vec![UserPermission {
                                    user_id: entry.value().id.to_string(),
                                    user_name: entry.value().display_name.to_string(),
                                    permission_level: PermissionLevel::from(perm.1) as i32,
                                }],
                            );
                        }
                    }
                }
            }
        }

        Ok(resource_perms)
    }

    pub fn check_proxy_ctxs(&self, endpoint_id: &DieselUlid, ctxs: &[Context]) -> bool {
        self.check_lock();
        ctxs.iter().all(|x| match &x.variant {
            ContextVariant::Activated => true,
            ContextVariant::Resource((id, _)) => {
                if let Some(obj) = self.get_object(id) {
                    dbg!(&obj);
                    obj.object.endpoints.0.contains_key(endpoint_id)
                } else {
                    false
                }
            }
            ContextVariant::User((uid, permlevel)) => {
                if *permlevel == DbPermissionLevel::READ {
                    if let Some(user) = self.get_user(uid) {
                        user.attributes
                            .0
                            .trusted_endpoints
                            .contains_key(endpoint_id)
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            ContextVariant::GlobalAdmin | ContextVariant::SelfUser => false,
            ContextVariant::GlobalProxy => true,
        })
    }

    pub fn check_permissions_with_contexts(
        &self,
        ctxs: &[Context],
        permitted: &[(DieselUlid, DbPermissionLevel)],
        user_id: &DieselUlid,
    ) -> bool {
        self.check_lock();
        let mut resources = HashMap::default();

        let user = match self.get_user(user_id) {
            Some(user) => user,
            None => return false,
        };

        if user.active && user.attributes.0.global_admin {
            return true;
        }

        for ctx in ctxs {
            match &ctx.variant {
                ContextVariant::Activated => return user.active,
                ContextVariant::Resource((id, perm)) => {
                    resources.insert(*id, *perm);
                }
                ContextVariant::User((uid, _)) => {
                    return if uid == user_id {
                        !user.attributes.0.service_account
                    } else {
                        false
                    }
                }
                ContextVariant::SelfUser => return true,
                ContextVariant::GlobalProxy | ContextVariant::GlobalAdmin => return false,
            }
        }

        for (id, got_perm) in permitted {
            if let Some(needed_perm) = resources.get(id) {
                if got_perm >= needed_perm {
                    resources.remove(id);
                    if resources.is_empty() {
                        return true;
                    }
                }
            }
            match self.traverse_down(id, *got_perm, &mut resources) {
                Ok(true) => return true,
                Ok(false) => continue,
                Err(_) => return false,
            }
        }
        false
    }

    pub fn traverse_down(
        &self,
        id: &DieselUlid,
        perm: DbPermissionLevel,
        ctxs: &mut HashMap<DieselUlid, DbPermissionLevel>,
    ) -> Result<bool> {
        self.check_lock();
        if ctxs.is_empty() {
            return Ok(true);
        }

        let mut queue = VecDeque::new();
        queue.push_back(*id);

        while let Some(x) = queue.pop_front() {
            if let Some(x) = self.get_object(&x) {
                for child in x.get_children() {
                    if let Some(got_perm) = ctxs.remove(&child) {
                        if got_perm > perm {
                            bail!("Invalid permissions")
                        }
                        if ctxs.is_empty() {
                            return Ok(true);
                        }
                    }
                    queue.push_back(child);
                }
            }
        }
        Ok(false)
    }

    pub fn get_subresources(&self, root_id: &DieselUlid) -> Result<Vec<DieselUlid>> {
        self.check_lock();

        let mut subresources: HashSet<DieselUlid> = HashSet::default();
        let mut queue = VecDeque::new();
        queue.push_back(*root_id);

        while let Some(resource_id) = queue.pop_front() {
            if let Some(resource) = self.get_object(&resource_id) {
                for child_id in resource.get_children() {
                    subresources.insert(child_id);
                    queue.push_front(child_id)
                }
            }
        }

        Ok(subresources.into_iter().collect_vec())
    }

    ///ToDo: Rust Doc
    pub fn upstream_dfs_iterative(
        &self,
        root: &DieselUlid,
    ) -> Result<Vec<Vec<ObjectMapping<DieselUlid>>>> {
        self.check_lock();
        let mut split_indexes = Vec::new(); // Used to store history where path splits
        let mut current_hierarchy = Vec::with_capacity(4); // Maximum length of hierarchy is 4
        let mut finished_hierarchies = vec![];

        // Fetch root object and push int queue
        let mut queue = VecDeque::new();
        queue.push_back(
            self.get_object(root)
                .ok_or_else(|| anyhow::anyhow!("Parent doesn't exist"))?,
        );

        while let Some(current_object) = queue.pop_front() {
            // Add current object to back of hierarchy
            current_hierarchy.push(match current_object.object.object_type {
                ObjectType::PROJECT => ObjectMapping::PROJECT(current_object.object.id),
                ObjectType::COLLECTION => ObjectMapping::COLLECTION(current_object.object.id),
                ObjectType::DATASET => ObjectMapping::DATASET(current_object.object.id),
                ObjectType::OBJECT => ObjectMapping::OBJECT(current_object.object.id),
            });

            // Check if current object is a project
            if current_object.object.object_type == ObjectType::PROJECT {
                // Save finished hierarchy
                finished_hierarchies.push(current_hierarchy.clone());

                // Truncate current hierarchy back to last path split
                if let Some(index) = split_indexes.pop() {
                    current_hierarchy.truncate(index) //
                }
            } else {
                // Add parents to the front of the queue for DFS
                for parent_id in current_object.get_parents() {
                    let parent = self
                        .get_object(&parent_id)
                        .ok_or_else(|| anyhow::anyhow!("Parent doesn't exist"))?;

                    queue.push_front(parent);
                }

                // Save index n times for hierarchy cleanup if more than 1 parent
                if current_object.get_parents().len() > 1 {
                    for _ in 0..(current_object.get_parents().len() - 1) {
                        split_indexes.push(current_hierarchy.len())
                    }
                }
            }
        }

        Ok(finished_hierarchies)
    }

    ///ToDo: Rust Doc
    pub fn upstream_dfs_recursive(
        &self,
        current_object_id: &DieselUlid,
        current_path: &mut Vec<ObjectMapping<DieselUlid>>,
        finished_hierarchies: &mut Vec<Vec<ObjectMapping<DieselUlid>>>,
    ) -> Result<()> {
        self.check_lock();
        // Fetch current object with relations
        if let Some(current_object) = self.get_object(current_object_id) {
            // End current hierarchy if node is project
            if current_object.object.object_type == ObjectType::PROJECT {
                current_path.push(ObjectMapping::PROJECT(current_object.object.id));
                finished_hierarchies.push(current_path.clone());
                current_path.pop();
            } else {
                // Add current object to path
                current_path.push(match current_object.object.object_type {
                    ObjectType::PROJECT => ObjectMapping::PROJECT(current_object.object.id),
                    ObjectType::COLLECTION => ObjectMapping::COLLECTION(current_object.object.id),
                    ObjectType::DATASET => ObjectMapping::DATASET(current_object.object.id),
                    ObjectType::OBJECT => ObjectMapping::OBJECT(current_object.object.id),
                });
                for parent_id in current_object.get_parents() {
                    self.upstream_dfs_recursive(&parent_id, current_path, finished_hierarchies)?
                }
                current_path.pop();
            }
        } else {
            return Err(anyhow::anyhow!("Parent does not exist"));
        }

        Ok(())
    }

    pub fn get_proxy_cache_iterator(&self, endpoint_id: &DieselUlid) -> ProxyCacheIterator {
        ProxyCacheIterator::new(
            Box::new(self.object_cache.iter()),
            Box::new(self.user_cache.iter()),
            Box::new(self.pubkeys.iter()),
            *endpoint_id,
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::database::dsls::{user_dsl::UserAttributes, Empty};

    use super::*;
    use diesel_ulid::DieselUlid;
    use postgres_types::Json;

    #[test]
    fn test_remove_object() {
        let cache = Cache::new();
        let object_ulid = DieselUlid::generate();
        let object_plus = ObjectWithRelations::random_object_v2(
            &object_ulid,
            ObjectType::PROJECT,
            vec![],
            vec![],
        );

        // Add random object
        cache.add_object(object_plus.clone());
        assert!(cache.get_object(&object_ulid).is_some());

        // Delete object
        cache.remove_object(&object_ulid);
        assert_eq!(
            cache.get_object(&object_ulid).unwrap().object.object_status,
            ObjectStatus::DELETED
        );
    }

    #[test]
    fn test_remove_endpoint_from_users() {
        let cache = Cache::new();

        // Generate random endpoint id
        let endpoint_id = DieselUlid::generate();
        let random_endpoint = DieselUlid::generate();

        // Insert users with trusted_endpoint
        let user_1 = DieselUlid::generate();
        cache.add_user(
            user_1,
            User {
                id: user_1,
                display_name: "user-1".to_string(),
                external_id: Some("my-external-id".to_string()),
                email: "test-1@example.com".to_string(),
                attributes: Json(UserAttributes {
                    global_admin: false,
                    service_account: false,
                    tokens: DashMap::default(),
                    trusted_endpoints: DashMap::from_iter([
                        (endpoint_id, Empty {}),
                        (random_endpoint, Empty {}),
                    ]),
                    custom_attributes: vec![],
                    permissions: DashMap::default(),
                }),
                active: true,
            },
        );

        // Insert users with trusted_endpoint
        let user_2 = DieselUlid::generate();
        cache.add_user(
            user_2,
            User {
                id: user_2,
                display_name: "user-2".to_string(),
                external_id: Some("my-other-external-id".to_string()),
                email: "test-2@example.com".to_string(),
                attributes: Json(UserAttributes {
                    global_admin: false,
                    service_account: false,
                    tokens: DashMap::default(),
                    trusted_endpoints: DashMap::from_iter([(endpoint_id, Empty {})]),
                    custom_attributes: vec![],
                    permissions: DashMap::default(),
                }),
                active: true,
            },
        );

        // Remove endpoint from users
        cache.remove_endpoint_from_users(&endpoint_id);

        // Fetch user and validate endpoint removal
        let user_1_upd = cache.get_user(&user_1).unwrap();

        assert_eq!(user_1_upd.attributes.0.trusted_endpoints.len(), 1);
        assert!(user_1_upd
            .attributes
            .0
            .trusted_endpoints
            .contains_key(&random_endpoint));

        let user_2_upd = cache.get_user(&user_2).unwrap();
        assert!(user_2_upd.attributes.0.trusted_endpoints.is_empty())
    }

    #[test]
    fn test_traverse_down_with_relations() {
        let cache = Cache::new();
        let id1 = DieselUlid::generate();
        let id2 = DieselUlid::generate();
        let id3 = DieselUlid::generate();
        let id4 = DieselUlid::generate();
        let id5 = DieselUlid::generate();

        let mut ctxs = HashMap::default();
        ctxs.insert(id2, DbPermissionLevel::READ);
        ctxs.insert(id3, DbPermissionLevel::READ);
        ctxs.insert(id4, DbPermissionLevel::READ);

        let mut ctxs1 = ctxs.clone();
        let mut ctxs2 = ctxs.clone();
        let mut ctxs3 = ctxs.clone();

        cache.add_object(ObjectWithRelations::random_object_to(&id1, &id2));
        cache.add_object(ObjectWithRelations::random_object_to(&id2, &id3));
        cache.add_object(ObjectWithRelations::random_object_to(&id3, &id4));
        cache.add_object(ObjectWithRelations::random_object_to(&id4, &id5));

        let result = cache.traverse_down(&id1, DbPermissionLevel::READ, &mut ctxs1);
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = cache.traverse_down(&id1, DbPermissionLevel::ADMIN, &mut ctxs2);
        assert!(result.is_ok());
        assert!(result.unwrap());

        let result = cache.traverse_down(&id1, DbPermissionLevel::NONE, &mut ctxs3);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Invalid permissions");
    }

    #[test]
    fn test_upstream_dfs_001() {
        // Init new cache
        let cache = Cache::new();

        // Create dummy hierarchies
        let id1 = DieselUlid::generate(); // Project
        let id2 = DieselUlid::generate(); // Collection: from id1 and to id4
        let id3 = DieselUlid::generate(); // Collection: from id1 and to id4
        let id4 = DieselUlid::generate(); // Dataset: from [id2, id3] to id5
        let id5 = DieselUlid::generate(); // Object: from id4
        let id6 = DieselUlid::generate(); // Object: from id1

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id1,
            ObjectType::PROJECT,
            vec![],
            vec![&id2],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id2,
            ObjectType::COLLECTION,
            vec![&id1],
            vec![&id4],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id3,
            ObjectType::COLLECTION,
            vec![&id1],
            vec![&id4],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id4,
            ObjectType::DATASET,
            vec![&id2, &id3],
            vec![&id5],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id5,
            ObjectType::OBJECT,
            vec![&id4],
            vec![],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id6,
            ObjectType::OBJECT,
            vec![&id1],
            vec![],
        ));

        // Extract paths of id5 object and evaluate result
        let iterative_result = cache.upstream_dfs_iterative(&id5).unwrap();

        let mut current_path = Vec::new();
        let mut recursive_result = Vec::new();
        cache
            .upstream_dfs_recursive(&id5, &mut current_path, &mut recursive_result)
            .unwrap();

        assert_eq!(recursive_result.len(), 2);
        assert_eq!(iterative_result.len(), 2);

        for path in [
            vec![
                ObjectMapping::OBJECT(id5),
                ObjectMapping::DATASET(id4),
                ObjectMapping::COLLECTION(id3),
                ObjectMapping::PROJECT(id1),
            ],
            vec![
                ObjectMapping::OBJECT(id5),
                ObjectMapping::DATASET(id4),
                ObjectMapping::COLLECTION(id2),
                ObjectMapping::PROJECT(id1),
            ],
        ] {
            assert!(recursive_result.contains(&path));
            assert!(iterative_result.contains(&path));
        }

        // Extract paths of id6 object and evaluate result
        let iterative_result = cache.upstream_dfs_iterative(&id6).unwrap();

        let mut current_path = Vec::new();
        let mut recursive_result = Vec::new();
        cache
            .upstream_dfs_recursive(&id6, &mut current_path, &mut recursive_result)
            .unwrap();

        assert_eq!(recursive_result.len(), 1);
        assert_eq!(iterative_result.len(), 1);
        assert!(recursive_result.contains(&vec![
            ObjectMapping::OBJECT(id6),
            ObjectMapping::PROJECT(id1)
        ]));
        assert!(iterative_result.contains(&vec![
            ObjectMapping::OBJECT(id6),
            ObjectMapping::PROJECT(id1)
        ]));
    }

    #[tokio::test]
    async fn test_upstream_dfs_002() {
        // Init new cache
        let cache = Cache::new();

        // Create dummy hierarchies
        let id1 = DieselUlid::generate(); // Project from [] to [id4]
        let id2 = DieselUlid::generate(); // Project from [] to [id4]
        let id3 = DieselUlid::generate(); // Project from [] to [id6]
        let id4 = DieselUlid::generate(); // Collection: from [id1,id2] and to [id5,id6]
        let id5 = DieselUlid::generate(); // Dataset: from [id4] to [id7]
        let id6 = DieselUlid::generate(); // Dataset: from [id3,id4] to [id7]
        let id7 = DieselUlid::generate(); // Object: from [id5,id6] to []

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id1,
            ObjectType::PROJECT,
            vec![],
            vec![&id4],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id2,
            ObjectType::PROJECT,
            vec![],
            vec![&id4],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id3,
            ObjectType::PROJECT,
            vec![],
            vec![&id6],
        ));

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id4,
            ObjectType::COLLECTION,
            vec![&id1, &id2],
            vec![&id5, &id6],
        ));

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id5,
            ObjectType::DATASET,
            vec![&id4],
            vec![&id7],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id6,
            ObjectType::DATASET,
            vec![&id3, &id4],
            vec![&id7],
        ));

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id7,
            ObjectType::OBJECT,
            vec![&id5, &id6],
            vec![],
        ));

        // Extract paths including "Performance measurement"
        use std::time::Instant;
        let now = Instant::now();
        let iterative_result = cache.upstream_dfs_iterative(&id7).unwrap();
        let elapsed = now.elapsed();
        log::debug!("Cache iterative traversal: {:.2?}", elapsed);

        // Evaluate result
        assert_eq!(iterative_result.len(), 5);
        for path in [
            vec![
                ObjectMapping::OBJECT(id7),
                ObjectMapping::DATASET(id6),
                ObjectMapping::PROJECT(id3),
            ],
            vec![
                ObjectMapping::OBJECT(id7),
                ObjectMapping::DATASET(id6),
                ObjectMapping::COLLECTION(id4),
                ObjectMapping::PROJECT(id1),
            ],
            vec![
                ObjectMapping::OBJECT(id7),
                ObjectMapping::DATASET(id6),
                ObjectMapping::COLLECTION(id4),
                ObjectMapping::PROJECT(id2),
            ],
            vec![
                ObjectMapping::OBJECT(id7),
                ObjectMapping::DATASET(id5),
                ObjectMapping::COLLECTION(id4),
                ObjectMapping::PROJECT(id1),
            ],
            vec![
                ObjectMapping::OBJECT(id7),
                ObjectMapping::DATASET(id5),
                ObjectMapping::COLLECTION(id4),
                ObjectMapping::PROJECT(id2),
            ],
        ] {
            assert!(iterative_result.contains(&path));
        }
    }

    #[tokio::test]
    async fn test_get_subresources() {
        // Init new cache
        let cache = Cache::new();

        // Create dummy hierarchies
        let id1 = DieselUlid::generate(); // Project from [] to [id4]
        let id2 = DieselUlid::generate(); // Project from [] to [id4]
        let id3 = DieselUlid::generate(); // Project from [] to [id6]
        let id4 = DieselUlid::generate(); // Collection: from [id1,id2] and to [id5,id6]
        let id5 = DieselUlid::generate(); // Dataset: from [id4] to [id7]
        let id6 = DieselUlid::generate(); // Dataset: from [id3,id4] to [id7]
        let id7 = DieselUlid::generate(); // Object: from [id5,id6] to []

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id1,
            ObjectType::PROJECT,
            vec![],
            vec![&id4],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id2,
            ObjectType::PROJECT,
            vec![],
            vec![&id4],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id3,
            ObjectType::PROJECT,
            vec![],
            vec![&id6],
        ));

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id4,
            ObjectType::COLLECTION,
            vec![&id1, &id2],
            vec![&id5, &id6],
        ));

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id5,
            ObjectType::DATASET,
            vec![&id4],
            vec![&id7],
        ));
        cache.add_object(ObjectWithRelations::random_object_v2(
            &id6,
            ObjectType::DATASET,
            vec![&id3, &id4],
            vec![&id7],
        ));

        cache.add_object(ObjectWithRelations::random_object_v2(
            &id7,
            ObjectType::OBJECT,
            vec![&id5, &id6],
            vec![],
        ));

        let subresources = cache.get_subresources(&id3).unwrap();

        vec![id6, id7]
            .into_iter()
            .for_each(|id| assert!(subresources.contains(&id)))
    }
}
