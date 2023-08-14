use std::collections::VecDeque;

use super::structs::PubKey;
use crate::auth::structs::Context;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::user_dsl::User;
use crate::database::enums::DbPermissionLevel;
use crate::database::enums::ObjectMapping;
use crate::database::enums::ObjectType;
use ahash::HashMap;
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::bail;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::User as APIUser;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;

pub struct Cache {
    pub object_cache: DashMap<DieselUlid, ObjectWithRelations, RandomState>,
    pub user_cache: DashMap<DieselUlid, User, RandomState>,
    pub pubkeys: DashMap<i32, PubKey, RandomState>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            object_cache: DashMap::default(),
            user_cache: DashMap::default(),
            pubkeys: DashMap::default(),
        }
    }

    pub fn get_object(&self, id: &DieselUlid) -> Option<ObjectWithRelations> {
        self.object_cache.get(id).map(|x| x.value().clone())
    }

    pub fn get_user(&self, id: &DieselUlid) -> Option<User> {
        self.user_cache.get(id).map(|x| x.value().clone())
    }

    pub fn get_pubkey(&self, serial: i32) -> Option<PubKey> {
        self.pubkeys.get(&serial).map(|x| x.value().clone())
    }

    pub fn get_pubkey_serial(&self, raw_pubkey: &str) -> Option<i32> {
        for entry in &self.pubkeys {
            match entry.value() {
                PubKey::DataProxy((raw_key, _, _)) | PubKey::Server((raw_key, _)) => {
                    if raw_pubkey == raw_key {
                        return Some(*entry.key());
                    }
                }
            }
        }

        None
    }

    pub fn update_object(&self, id: &DieselUlid, object: ObjectWithRelations) {
        if let Some(mut x) = self.object_cache.get_mut(id) {
            *x.value_mut() = object;
        }
    }

    pub fn update_user(&self, id: &DieselUlid, user: User) {
        if let Some(mut x) = self.user_cache.get_mut(id) {
            *x.value_mut() = user;
        }
    }

    pub fn add_object(&self, rel: ObjectWithRelations) {
        self.object_cache.insert(rel.object.id, rel);
    }

    pub fn remove_object(&self, id: &DieselUlid) {
        self.object_cache.remove(id);
    }

    pub fn add_user(&self, id: DieselUlid, user: User) {
        self.user_cache.insert(id, user);
    }

    pub fn add_pubkey(&self, id: i32, key: PubKey) {
        self.pubkeys.insert(id, key);
    }

    pub fn remove_pubkey(&self, id: &i32) {
        self.pubkeys.remove(id);
    }

    pub fn remove_user(&self, id: &DieselUlid) {
        self.user_cache.remove(id);
    }

    pub fn get_user_by_oidc(&self, external_id: &str) -> Result<User> {
        self.user_cache
            .iter()
            .find(|x| x.value().external_id == Some(external_id.to_string()))
            .map(|x| x.value().clone())
            .ok_or_else(|| anyhow!("User not found"))
    }

    pub async fn get_all(&self) -> Vec<APIUser> {
        Vec::from_iter(self.user_cache.iter().map(|u| u.clone().into()))
    }

    pub async fn get_all_deactivated(&self) -> Vec<APIUser> {
        Vec::from_iter(self.user_cache.iter().filter_map(|u| {
            if u.active {
                Some(u.clone().into())
            } else {
                None
            }
        }))
    }
    // pub fn get_hierarchy(&self, id: &DieselUlid) -> Result<Graph> {
    //     let init = self
    //         .object_cache
    //         .get(&id)
    //         .map(|x| x.value().clone())
    //         .unwrap()
    //         .resource;

    //     let mut graph = Graph::default();

    //     let get_objects = |x| {
    //         if x.relation_name == INTERNAL_RELATION_VARIANT_BELONGS_TO
    //             && x.target_type == ObjectType::OBJECT
    //         {
    //             Some(x.to_string())
    //         } else {
    //             None
    //         }
    //     };

    //     let get_datasetcs = |x| {
    //         if x.relation_name == INTERNAL_RELATION_VARIANT_BELONGS_TO
    //             && x.target_type == ObjectType::DATASET
    //         {
    //             Some(DatasetRelations {
    //                 origin: x.target_id,
    //                 object_children: self
    //                     .get_object(x.target_id)?
    //                     .resource
    //                     .outbound
    //                     .0
    //                      .0
    //                     .iter()
    //                     .filter_map(get_objects)
    //                     .collect(),
    //             })
    //         } else {
    //             None
    //         }
    //     };

    //     match init.object.object_type {
    //         crate::database::enums::ObjectType::PROJECT => todo!(),
    //         crate::database::enums::ObjectType::COLLECTION => {
    //             Ok(Graph::Collection(CollectionRelations {
    //                 origin: id.to_string(),
    //                 dataset_children: init.outbound.0 .0.iter().filter_map(),
    //                 object_children: init.outbound.0 .0.iter().filter_map(get_objects).collect(),
    //             }))
    //         }
    //         crate::database::enums::ObjectType::DATASET => Ok(Graph::Dataset(DatasetRelations {
    //             origin: id.to_string(),
    //             object_children: init.outbound.0 .0.iter().filter_map(get_objects).collect(),
    //         })),
    //         crate::database::enums::ObjectType::OBJECT => bail!("Objects have no hierarchy"),
    //     }
    // }

    pub fn check_proxy_ctxs(&self, endpoint_id: &DieselUlid, ctxs: &[Context]) -> bool {
        ctxs.iter().all(|x| match &x.variant {
            crate::auth::structs::ContextVariant::Activated => true,
            crate::auth::structs::ContextVariant::ResourceContext((id, _)) => {
                if let Some(obj) = self.get_object(id) {
                    obj.object.endpoints.0.contains_key(endpoint_id)
                } else {
                    false
                }
            }
            crate::auth::structs::ContextVariant::User((uid, permlevel)) => {
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
            crate::auth::structs::ContextVariant::GlobalAdmin => false,
            crate::auth::structs::ContextVariant::GlobalProxy => true,
        })
    }

    pub fn check_permissions_with_contexts(
        &self,
        ctxs: &[Context],
        permitted: &[(DieselUlid, DbPermissionLevel)],
        user_id: &DieselUlid,
    ) -> bool {
        let mut resources = HashMap::default();

        for ctx in ctxs {
            match &ctx.variant {
                crate::auth::structs::ContextVariant::Activated => {
                    return self.get_user(user_id).map(|e| e.active).unwrap_or_default()
                }
                crate::auth::structs::ContextVariant::ResourceContext((id, perm)) => {
                    resources.insert(*id, *perm);
                }
                crate::auth::structs::ContextVariant::User((uid, _)) => {
                    return if uid == user_id {
                        true
                    } else {
                        self.get_user(user_id)
                            .map(|e| !e.attributes.0.service_account)
                            .unwrap_or_default()
                    }
                }
                crate::auth::structs::ContextVariant::GlobalAdmin
                | crate::auth::structs::ContextVariant::GlobalProxy => {
                    return self
                        .get_user(user_id)
                        .map(|e| !e.attributes.0.global_admin)
                        .unwrap_or_default()
                }
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

    ///ToDo: Rust Doc
    pub fn upstream_dfs_iterative(
        &self,
        root: &DieselUlid,
    ) -> Result<Vec<Vec<ObjectMapping<DieselUlid>>>> {
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
    ) -> anyhow::Result<()> {
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use diesel_ulid::DieselUlid;

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
}
