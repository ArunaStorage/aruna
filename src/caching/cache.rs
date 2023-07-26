use super::structs::{PubKey, ResourceInfo};
use crate::database::dsls::user_dsl::User;
use crate::database::dsls::{
    internal_relation_dsl::INTERNAL_RELATION_VARIANT_BELONGS_TO, object_dsl::ObjectWithRelations,
};
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;

pub struct Cache {
    pub object_cache: DashMap<DieselUlid, ResourceInfo, RandomState>,
    pub user_cache: DashMap<DieselUlid, User, RandomState>,
    pub pubkeys: DashMap<i32, PubKey, RandomState>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            object_cache: DashMap::new(),
            user_cache: DashMap::new(),
            pubkeys: DashMap::new(),
        }
    }

    pub fn get_object(&self, id: &DieselUlid) -> Option<ResourceInfo> {
        self.object_cache.get(id).map(|x| x.value().clone())
    }

    pub fn get_user(&self, id: &DieselUlid) -> Option<User> {
        self.user_cache.get(id).map(|x| x.value().clone())
    }

    pub fn update_object(&self, id: &DieselUlid, object: ObjectWithRelations) {
        self.object_cache.get_mut(id).map(|mut x| {
            x.value_mut().resource = object;
        });
    }

    pub fn add_object(&self, id: DieselUlid, info: ResourceInfo) {
        self.object_cache.insert(id, info);
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
            .find(|x| x.value().external_id == external_id)
            .map(|x| x.value().clone())
            .ok_or_else(|| anyhow!("User not found"))
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
}

#[cfg(test)]
mod tests {
    use crate::caching::structs::ResourceInfo;

    use super::*;
    use diesel_ulid::DieselUlid as Ulid;

    #[test]
    fn test_get_object() {
        let cache = Cache::new();
        let id = Ulid::new();
        let info = ResourceInfo::default();
        cache.add_object(id.into(), info.clone());

        assert_eq!(cache.get_object(&id.into()), Some(info));
    }

    #[test]
    fn test_get_user() {
        let cache = Cache::new();
        let id = Ulid::new();
        let user = User::default();
        cache.user_cache.insert(id.into(), user.clone());

        assert_eq!(cache.get_user(&id.into()), Some(user));
    }

    #[test]
    fn test_update_object() {
        let cache = Cache::new();
        let id = Ulid::new();
        let object = ObjectWithRelations::default();
        let info = ResourceInfo::default();
        cache.add_object(id.into(), info.clone());

        cache.update_object(&id.into(), object.clone());

        assert_eq!(cache.get_object(&id.into()), Some(info));
    }

    #[test]
    fn test_add_object() {
        let cache = Cache::new();
        let id = Ulid::new();
        let info = ResourceInfo::default();
        cache.add_object(id.into(), info.clone());

        assert_eq!(cache.get_object(&id.into()), Some(info));
    }

    #[test]
    fn test_remove_object() {
        let cache = Cache::new();
        let id = Ulid::new();
        let info = ResourceInfo::default();
        cache.add_object(id.into(), info.clone());

        cache.remove_object(&id.into());

        assert_eq!(cache.get_object(&id.into()), None);
    }
}
