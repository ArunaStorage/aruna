use super::structs::PubKey;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::dsls::user_dsl::User;
use ahash::RandomState;
use anyhow::anyhow;
use anyhow::Result;
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

    pub fn update_object(&self, id: &DieselUlid, object: ObjectWithRelations) {
        if let Some(mut x) = self.object_cache.get_mut(id) {
            *x.value_mut() = object;
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
