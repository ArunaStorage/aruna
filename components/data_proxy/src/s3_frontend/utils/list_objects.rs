use std::collections::BTreeMap;
use ahash::RandomState;
use aruna_rust_api::api::storage::models::v2::DataClass;
use dashmap::DashMap;
use dashmap::mapref::multiple::RefMulti;
use diesel_ulid::DieselUlid;
use crate::caching::cache::{ResourceIds, ResourceString};
use crate::structs::{Object, ObjectLocation};

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct Contents {
    pub key: String,
    pub etag: DieselUlid,
    pub size: i64,
    pub storage_class: DataClass,
}
impl From<(&String, &(Object, Option<ObjectLocation>))> for Contents {
    fn from(value: (&String, &(Object, Option<ObjectLocation>))) -> Self {
        Contents {
            key: value.0.clone(),
            etag: value.1 .0.id,
            size: match &value.1 .1 {
                Some(s) => s.raw_content_len,
                None => 0,
            },
            storage_class: value.1 .0.data_class,
        }
    }
}

pub fn filter_list_objects(map: &DashMap<ResourceString, ResourceIds, RandomState>, root: &str) -> BTreeMap<String, DieselUlid> {
    map.iter().filter_map(|e|
    match e.key().clone() {
        ResourceString::Collection(temp_root, collection) if temp_root == root => {
            Some(([temp_root, collection].join("/"), e.value().into()))
        }
        ResourceString::Dataset(temp_root, collection, dataset) if temp_root == root => {
            Some((
                [temp_root, collection.unwrap_or("".to_string()), dataset].join("/"),
                e.value().into(),
            ))
        }
        ResourceString::Object(temp_root, collection, dataset, object)
        if temp_root == root =>
            {
                Some((
                    [
                        temp_root,
                        collection.unwrap_or("".to_string()),
                        dataset.unwrap_or("".to_string()),
                        object,
                    ]
                        .join("/"),
                    e.value().into(),
                ))
            }
        ResourceString::Project(temp_root) if temp_root == root => {
            Some((temp_root, e.value().into()))
        }
        _ => None,
    }).collect()
}