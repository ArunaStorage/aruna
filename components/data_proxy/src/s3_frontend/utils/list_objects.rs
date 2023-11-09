use crate::caching::cache::Cache;
use crate::structs::{Object, ObjectLocation};
use crate::structs::{ResourceIds, ResourceString};
use crate::trace_err;
use ahash::RandomState;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::DataClass;
use base64::engine::general_purpose;
use base64::Engine;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use s3s::s3_error;
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

#[derive(Eq, PartialEq, Hash, Clone)]
pub struct Contents {
    pub key: String,
    pub etag: DieselUlid,
    pub size: i64,
    pub storage_class: DataClass,
}
impl From<(&String, &(Object, Option<ObjectLocation>))> for Contents {
    #[tracing::instrument(level = "trace", skip(value))]
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

#[tracing::instrument(level = "trace", skip(map, root))]
/// Creates a filtered and ordered BTreeMap for ListObjectsV2
pub fn filter_list_objects(
    map: &DashMap<ResourceString, ResourceIds, RandomState>,
    root: &str,
) -> BTreeMap<String, DieselUlid> {
    map.iter()
        .filter_map(|e| match e.key().clone() {
            ResourceString::Collection(temp_root, collection) if temp_root == root => {
                Some(([temp_root, collection].join("/"), e.value().into()))
            }
            ResourceString::Dataset(temp_root, collection, dataset) if temp_root == root => Some((
                [temp_root, collection.unwrap_or("".to_string()), dataset].join("/"),
                e.value().into(),
            )),
            ResourceString::Object(temp_root, collection, dataset, object) if temp_root == root => {
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
        })
        .collect()
}
#[tracing::instrument(
    level = "trace",
    skip(sorted, cache, delimiter, prefix, start_after, max_keys)
)]
pub fn list_response(
    sorted: BTreeMap<String, DieselUlid>,
    cache: &Arc<Cache>,
    delimiter: &Option<String>,
    prefix: &Option<String>,
    start_after: &str,
    max_keys: usize,
) -> Result<(HashSet<Contents>, HashSet<String>, Option<String>)> {
    let mut keys: HashSet<Contents> = HashSet::default();
    let mut common_prefixes: HashSet<String> = HashSet::default();
    let mut new_continuation_token: Option<String> = None;

    match (delimiter.clone(), prefix.clone()) {
        (Some(delimiter), Some(prefix)) => {
            for (idx, (path, id)) in sorted.range(start_after.to_owned()..).enumerate() {
                if let Some(split) = path.strip_prefix(&prefix) {
                    if let Some((sub_prefix, _)) = split.split_once(&delimiter) {
                        // If Some split -> common prefixes
                        if idx == max_keys + 1 {
                            new_continuation_token =
                                Some(general_purpose::STANDARD_NO_PAD.encode(path));
                            break;
                        }
                        common_prefixes
                            .insert([prefix.to_string(), sub_prefix.to_string()].join(""));
                    } else {
                        let entry: Contents = (
                            path,
                            cache
                                .resources
                                .get(id)
                                .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?
                                .value(),
                        )
                            .into();
                        if idx == max_keys + 1 {
                            new_continuation_token =
                                Some(general_purpose::STANDARD_NO_PAD.encode(path));
                            break;
                        }
                        keys.insert(entry);
                    };
                } else {
                    continue;
                };
                if idx == max_keys {
                    if idx == max_keys + 1 {
                        new_continuation_token =
                            Some(general_purpose::STANDARD_NO_PAD.encode(path));
                    }
                    break;
                }
            }
        }
        (Some(delimiter), None) => {
            for (idx, (path, id)) in sorted.range(start_after.to_owned()..).enumerate() {
                if let Some((pre, _)) = path.split_once(&delimiter) {
                    // If Some split -> common prefixes
                    if idx == max_keys + 1 {
                        new_continuation_token =
                            Some(general_purpose::STANDARD_NO_PAD.encode(path));
                        break;
                    }
                    common_prefixes.insert(pre.to_string());
                } else {
                    // If None split -> Entry
                    let entry: Contents = (
                        path,
                        trace_err!(cache
                            .resources
                            .get(id)
                            .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path")))?
                        .value(),
                    )
                        .into();
                    if idx == max_keys + 1 {
                        new_continuation_token =
                            Some(general_purpose::STANDARD_NO_PAD.encode(path));
                        break;
                    }
                    keys.insert(entry);
                };
            }
        }
        (None, Some(prefix)) => {
            for (idx, (path, id)) in sorted.range(start_after.to_owned()..).enumerate() {
                let entry: Contents = if path.strip_prefix(&prefix).is_some() {
                    (
                        path,
                        trace_err!(cache
                            .resources
                            .get(id)
                            .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path")))?
                        .value(),
                    )
                        .into()
                } else {
                    continue;
                };
                if idx == max_keys + 1 {
                    new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                    break;
                }
                keys.insert(entry.clone());
            }
        }

        (None, None) => {
            for (idx, (path, id)) in sorted.range(start_after.to_owned()..).enumerate() {
                let entry: Contents = (
                    path,
                    trace_err!(cache
                        .resources
                        .get(id)
                        .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path")))?
                    .value(),
                )
                    .into();
                if idx == max_keys + 1 {
                    new_continuation_token =
                        Some(general_purpose::STANDARD_NO_PAD.encode(entry.key));
                    break;
                }
                keys.insert(entry.clone());
            }
        }
    }
    Ok((keys, common_prefixes, new_continuation_token))
}
