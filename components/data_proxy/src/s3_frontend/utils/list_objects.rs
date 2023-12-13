use crate::caching::cache::Cache;
use crate::structs::{Object, ObjectLocation};
use crate::structs::{ResourceIds, ResourceString};
use crate::trace_err;
use ahash::RandomState;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::DataClass;
use base64::engine::general_purpose;
use base64::Engine;
use chrono::NaiveDateTime;
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use s3s::s3_error;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Debug, Eq, PartialEq, Hash, Clone, PartialOrd, Ord)]
pub struct Contents {
    pub key: String,
    pub etag: DieselUlid,
    pub size: i64,
    pub storage_class: DataClass,
    pub created_at: Option<NaiveDateTime>,
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
            created_at: value.1 .0.created_at,
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
            ResourceString::Object(temp_root, collection, dataset, object) if temp_root == root => {
                let mut path_components = vec![];
                if let Some(coll_name) = collection {
                    path_components.push(coll_name)
                }
                if let Some(dataset_name) = dataset {
                    path_components.push(dataset_name)
                }
                path_components.push(object);

                Some((path_components.join("/"), e.value().into()))
            }
            _ => None,
        })
        .collect()
}

#[tracing::instrument(
    level = "trace",
    skip(sorted, cache, delimiter, prefix, start_at, max_keys)
)]
pub fn list_response(
    sorted: BTreeMap<String, DieselUlid>,
    cache: &Arc<Cache>,
    delimiter: &Option<String>,
    prefix: &Option<String>,
    start_at: &str,
    max_keys: usize,
) -> Result<(BTreeSet<Contents>, BTreeSet<String>, Option<String>)> {
    let mut keys: BTreeSet<Contents> = BTreeSet::default();
    let mut common_prefixes: BTreeSet<String> = BTreeSet::default();
    let mut new_continuation_token: Option<String> = None;

    match (delimiter.clone(), prefix.clone()) {
        (Some(delimiter), Some(prefix)) => {
            for (path, id) in sorted.range(start_at.to_owned()..) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                    break;
                }

                if let Some(stripped_path) = path.strip_prefix(&prefix) {
                    if let Some((common_prefix, _)) = stripped_path.split_once(&delimiter) {
                        common_prefixes.insert(format!(
                            "{}{}",
                            [prefix.to_string(), common_prefix.to_string()].join(""),
                            delimiter
                        ));
                    } else {
                        keys.insert(
                            (
                                path,
                                cache
                                    .resources
                                    .get(id)
                                    .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?
                                    .value(),
                            )
                                .into(),
                        );
                    };
                } else {
                    continue;
                };
            }
        }
        (Some(delimiter), None) => {
            for (path, id) in sorted.range(start_at.to_owned()..) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                    break;
                }

                if let Some((common_prefix, _)) = path.split_once(&delimiter) {
                    // Collect common prefix with delimiter at its end
                    common_prefixes.insert(format!("{}{}", common_prefix, delimiter));
                } else {
                    // If None split -> Entry
                    keys.insert(
                        (
                            path,
                            cache
                                .resources
                                .get(id)
                                .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path"))?
                                .value(),
                        )
                            .into(),
                    );
                };
            }
        }
        (None, Some(prefix)) => {
            for (path, id) in sorted.range(start_at.to_owned()..) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                    break;
                }

                if path.strip_prefix(&prefix).is_some() {
                    keys.insert(
                        (
                            path,
                            trace_err!(cache
                                .resources
                                .get(id)
                                .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path")))?
                            .value(),
                        )
                            .into(),
                    );
                } else {
                    continue;
                };
            }
        }
        (None, None) => {
            for (path, id) in sorted.range(start_at.to_owned()..) {
                // Breaks with next path to start at after max_keys is reached
                let num_keys = keys.len() + common_prefixes.len();
                if num_keys == max_keys {
                    new_continuation_token = Some(general_purpose::STANDARD_NO_PAD.encode(path));
                    break;
                }

                keys.insert(
                    (
                        path,
                        trace_err!(cache
                            .resources
                            .get(id)
                            .ok_or_else(|| s3_error!(NoSuchKey, "No key found for path")))?
                        .value(),
                    )
                        .into(),
                );
            }
        }
    }

    Ok((keys, common_prefixes, new_continuation_token))
}
