use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::ObjectType;
use aruna_rust_api::api::storage::services::v2::{
    CollectionRelations, DatasetRelations, ProjectRelations,
};
use std::sync::Arc;

pub fn get_object_children(input: &ObjectWithRelations) -> Vec<String> {
    Vec::from_iter(
        input
            .outbound_belongs_to
            .0
            .iter()
            .filter_map(|r| match r.target_type {
                ObjectType::OBJECT => Some(r.target_pid.to_string()),
                _ => None,
            }),
    )
}
pub fn get_dataset_relations(input: &ObjectWithRelations) -> DatasetRelations {
    let origin = input.object.id.to_string();
    let children = get_object_children(input);
    DatasetRelations {
        origin,
        object_children: children,
    }
}

pub fn get_collection_children(input: &ObjectWithRelations, cache: &Cache) -> CollectionRelations {
    let origin = input.object.id;
    let mut dataset_children: Vec<DatasetRelations> = Vec::new();
    let object_children = get_object_children(input);
    for d in input
        .outbound_belongs_to
        .0
        .iter()
        .filter(|r| r.target_type == ObjectType::DATASET)
    {
        if let Some(ds) = cache.get_object(&d.origin_pid) {
            dataset_children.push(get_dataset_relations(&ds))
        };
    }
    CollectionRelations {
        origin: origin.to_string(),
        dataset_children,
        object_children,
    }
}
pub fn get_project_children(input: &ObjectWithRelations, cache: &Cache) -> ProjectRelations {
    let origin = input.object.id;
    let mut collection_children: Vec<CollectionRelations> = Vec::new();
    let mut dataset_children: Vec<DatasetRelations> = Vec::new();
    let object_children = get_object_children(input);
    for c in input
        .outbound_belongs_to
        .0
        .iter()
        .filter(|r| r.target_type == ObjectType::COLLECTION)
    {
        if let Some(col) = cache.get_object(&c.origin_pid) {
            collection_children.push(get_collection_children(&col, cache))
        };
    }
    for d in input
        .outbound_belongs_to
        .0
        .iter()
        .filter(|r| r.target_type == ObjectType::DATASET)
    {
        if let Some(ds) = cache.get_object(&d.origin_pid) {
            dataset_children.push(get_dataset_relations(&ds))
        };
    }
    ProjectRelations {
        origin: origin.to_string(),
        collection_children,
        dataset_children,
        object_children,
    }
}
