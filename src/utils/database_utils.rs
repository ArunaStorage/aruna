use postgres_types::ToSql;

use crate::database::{dsls::object_dsl::ObjectWithRelations, enums::ObjectType};

pub fn create_multi_query(vec: &Vec<&(dyn ToSql + Sync)>) -> String {
    let mut result = "(".to_string();
    for count in 1..vec.len() {
        result.push_str(format!("${count},").as_str());
    }
    let last = vec.len();
    result.push_str(format!("${last})").as_str());
    result
}

pub fn sort_objects(vec: &mut [ObjectWithRelations]) {
    vec.sort_by(|x, y| match (x.object.object_type, y.object.object_type) {
        (ObjectType::PROJECT, ObjectType::PROJECT) => std::cmp::Ordering::Equal,
        (ObjectType::PROJECT, ObjectType::COLLECTION)
        | (ObjectType::PROJECT, ObjectType::DATASET)
        | (ObjectType::PROJECT, ObjectType::OBJECT) => std::cmp::Ordering::Less,
        (ObjectType::COLLECTION, ObjectType::PROJECT) => std::cmp::Ordering::Greater,
        (ObjectType::COLLECTION, ObjectType::COLLECTION) => std::cmp::Ordering::Equal,
        (ObjectType::COLLECTION, ObjectType::DATASET) => std::cmp::Ordering::Less,
        (ObjectType::COLLECTION, ObjectType::OBJECT) => std::cmp::Ordering::Less,
        (ObjectType::DATASET, ObjectType::PROJECT) => std::cmp::Ordering::Greater,
        (ObjectType::DATASET, ObjectType::COLLECTION) => std::cmp::Ordering::Greater,
        (ObjectType::DATASET, ObjectType::DATASET) => std::cmp::Ordering::Equal,
        (ObjectType::DATASET, ObjectType::OBJECT) => std::cmp::Ordering::Less,
        (ObjectType::OBJECT, ObjectType::PROJECT) => std::cmp::Ordering::Greater,
        (ObjectType::OBJECT, ObjectType::COLLECTION) => std::cmp::Ordering::Greater,
        (ObjectType::OBJECT, ObjectType::DATASET) => std::cmp::Ordering::Greater,
        (ObjectType::OBJECT, ObjectType::OBJECT) => std::cmp::Ordering::Equal,
    })
}
