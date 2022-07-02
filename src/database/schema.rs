// @generated automatically by Diesel CLI.

diesel::table! {
    collection_labels (collection_id, label_id) {
        collection_id -> Uuid,
        label_id -> Uuid,
    }
}

diesel::table! {
    collections (id) {
        id -> Uuid,
        name -> Text,
        description -> Text,
    }
}

diesel::table! {
    labels (id) {
        id -> Uuid,
        key -> Text,
        value -> Text,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    collection_labels,
    collections,
    labels,
);
