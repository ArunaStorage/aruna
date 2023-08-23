use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_server::{
    database::{
        dsls::object_dsl::{KeyValue, KeyValueVariant},
        enums::{DataClass, ObjectStatus, ObjectType},
    },
    search::meilisearch_client::{MeilisearchClient, MeilisearchIndexes, ObjectDocument},
};
use chrono::NaiveDateTime;
use diesel_ulid::DieselUlid;
use rand::{seq::IteratorRandom, thread_rng, Rng};

mod common;

#[tokio::test]
async fn search_test() {
    // Create Meilisearch client
    let meilisearch_client =
        MeilisearchClient::new("http://localhost:7700", Some("MASTER_KEY")).unwrap();

    // Create index
    meilisearch_client
        .get_or_create_index("objects", Some("id"))
        .await
        .unwrap();

    // Generate random index objects
    let index_documents = (0..10)
        .into_iter()
        .map(|_| generate_random_object_document())
        .collect::<Vec<_>>();

    // Put objects in index
    meilisearch_client
        .add_or_update_stuff(index_documents.as_slice(), MeilisearchIndexes::OBJECT)
        .await
        .unwrap()
        .wait_for_completion(&meilisearch_client.client, None, None)
        .await
        .unwrap();

    // List index for and check if index contains all created documents
    let all_documents = meilisearch_client.list_index("objects").await.unwrap();
/* 
    for doc in &index_documents {
        if !(all_documents.contains(doc)) {
            dbg!(all_documents.len());
            dbg!(doc);
        }

        assert!(all_documents.contains(doc))
    }
*/
    
       index_documents
           .iter()
           .for_each(|doc| assert!(all_documents.contains(doc)));
    
    // Query some specific stuff without filter/sorting
    let mut specific_document = index_documents.first().unwrap().to_owned();
    let document_id = specific_document.id.to_string();
    let search_query = specific_document.size.to_string();

    let (hits, estimated_total) = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", &search_query, "", 1000, 0)
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(estimated_total, 1);

    // Query some stuff with broken filter
    let mut query_filter = r#"resource_status IN [AVAILABLE, "ERROR"]"#;
    let result = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", "whatev", query_filter, 1000, 0)
        .await;
    assert!(result.is_err()); // resource_status is not in the list of filterable attributes

    // Update an index document
    specific_document.data_class = DataClass::PRIVATE;
    meilisearch_client
        .add_or_update_stuff(&[specific_document], MeilisearchIndexes::OBJECT)
        .await
        .unwrap()
        .wait_for_completion(&meilisearch_client.client, None, None)
        .await
        .unwrap();

    // Query updated document by unique dataclass
    query_filter = r#"data_class = PRIVATE"#;
    let (hits, estimated_total) = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", "ChatGPT", query_filter, 1000, 0)
        .await
        .unwrap();

    assert_eq!(hits.len(), 1);
    assert_eq!(estimated_total, 1);

    // Remove some index document
    meilisearch_client
        .delete_stuff(&[document_id], MeilisearchIndexes::OBJECT)
        .await
        .unwrap()
        .wait_for_completion(&meilisearch_client.client, None, None)
        .await
        .unwrap();

    let (hits, estimated_total) = meilisearch_client
        .query_generic_stuff::<ObjectDocument>("objects", &search_query, "", 1000, 0)
        .await
        .unwrap();

    assert_eq!(hits.len(), 0);
    assert_eq!(estimated_total, 0); // specific document is not in search index anymore

    // Convert all index documents to proto representation
    for doc in all_documents {
        generic_resource::Resource::try_from(doc).unwrap();
    }
}

fn generate_random_object_document() -> ObjectDocument {
    let mut rng = thread_rng();
    let name_parts = vec![
        "rna", "hyper", "breaking", "sequence", "mapper", "stuff", "creature", "bacteria",
        "science", "dna", "cdna",
    ];
    let project_name = name_parts
        .into_iter()
        .choose_multiple(&mut rng, 2)
        .to_vec()
        .join("-");
    let rand_size = rng.gen_range(123..123456789);
    let hook_run_success = rng.gen_bool(0.5).to_string();
    let created_at = format!("{}-01-01 23:59:59", rng.gen_range(2001..2023));

    ObjectDocument {
        id: DieselUlid::generate(),
        object_type: ObjectType::try_from(rng.gen_range(1..5)).unwrap() as u8,
        object_status: ObjectStatus::try_from(rng.gen_range(1..6)).unwrap(),
        name: project_name,
        description: "ChatGPT should create some hallucinated description of this project."
            .to_string(),
        size: rand_size,
        labels: vec![
            KeyValue {
                key: "validated".to_string(),
                value: hook_run_success.clone(),
                variant: KeyValueVariant::LABEL,
            },
            KeyValue {
                key: "submitted".to_string(),
                value: hook_run_success,
                variant: KeyValueVariant::LABEL,
            },
            KeyValue {
                key: "validate_and_submit".to_string(),
                value: "fastq;ENA".to_string(),
                variant: KeyValueVariant::HOOK,
            },
        ],
        data_class: DataClass::PUBLIC,
        created_at: NaiveDateTime::parse_from_str(&created_at, "%Y-%m-%d %H:%M:%S")
            .unwrap()
            .timestamp(),
        dynamic: rng.gen_bool(0.5).to_string().parse::<bool>().unwrap(),
    }
}
