use crate::caching::structs::ObjectWrapper;
use crate::database::dsls::license_dsl::License;
use crate::database::dsls::object_dsl::{
    Author as DBAuthor, Hash as DBHash, Hashes, KeyValue as DBKeyValue, KeyValueVariant, KeyValues,
    Object,
};
use crate::database::dsls::workspaces_dsl::WorkspaceTemplate;
use crate::database::dsls::{
    internal_relation_dsl::InternalRelation, object_dsl::ObjectWithRelations,
};
use crate::database::enums::ObjectType;
use crate::middlelayer::create_request_types::Parent;
use crate::utils::conversions::relations::from_db_internal_relation;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::{
    generic_resource, relation::Relation as RelationEnum, Collection as GRPCCollection,
    Dataset as GRPCDataset, Hash, License as APILicense, Object as GRPCObject,
    Project as GRPCProject, Relation,
};
use aruna_rust_api::api::storage::models::v2::{
    Author, DataEndpoint, KeyValue, ReplicationStatus as APIReplicationStatus, RuleBinding, Stats,
};
use aruna_rust_api::api::storage::services::v2::{
    create_collection_request, create_dataset_request, create_object_request, CreateLicenseRequest,
    WorkspaceInfo,
};
use diesel_ulid::DieselUlid;
use serde::ser::SerializeStruct;
use serde::Serialize;
use std::str::FromStr;

impl From<ObjectWrapper> for generic_resource::Resource {
    fn from(wrapped: ObjectWrapper) -> generic_resource::Resource {
        let object_with_relations = wrapped.object_with_relations;
        let rules = wrapped
            .rules
            .iter()
            .map(|r| RuleBinding {
                rule_id: r.rule_id.to_string(),
                origin: r.origin_id.to_string(),
            })
            .collect::<Vec<RuleBinding>>();
        let (inbound, outbound) = (
            object_with_relations
                .inbound
                .0
                .iter()
                .chain(object_with_relations.inbound_belongs_to.0.iter())
                .map(|r| r.clone())
                .collect::<Vec<InternalRelation>>(),
            object_with_relations
                .outbound
                .0
                .iter()
                .chain(object_with_relations.outbound_belongs_to.0.iter())
                .map(|r| r.clone())
                .collect::<Vec<InternalRelation>>(),
        );

        let mut inbound = inbound
            .into_iter()
            .map(|r| from_db_internal_relation(r, true))
            .collect::<Vec<_>>();

        let mut outbound = outbound
            .into_iter()
            .map(|r| from_db_internal_relation(r, false))
            .collect::<Vec<_>>();
        let mut relations: Vec<Relation> = object_with_relations
            .object
            .external_relations
            .0
             .0
            .into_iter()
            .map(|r| Relation {
                relation: Some(RelationEnum::External(r.1.into())),
            })
            .collect();
        relations.append(&mut inbound);
        relations.append(&mut outbound);
        let stats = Some(Stats {
            count: object_with_relations.object.count,
            size: object_with_relations.object.content_len,
            // Should this not be the last updated object?
            last_updated: object_with_relations.object.created_at.map(|t| t.into()),
        });

        match object_with_relations.object.object_type {
            ObjectType::PROJECT => generic_resource::Resource::Project(GRPCProject {
                id: object_with_relations.object.id.to_string(),
                name: object_with_relations.object.name,
                title: object_with_relations.object.title.to_string(),
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                stats,
                created_by: object_with_relations.object.created_by.to_string(),
                authors: object_with_relations
                    .object
                    .authors
                    .0
                    .into_iter()
                    .map(|a| a.into())
                    .collect(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        variant: Some(e.replication.into()),
                        status: None,
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                default_data_license_tag: object_with_relations.object.data_license,
                rule_bindings: rules,
            }),
            ObjectType::COLLECTION => generic_resource::Resource::Collection(GRPCCollection {
                id: object_with_relations.object.id.to_string(),
                name: object_with_relations.object.name,
                title: object_with_relations.object.title.to_string(),
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                stats,
                created_by: object_with_relations.object.created_by.to_string(),
                authors: object_with_relations
                    .object
                    .authors
                    .0
                    .into_iter()
                    .map(|a| a.into())
                    .collect(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        variant: Some(e.replication.into()),
                        status: None,
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                default_data_license_tag: object_with_relations.object.data_license,
                rule_bindings: rules,
            }),
            ObjectType::DATASET => generic_resource::Resource::Dataset(GRPCDataset {
                id: object_with_relations.object.id.to_string(),
                name: object_with_relations.object.name,
                title: object_with_relations.object.title.to_string(),
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                stats,
                created_by: object_with_relations.object.created_by.to_string(),
                authors: object_with_relations
                    .object
                    .authors
                    .0
                    .into_iter()
                    .map(|a| a.into())
                    .collect(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        variant: Some(e.replication.into()),
                        status: None,
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                default_data_license_tag: object_with_relations.object.data_license,
                rule_bindings: rules,
            }),
            ObjectType::OBJECT => generic_resource::Resource::Object(GRPCObject {
                id: object_with_relations.object.id.to_string(),
                content_len: object_with_relations.object.content_len,
                name: object_with_relations.object.name,
                title: object_with_relations.object.title.to_string(),
                description: object_with_relations.object.description,
                created_at: object_with_relations.object.created_at.map(|t| t.into()),
                created_by: object_with_relations.object.created_by.to_string(),
                authors: object_with_relations
                    .object
                    .authors
                    .0
                    .into_iter()
                    .map(|a| a.into())
                    .collect(),
                data_class: object_with_relations.object.data_class.into(),
                dynamic: object_with_relations.object.dynamic,
                hashes: object_with_relations.object.hashes.0.into(),
                key_values: object_with_relations.object.key_values.0.into(),
                status: object_with_relations.object.object_status.into(),
                relations,
                endpoints: object_with_relations
                    .object
                    .endpoints
                    .0
                    .iter()
                    .map(|e| DataEndpoint {
                        id: e.key().to_string(),
                        variant: Some(e.replication.into()),
                        status: e.status.map(|s| APIReplicationStatus::from(s) as i32),
                    })
                    .collect(),
                metadata_license_tag: object_with_relations.object.metadata_license,
                data_license_tag: object_with_relations.object.data_license,
                rule_bindings: rules,
            }),
        }
    }
}

impl From<create_collection_request::Parent> for Parent {
    fn from(value: create_collection_request::Parent) -> Self {
        match value {
            create_collection_request::Parent::ProjectId(pid) => Parent::Project(pid),
        }
    }
}

impl From<create_dataset_request::Parent> for Parent {
    fn from(value: create_dataset_request::Parent) -> Self {
        match value {
            create_dataset_request::Parent::ProjectId(pid) => Parent::Project(pid),
            create_dataset_request::Parent::CollectionId(cid) => Parent::Collection(cid),
        }
    }
}

impl From<create_object_request::Parent> for Parent {
    fn from(value: create_object_request::Parent) -> Self {
        match value {
            create_object_request::Parent::ProjectId(pid) => Parent::Project(pid),
            create_object_request::Parent::CollectionId(cid) => Parent::Collection(cid),
            create_object_request::Parent::DatasetId(did) => Parent::Dataset(did),
        }
    }
}

impl Serialize for Object {
    fn serialize<S>(&self, serializer: S) -> std::prelude::v1::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("object", 18)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("revision_number", &self.revision_number)?;
        state.serialize_field("title", &self.title)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("description", &self.description)?;
        state.serialize_field("created_at", &self.created_at)?;
        state.serialize_field("created_by", &self.created_by)?;
        state.serialize_field("authors", &self.authors.0)?;
        state.serialize_field("content_len", &self.content_len)?;
        state.serialize_field("count", &self.count)?;
        state.serialize_field("key_values", &self.key_values.0)?;
        state.serialize_field("object_status", &self.object_status)?;
        state.serialize_field("data_class", &self.data_class)?;
        state.serialize_field("object_type", &self.object_type)?;
        state.serialize_field("external_relations", &self.external_relations.0)?;
        state.serialize_field("hashes", &self.hashes.0)?;
        state.serialize_field("dynamic", &self.dynamic)?;
        state.serialize_field("endpoints", &self.endpoints.0)?;
        state.serialize_field("metadata_license", &self.metadata_license)?;
        state.serialize_field("data_license", &self.data_license)?;
        state.end()
    }
}

impl Serialize for ObjectWithRelations {
    fn serialize<S>(&self, serializer: S) -> std::prelude::v1::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("object", 5)?;
        state.serialize_field("object", &self.object)?;
        state.serialize_field("inbound", &self.inbound.0)?;
        state.serialize_field("inbound_belongs_to", &self.inbound_belongs_to.0)?;
        state.serialize_field("outbound", &self.outbound.0)?;
        state.serialize_field("outbound_belongs_to", &self.outbound_belongs_to.0)?;
        state.end()
    }
}

impl TryFrom<&Vec<KeyValue>> for KeyValues {
    type Error = anyhow::Error;
    fn try_from(key_val: &Vec<KeyValue>) -> Result<Self> {
        let mut key_vals: Vec<DBKeyValue> = Vec::new();
        for kv in key_val {
            let kv = kv.try_into()?;
            key_vals.push(kv);
        }
        Ok(KeyValues(key_vals))
    }
}

impl TryFrom<&KeyValue> for DBKeyValue {
    type Error = anyhow::Error;
    fn try_from(key_val: &KeyValue) -> Result<Self> {
        Ok(DBKeyValue {
            key: key_val.key.clone(),
            value: key_val.value.clone(),
            variant: key_val.variant.try_into()?,
        })
    }
}
impl From<KeyValues> for Vec<KeyValue> {
    //noinspection ALL
    fn from(keyval: KeyValues) -> Self {
        keyval
            .0
            .into_iter()
            .map(|kv| KeyValue {
                key: kv.key,
                value: kv.value,
                variant: match kv.variant {
                    KeyValueVariant::LABEL => 1,
                    KeyValueVariant::STATIC_LABEL => 2,
                    KeyValueVariant::HOOK => 3,
                    KeyValueVariant::HOOK_STATUS => 4,
                },
            })
            .collect()
    }
}

impl TryFrom<Vec<Hash>> for Hashes {
    type Error = anyhow::Error;
    fn try_from(h: Vec<Hash>) -> Result<Self> {
        let mut hashes = Vec::new();
        for h in h {
            hashes.push(DBHash {
                alg: h.alg.try_into()?,
                hash: h.hash,
            })
        }

        Ok(Hashes(hashes))
    }
}

impl From<WorkspaceTemplate> for WorkspaceInfo {
    fn from(ws: WorkspaceTemplate) -> Self {
        WorkspaceInfo {
            workspace_id: ws.id.to_string(),
            name: ws.name,
            description: ws.description,
            owner: ws.owner.to_string(),
            prefix: ws.prefix,
            hook_ids: ws.hook_ids.0.iter().map(DieselUlid::to_string).collect(),
            endpoint_ids: ws
                .endpoint_ids
                .0
                .iter()
                .map(DieselUlid::to_string)
                .collect(),
        }
    }
}

impl From<License> for APILicense {
    fn from(lic: License) -> Self {
        APILicense {
            tag: lic.tag,
            name: lic.name,
            text: lic.text,
            url: lic.url,
        }
    }
}
impl From<CreateLicenseRequest> for License {
    fn from(req: CreateLicenseRequest) -> Self {
        License {
            tag: req.tag,
            name: req.name,
            text: req.text,

            url: req.url,
        }
    }
}

impl From<DBAuthor> for Author {
    fn from(value: DBAuthor) -> Self {
        Author {
            first_name: value.first_name.clone(),
            last_name: value.last_name.clone(),
            email: value.email.map(|e| e.to_string()),
            orcid: value.orcid.map(|o| o.to_string()),
            id: value.user_id.map(|o| o.to_string()),
        }
    }
}

impl TryFrom<Author> for DBAuthor {
    type Error = anyhow::Error;
    fn try_from(value: Author) -> Result<Self> {
        let user_id = if let Some(user_id) = value.id {
            Some(DieselUlid::from_str(&user_id)?)
        } else {
            None
        };
        Ok(DBAuthor {
            first_name: value.first_name,
            last_name: value.last_name,
            email: value.email,
            orcid: value.orcid,
            user_id,
        })
    }
}
