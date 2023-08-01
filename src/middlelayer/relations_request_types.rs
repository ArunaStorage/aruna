use crate::auth::structs::Context;
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{ExternalRelation, Object};
use crate::database::enums::DbPermissionLevel;
use crate::database::enums::ObjectType;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v2::{relation, Relation};
use aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct ModifyRelations(pub ModifyRelationsRequest);

pub struct ModifyLabels {
    pub labels_to_add: LabelsToAdd,
    pub labels_to_remove: LabelsToRemove,
    pub resources_to_check: Vec<Context>,
}
pub struct LabelsToAdd {
    pub external: Vec<ExternalRelation>,
    pub internal: Vec<InternalRelation>,
}
pub struct LabelsToRemove {
    pub external: Vec<ExternalRelation>,
    pub internal: Vec<InternalRelation>,
}
impl ModifyRelations {
    pub fn get_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.resource_id)?)
    }
    pub fn get_labels(&self, resource: Object) -> Result<ModifyLabels> {
        let resource_id = resource.id;
        let resource_variant = resource.object_type;

        let mut ulids_to_check: Vec<Context> = vec![Context::res_ctx(
            resource_id,
            DbPermissionLevel::WRITE,
            true,
        )];
        let (external_add_relations, internal_add_relations, mut added_to_check) =
            ModifyRelations::convert_labels(&self.0.add_relations, resource_id, resource_variant)?;
        let (external_rm_relations, internal_rm_relations, mut removed_to_check) =
            ModifyRelations::convert_labels(&self.0.add_relations, resource_id, resource_variant)?;
        ulids_to_check.append(&mut added_to_check);
        ulids_to_check.append(&mut removed_to_check);
        Ok(ModifyLabels {
            labels_to_add: LabelsToAdd {
                external: external_add_relations,
                internal: internal_add_relations,
            },
            labels_to_remove: LabelsToRemove {
                external: external_rm_relations,
                internal: internal_rm_relations,
            },
            resources_to_check: ulids_to_check,
        })
    }
    fn convert_labels(
        api_relations: &Vec<Relation>,
        resource_id: DieselUlid,
        resource_variant: ObjectType,
    ) -> Result<(Vec<ExternalRelation>, Vec<InternalRelation>, Vec<Context>)> {
        let mut external_relations: Vec<ExternalRelation> = Vec::new();
        let mut internal_relations: Vec<InternalRelation> = Vec::new();
        let mut resources_to_check: Vec<Context> = Vec::new();
        for relation in api_relations {
            if let Some(rel) = &relation.relation {
                match rel {
                    relation::Relation::External(external) => {
                        external_relations.push(external.try_into()?)
                    }
                    relation::Relation::Internal(internal) => {
                        resources_to_check.push(Context::res_ctx(
                            DieselUlid::from_str(&internal.resource_id)?,
                            DbPermissionLevel::WRITE,
                            true,
                        ));
                        internal_relations
                            .push((internal, (resource_id, resource_variant)).try_into()?);
                    }
                }
            }
        }
        Ok((external_relations, internal_relations, resources_to_check))
    }
}
