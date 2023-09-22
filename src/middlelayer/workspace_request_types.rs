use crate::database::{
    dsls::{
        object_dsl::Object,
        user_dsl::{User, UserAttributes},
        workspaces_dsl::WorkspaceTemplate,
        Empty,
    },
    enums::ObjectMapping,
};
use anyhow::Result;
use aruna_rust_api::api::storage::services::v2::{
    CreateWorkspaceRequest, CreateWorkspaceTemplateRequest,
};
use dashmap::DashMap;
use diesel_ulid::DieselUlid;
use postgres_types::Json;

pub struct CreateTemplate(pub CreateWorkspaceTemplateRequest);
pub struct CreateWorkspace(pub CreateWorkspaceRequest);

impl CreateTemplate {
    pub fn get_template(&self, owner: DieselUlid) -> Result<WorkspaceTemplate> {
        let workspace = WorkspaceTemplate {
            id: DieselUlid::generate(),
            name: self.0.name.clone(),
            description: "PLACEHOLDER".to_string(), //TODO: Workspace descriptions
            owner,
            prefix: self.0.prefix.clone(),
            // TODO: Remove key values or implement label validation & enforcement
            key_values: Json((&self.0.key_values).try_into()?),
            // TODO: Missing hooks
            // TODO: Add option to specify endpoint
            // TODO (optional): Add subresource structure
        };
        Ok(workspace)
    }
}

impl CreateWorkspace {
    pub fn get_name(&self) -> String {
        self.0.workspace_template.clone()
    }

    pub fn make_project(template: WorkspaceTemplate, endpoint: DieselUlid) -> Object {
        let id = DieselUlid::generate();
        let project = Object {
            id,
            revision_number: 0,
            name: [template.prefix, id.to_string()].join("-"),
            description: "".to_string(), // TODO: Add template descriptions
            created_at: None,
            created_by: template.owner,
            content_len: 0,
            count: 0,
            key_values: template.key_values, //TODO: Using keyvals this way is weird
            object_status: crate::database::enums::ObjectStatus::AVAILABLE,
            data_class: crate::database::enums::DataClass::WORKSPACE,
            object_type: crate::database::enums::ObjectType::PROJECT,
            external_relations: Json(crate::database::dsls::object_dsl::ExternalRelations(
                DashMap::default(),
            )),
            hashes: Json(crate::database::dsls::object_dsl::Hashes(Vec::new())),
            dynamic: true,
            endpoints: Json(DashMap::from_iter(vec![(endpoint, true)].into_iter())),
        };

        project
    }

    pub fn create_service_account(endpoint: DieselUlid, workspace_id: DieselUlid) -> User {
        let user_id = DieselUlid::generate();
        let service_account = User {
            id: user_id,
            display_name: ["SERVICE_ACCOUNT".to_string(), user_id.to_string()].join("#"),
            external_id: None,
            email: "".to_string(),
            attributes: Json(UserAttributes {
                global_admin: false,
                service_account: true,
                tokens: DashMap::default(),
                trusted_endpoints: dashmap::DashMap::from_iter(
                    vec![(endpoint, Empty {})].into_iter(),
                ),
                custom_attributes: vec![],
                permissions: DashMap::from_iter(vec![(
                    workspace_id,
                    ObjectMapping::PROJECT(crate::database::enums::DbPermissionLevel::APPEND),
                )]),
            }),
            active: true,
        };
        service_account
    }
}
