use crate::{
    database::{
        connection::Database,
        crud::utils::is_bit_set,
        models::{
            auth::{ApiToken, Project, User},
            collection::{Collection, CollectionKeyValue},
        },
    },
    error::ArunaError,
};
use aruna_rust_api::api::storage::services::v1::{CreateWorkspaceRequest, CreateWorkspaceResponse};
use chrono::Months;
use diesel::{
    insert_into, Connection, ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl,
};
use rand::{distributions::Alphanumeric, thread_rng, Rng};

impl Database {
    pub fn create_workspace(
        &self,
        request: CreateWorkspaceRequest,
        pubkey_id: i64,
    ) -> Result<CreateWorkspaceResponse, ArunaError> {
        use crate::database::schema::api_tokens::dsl as tokens_dsl;
        use crate::database::schema::collection_key_value::dsl as col_kv_dsl;
        use crate::database::schema::collections::dsl as col_dsl;
        use crate::database::schema::projects::dsl as project_dsl;
        use crate::database::schema::users::dsl as user_dsl;

        self.pg_connection
            .get()?
            .transaction::<CreateWorkspaceResponse, ArunaError, _>(|conn| {
                let project: Project = project_dsl::projects
                    .filter(project_dsl::name.eq(request.project_name))
                    .first::<Project>(conn)?;

                // Check if project is workspace "enabled" via 0b10 == 2 flag
                if !is_bit_set(project.flag, 1) {
                    return Err(ArunaError::InvalidRequest(
                        "Cannot create workspace for project".to_string(),
                    ));
                }

                let workspace_template: Option<Collection> = col_dsl::collections
                    .filter(col_dsl::project_id.eq(&project.id))
                    .filter(col_dsl::name.eq(format!("{}-template", project.name)))
                    .first::<Collection>(conn)
                    .optional()?;

                let workspace_id = diesel_ulid::DieselUlid::generate();
                let workspace_user_id = diesel_ulid::DieselUlid::generate();

                let workspace_user = User {
                    id: workspace_user_id,
                    external_id: String::new(),
                    display_name: format!(
                        "workspace-user-{}",
                        workspace_id.to_string().to_lowercase()
                    ),
                    active: true,
                    is_service_account: true,
                    email: String::new(),
                };

                insert_into(user_dsl::users)
                    .values(&workspace_user)
                    .execute(conn)?;

                let collection = Collection {
                    id: workspace_id,
                    shared_version_id: diesel_ulid::DieselUlid::generate(),
                    name: workspace_id.to_string().to_lowercase(),
                    description: format!("Anonymous workspace: {}", workspace_id),
                    created_at: chrono::Utc::now().naive_utc(),
                    created_by: workspace_user_id,
                    version_id: None,
                    dataclass: None,
                    project_id: project.id,
                };

                insert_into(col_dsl::collections)
                    .values(&collection)
                    .execute(conn)?;

                if let Some(template) = workspace_template {
                    let template_key_values: Vec<CollectionKeyValue> =
                        col_kv_dsl::collection_key_value
                            .filter(col_kv_dsl::collection_id.eq(&template.id))
                            .load::<CollectionKeyValue>(conn)?;

                    let mapped = template_key_values
                        .into_iter()
                        .map(|mut kv| {
                            kv.id = diesel_ulid::DieselUlid::generate();
                            kv.collection_id = workspace_id;
                            kv
                        })
                        .collect::<Vec<_>>();

                    insert_into(col_kv_dsl::collection_key_value)
                        .values(&mapped)
                        .execute(conn)?;
                }

                // Create random access_key
                let secret_key: String = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(30)
                    .map(char::from)
                    .collect();

                let token_id = diesel_ulid::DieselUlid::generate();

                let api_token = ApiToken {
                    id: token_id,
                    creator_user_id: workspace_user_id,
                    pub_key: pubkey_id,
                    name: Some(format!(
                        "ws-token-{}",
                        workspace_id.to_string().to_lowercase()
                    )),
                    created_at: chrono::Utc::now().naive_utc(),
                    expires_at: chrono::Utc::now()
                        .naive_utc()
                        .checked_add_months(Months::new(1)),
                    project_id: None,
                    collection_id: Some(workspace_id),
                    user_right: Some(crate::database::models::enums::UserRights::WRITE),
                    secretkey: secret_key,
                    used_at: chrono::Utc::now().naive_utc(),
                    is_session: false,
                };

                insert_into(tokens_dsl::api_tokens)
                    .values(api_token)
                    .execute(conn)?;

                Ok(CreateWorkspaceResponse {
                    workspace_id: workspace_id.to_string(),
                    token: String::new(),
                    access_key: token_id.to_string(),
                    secret_key: secret_key.to_string(),
                })
            })
    }
}
