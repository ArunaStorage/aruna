use crate::database::dsls::hook_dsl::{BasicTemplate, Credentials, ExternalHook, TemplateVariant};
use crate::database::dsls::user_dsl::APIToken;
use crate::database::enums::{ObjectMapping, ObjectStatus, ObjectType};
use crate::middlelayer::hooks_request_types::CustomTemplate;
use crate::middlelayer::presigned_url_handler::PresignedDownload;
use crate::middlelayer::relations_request_types::ModifyRelations;
use crate::{
    auth::permission_handler::PermissionHandler,
    database::dsls::{
        hook_dsl::{HookStatusValues, HookStatusVariant, HookWithAssociatedProject},
        object_dsl::{KeyValue, KeyValueVariant, Object, ObjectWithRelations},
    },
    middlelayer::db_handler::DatabaseHandler,
};
use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsResponse;
use aruna_rust_api::api::storage::models::v2::{
    KeyValue as APIKeyVals, KeyValueVariant as APIKeyValVariant,
};
use aruna_rust_api::api::storage::services::v2::{
    GetDownloadUrlRequest, UpdateCollectionKeyValuesRequest, UpdateObjectRequest,
    UpdateProjectKeyValuesRequest,
};
use async_channel::Receiver;
use diesel_ulid::DieselUlid;
use http::header::CONTENT_TYPE;
use std::sync::Arc;

#[derive(Clone)]
pub struct HookHandler {
    pub reciever: Receiver<HookMessage>,
    pub authorizer: Arc<PermissionHandler>,
    pub database_handler: Arc<DatabaseHandler>,
}

#[derive(Debug, Clone)]
pub struct HookMessage {
    pub hook: HookWithAssociatedProject,
    pub object: ObjectWithRelations,
    pub user_id: DieselUlid,
}

impl HookHandler {
    pub async fn new(
        reciever: Receiver<HookMessage>,
        authorizer: Arc<PermissionHandler>,
        database_handler: Arc<DatabaseHandler>,
    ) -> Self {
        HookHandler {
            reciever,
            authorizer,
            database_handler,
        }
    }
    pub async fn run(&self) -> Result<()> {
        let handler = self.clone();
        let client = reqwest::Client::new();
        tokio::spawn(async move {
            while let Ok(message) = handler.reciever.recv().await {
                // TODO:
                // - queue logic
                // - deduplication
                // - retries
                if let Err(action) = handler.hook_action(message, client.clone()).await {
                    log::error!("[HookHandler] ERROR: {:?}", action);
                };
            }
        });
        Ok(())
    }
    pub async fn hook_action(&self, message: HookMessage, client: reqwest::Client) -> Result<()> {
        let HookMessage {
            hook,
            object,
            user_id,
        } = message;
        let object_id = object.object.id;

        // Add running status:
        self.add_status(&hook, &object, HookStatusVariant::RUNNING)
            .await?;

        match hook.hook.0 {
            crate::database::dsls::hook_dsl::HookVariant::Internal(ref internal_hook) => {
                match internal_hook {
                    crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                        self.add_keyvals(
                            object.clone(),
                            user_id,
                            key.to_string(),
                            value.to_string(),
                            APIKeyValVariant::Label,
                        )
                        .await?;
                        // Add finished status
                        self.add_status(&hook, &object, HookStatusVariant::FINISHED)
                            .await?;
                    }
                    crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                        self.add_keyvals(
                            object.clone(),
                            user_id,
                            key.to_string(),
                            value.to_string(),
                            APIKeyValVariant::Hook,
                        )
                        .await?;
                        // Add finished status
                        self.add_status(&hook, &object, HookStatusVariant::FINISHED)
                            .await?;
                    }
                    crate::database::dsls::hook_dsl::InternalHook::CreateRelation { relation } => {
                        let relation = aruna_rust_api::api::storage::models::v2::Relation {
                            relation: Some(relation.clone()),
                        };
                        let request = ModifyRelations(
                            aruna_rust_api::api::storage::services::v2::ModifyRelationsRequest {
                                resource_id: object_id.to_string(),
                                add_relations: vec![relation],
                                remove_relations: vec![],
                            },
                        );
                        let (resource, labels_info) = self
                            .database_handler
                            .get_resource(request, self.database_handler.cache.clone())
                            .await?;
                        self.database_handler
                            .modify_relations(
                                resource,
                                labels_info.relations_to_add,
                                labels_info.relations_to_remove,
                            )
                            .await?;
                        self.add_status(&hook, &object, HookStatusVariant::FINISHED)
                            .await?;
                    }
                }
            }
            crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook {
                ref url,
                ref credentials,
                ref template,
                ref method,
            }) => {
                log::info!("[HookHandler] Starting external hook");
                // This creates only presigned download urls for available objects.
                // If ObjectType is not OBJECT, only s3 credentials are generated.
                // This should allow for generic external hooks that can also be
                // triggered for other ObjectTypes than OBJECTs
                let (secret, download, pubkey_serial, upload_credentials) = self
                    .get_template_input(object.clone(), hook.clone(), user_id)
                    .await?;
                let (access_key, secret_key) = match upload_credentials {
                    Some(ref creds) => (
                        Some(creds.access_key.to_string()),
                        Some(creds.secret_key.to_string()),
                    ),
                    None => (None, None),
                };

                // Create & send request
                //let client = reqwest::Client::new();
                let base_request = match method {
                    crate::database::dsls::hook_dsl::Method::PUT => match credentials {
                        Some(Credentials { token }) => client.put(url).bearer_auth(token),
                        None => client.put(url),
                    },
                    crate::database::dsls::hook_dsl::Method::POST => match credentials {
                        Some(Credentials { token }) => client.post(url).bearer_auth(token),
                        None => client.post(url),
                    },
                };
                dbg!(&base_request);
                // Put everything into template
                let data_request = match template {
                    TemplateVariant::Basic => {
                        let json = serde_json::to_string(&BasicTemplate {
                            hook_id: hook.id,
                            object: object.clone().into(),
                            secret,
                            download,
                            pubkey_serial,
                            access_key,
                            secret_key,
                        })?;
                        dbg!("[HookHandler] BasicTemplate: {:?}", &json);
                        //let request = base_request.json(&json);
                        //dbg!("Created request: ", &request);
                        //let response = request.send().await?;
                        //let response = base_request.json(&json);
                        //dbg!("External hook response: {:?}", response);
                        base_request.json(&json)
                    }
                    TemplateVariant::Custom(template) => {
                        let template = CustomTemplate::create_custom_template(
                            template.to_string(),
                            hook.id,
                            &object.object,
                            secret,
                            download,
                            upload_credentials,
                            pubkey_serial,
                        )?;
                        dbg!("[HookHandler] Template: {:?}", &template);
                        //let response = base_request
                        //    .header(CONTENT_TYPE, "text/plain")
                        //    .body(template);
                        //dbg!("Custom template hook response: {:?}", response);
                        //response
                        base_request
                            .header(CONTENT_TYPE, "text/plain")
                            .body(template)
                    }
                };
                let response = data_request.send().await?;
                dbg!("[HookHandler] ExternalHook response: {:?}", &response);
            }
        };
        Ok(())
    }

    async fn add_keyvals(
        &self,
        object: ObjectWithRelations,
        user_id: DieselUlid,
        key: String,
        value: String,
        variant: APIKeyValVariant,
    ) -> Result<()> {
        match object.object.object_type {
            ObjectType::PROJECT => {
                let request = crate::middlelayer::update_request_types::KeyValueUpdate::Project(
                    UpdateProjectKeyValuesRequest {
                        project_id: object.object.id.to_string(),
                        add_key_values: vec![APIKeyVals {
                            key,
                            value,
                            variant: variant as i32,
                        }],
                        remove_key_values: Vec::new(),
                    },
                );
                self.database_handler
                    .update_keyvals(request, user_id)
                    .await?;
            }
            ObjectType::COLLECTION => {
                let request = crate::middlelayer::update_request_types::KeyValueUpdate::Collection(
                    UpdateCollectionKeyValuesRequest {
                        collection_id: object.object.id.to_string(),
                        add_key_values: vec![APIKeyVals {
                            key,
                            value,
                            variant: variant as i32,
                        }],
                        remove_key_values: Vec::new(),
                    },
                );
                self.database_handler
                    .update_keyvals(request, user_id)
                    .await?;
            }
            ObjectType::DATASET => {
                let request = crate::middlelayer::update_request_types::KeyValueUpdate::Collection(
                    UpdateCollectionKeyValuesRequest {
                        collection_id: object.object.id.to_string(),
                        add_key_values: vec![APIKeyVals {
                            key,
                            value,
                            variant: variant as i32,
                        }],
                        remove_key_values: Vec::new(),
                    },
                );
                self.database_handler
                    .update_keyvals(request, user_id)
                    .await?;
            }
            ObjectType::OBJECT => {
                let request = UpdateObjectRequest {
                    object_id: object.object.id.to_string(),
                    name: None,
                    description: None,
                    add_key_values: vec![APIKeyVals {
                        key,
                        value,
                        variant: APIKeyValVariant::Label as i32,
                    }],
                    remove_key_values: Vec::new(),
                    data_class: 0,
                    hashes: Vec::new(),
                    force_revision: false,
                    metadata_license_tag: None,
                    data_license_tag: None,
                    parent: None,
                };
                let is_service_account = self
                    .database_handler
                    .cache
                    .get_user(&user_id)
                    .ok_or_else(|| anyhow!("User not found"))?
                    .attributes
                    .0
                    .service_account;
                self.database_handler
                    .update_grpc_object(request, user_id, is_service_account)
                    .await?;
            }
        }
        Ok(())
    }

    async fn add_status(
        &self,
        hook: &HookWithAssociatedProject,
        object: &ObjectWithRelations,
        status: HookStatusVariant,
    ) -> Result<()> {
        let client = self.database_handler.database.get_client().await?;
        let mut object = object.clone();
        let status_value = HookStatusValues {
            name: hook.name.clone(),
            status,
            trigger: hook.trigger.0.clone(),
        };
        let hook_status = KeyValue {
            key: hook.id.to_string(),
            value: serde_json::to_string(&status_value)?,
            variant: KeyValueVariant::HOOK_STATUS,
        };
        dbg!("HookStatus: {:?}", &hook_status);
        object.object.key_values.0 .0.push(hook_status.clone());
        Object::add_key_value(&object.object.id, &client, hook_status).await?;
        self.database_handler
            .cache
            .upsert_object(&object.object.id, object.clone());
        Ok(())
    }

    async fn get_template_input(
        &self,
        object: ObjectWithRelations,
        hook: HookWithAssociatedProject,
        user_id: DieselUlid,
    ) -> Result<(String, Option<String>, i32, Option<GetCredentialsResponse>)> {
        let object_id = object.object.id;
        // This creates only presigned download urls for available objects.
        // If ObjectType is not OBJECT, only s3 credentials are generated.
        // This should allow for generic external hooks that can also be
        // triggered for other ObjectTypes than OBJECTs
        let (secret, download, pubkey_serial, upload_credentials) =
            match (object.object.object_type, &object.object.object_status) {
                // Get download url and s3-credentials for upload
                (ObjectType::OBJECT, ObjectStatus::AVAILABLE) => {
                    let (secret, pubkey_serial) = self
                        .authorizer
                        .token_handler
                        .sign_hook_secret(self.database_handler.cache.clone(), object_id, hook.id)
                        .await?;
                    // Create append only s3-credentials
                    let append_only_token = APIToken {
                        pub_key: pubkey_serial,
                        name: format!("{}-append_only", hook.id),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: hook.timeout,
                        // TODO: Custom resource permissions for hooks
                        object_id: Some(ObjectMapping::PROJECT(hook.project_id)),
                        user_rights: crate::database::enums::DbPermissionLevel::APPEND,
                    };
                    let token_id = self
                        .database_handler
                        .create_hook_token(&user_id, append_only_token)
                        .await?;

                    // Create download url for response
                    let request = PresignedDownload(GetDownloadUrlRequest {
                        object_id: object_id.to_string(),
                    });
                    let (download, upload_credentials) = self
                        .database_handler
                        .get_presigned_download_with_credentials(
                            self.database_handler.cache.clone(),
                            self.authorizer.clone(),
                            request,
                            user_id,
                            Some(token_id),
                        )
                        .await?;
                    let download = Some(download);
                    (secret, download, pubkey_serial, upload_credentials)
                }
                // Get only s3-credentials for upload
                (_, _) => {
                    let (secret, pubkey_serial) = self
                        .authorizer
                        .token_handler
                        .sign_hook_secret(self.database_handler.cache.clone(), object_id, hook.id)
                        .await?;
                    // Create append only s3-credentials
                    let append_only_token = APIToken {
                        pub_key: pubkey_serial,
                        name: format!("{}-append_only", hook.id),
                        created_at: chrono::Utc::now().naive_utc(),
                        expires_at: hook.timeout,
                        // TODO: Custom resource permissions for hooks
                        object_id: Some(ObjectMapping::PROJECT(hook.project_id)),
                        user_rights: crate::database::enums::DbPermissionLevel::APPEND,
                    };
                    let token_id = self
                        .database_handler
                        .create_hook_token(&user_id, append_only_token)
                        .await?;
                    // Create download url for response
                    let upload_credentials = self
                        .database_handler
                        .get_s3_credentials(
                            self.database_handler.cache.clone(),
                            self.authorizer.clone(),
                            object_id,
                            user_id,
                            Some(token_id),
                        )
                        .await?;
                    (secret, None, pubkey_serial, Some(upload_credentials))
                }
            };
        Ok((secret, download, pubkey_serial, upload_credentials))
    }
}
