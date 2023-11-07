use crate::database::crud::CrudDb;
use crate::database::dsls::hook_dsl::{BasicTemplate, Credentials, ExternalHook, TemplateVariant};
use crate::database::dsls::user_dsl::APIToken;
use crate::database::enums::{ObjectMapping, ObjectStatus, ObjectType};
use crate::middlelayer::hooks_request_types::CustomTemplate;
use crate::middlelayer::presigned_url_handler::PresignedDownload;
use crate::{
    auth::permission_handler::PermissionHandler,
    database::dsls::{
        hook_dsl::{HookStatusValues, HookStatusVariant, HookWithAssociatedProject},
        internal_relation_dsl::InternalRelation,
        object_dsl::{ExternalRelation, KeyValue, KeyValueVariant, Object, ObjectWithRelations},
    },
    middlelayer::db_handler::DatabaseHandler,
};
use ahash::HashSet;
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
use std::{str::FromStr, sync::Arc};

pub struct HookHandler {
    pub reciever: Receiver<HookMessage>,
    pub authorizer: Arc<PermissionHandler>,
    pub database_handler: Arc<DatabaseHandler>,
    pub queue: Vec<HookMessage>,
}

#[derive(Debug)]
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
            queue: Vec::new(),
        }
    }
    pub async fn run(&self) -> Result<()> {
        //let mut message_queue = HashSet::default();
        while let Ok(message) = self.reciever.recv().await {
            dbg!("Got hook message: {:?}", &message);
            //message_queue.insert(message);
            // TODO:
            // - queue logic
            // - deduplication
            // - retries
            self.hook_action(message).await?
        }
        Ok(())
    }
    pub async fn hook_action(&self, message: HookMessage) -> Result<()> {
        let HookMessage {
            hook,
            object,
            user_id,
        } = message;
        let mut client = self.database_handler.database.get_client().await?;
        let mut affected_parents: Vec<DieselUlid> = Vec::new();
        dbg!("Hook: {:?}", &hook);
        let mut object = object.clone();
        let status_value = HookStatusValues {
            name: hook.name.clone(),
            status: HookStatusVariant::RUNNING,
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
        let object_id = object.object.id;
        self.database_handler
            .cache
            .upsert_object(&object.object.id, object.clone());

        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();
        let affected_parent = match hook.hook.0 {
                crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                    match internal_hook {
                        crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                            self.add_keyvals(object.clone(), user_id, key, value, APIKeyValVariant::Label).await?;
                            None
                        }
                        crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                            self.add_keyvals(object.clone(), user_id, key, value, APIKeyValVariant::Hook).await?;
                            None
                        }
                        crate::database::dsls::hook_dsl::InternalHook::CreateRelation {
                            relation,
                        } => {
                            match relation {
                                aruna_rust_api::api::storage::models::v2::relation::Relation::External(external) => {
                                    let relation: ExternalRelation = (&external).try_into()?;
                                    Object::add_external_relations(&object_id, transaction_client, vec![relation]).await?;
                                    None
                                },
                                aruna_rust_api::api::storage::models::v2::relation::Relation::Internal(internal) => {
                                    let affected_parent = Some(DieselUlid::from_str(&internal.resource_id)?);
                                    let mut internal = InternalRelation::from_api(&internal, object_id, self.database_handler.cache.clone())?;
                                    internal.create(transaction_client).await?;
                                    affected_parent
                                },
                            }
                        }
                    }
                }
                crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook {
                    ref url,
                    ref credentials,
                    ref template,
                    ref method,
                }) => {
                    dbg!("[ STARTING EXTERNAL HOOK ]");
                    // This creates only presigned download urls for available objects.
                    // If ObjectType is not OBJECT, only s3 credentials are generated.
                    // This should allow for generic external hooks that can also be
                    // triggered for other ObjectTypes than OBJECTs
                    let (secret, download, pubkey_serial, upload_credentials) = self.get_template_input(object.clone(), hook.clone(), user_id).await?;
                    let (access_key, secret_key) = match upload_credentials {
                        Some(ref creds) => (
                            Some(creds.access_key.to_string()),
                            Some(creds.secret_key.to_string()),
                        ),
                        None => (None, None),
                    };

                    // Create & send request
                    // TODO: 
                    // - Sometimes the connection is dropped before finishing, why?
                    let client = reqwest::Client::new(); 
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
                    match template {
                        TemplateVariant::Basic => {
                            let json = serde_json::to_string(&BasicTemplate {
                                hook_id: hook.id,
                                object: object.clone().try_into()?,
                                secret,
                                download,
                                pubkey_serial,
                                access_key,
                                secret_key,
                            })?;
                            //dbg!("Template: {:?}", &json);
                            //let request = base_request.json(&json);
                            //dbg!("Created request: ", &request);
                            //let response = request.send().await?;
                            let response = base_request.json(&json).send().await?;
                            dbg!("External hook response: {:?}", response);
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
                            let response = base_request
                                .header(CONTENT_TYPE, "text/plain")
                                .body(template)
                                .send()
                                .await?;
                            dbg!("Custom template hook response: {:?}", response);
                        }
                    };
                    None
                }
            };
        transaction.commit().await?;
        if let Some(p) = affected_parent {
            affected_parents.push(p);
        }
        let updated = Object::get_object_with_relations(&object.object.id, &client).await?;
        if !affected_parents.is_empty() {
            let mut affected =
                Object::get_objects_with_relations(&affected_parents, &client).await?;
            affected.push(updated);
            for object in affected {
                self.database_handler
                    .cache
                    .upsert_object(&object.object.id.clone(), object);
            }
        }
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
