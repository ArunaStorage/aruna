use crate::auth::permission_handler::PermissionHandler;
use crate::database::crud::CrudDb;
use crate::database::dsls::hook_dsl::{
    BasicTemplate, Credentials, ExternalHook, Filter, Hook, HookStatusValues, HookStatusVariant,
    HookWithAssociatedProject, TemplateVariant, TriggerVariant,
};
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{ExternalRelation, KeyValue, KeyValueVariant};
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::dsls::user_dsl::APIToken;
use crate::database::enums::{ObjectMapping, ObjectStatus, ObjectType};
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::{Callback, CreateHook, CustomTemplate};
use crate::middlelayer::presigned_url_handler::PresignedDownload;
use anyhow::{anyhow, Result};

use crate::middlelayer::hooks_request_types::ListBy;
use aruna_rust_api::api::hooks::services::v2::AddProjectsToHookRequest;
use aruna_rust_api::api::storage::services::v2::GetDownloadUrlRequest;
use diesel_ulid::DieselUlid;
use http::header::CONTENT_TYPE;
use postgres_types::Json;
use regex::Regex;
use std::str::FromStr;
use std::sync::Arc;

impl DatabaseHandler {
    pub async fn create_hook(&self, request: CreateHook, user_id: &DieselUlid) -> Result<Hook> {
        let client = self.database.get_client().await?;
        let mut hook = request.get_hook(user_id)?;
        hook.create(&client).await?;
        Ok(hook)
    }
    pub async fn list_hook(&self, request: ListBy) -> Result<Vec<Hook>> {
        let client = self.database.get_client().await?;
        let hooks = match request {
            ListBy::PROJECT(_) => {
                let project_id = request.get_id()?;

                Hook::list_hooks(&project_id, &client).await?
            }
            ListBy::OWNER(id) => Hook::list_owned(&id, &client).await?,
        };
        Ok(hooks)
    }
    pub async fn delete_hook(&self, hook_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        Hook::delete_by_id(&hook_id, &client).await?;
        Ok(())
    }
    pub async fn get_project_by_hook(&self, hook_id: &DieselUlid) -> Result<Vec<DieselUlid>> {
        let client = self.database.get_client().await?;
        let project_ids = Hook::get_project_from_hook(hook_id, &client).await?;
        Ok(project_ids)
    }

    pub async fn append_project_to_hook(
        &self,
        request: AddProjectsToHookRequest,
        user_id: &DieselUlid,
    ) -> Result<()> {
        let hook_id = DieselUlid::from_str(&request.hook_id)?;
        let client = self.database.get_client().await?;
        let hook = Hook::get(hook_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Hook not found"))?;
        if hook.owner != *user_id {
            return Err(anyhow!("User is not allowed to add projects to hook"));
        }
        let projects = request
            .project_ids
            .iter()
            .map(|id| DieselUlid::from_str(id).map_err(|_| anyhow!("Invalid project id")))
            .collect::<Result<Vec<_>>>()?;
        Hook::add_projects_to_hook(&projects, &hook_id, &client).await?;
        Ok(())
    }
    pub async fn hook_callback(&self, request: Callback) -> Result<()> {
        // Parsing
        let (_, object_id) = request.get_ids()?;
        dbg!(&object_id);

        // Client creation
        let mut client = self.database.get_client().await?;

        let mut object = Object::get(object_id, &client)
            .await?
            .ok_or_else(|| anyhow!("Object not found"))?;
        //let owr = self.cache.get_object(&object_id).ok_or_else(||anyhow!("Object not found"))?;
        //let mut object = owr.object.clone();
        dbg!(&object);
        let status = object
            .key_values
            .0
             .0
            .iter()
            .find(|kv| kv.key == request.0.hook_id)
            .ok_or_else(|| anyhow!("Hook status not found"))?
            .clone();
        dbg!("HOOK_STATUS: {:?}", &status);
        let mut value: HookStatusValues = serde_json::from_str(&status.value)?;
        let transaction = client.transaction().await?;
        let transaction_client = transaction.client();

        match request.0.status {
            Some(
                aruna_rust_api::api::hooks::services::v2::hook_callback_request::Status::Finished(
                    req,
                ),
            ) => {
                let (add, rm) = Callback::get_keyvals(req)?;
                // Adding kvs from callback
                if !add.0.is_empty() {
                    for kv in add.0 {
                        Object::add_key_value(&object_id, transaction_client, kv).await?;
                    }
                }
                // Removing kvs from callback
                if !rm.0.is_empty() {
                    for kv in rm.0 {
                        if !(kv.variant == KeyValueVariant::STATIC_LABEL) {
                            object.remove_key_value(transaction_client, kv).await?;
                        } else {
                            return Err(anyhow!("Cannot remove static labels."));
                        }
                    }
                }

                value.status = HookStatusVariant::FINISHED;
            }
            Some(
                aruna_rust_api::api::hooks::services::v2::hook_callback_request::Status::Error(
                    aruna_rust_api::api::hooks::services::v2::Error { error },
                ),
            ) => {
                value.status = HookStatusVariant::ERROR(error);
            }
            None => return Err(anyhow!("No status provided")),
        };
        // Update status
        let kvs = object
            .key_values
            .0
             .0
            .iter()
            .map(|kv| -> Result<KeyValue> {
                if kv.key == request.0.hook_id {
                    let value = serde_json::to_string(&value)?;
                    Ok(KeyValue {
                        key: kv.key.clone(),
                        value,
                        variant: KeyValueVariant::HOOK_STATUS,
                    })
                } else {
                    Ok(kv.clone())
                }
            })
            .collect::<Result<Vec<KeyValue>>>()?;
        object.key_values = Json(crate::database::dsls::object_dsl::KeyValues(kvs));
        object.update(transaction_client).await?;

        transaction.commit().await?;

        // Update object in cache
        let owr = Object::get_object_with_relations(&object_id, &client).await?;
        dbg!(&owr);
        self.cache.upsert_object(&object_id, owr);

        Ok(())
    }
    fn collect_projects(parents: Vec<Vec<ObjectMapping<DieselUlid>>>) -> Vec<DieselUlid> {
        let mut projects: Vec<DieselUlid> = Vec::new();
        for branch in parents {
            projects.append(
                &mut branch
                    .iter()
                    .filter_map(|parent| match parent {
                        ObjectMapping::PROJECT(id) => Some(*id),
                        _ => None,
                    })
                    .collect(),
            );
        }
        projects
    }
    pub async fn trigger_hooks(
        &self,
        authorizer: Arc<PermissionHandler>,
        object: ObjectWithRelations,
        user_id: DieselUlid,
        triggers: Vec<TriggerVariant>,
        updated_labels: Option<Vec<KeyValue>>,
    ) -> Result<()> {
        dbg!("Trigger hooks started");
        let client = self.database.get_client().await?;
        let parents = self.cache.upstream_dfs_iterative(&object.object.id)?;
        let projects = DatabaseHandler::collect_projects(parents);
        let labels = if let Some(labels) = &updated_labels {
            labels
        } else {
            &object.object.key_values.0 .0
        };

        // Get hooks that are associated with triggered-object parent-projects
        let hooks: Vec<HookWithAssociatedProject> = {
            let mut hooks = Vec::new();
            // Filter through hooks
            for h in Hook::get_hooks_for_projects(&projects, &client).await? {
                // Only get hooks that are triggered
                if triggers.contains(&h.trigger.0.variant) {
                    let mut is_match = false;
                    // Only get hooks that are matched by filter
                    for filter in h.trigger.0.filter.clone() {
                        match filter {
                            Filter::Name(name) => {
                                let regex = Regex::new(&name)?;
                                if regex.is_match(&object.object.name) {
                                    is_match = true;
                                    break;
                                } else {
                                    continue;
                                }
                            }
                            Filter::KeyValue(KeyValue {
                                key,
                                value,
                                variant,
                            }) => {
                                let key_regex = Regex::new(&key)?;
                                let value_regex = Regex::new(&value)?;
                                for label in labels {
                                    if (label.variant == variant)
                                        && (key_regex.is_match(&label.key))
                                        && (value_regex.is_match(&label.value))
                                    {
                                        is_match = true;
                                        break;
                                    } else {
                                        continue;
                                    }
                                }
                            }
                        }
                    }
                    if is_match {
                        hooks.push(h)
                    }
                }
            }
            hooks
        };
        if hooks.is_empty() {
            Ok(())
        } else {
            self.hook_action(authorizer.clone(), hooks, object, user_id)
                .await?;
            Ok(())
        }
    }
    //pub async fn trigger_on_creation(
    //    &self,
    //    authorizer: Arc<PermissionHandler>,
    //    object_id: DieselUlid,
    //    user_id: DieselUlid,
    //) -> Result<()> {
    //    dbg!("Trigger creation triggered");
    //    let client = self.database.get_client().await?;
    //    dbg!(object_id);
    //    let parents = self.cache.upstream_dfs_iterative(&object_id)?;
    //    let projects = DatabaseHandler::collect_projects(parents);
    //    let hooks: Vec<HookWithAssociatedProject> =
    //        Hook::get_hooks_for_projects(&projects, &client)
    //            .await?
    //            .into_iter()
    //            .filter(|h| h.trigger.0.variant == TriggerVariant::RESOURCE_CREATED)
    //            .collect();
    //    if hooks.is_empty() {
    //        Ok(())
    //    } else {
    //        self.hook_action(authorizer.clone(), hooks, object_id, user_id)
    //            .await?;
    //        Ok(())
    //    }
    //}

    //pub async fn trigger_on_append_hook(
    //    &self,
    //    authorizer: Arc<PermissionHandler>,
    //    user_id: DieselUlid,
    //    object_id: DieselUlid,
    //    keyvals: Vec<KeyValue>,
    //) -> Result<()> {
    //    dbg!("ON_APPEND TRIGGERED");
    //    let parents = self.cache.upstream_dfs_iterative(&object_id)?;
    //    dbg!(&parents);
    //    let projects = DatabaseHandler::collect_projects(parents);
    //    let keyvals: Vec<(String, String)> =
    //        keyvals.into_iter().map(|k| (k.key, k.value)).collect();
    //    dbg!("KEYVALS: {:?}", &keyvals);
    //    let client = self.database.get_client().await?;
    //    let hooks: Vec<HookWithAssociatedProject> =
    //        Hook::get_hooks_for_projects(&projects, &client).await?;
    //    dbg!("UNFILTERED: {:?}", &hooks);
    //    let hooks: Vec<HookWithAssociatedProject> = hooks
    //        .into_iter()
    //        .filter_map(|h| {
    //            if h.trigger.0.variant == TriggerVariant::HOOK_ADDED {
    //                for filter in h.trigger.0.filter {
    //                    //TODO: Regex matching
    //                    match filter {
    //                        Filter::Name(name) => {}
    //                        Filter::KeyValue(kv) => {}
    //                    }
    //                }
    //                if keyvals.contains(&(h.trigger.0.filter.clone(), h.trigger_value.clone())) {
    //                    Some(h)
    //                } else {
    //                    None
    //                }
    //            } else {
    //                None
    //            }
    //        })
    //        .collect();
    //    dbg!("FILTERED: {:?}", &hooks);
    //    if hooks.is_empty() {
    //        Ok(())
    //    } else {
    //        self.hook_action(authorizer.clone(), hooks, object_id, user_id)
    //            .await?;
    //        Ok(())
    //    }
    //}

    async fn hook_action(
        &self,
        authorizer: Arc<PermissionHandler>,
        hooks: Vec<HookWithAssociatedProject>,
        object: ObjectWithRelations,
        user_id: DieselUlid,
    ) -> Result<()> {
        let mut client = self.database.get_client().await?;
        let mut affected_parents: Vec<DieselUlid> = Vec::new();
        for hook in hooks {
            dbg!("Hook: {:?}", &hook);
            let mut object = object.clone();
            let status_value = HookStatusValues {
                name: hook.name,
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
            self.cache.upsert_object(&object.object.id, object.clone());

            let transaction = client.transaction().await?;
            let transaction_client = transaction.client();
            let affected_parent = match hook.hook.0 {
                crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                    match internal_hook {
                        crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                            Object::add_key_value(
                                &object_id,
                                transaction_client,
                                KeyValue {
                                    key,
                                    value,
                                    variant: KeyValueVariant::LABEL,
                                },
                            )
                            .await?;
                            None
                        }
                        crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                            Object::add_key_value(
                                &object_id,
                                transaction_client,
                                KeyValue {
                                    key,
                                    value,
                                    variant: KeyValueVariant::HOOK,
                                },
                            )
                            .await?;
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
                                    let mut internal = InternalRelation::from_api(&internal, object_id, self.cache.clone())?;
                                    internal.create(transaction_client).await?;
                                    affected_parent
                                },
                            }
                        }
                    }
                }
                crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook{ url, credentials, template, method }) => {
                    dbg!("REACHED EXTERNAL TRIGGER");
                    // Get Object for response
                    let object = self.cache.get_object(&object_id).ok_or_else(|| anyhow!("Object not found"))?;
                    if object.object.object_type != ObjectType::OBJECT || object.object.object_status == ObjectStatus::INITIALIZING {
                        continue
                    }

                    // Create secret for callback
                    let (secret, pubkey_serial) = authorizer.token_handler.sign_hook_secret(self.cache.clone(), object_id, hook.id).await?;
                    // TODO: Only create token when specified in custom template
                    // Create append only s3-credentials
                    let append_only_token = APIToken{
                          pub_key: pubkey_serial,
                          name: format!("{}-append_only", hook.id),
                          created_at: chrono::Utc::now().naive_utc(),
                          expires_at: hook.timeout
                          ,
                          object_id: Some(ObjectMapping::PROJECT(hook.project_id)),
                          user_rights: crate::database::enums::DbPermissionLevel::APPEND,
                    };
                    dbg!("Trigger token: {:?}", &append_only_token);
                    let token_id = self.create_hook_token(&user_id, append_only_token).await?;
                    dbg!("Trigger token id: {:?}", &token_id);

                    // Create download url for response
                    let request = PresignedDownload(GetDownloadUrlRequest{ object_id: object_id.to_string()});
                    let (download, upload_credentials) = self.get_presigned_download_with_credentials(self.cache.clone(), authorizer.clone(), request, user_id, Some(token_id)).await?;
                    dbg!("Presigned download: {:?}", &download);
                    let (access_key, secret_key) = match upload_credentials {
                        Some(ref creds) => (Some(creds.access_key.to_string()), Some(creds.secret_key.to_string())),
                        None => (None, None)
                    };
                    dbg!("Upload creds: ({}, {})", &access_key, &secret_key);

                    // Create & send request
                    let client = reqwest::Client::new();
                    let base_request = match method {
                        crate::database::dsls::hook_dsl::Method::PUT => {
                            match credentials {
                                Some(Credentials{token}) => client.put(url).bearer_auth(token),
                                None => client.put(url),
                            }
                        },
                        crate::database::dsls::hook_dsl::Method::POST => {
                            match credentials {
                                Some(Credentials{token}) =>  {
                                    client.post(url).bearer_auth(token)
                                },
                                None => {
                                    client.post(url)
                                }
                            }
                        }
                    };
                    // Put everything into template
                    match template {
                        TemplateVariant::Basic => {
                            let json = serde_json::to_string(&BasicTemplate {
                                hook_id: hook.id,
                                object: object.try_into()?,
                                secret,
                                download,
                                pubkey_serial,
                                access_key,
                                secret_key,
                            })?;
                            dbg!("Template: {json}");
                            let response = base_request.json(&json).send().await?;
                            dbg!("External hook response: {:?}", response);
                        }
                        TemplateVariant::Custom(template) => {
                            let template = CustomTemplate::create_custom_template(template, hook.id, &object.object, secret, download, upload_credentials, pubkey_serial)?;
                            let response = base_request.header(CONTENT_TYPE, "text/plain").body(template).send().await?;
                            dbg!("Custom template hook response: {:?}", response);
                        },
                    };
                    None
                }
            };
            if let Some(p) = affected_parent {
                affected_parents.push(p);
            }
            transaction.commit().await?;
        }
        let updated = Object::get_object_with_relations(&object.object.id, &client).await?;
        if !affected_parents.is_empty() {
            let mut affected =
                Object::get_objects_with_relations(&affected_parents, &client).await?;
            affected.push(updated);
            for object in affected {
                self.cache.upsert_object(&object.object.id.clone(), object);
            }
        }
        Ok(())
    }
}
