use crate::auth::permission_handler::PermissionHandler;
use crate::auth::token_handler::TokenHandler;
use crate::caching::cache::Cache;
use crate::database::dsls::hook_dsl::{
    BasicTemplate, ExternalHook, Hook, InternalHook, TemplateVariant, TriggerType, Credentials,
};
use crate::database::dsls::internal_relation_dsl::InternalRelation;
use crate::database::dsls::object_dsl::{ExternalRelation, KeyValue, KeyValueVariant};
use crate::database::dsls::object_dsl::{Object, ObjectWithRelations};
use crate::database::enums::ObjectMapping; use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::hooks_request_types::CreateHook;
use crate::database::crud::CrudDb;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::hooks::services::v2::{HookCallbackRequest, ListHooksRequest};
use aruna_rust_api::api::storage::models::v2::{Permission, PermissionLevel};
use aruna_rust_api::api::storage::services::v2::{GetDownloadUrlRequest, CreateApiTokenRequest};
use chrono::{NaiveDateTime, Duration};
use diesel_ulid::DieselUlid;
use sha2::Sha256;
use std::str::FromStr;
use std::sync::Arc;
use hmac::{Hmac, Mac};

use super::create_request_types::Parent;
use super::presigned_url_handler::PresignedDownload;
use super::token_request_types::CreateToken;
type HmacSha256 = Hmac<Sha256>;

impl DatabaseHandler {
    pub async fn create_hook(&self, request: CreateHook) -> Result<Hook> {
        let client = self.database.get_client().await?;
        let mut hook = request.get_hook()?;
        hook.create(&client).await?;
        Ok(hook)
    }
    pub async fn list_hook(&self, request: ListHooksRequest) -> Result<Vec<Hook>> {
        let client = self.database.get_client().await?;
        let project_id = DieselUlid::from_str(&request.project_id)?;
        let hooks = Hook::list_hooks(&project_id, &client).await?;
        Ok(hooks)
    }
    pub async fn delete_hook(&self, hook_id: DieselUlid) -> Result<()> {
        let client = self.database.get_client().await?;
        Hook::delete_by_id(&hook_id, &client).await?;
        Ok(())
    }
    pub async fn get_project_by_hook(&self, hook_id: &DieselUlid) -> Result<DieselUlid> {
        let client = self.database.get_client().await?;
        let project_id = Hook::get_project_from_hook(hook_id, &client).await?;
        Ok(project_id)
    }
    pub async fn hook_callback(&self, request: HookCallbackRequest) -> Result<()> {
        //let callback: Callback = request.try_into()?;
        return Err(anyhow!("Hook callback not implemented"));
    }

    // TODO : TRANSACTIONS!
    pub async fn trigger_on_creation(
        &self,
        authorizer: Arc<PermissionHandler>,
        parent: Parent,
        object_id: DieselUlid,
        user_id: DieselUlid,
    ) -> Result<()> {
        let client = self.database.get_client().await?;
        let parent_id = parent.get_id()?;
        let parents = self.cache.upstream_dfs_iterative(&parent_id)?;
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
        let hooks = Hook::get_hooks_for_projects(&projects, &client)
            .await?
            .into_iter()
            .filter(|h| h.trigger_type == TriggerType::OBJECT_CREATED)
            .collect();

        self.hook_action(authorizer.clone(), hooks, object_id, user_id).await?;
        return Err(anyhow!("Hook trigger not implemented"));
    }
    pub async fn trigger_on_append_hook(
        &self,
        _cache: Arc<Cache>,
        _object: ObjectWithRelations,
    ) -> Result<()> {
        return Err(anyhow!("Hook trigger not implemented"));
    }

    async fn hook_action(
        &self,
        authorizer: Arc<PermissionHandler>,
        hooks: Vec<Hook>,
        object_id: DieselUlid,
        user_id: DieselUlid,
    ) -> Result<()> {
        let mut client = self.database.get_client().await?;
        let transaction = client.transaction().await?;
        let client = transaction.client();
        for hook in hooks {
            match hook.hook.0 {
                crate::database::dsls::hook_dsl::HookVariant::Internal(internal_hook) => {
                    match internal_hook {
                        crate::database::dsls::hook_dsl::InternalHook::AddLabel { key, value } => {
                            Object::add_key_value(
                                &object_id,
                                client,
                                KeyValue {
                                    key,
                                    value,
                                    variant: KeyValueVariant::LABEL,
                                },
                            )
                            .await?;
                        }
                        crate::database::dsls::hook_dsl::InternalHook::AddHook { key, value } => {
                            Object::add_key_value(
                                &object_id,
                                client,
                                KeyValue {
                                    key,
                                    value,
                                    variant: KeyValueVariant::HOOK,
                                },
                            )
                            .await?;
                        }
                        crate::database::dsls::hook_dsl::InternalHook::CreateRelation {
                            relation,
                        } => {
                            match relation {
                                aruna_rust_api::api::storage::models::v2::relation::Relation::External(external) => {
                                    let relation: ExternalRelation = (&external).try_into()?;
                                    Object::add_external_relations(&object_id, &client, vec![relation]).await?;

                                },
                                aruna_rust_api::api::storage::models::v2::relation::Relation::Internal(internal) => {
                                    let mut internal = InternalRelation::from_api(&internal, object_id, self.cache.clone())?;
                                    internal.create(&client).await?;
                                },
                            }
                        }
                    }
                }
                crate::database::dsls::hook_dsl::HookVariant::External(ExternalHook{ url, credentials, template, method }) => {
                    let object = self.cache.get_object(&object_id).ok_or_else(|| anyhow!("Object not found"))?;
                    let (secret, pubkey_serial) = authorizer.token_handler.sign_hook_secret(self.cache.clone(), object_id, hook.id).await?;
                    let request = PresignedDownload(GetDownloadUrlRequest{ object_id: object_id.to_string()});
                    let download = self.get_presigned_download(self.cache.clone(), authorizer.clone(), request, user_id).await?;
                    let resource_id = match object.object.object_type {
                        crate::database::enums::ObjectType::OBJECT => Some( aruna_rust_api::api::storage::models::v2::permission::ResourceId::ObjectId(object_id.to_string())),
                        _ => return Err(anyhow!("Only hooks on objects are allowed"))};
                    let expiry: prost_wkt_types::Timestamp = chrono::Utc::now().checked_add_signed(Duration::seconds(604800)).ok_or_else(|| anyhow!("Timestamp error"))?.try_into()?;
                    let token_request = CreateToken(CreateApiTokenRequest{ 
                        name: "HookToken".to_string(), 
                        permission: Some(Permission{ 
                            permission_level: PermissionLevel::Append as i32, 
                            resource_id,
                        }), 
                        expires_at: Some(expiry.clone()) ,
                    }
                    );
                    let (token_id, token) = self.create_token(&user_id, pubkey_serial, token_request).await?;
                    let user = self.cache.get_user(&user_id).ok_or_else(|| anyhow!("User not found"))?;
                    user.attributes.0.tokens.insert(token_id, token.clone());
                    self.cache.update_user(&user_id, user);

                    // Sign token
                    let upload_token = 
                        authorizer.token_handler.sign_user_token(
                            &user_id,
                            &token_id,
                            Some(expiry),
                        )?;
                    let template = match template {
                        TemplateVariant::BasicTemplate => 
                            BasicTemplate { 
                                hook_id: hook.id, 
                                object: object.try_into()?, 
                                secret,
                                download, 
                                upload_token,
                                pubkey_serial,
                            }
                    };
                    let client = reqwest::Client::new();
                    match method {
                        crate::database::dsls::hook_dsl::Method::PUT => {
                            match credentials {
                                Some(Credentials{token}) =>  {
                                    client.put(url).bearer_auth(token).body(serde_json::to_string(&template)?).send().await?;
                                },
                                None => { client.put(url).body(serde_json::to_string(&template)?).send().await?;
                                }
                            }
                        },
                        crate::database::dsls::hook_dsl::Method::POST => {
                            match credentials {
                                Some(Credentials{token}) =>  {
                                    client.post(url).bearer_auth(token).body(serde_json::to_string(&template)?).send().await?;
                                },
                                None => { client.post(url).body(serde_json::to_string(&template)?).send().await?;
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
