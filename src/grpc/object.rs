use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::auth::token_handler::{Action, Intent};
use crate::caching::cache::Cache;
use crate::database::dsls::endpoint_dsl::HostConfig;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::{DataProxyFeature, DbPermissionLevel, ObjectMapping, ObjectType};
use crate::middlelayer::clone_request_types::CloneObject;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::endpoints_request_types::GetEP;
use crate::middlelayer::update_request_types::UpdateObject;
use crate::search::meilisearch_client::{MeilisearchClient, ObjectDocument};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::{self, get_id_and_ctx, query, IntoGenericInner};
use aruna_rust_api::api::dataproxy::services::v2::dataproxy_user_service_client::DataproxyUserServiceClient;
use aruna_rust_api::api::dataproxy::services::v2::GetCredentialsRequest;
use aruna_rust_api::api::storage::models::v2::{generic_resource, Object};
use aruna_rust_api::api::storage::services::v2::get_endpoint_request::Endpoint as APIEndpointEnum;
use aruna_rust_api::api::storage::services::v2::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v2::{
    CloneObjectRequest, CloneObjectResponse, CreateObjectRequest, CreateObjectResponse,
    DeleteObjectRequest, DeleteObjectResponse, FinishObjectStagingRequest,
    FinishObjectStagingResponse, GetDownloadUrlRequest, GetDownloadUrlResponse, GetEndpointRequest,
    GetObjectRequest, GetObjectResponse, GetObjectsRequest, GetObjectsResponse,
    GetUploadUrlRequest, GetUploadUrlResponse, UpdateObjectRequest, UpdateObjectResponse,
};
use diesel_ulid::DieselUlid;
use http::{HeaderMap, Method};
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::{Request, Response, Result, Status};

crate::impl_grpc_server!(ObjectServiceImpl, search_client: Arc<MeilisearchClient>);

#[tonic::async_trait]
impl ObjectService for ObjectServiceImpl {
    async fn create_object(
        &self,
        request: Request<CreateObjectRequest>,
    ) -> Result<Response<CreateObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRequest::Object(request.into_inner());

        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );
        let (user_id, _, is_dataproxy) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, vec![parent_ctx])
                .await,
            "Unauthorized"
        );

        let (object_plus, _) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id, is_dataproxy, self.cache.clone())
                .await,
            "Internal database error"
        );

        self.cache.add_object(object_plus.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(object_plus.object.clone())],
        )
        .await;

        let generic_object: generic_resource::Resource =
            tonic_invalid!(object_plus.try_into(), "Invalid object");

        let response = CreateObjectResponse {
            object: Some(generic_object.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_upload_url(
        &self,
        request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let object_id = tonic_invalid!(DieselUlid::from_str(&request.object_id), "Invalid id");
        let user_id = tonic_auth!(
            self.authorizer
                .check_permissions(
                    &token,
                    vec![Context::res_ctx(object_id, DbPermissionLevel::WRITE, true)]
                )
                .await,
            "Unauthorized"
        );

        let object = self
            .cache
            .get_object(&object_id)
            .ok_or_else(|| Status::not_found("Object not found"))?;

        let multipart = request.multipart;
        let part_number = request.part_number;
        let parts = match (part_number, multipart) {
            (0, true) => {
                return Err(Status::invalid_argument(
                    "No part number provided for multipart upload",
                ))
            }
            (n, true) if n < 1 => n,
            (_, false) => 1,
            _ => return Err(Status::invalid_argument("Invalid part number")),
        };

        let paths = tonic_internal!(
            self.cache.upstream_dfs_iterative(&object_id),
            "Error while creating paths"
        );
        // only get first path
        let mut path: (DieselUlid, String, String) =
            (DieselUlid::default(), String::new(), String::new());
        if let Some(path_components) = paths.into_iter().next() {
            let mut project_id = DieselUlid::default();
            let mut project_name = String::new();
            let mut collection_name = String::new();
            let mut dataset_name = String::new();
            let mut object_name = String::new();
            for component in path_components {
                match component {
                    ObjectMapping::PROJECT(id) => {
                        project_name = self
                            .cache
                            .get_object(&id)
                            .ok_or_else(|| Status::not_found("Parent not found"))?
                            .object
                            .name;
                        project_id = id
                    }
                    ObjectMapping::COLLECTION(id) => {
                        collection_name = self
                            .cache
                            .get_object(&id)
                            .ok_or_else(|| Status::not_found("Parent not found"))?
                            .object
                            .name
                    }
                    ObjectMapping::DATASET(id) => {
                        dataset_name = self
                            .cache
                            .get_object(&id)
                            .ok_or_else(|| Status::not_found("Parent not found"))?
                            .object
                            .name
                    }
                    ObjectMapping::OBJECT(id) => {
                        object_name = self
                            .cache
                            .get_object(&id)
                            .ok_or_else(|| Status::not_found("Parent not found"))?
                            .object
                            .name
                    }
                }
            }
            let key = if project_name.is_empty() || object_name.is_empty() {
                return Err(Status::internal("No project or object found"));
            } else {
                match (collection_name.is_empty(), dataset_name.is_empty()) {
                    (true, true) => {
                        format!("{}/{}/{}", collection_name, dataset_name, object_name)
                    }
                    (false, true) => format!("{}/{}", dataset_name, object_name),
                    (true, false) => {
                        format!("{}/{}", collection_name, object_name)
                    }
                    (false, false) => format!("{}", object_name),
                }
            };
            path = (project_id, project_name, key);
        }

        // TODO: Get endpoints for every path
        let (project_id, bucket_name, key) = path;
        // Only gets first endpoint
        let project_endpoint = Vec::from_iter(
            self.cache
                .get_object(&project_id)
                .ok_or_else(|| Status::not_found("Parent project not found"))?
                .object
                .endpoints
                .0,
        )[0];
        // Fetch endpoint from cache/database
        let endpoint = tonic_invalid!(
            self.database_handler
                .get_endpoint(GetEP(GetEndpointRequest {
                    endpoint: Some(APIEndpointEnum::EndpointId(project_endpoint.0.to_string())),
                }))
                .await,
            "Could not find specified endpoint"
        );

        // TODO: Get s3 creds
        // Create short-lived token with intent
        let slt = tonic_internal!(
            self.authorizer.token_handler.sign_dataproxy_slt(
                &user_id,
                None,
                Some(Intent {
                    target: project_endpoint.0,
                    action: Action::CreateSecrets
                }),
            ),
            "Token signing failed"
        );

        // Request S3 credentials from Dataproxy
        let mut ssl: bool = true;
        let mut endpoint_host_url: String = String::new();
        for endpoint_config in endpoint.host_config.0 .0 {
            if let HostConfig {
                feature: DataProxyFeature::PROXY,
                is_primary: true,
                ..
            } = endpoint_config
            {
                endpoint_host_url = endpoint_config.url;
                ssl = endpoint_config.ssl;
                break;
            }
        }
        if endpoint_host_url.is_empty() {
            return Err(Status::not_found("No valid endpoint config found"));
        }

        let mut dp_conn = tonic_internal!(
            DataproxyUserServiceClient::connect(endpoint_host_url.clone()).await,
            "Could not connect to endpoint"
        );

        // Create GetCredentialsRequest with one-shot token in header ...
        let mut credentials_request = Request::new(GetCredentialsRequest {});
        credentials_request.metadata_mut().append(
            tonic_internal!(
                AsciiMetadataKey::from_bytes("Authorization".as_bytes()),
                "Request creation failed"
            ),
            tonic_internal!(
                AsciiMetadataValue::try_from(format!("Bearer {}", slt)),
                "Request creation failed"
            ),
        );

        let response = tonic_internal!(
            dp_conn.get_credentials(credentials_request).await,
            "Could not get S3 credentials from Dataproxy"
        )
        .into_inner();
        // TODO: Impersonate User and InitMultiPartUpload via S3 and endpoint_host_url
        let client = reqwest::Client::new();
        let result = client.post(&endpoint_host_url);
        //.headers()
        //.body();

        let signed_url = tonic_invalid!(
            grpc_utils::sign_url(
                Method::PUT,
                &response.access_key,
                &response.secret_key,
                ssl,
                multipart,
                parts,
                // TODO: UploadId Placeholder
                "UploadID Placeholder",
                &bucket_name,
                &key,
                &endpoint_host_url,
                604800,
            ),
            "Error while signing url"
        );

        let result = GetUploadUrlResponse { url: signed_url };

        return_with_log!(result);
    }
    async fn get_download_url(
        &self,
        _request: Request<GetDownloadUrlRequest>,
    ) -> Result<Response<GetDownloadUrlResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetDownloadURL is not implemented.",
        ))
    }

    async fn finish_object_staging(
        &self,
        request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = request.into_inner();

        let (_, _, is_dataproxy) = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(
                    &token,
                    vec![Context::res_ctx(
                        tonic_invalid!(
                            DieselUlid::from_str(&request.object_id),
                            "Invalid object_id"
                        ),
                        DbPermissionLevel::APPEND,
                        true
                    )]
                )
                .await,
            "Unauthorized"
        );
        if !is_dataproxy {
            let object = self
                .cache
                .get_object(&tonic_invalid!(
                    DieselUlid::from_str(&request.object_id),
                    "Invalid id"
                ))
                .ok_or_else(|| Status::not_found("Object not found"))?;
            let object: generic_resource::Resource =
                tonic_internal!(object.try_into(), "Object conversion error");
            let response = FinishObjectStagingResponse {
                object: Some(object.into_inner()?),
            };
            return_with_log!(response);
        }

        let object = tonic_internal!(
            self.database_handler.finish_object(request).await,
            "Internal database error."
        );

        self.cache.update_object(&object.object.id, object.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(object.object.clone())],
        )
        .await;

        let object: generic_resource::Resource =
            tonic_internal!(object.try_into(), "Object conversion error");
        let response = FinishObjectStagingResponse {
            object: Some(object.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_object(
        &self,
        request: Request<UpdateObjectRequest>,
    ) -> Result<Response<UpdateObjectResponse>> {
        log_received!(&request);
        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );
        let inner = request.into_inner();
        let req = UpdateObject(inner.clone());
        let object_id = tonic_invalid!(req.get_id(), "Invalid object id.");

        let ctx = Context::res_ctx(object_id, DbPermissionLevel::WRITE, true);

        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let (object, new_revision) = tonic_internal!(
            self.database_handler
                .update_grpc_object(inner, user_id)
                .await,
            "Internal database error."
        );

        self.cache.update_object(&object.object.id, object.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(object.object.clone())],
        )
        .await;

        let object: generic_resource::Resource =
            tonic_internal!(object.try_into(), "Object conversion error");
        let response = UpdateObjectResponse {
            object: Some(object.into_inner()?),
            new_revision,
        };
        return_with_log!(response);
    }

    async fn clone_object(
        &self,
        request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = CloneObject(request.into_inner());
        let object_id = tonic_invalid!(request.get_object_id(), "Invalid object id");
        let (parent_id, parent_mapping) = tonic_invalid!(request.get_parent(), "Invalid object id");
        let ctx = Context::res_ctx(parent_id, DbPermissionLevel::WRITE, true);
        let user_id = tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );
        let new = tonic_internal!(
            self.database_handler
                .clone_object(&user_id, &object_id, parent_mapping)
                .await,
            "Internal clone object error"
        );
        self.cache.add_object(new.clone());

        // Add or update object in search index
        grpc_utils::update_search_index(
            &self.search_client,
            vec![ObjectDocument::from(new.object.clone())],
        )
        .await;

        let converted: generic_resource::Resource =
            tonic_internal!(new.try_into(), "Conversion error");
        let response = CloneObjectResponse {
            object: Some(converted.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn delete_object(
        &self,
        request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Object(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid object id");

        let ctx = Context::res_ctx(id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized."
        );

        let updates: Vec<ObjectWithRelations> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        let mut search_update: Vec<ObjectDocument> = vec![];
        for o in updates {
            self.cache.remove_object(&o.object.id);
            search_update.push(ObjectDocument::from(o.object))
        }

        // Add or update object(s) in search index
        grpc_utils::update_search_index(&self.search_client, search_update).await;

        let response = DeleteObjectResponse {};

        return_with_log!(response);
    }

    async fn get_object(
        &self,
        request: Request<GetObjectRequest>,
    ) -> Result<Response<GetObjectResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let object_id = tonic_invalid!(
            DieselUlid::from_str(&request.object_id),
            "ULID conversion error"
        );

        let ctx = Context::res_ctx(object_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_object(&object_id)
            .ok_or_else(|| tonic::Status::not_found("Object not found"))?;

        let generic_object: generic_resource::Resource =
            tonic_invalid!(res.try_into(), "Invalid object");

        let response = GetObjectResponse {
            object: Some(generic_object.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_objects(
        &self,
        request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let (ids, ctxs): (Vec<DieselUlid>, Vec<Context>) = get_id_and_ctx(request.object_ids)?;

        tonic_auth!(
            self.authorizer.check_permissions(&token, ctxs).await,
            "Unauthorized"
        );

        let res: Result<Vec<Object>> = ids
            .iter()
            .map(|id| -> Result<Object> {
                let obj = query(&self.cache, id)?;
                obj.into_inner()
            })
            .collect();

        let response = GetObjectsResponse { objects: res? };

        return_with_log!(response);
    }
}
