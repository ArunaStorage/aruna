use crate::auth::permission_handler::{PermissionCheck, PermissionHandler};
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::snapshot_request_types::SnapshotRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, LicenseUpdate, NameUpdate,
};
use crate::search::meilisearch_client::{MeilisearchClient, ObjectDocument};
use crate::utils::grpc_utils::get_token_from_md;
use crate::utils::grpc_utils::{get_id_and_ctx, query, IntoGenericInner};
use crate::utils::search_utils;
use aruna_rust_api::api::storage::models::v2::{generic_resource, Dataset};
use aruna_rust_api::api::storage::services::v2::dataset_service_server::DatasetService;
use aruna_rust_api::api::storage::services::v2::{
    CreateDatasetRequest, CreateDatasetResponse, DeleteDatasetRequest, DeleteDatasetResponse,
    GetDatasetRequest, GetDatasetResponse, GetDatasetsRequest, GetDatasetsResponse,
    SnapshotDatasetRequest, SnapshotDatasetResponse, UpdateDatasetAuthorsRequest,
    UpdateDatasetAuthorsResponse, UpdateDatasetDataClassRequest, UpdateDatasetDataClassResponse,
    UpdateDatasetDescriptionRequest, UpdateDatasetDescriptionResponse,
    UpdateDatasetKeyValuesRequest, UpdateDatasetKeyValuesResponse, UpdateDatasetLicensesRequest,
    UpdateDatasetLicensesResponse, UpdateDatasetNameRequest, UpdateDatasetNameResponse,
    UpdateDatasetTitleRequest, UpdateDatasetTitleResponse,
};
use diesel_ulid::DieselUlid;
use itertools::Itertools;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};
use crate::caching::structs::ObjectWrapper;

crate::impl_grpc_server!(DatasetServiceImpl, search_client: Arc<MeilisearchClient>);

#[tonic::async_trait]
impl DatasetService for DatasetServiceImpl {
    async fn create_dataset(
        &self,
        request: Request<CreateDatasetRequest>,
    ) -> Result<Response<CreateDatasetResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = CreateRequest::Dataset(request.into_inner());
        let mut ctxs = request.get_relation_contexts()?;
        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );
        ctxs.push(parent_ctx);

        let PermissionCheck {
            user_id, is_proxy, ..
        } = tonic_auth!(
            self.authorizer
                .check_permissions_verbose(&token, ctxs)
                .await,
            "Unauthorized"
        );

        let is_service_account = self
            .cache
            .get_user(&user_id)
            .ok_or_else(|| tonic::Status::not_found("User not found"))?
            .attributes
            .0
            .service_account;
        if is_service_account && (request.get_data_class() != 4) {
            return Err(tonic::Status::invalid_argument(
                "Workspaces have to be claimed for dataclass changes",
            ));
        }

        let (dataset, _) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id, is_proxy)
                .await,
            "Internal database error"
        );

        self.cache.add_object(dataset.clone());

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&dataset.object.id).unwrap_or_default();
        let generic_dataset: generic_resource::Resource = ObjectWrapper{
            object_with_relations: dataset,
            rules
        }.into();

        let response = CreateDatasetResponse {
            dataset: Some(generic_dataset.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_dataset(
        &self,
        request: Request<GetDatasetRequest>,
    ) -> Result<Response<GetDatasetResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let dataset_id = tonic_invalid!(
            DieselUlid::from_str(&request.dataset_id),
            "ULID conversion error"
        );

        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::READ, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let proto_dataset = query(&self.cache, &dataset_id)?;

        let response = GetDatasetResponse {
            dataset: Some(proto_dataset.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_datasets(
        &self,
        request: Request<GetDatasetsRequest>,
    ) -> Result<Response<GetDatasetsResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error"
        );

        let request = request.into_inner();

        let (ids, ctxs): (Vec<DieselUlid>, Vec<Context>) = get_id_and_ctx(request.dataset_ids)?;

        tonic_auth!(
            self.authorizer.check_permissions(&token, ctxs).await,
            "Unauthorized"
        );

        let res: Result<Vec<Dataset>> = ids
            .iter()
            .map(|id| -> Result<Dataset> { query(&self.cache, id)?.into_inner() })
            .collect();

        let response = GetDatasetsResponse { datasets: res? };

        return_with_log!(response);
    }

    async fn delete_dataset(
        &self,
        request: Request<DeleteDatasetRequest>,
    ) -> Result<Response<DeleteDatasetResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Dataset(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid dataset id");

        let ctx = Context::res_ctx(id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized."
        );

        let updates: Vec<ObjectWithRelations> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        // Remove deleted resources from search index
        search_utils::remove_from_search_index(
            &self.search_client,
            updates.iter().map(|o| o.object.id).collect_vec(),
        )
        .await;

        let response = DeleteDatasetResponse {};

        return_with_log!(response);
    }

    async fn update_dataset_name(
        &self,
        request: Request<UpdateDatasetNameRequest>,
    ) -> Result<Response<UpdateDatasetNameResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = NameUpdate::Dataset(request.into_inner());
        let dataset_id = tonic_invalid!(request.get_id(), "Invalid dataset id.");

        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut dataset = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&dataset.object.id, dataset.clone());
        self.cache.add_stats_to_object(&mut dataset);

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&dataset_id).unwrap_or_default();
        let dataset: generic_resource::Resource = ObjectWrapper {
            object_with_relations: dataset,
            rules,
        }.into();

        let response = UpdateDatasetNameResponse {
            dataset: Some(dataset.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_dataset_description(
        &self,
        request: Request<UpdateDatasetDescriptionRequest>,
    ) -> Result<Response<UpdateDatasetDescriptionResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DescriptionUpdate::Dataset(request.into_inner());
        let dataset_id = tonic_invalid!(request.get_id(), "Invalid dataset id.");
        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut dataset = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&dataset.object.id, dataset.clone());
        self.cache.add_stats_to_object(&mut dataset);

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&dataset_id).unwrap_or_default();
        let dataset: generic_resource::Resource = ObjectWrapper {
            object_with_relations: dataset,
            rules,
        }.into();

        let response = UpdateDatasetDescriptionResponse {
            dataset: Some(dataset.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_dataset_key_values(
        &self,
        request: Request<UpdateDatasetKeyValuesRequest>,
    ) -> Result<Response<UpdateDatasetKeyValuesResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = KeyValueUpdate::Dataset(request.into_inner());
        let dataset_id = tonic_invalid!(request.get_id(), "Invalid dataset id.");
        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut dataset = tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&dataset.object.id, dataset.clone());
        self.cache.add_stats_to_object(&mut dataset);

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&dataset_id).unwrap_or_default();
        let dataset: generic_resource::Resource = ObjectWrapper {
            object_with_relations: dataset,
            rules,
        }.into();

        let response = UpdateDatasetKeyValuesResponse {
            dataset: Some(dataset.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_dataset_data_class(
        &self,
        request: Request<UpdateDatasetDataClassRequest>,
    ) -> Result<Response<UpdateDatasetDataClassResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DataClassUpdate::Dataset(request.into_inner());
        let dataset_id = tonic_invalid!(request.get_id(), "Invalid dataset id.");
        // Dataclass can only be set by non-serivceaccounts
        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::WRITE, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut dataset = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        );
        self.cache
            .upsert_object(&dataset.object.id, dataset.clone());
        self.cache.add_stats_to_object(&mut dataset);

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&dataset_id).unwrap_or_default();
        let dataset: generic_resource::Resource = ObjectWrapper {
            object_with_relations: dataset,
            rules,
        }.into();

        let response = UpdateDatasetDataClassResponse {
            dataset: Some(dataset.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn snapshot_dataset(
        &self,
        request: Request<SnapshotDatasetRequest>,
    ) -> Result<Response<SnapshotDatasetResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = SnapshotRequest::Dataset(request.into_inner());
        let dataset_id = tonic_invalid!(request.get_id(), "Invalid dataset id.");
        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::ADMIN, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let (new_id, dataset) = tonic_internal!(
            self.database_handler.snapshot(request).await,
            "Internal database error."
        );

        // For datasets, this vector only contains one entry
        self.cache
            .upsert_object(&dataset[0].object.id, dataset[0].clone());

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset[0].object.clone())],
        )
        .await;

        let proto_dataset: generic_resource::Resource = self
            .cache
            .get_protobuf_object(&new_id)
            .ok_or_else(|| tonic::Status::not_found("Dataset not found"))?;

        let response = SnapshotDatasetResponse {
            dataset: Some(proto_dataset.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_dataset_licenses(
        &self,
        request: Request<UpdateDatasetLicensesRequest>,
    ) -> Result<Response<UpdateDatasetLicensesResponse>> {
        log_received!(&request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = LicenseUpdate::Dataset(request.into_inner());
        let dataset_id = tonic_invalid!(request.get_id(), "Invalid dataset id");
        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::WRITE, false);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let mut dataset = tonic_invalid!(
            self.database_handler.update_license(request).await,
            "Invalid update license request"
        );
        self.cache
            .upsert_object(&dataset.object.id, dataset.clone());
        self.cache.add_stats_to_object(&mut dataset);

        // Add or update dataset in search index
        search_utils::update_search_index(
            &self.search_client,
            &self.cache,
            vec![ObjectDocument::from(dataset.object.clone())],
        )
        .await;

        let rules = self.cache.get_rule_bindings(&dataset_id).unwrap_or_default();
        let generic_resource: generic_resource::Resource = ObjectWrapper {
            object_with_relations: dataset,
            rules,
        }.into();
        let response = UpdateDatasetLicensesResponse {
            dataset: Some(generic_resource.into_inner()?),
        };
        return_with_log!(response);
    }

    async fn update_dataset_authors(
        &self,
        _request: Request<UpdateDatasetAuthorsRequest>,
    ) -> Result<Response<UpdateDatasetAuthorsResponse>> {
        // TODO
        Err(tonic::Status::unimplemented(
            "Updating dataset authors is not yet implemented",
        ))
    }
    async fn update_dataset_title(
        &self,
        _request: Request<UpdateDatasetTitleRequest>,
    ) -> Result<Response<UpdateDatasetTitleResponse>> {
        // TODO
        Err(tonic::Status::unimplemented(
            "Updating dataset titles is not yet implemented",
        ))
    }
}
