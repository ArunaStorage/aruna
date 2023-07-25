use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
use aruna_cache::notifications::NotificationCache;
use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
use aruna_policy::ape::structs::{
    ApeResourcePermission, Context, PermissionLevels, ResourceContext,
};
use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_rust_api::api::storage::services::v2::dataset_service_server::DatasetService;
use aruna_rust_api::api::storage::services::v2::{
    CreateDatasetRequest, CreateDatasetResponse, DeleteDatasetRequest, DeleteDatasetResponse,
    GetDatasetRequest, GetDatasetResponse, GetDatasetsRequest, GetDatasetsResponse,
    SnapshotDatasetRequest, SnapshotDatasetResponse, UpdateDatasetDataClassRequest,
    UpdateDatasetDataClassResponse, UpdateDatasetDescriptionRequest,
    UpdateDatasetDescriptionResponse, UpdateDatasetKeyValuesRequest,
    UpdateDatasetKeyValuesResponse, UpdateDatasetNameRequest, UpdateDatasetNameResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(DatasetServiceImpl);

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

        let parent_ctx = tonic_invalid!(
            request
                .get_parent()
                .ok_or(tonic::Status::invalid_argument("Parent missing."))?
                .get_context(),
            "invalid parent"
        );

        let user_id = tonic_auth!(
            self.authorizer.check_context(&token, parent_ctx).await,
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let (generic_dataset, shared_id, cache_res) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache.cache.process_api_resource_update(
                generic_dataset.clone(),
                shared_id,
                cache_res
            ),
            "Caching error"
        );

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

        let ctx = Context::res_ds(dataset_id, PermissionLevels::READ, true);

        tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_resource(&aruna_cache::structs::Resource::Dataset(dataset_id))
            .ok_or_else(|| tonic::Status::not_found("Dataset not found"))?;

        let response = GetDatasetResponse {
            dataset: Some(res.into_inner()?),
        };

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
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: dataset_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let dataset = match tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Dataset(d) => Some(d),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateDatasetNameResponse { dataset }))
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
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: dataset_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let dataset = match tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Dataset(d) => Some(d),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateDatasetDescriptionResponse {
            dataset,
        }))
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
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: dataset_id,
            level: PermissionLevels::WRITE, // append?
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let dataset = match tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Dataset(d) => Some(d),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateDatasetDataClassResponse {
            dataset,
        }))
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
        let ctx = Context::ResourceContext(ResourceContext::Collection(ApeResourcePermission {
            id: dataset_id,
            level: PermissionLevels::WRITE,
            allow_sa: true,
        }));

        let _user_id = tonic_auth!(
            self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        )
        .ok_or(tonic::Status::invalid_argument("User id missing."))?;

        let dataset = match tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        ) {
            generic_resource::Resource::Dataset(d) => Some(d),
            _ => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateDatasetKeyValuesResponse {
            dataset,
        }))
    }

    async fn delete_dataset(
        &self,
        request: Request<DeleteDatasetRequest>,
    ) -> Result<Response<DeleteDatasetResponse>> {
        log_received!(request);

        let token = tonic_auth!(
            get_token_from_md(request.metadata()),
            "Token authentication error."
        );

        let request = DeleteRequest::Dataset(request.into_inner());
        let id = tonic_invalid!(request.get_id(), "Invalid collection id.");
        let ctx = Context::ResourceContext(ResourceContext::Dataset(ApeResourcePermission {
            id,
            level: PermissionLevels::WRITE,
            allow_sa: true,
        }));

        tonic_auth!(
            &self.authorizer.check_context(&token, ctx).await,
            "Unauthorized."
        );

        tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error."
        );

        Ok(tonic::Response::new(DeleteDatasetResponse {}))
    }
    async fn get_datasets(
        &self,
        _request: Request<GetDatasetsRequest>,
    ) -> Result<Response<GetDatasetsResponse>> {
        todo!()
    }
    async fn snapshot_dataset(
        &self,
        _request: Request<SnapshotDatasetRequest>,
    ) -> Result<Response<SnapshotDatasetResponse>> {
        todo!()
    }
}
