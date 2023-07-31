use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::dsls::object_dsl::ObjectWithRelations;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::delete_request_types::DeleteRequest;
use crate::middlelayer::snapshot_request_types::SnapshotRequest;
use crate::middlelayer::update_request_types::{
    DataClassUpdate, DescriptionUpdate, KeyValueUpdate, NameUpdate,
};
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
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
            self.authorizer
                .check_permissions(&token, vec![parent_ctx])
                .await,
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let object_with_rel = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        self.cache.add_object(object_with_rel.clone());

        let generic_dataset: generic_resource::Resource =
            tonic_invalid!(object_with_rel.try_into(), "Invalid dataset");

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

        let res = self
            .cache
            .get_object(&dataset_id)
            .ok_or_else(|| tonic::Status::not_found("Dataset not found"))?;

        let generic_dataset: generic_resource::Resource =
            tonic_invalid!(res.try_into(), "Invalid dataset");

        let response = GetDatasetResponse {
            dataset: Some(generic_dataset.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_datasets(
        &self,
        _request: Request<GetDatasetsRequest>,
    ) -> Result<Response<GetDatasetsResponse>> {
        todo!()
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
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized."
        );

        let updates: Vec<ObjectWithRelations> = tonic_internal!(
            self.database_handler.delete_resource(request).await,
            "Internal database error"
        );

        for o in updates {
            self.cache.update_object(&o.object.id, o.clone());
        }

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

        let dataset = tonic_internal!(
            self.database_handler.update_name(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&dataset.object.id, dataset.clone());
        let dataset: generic_resource::Resource =
            tonic_internal!(dataset.try_into(), "Dataset conversion error");

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

        let dataset = tonic_internal!(
            self.database_handler.update_description(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&dataset.object.id, dataset.clone());
        let dataset: generic_resource::Resource =
            tonic_internal!(dataset.try_into(), "Dataset conversion error");

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
            self.authorizer.check_permissions(&token, vec![ctx]),
            "Unauthorized"
        );

        let dataset = tonic_internal!(
            self.database_handler.update_keyvals(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&dataset.object.id, dataset.clone());
        let dataset: generic_resource::Resource =
            tonic_internal!(dataset.try_into(), "Dataset conversion error");

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
        let ctx = Context::res_ctx(dataset_id, DbPermissionLevel::WRITE, true);

        tonic_auth!(
            self.authorizer.check_permissions(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let dataset = tonic_internal!(
            self.database_handler.update_dataclass(request).await,
            "Internal database error."
        );
        self.cache
            .update_object(&dataset.object.id, dataset.clone());
        let dataset: generic_resource::Resource =
            tonic_internal!(dataset.try_into(), "Dataset conversion error");

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

        // this only contains one entry with a dataset
        let dataset = tonic_internal!(
            self.database_handler.snapshot(request).await,
            "Internal database error."
        )[0]
        .clone();
        self.cache
            .update_object(&dataset.object.id, dataset.clone());

        let dataset: generic_resource::Resource =
            tonic_internal!(dataset.try_into(), "Dataset conversion error");

        let response = SnapshotDatasetResponse {
            dataset: Some(dataset.into_inner()?),
        };
        return_with_log!(response);
    }
}
