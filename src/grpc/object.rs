use crate::auth::permission_handler::PermissionHandler;
use crate::auth::structs::Context;
use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::middlelayer::create_request_types::CreateRequest;
use crate::middlelayer::db_handler::DatabaseHandler;
use crate::middlelayer::update_request_types::UpdateObject;
use crate::utils::conversions::get_token_from_md;
use crate::utils::grpc_utils::IntoGenericInner;
use aruna_rust_api::api::storage::models::v2::generic_resource;
use aruna_rust_api::api::storage::services::v2::object_service_server::ObjectService;
use aruna_rust_api::api::storage::services::v2::{
    CloneObjectRequest, CloneObjectResponse, CreateObjectRequest, CreateObjectResponse,
    DeleteObjectRequest, DeleteObjectResponse, FinishObjectStagingRequest,
    FinishObjectStagingResponse, GetDownloadUrlRequest, GetDownloadUrlResponse, GetObjectRequest,
    GetObjectResponse, GetObjectsRequest, GetObjectsResponse, GetUploadUrlRequest,
    GetUploadUrlResponse, UpdateObjectRequest, UpdateObjectResponse,
};
use diesel_ulid::DieselUlid;
use std::str::FromStr;
use std::sync::Arc;
use tonic::{Request, Response, Result};

crate::impl_grpc_server!(ObjectServiceImpl);

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
        let user_id = tonic_auth!(
            self.authorizer.check_context(&token, parent_ctx).await,
            "Unauthorized"
        )
        .ok_or(tonic::Status::invalid_argument("Missing user id"))?;

        let (generic_object, shared_id, cache_res) = tonic_internal!(
            self.database_handler
                .create_resource(request, user_id)
                .await,
            "Internal database error"
        );

        tonic_internal!(
            self.cache.cache.process_api_resource_update(
                generic_object.clone(),
                shared_id,
                cache_res
            ),
            "Caching error"
        );

        let response = CreateObjectResponse {
            object: Some(generic_object.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn finish_object_staging(
        &self,
        _request: Request<FinishObjectStagingRequest>,
    ) -> Result<Response<FinishObjectStagingResponse>> {
        todo!()
        // log::info!("Recieved FinishObjectStagingRequest.");
        // log::debug!("{:?}", &request);

        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();

        // let ctx = Context::Object(ResourcePermission {
        //     id: DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::internal("ULID conversion error")
        //     })?,
        //     level: crate::database::enums::PermissionLevels::WRITE, // append?
        //     allow_sa: true,
        // });
        // let user_id = self
        //     .authorizer
        //     .check_permissions(&token, ctx)
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::permission_denied("Permission denied.")
        //     })?;

        // let client = self.database.get_client().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;
        // let object_pid = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error.")
        // })?;
        // let to_update_object = match Object::get(object_pid, &client).await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::aborted("Database call failed.")
        // })? {
        //     Some(o) => o,
        //     None => return Err(tonic::Status::aborted("Database call failed.")),
        // };

        // let hashes = if !inner_request.hashes.is_empty() {
        //     let req_hashes: Hashes = inner_request.hashes.try_into().map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::internal("Hash conversion error.")
        //     })?;
        //     if !to_update_object.hashes.0 .0.is_empty() {
        //         let comp_hashes = to_update_object.hashes.0.clone();

        //         if comp_hashes != req_hashes {
        //             return Err(tonic::Status::internal("Hashes diverge."));
        //         }
        //         None
        //     } else {
        //         Some(req_hashes)
        //     }
        // } else {
        //     None
        // };

        // if !inner_request.completed_parts.is_empty() {
        //     return Err(tonic::Status::unimplemented(
        //         "Finish multipart objects is not yet implemented.",
        //     ));
        // }

        // Object::finish_object_staging(
        //     &object_pid,
        //     &client,
        //     hashes.clone(),
        //     inner_request.content_len,
        //     crate::database::enums::ObjectStatus::AVAILABLE,
        // )
        // .await
        // .map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::aborted("Database update failed.")
        // })?;

        // let grpc_object = GRPCObject {
        //     id: to_update_object.id.to_string(),
        //     name: to_update_object.name,
        //     description: to_update_object.description,
        //     key_values: to_update_object.key_values.0.into(),
        //     relations: to_update_object
        //         .external_relations
        //         .0
        //          .0
        //         .into_iter()
        //         .map(|r| Relation {
        //             relation: Some(RelationEnum::External(r.into())),
        //         })
        //         .collect(),
        //     content_len: inner_request.content_len,
        //     data_class: to_update_object.data_class.into(),
        //     created_at: to_update_object.created_at.map(|t| t.into()),
        //     created_by: user_id.to_string(),
        //     status: 3,
        //     dynamic: false,
        //     hashes: match hashes {
        //         Some(h) => h.into(),
        //         None => to_update_object.hashes.0.into(),
        //     },
        // };
        // Ok(tonic::Response::new(FinishObjectStagingResponse {
        //     object: Some(grpc_object),
        // }))
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
            self.authorizer.check_context(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let (object, new_revision) = match tonic_internal!(
            self.database_handler
                .update_grpc_object(inner, user_id)
                .await,
            "Internal database error."
        ) {
            (generic_resource::Resource::Object(o), new_rev) => (Some(o), new_rev),
            (_, _) => return Err(tonic::Status::unknown("This should not happen.")),
        };

        Ok(tonic::Response::new(UpdateObjectResponse {
            object,
            new_revision,
        }))
    }

    async fn delete_object(
        &self,
        _request: Request<DeleteObjectRequest>,
    ) -> Result<Response<DeleteObjectResponse>> {
        todo!()
        // log::info!("Recieved DeleteObjectRequest.");
        // log::debug!("{:?}", &request);

        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();
        // let object_id = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error")
        // })?;
        // let ctx = Context::Object(ResourcePermission {
        //     id: object_id,
        //     level: crate::database::enums::PermissionLevels::ADMIN,
        //     allow_sa: false,
        // });

        // match &self.authorizer.check_permissions(&token, ctx) {
        //     Ok(b) => {
        //         if *b {
        //             // ToDo!
        //             // PLACEHOLDER!
        //             DieselUlid::generate()
        //         } else {
        //             return Err(tonic::Status::permission_denied("Not allowed."));
        //         }
        //     }
        //     Err(e) => {
        //         log::debug!("{}", e);
        //         return Err(tonic::Status::permission_denied("Not allowed."));
        //     }
        // };

        // let object_id = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error.")
        // })?;
        // let mut client = self.database.get_client().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;
        // let transaction = client.transaction().await.map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::unavailable("Database not avaliable.")
        // })?;
        // let transaction_client = transaction.client();
        // let object = Object::get(object_id, transaction_client)
        //     .await
        //     .map_err(|e| {
        //         log::error!("{}", e);
        //         tonic::Status::unavailable("Database call failed.")
        //     })?
        //     .ok_or(tonic::Status::not_found("Object not found."))?;
        // // Should only mark as deleted
        // match inner_request.with_revisions {
        //     true => {
        //         let revisions = Object::get_all_revisions(&object.shared_id, transaction_client)
        //             .await
        //             .map_err(|e| {
        //                 log::error!("{}", e);
        //                 tonic::Status::unavailable("Revisions not found")
        //             })?;
        //         for r in revisions {
        //             r.delete(transaction_client).await.map_err(|e| {
        //                 log::error!("{}", e);
        //                 tonic::Status::aborted("Database delete transaction failed.")
        //             })?;
        //         }
        //     }
        //     false => {
        //         object.delete(transaction_client).await.map_err(|e| {
        //             log::error!("{}", e);
        //             tonic::Status::aborted("Database delete transaction failed.")
        //         })?;
        //     }
        // };
        // Ok(tonic::Response::new(DeleteObjectResponse {}))
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
            self.authorizer.check_context(&token, vec![ctx]).await,
            "Unauthorized"
        );

        let res = self
            .cache
            .get_resource(&object_id)
            .ok_or_else(|| tonic::Status::not_found("Object not found"))?;

        let response = GetObjectResponse {
            object: Some(res.into_inner()?),
        };

        return_with_log!(response);
    }

    async fn get_upload_url(
        &self,
        _request: Request<GetUploadUrlRequest>,
    ) -> Result<Response<GetUploadUrlResponse>> {
        todo!()
        // log::info!("Recieved CreateObjectRequest.");
        // log::debug!("{:?}", &request);

        // let token = get_token_from_md(request.metadata()).map_err(|e| {
        //     log::debug!("{}", e);
        //     tonic::Status::unauthenticated("Token authentication error.")
        // })?;

        // let inner_request = request.into_inner();

        // let object_id = DieselUlid::from_str(&inner_request.object_id).map_err(|e| {
        //     log::error!("{}", e);
        //     tonic::Status::internal("ULID conversion error.")
        // })?;
        // let ctx = Context::Object(ResourcePermission {
        //     id: object_id,
        //     level: crate::database::enums::PermissionLevels::WRITE, // append?
        //     allow_sa: true,
        // });

        // let user_id = match &self.authorizer.check_permissions(&token, ctx) {
        //     Ok(b) => {
        //         if *b {
        //             // ToDo!
        //             // PLACEHOLDER!
        //             DieselUlid::generate()
        //         } else {
        //             return Err(tonic::Status::permission_denied("Not allowed."));
        //         }
        //     }
        //     Err(e) => {
        //         log::debug!("{}", e);
        //         return Err(tonic::Status::permission_denied("Not allowed."));
        //     }
        // };
        // //TODO
        // Err(tonic::Status::unimplemented(
        //     "GetUploadURL is not implemented.",
        // ))
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
    async fn clone_object(
        &self,
        _request: Request<CloneObjectRequest>,
    ) -> Result<Response<CloneObjectResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "CloneObject is not implemented.",
        ))
    }
    async fn get_objects(
        &self,
        _request: Request<GetObjectsRequest>,
    ) -> Result<Response<GetObjectsResponse>> {
        //TODO
        Err(tonic::Status::unimplemented(
            "GetObjects is not implemented.",
        ))
    }
}
