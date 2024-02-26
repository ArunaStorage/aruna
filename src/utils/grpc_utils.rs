use crate::caching::cache::Cache;
use crate::database::enums::DbPermissionLevel;
use crate::grpc::users::UserServiceImpl;
use crate::{auth::structs::Context, database::enums::ObjectMapping};
use anyhow::{anyhow, Result as AnyhowResult};
use aruna_rust_api::api::storage::models::v2::{
    generic_resource, Collection, Dataset, Object, Project, User,
};
use base64::{engine::general_purpose, Engine};
use diesel_ulid::DieselUlid;
use rusty_ulid::DecodingError;
use std::str::FromStr;
use std::sync::Arc;
use tonic::metadata::MetadataMap;
use tonic::{Result, Status};
use xxhash_rust::xxh3::xxh3_128;

pub fn type_name_of<T>(_: T) -> &'static str {
    std::any::type_name::<T>()
}

pub trait IntoGenericInner<T> {
    fn into_inner(self) -> Result<T, Status>;
}

impl IntoGenericInner<Project> for generic_resource::Resource {
    fn into_inner(self) -> Result<Project, Status> {
        match self {
            generic_resource::Resource::Project(project) => Ok(project),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Collection> for generic_resource::Resource {
    fn into_inner(self) -> Result<Collection> {
        match self {
            generic_resource::Resource::Collection(collection) => Ok(collection),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Dataset> for generic_resource::Resource {
    fn into_inner(self) -> Result<Dataset> {
        match self {
            generic_resource::Resource::Dataset(dataset) => Ok(dataset),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}
impl IntoGenericInner<Object> for generic_resource::Resource {
    fn into_inner(self) -> Result<Object> {
        match self {
            generic_resource::Resource::Object(object) => Ok(object),
            _ => Err(Status::invalid_argument("Invalid conversion")),
        }
    }
}

impl IntoGenericInner<DbPermissionLevel> for ObjectMapping<DbPermissionLevel> {
    fn into_inner(self) -> Result<DbPermissionLevel, Status> {
        match self {
            ObjectMapping::PROJECT(perm) => Ok(perm),
            ObjectMapping::COLLECTION(perm) => Ok(perm),
            ObjectMapping::DATASET(perm) => Ok(perm),
            ObjectMapping::OBJECT(perm) => Ok(perm),
        }
    }
}

impl UserServiceImpl {
    pub async fn match_ctx(
        &self,
        tuple: (Option<DieselUlid>, Context),
        token: String,
    ) -> Result<DieselUlid> {
        match tuple {
            (Some(id), ctx) => {
                tonic_auth!(
                    self.authorizer.check_permissions(&token, vec![ctx]).await,
                    "Unauthorized"
                );
                Ok(id)
            }

            (None, ctx) => {
                let user_id = tonic_auth!(
                    self.authorizer.check_permissions(&token, vec![ctx]).await,
                    "Unauthorized"
                );
                Ok(user_id)
            }
        }
    }
}

///ToDo: Rust Doc
pub fn checksum_resource(gen_res: generic_resource::Resource) -> anyhow::Result<String> {
    match gen_res {
        generic_resource::Resource::Project(mut proj) => {
            proj.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(xxh3_128(&bincode::serialize(&proj)?).to_be_bytes())
                .to_string())
        }
        generic_resource::Resource::Collection(mut col) => {
            col.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(xxh3_128(&bincode::serialize(&col)?).to_be_bytes())
                .to_string())
        }
        generic_resource::Resource::Dataset(mut ds) => {
            ds.stats = None;
            Ok(general_purpose::STANDARD_NO_PAD
                .encode(xxh3_128(&bincode::serialize(&ds)?).to_be_bytes())
                .to_string())
        }
        generic_resource::Resource::Object(obj) => Ok(general_purpose::STANDARD_NO_PAD
            .encode(xxh3_128(&bincode::serialize(&obj)?).to_be_bytes())
            .to_string()),
    }
}

///ToDo: Rust Doc
pub fn checksum_user(user: &User) -> anyhow::Result<String> {
    Ok(general_purpose::STANDARD_NO_PAD
        .encode(xxh3_128(&bincode::serialize(&user.attributes)?).to_be_bytes())
        .to_string())
}

pub fn get_id_and_ctx(ids: Vec<String>) -> Result<(Vec<DieselUlid>, Vec<Context>)> {
    let zipped = tonic_invalid!(
        ids.iter()
            .map(
                |id| -> std::result::Result<(DieselUlid, Context), DecodingError> {
                    let id = DieselUlid::from_str(id)?;
                    let ctx = Context::res_ctx(id, DbPermissionLevel::READ, true);
                    Ok((id, ctx))
                },
            )
            .collect::<std::result::Result<Vec<(DieselUlid, Context)>, DecodingError>>(),
        "Invalid ids"
    );
    let (ids, ctxs) = zipped.into_iter().unzip();
    Ok((ids, ctxs))
}

pub fn query(cache: &Arc<Cache>, id: &DieselUlid) -> Result<generic_resource::Resource, Status> {
    cache
        .get_protobuf_object(id)
        .ok_or_else(|| Status::not_found("Resource not found"))
}

pub fn get_token_from_md(md: &MetadataMap) -> AnyhowResult<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))?
        .to_str()?;

    let split = token_string.split(' ').collect::<Vec<_>>();

    if split.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            split.len()
        );
        return Err(anyhow!("Authorization flow error"));
    }

    if split[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid token type, expected: Bearer, got: {:?}",
            split[0]
        );

        return Err(anyhow!("Authorization flow error"));
    }

    if split[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid token length, expected: >0, got: {:?}",
            split[1].len()
        );

        return Err(anyhow!("Authorization flow error"));
    }

    Ok(split[1].to_string())
}
