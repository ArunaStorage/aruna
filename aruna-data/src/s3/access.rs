use std::sync::Arc;

use s3s::{
    access::{S3Access, S3AccessContext},
    s3_error, S3Result,
};

use crate::{grpc::ServerClient, lmdbstore::LmdbStore};

pub struct AccessChecker {
    store: Arc<LmdbStore>,
    client: ServerClient,
}

impl AccessChecker {
    pub fn new(store: Arc<LmdbStore>, client: ServerClient) -> Self {
        Self { store, client }
    }
}

#[async_trait::async_trait]
impl S3Access for AccessChecker {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Default implementation
        match cx.s3_path() {
            s3s::path::S3Path::Root => {
                if cx.credentials().is_none() {
                    return Err(s3_error!(
                        AccessDenied,
                        "Anonymous base request is not allowed"
                    ));
                }

                // TODO: Check credentials for aruna requests
            }
            s3s::path::S3Path::Bucket { bucket } => {
                let exists = self.store.get_object(&bucket);
                match exists {
                    Some(crate::structs::ObjectInfo::Project { public, .. }) => {
                        if public && cx.method() == hyper::Method::GET {
                            return Ok(());
                        }
                    }
                    _ => return Err(s3_error!(NoSuchBucket, "Bucket does not exist")),
                }
                if cx.credentials().is_none() {
                    return Err(s3_error!(
                        AccessDenied,
                        "Anonymous base request is not allowed"
                    ));
                }
            }
            s3s::path::S3Path::Object { bucket, key } => todo!(),
        }

        Ok(())
    }
}
