use s3s::dto::*;
use s3s::S3Request;
use s3s::S3Result;
use s3s::S3;

#[derive(Debug)]
pub struct S3ServiceServer {}

impl S3ServiceServer {
    pub async fn new() -> Self {
        S3ServiceServer {}
    }
}

#[async_trait::async_trait]
impl S3 for S3ServiceServer {
    #[tracing::instrument]
    async fn create_bucket(
        &self,
        _req: S3Request<CreateBucketInput>,
    ) -> S3Result<CreateBucketOutput> {
        let output = CreateBucketOutput::default();
        Ok(output)
    }
}
