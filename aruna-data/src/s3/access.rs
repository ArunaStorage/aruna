use s3s::{
    access::{S3Access, S3AccessContext},
    s3_error, S3Result,
};

pub struct AccessChecker {}

#[async_trait::async_trait]
impl S3Access for AccessChecker {
    async fn check(&self, cx: &mut S3AccessContext<'_>) -> S3Result<()> {
        // Default implementation
        return Ok(());
        match cx.credentials() {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied, "Signature is required")),
        }
    }
}
