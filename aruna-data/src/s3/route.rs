use http::{Extensions, HeaderMap, Method, StatusCode, Uri};
use s3s::{route::S3Route, Body, S3Request, S3Response, S3Result};

pub struct CustomRoute {}

#[async_trait::async_trait]
impl S3Route for CustomRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        headers: &HeaderMap,
        extensions: &mut Extensions,
    ) -> bool {
        true
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<(StatusCode, Body)>> {
        Ok(S3Response::new((StatusCode::OK, Body::empty())))
    }
}
