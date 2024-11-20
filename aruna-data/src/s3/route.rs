use bytes::Bytes;
use http::{Extensions, HeaderMap, Method, StatusCode, Uri};
use lazy_static::lazy_static;
use s3s::{route::S3Route, Body, S3Request, S3Response, S3Result};

use crate::CONFIG;

lazy_static! {
    static ref HOSTNAMEDOTS: usize = {
        CONFIG
            .frontend
            .hostname
            .chars()
            .filter(|c| *c == '.')
            .count()
            .saturating_sub(1)
    };
}

/// Handler for custom routes
/// This route will handle the BUNDLES api
/// as well as the custom
pub struct CustomRoute {}

#[async_trait::async_trait]
impl S3Route for CustomRoute {
    fn is_match(
        &self,
        method: &Method,
        uri: &Uri,
        _headers: &HeaderMap,
        _extensions: &mut Extensions,
    ) -> bool {
        if method == Method::POST
            && uri.path() == "/custom"
            && uri
                .host()
                .and_then(|e| Some(e.chars().filter(|c| *c == '.').count()))
                .unwrap_or(0) // No subdomains
                > *HOSTNAMEDOTS
        {
            return true;
        }
        false
    }

    async fn call(&self, req: S3Request<Body>) -> S3Result<S3Response<(StatusCode, Body)>> {
        let bytes = Bytes::from("Hello, world!");

        Ok(S3Response::new((StatusCode::OK, bytes.into())))
    }
}
