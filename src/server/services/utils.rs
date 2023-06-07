use aruna_rust_api::api::internal::v1::CorsConfig;
use aruna_rust_api::api::storage::models::v1::KeyValue;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// This functions unpacks the header metadata and the inner request
/// and formats them together in a string.
///
/// ## Arguments:
///
/// * request - A generic tonic gRPC Request
///
/// ## Returns:
///
/// * String - A formatted string containing the request header metadata and its body
///
pub fn format_grpc_request<T>(request: &tonic::Request<T>) -> String
where
    T: Debug,
{
    let metadata = request.metadata();
    let body = request.get_ref();

    format!("\n{:#?}\n{:#?}", metadata, body)
}

/// This functions unpacks the header metadata and the inner response
/// and formats them together in a string.
///
/// ## Arguments:
///
/// * request - A generic tonic gRPC Response
///
/// ## Returns:
///
/// * String - A formatted string containing the response header metadata and its body
///
pub fn format_grpc_response<T>(response: &tonic::Response<T>) -> String
where
    T: Debug,
{
    let metadata = response.metadata();
    let body = response.get_ref();

    format!("\n{:#?}\n{:#?}", metadata, body)
}

#[derive(Serialize, Deserialize)]
pub struct CORSVec {
    pub cors: Vec<CORS>,
}

#[derive(Serialize, Deserialize)]
pub struct CORS {
    pub methods: Vec<String>,
    pub origins: Vec<String>,
    pub headers: Option<Vec<String>>,
}

impl From<Vec<KeyValue>> for CORSVec {
    fn from(label_vec: Vec<KeyValue>) -> Self {
        let mut cors_vec = CORSVec { cors: vec![] };
        for label in label_vec {
            if label.key.contains("apps.aruna-storage.org/cors") {
                let cors = match serde_json::from_str::<CORS>(&label.value) {
                    Ok(c) => CORS {
                        methods: c.methods,
                        origins: c.origins,
                        headers: c.headers,
                    },
                    // Should not occur, but even if this happens, it should not crash
                    // because CORS headers should not be responsible for panics or errors
                    // when returning objects
                    Err(_) => CORS {
                        methods: vec![],
                        origins: vec![],
                        headers: Some(vec![]),
                    },
                };
                cors_vec.cors.push(cors);
            } else {
            }
        }
        cors_vec
    }
}

impl From<CORSVec> for Vec<CorsConfig> {
    fn from(cors_vec: CORSVec) -> Self {
        cors_vec
            .cors
            .into_iter()
            .map(|c| CorsConfig {
                allowed_methods: c.methods,
                allowed_origins: c.origins,
                allowed_headers: c.headers.unwrap_or(vec![]),
            })
            .collect()
    }
}
