use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::fmt::Debug;

use diesel_ulid::DieselUlid;

use crate::error::ArunaError;

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

/// Sign bundle

type HmacSha256 = Hmac<Sha256>;

pub fn create_bundle_id(
    object_ids: &Vec<DieselUlid>,
    collection_id: &DieselUlid,
) -> Result<String, ArunaError> {
    let get_secret = std::env::var("HMAC_BUNDLER_KEY").map_err(|_| {
        ArunaError::TypeConversionError(crate::error::TypeConversionError::PARSECONFIG)
    })?;
    let mut mac = HmacSha256::new_from_slice(get_secret.as_bytes()).map_err(|_| {
        ArunaError::TypeConversionError(crate::error::TypeConversionError::PARSECONFIG)
    })?;
    let mut data: [u8; 16] = collection_id.as_byte_array();
    for o in object_ids {
        xor_in_place(&mut data, &o.as_byte_array())
    }
    mac.update(&data);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

#[inline(always)]
pub fn xor_in_place(a: &mut [u8; 16], b: &[u8; 16]) {
    for (b1, b2) in a.iter_mut().zip(b.iter()) {
        *b1 ^= *b2;
    }
}
