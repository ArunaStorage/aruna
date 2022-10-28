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
