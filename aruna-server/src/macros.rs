// #[macro_export]
// macro_rules! impl_from_req {
//     ( $( $request:ident ),* ) => {
//         $(
//             impl FromRequest<aruna_rust_api::api::storage::services::v2::$request> for ArunaTransaction {
//                 fn from_request(request: $request, meta: Metadata) -> Self {
//                     ArunaTransaction {
//                         request: $crate::storage::transaction::Requests::$request(request),
//                         metadata: meta,
//                     }
//                 }
//             }
//         )*
//     };
// }
// //
// #[macro_export]
// macro_rules! impl_into_resp {
//     ( $( $request:ident ),* ) => {
//         $(
//             impl IntoResponse<$request> for TransactionOk {
//                 fn into_response(self) -> Result<Response<$request>, tonic::Status> {
//                     match self {
//                         TransactionOk::$request(resp) => Ok(Response::new(resp)),
//                         _ => Err(tonic::Status::internal("Invalid response type")),
//                     }
//                 }
//             }
//         )*
//     };
// }
