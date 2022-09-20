///  Locations is the path to the requested data.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Location {
    #[prost(enumeration="LocationType", tag="1")]
    pub r#type: i32,
    ///  This is the bucket name for S3. This is the folder name
    #[prost(string, tag="2")]
    pub bucket: ::prost::alloc::string::String,
    ///  for local file.
    ///
    ///  This is the key name for S3. This is the file name for local file.
    #[prost(string, tag="3")]
    pub path: ::prost::alloc::string::String,
}
///  Etag / Part combination to finish a presigned multipart upload.
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct PartETag {
    #[prost(int64, tag="1")]
    pub part_number: i64,
    #[prost(string, tag="2")]
    pub etag: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct InitPresignedUploadRequest {
    #[prost(message, optional, tag="1")]
    pub location: ::core::option::Option<Location>,
    ///  True if multipart upload is requested.
    #[prost(bool, tag="2")]
    pub multipart: bool,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct InitPresignedUploadResponse {
    #[prost(string, tag="1")]
    pub upload_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CreatePresignedUploadUrlRequest {
    #[prost(message, optional, tag="1")]
    pub location: ::core::option::Option<Location>,
    #[prost(string, tag="2")]
    pub upload_id: ::prost::alloc::string::String,
    #[prost(int64, tag="3")]
    pub part_number: i64,
    #[prost(bool, tag="4")]
    pub multipart: bool,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CreatePresignedUploadUrlResponse {
    ///  The presigned URL to upload the file to.
    #[prost(string, tag="1")]
    pub url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct FinishPresignedUploadRequest {
    #[prost(string, tag="1")]
    pub upload_id: ::prost::alloc::string::String,
    #[prost(message, repeated, tag="2")]
    pub part_etags: ::prost::alloc::vec::Vec<PartETag>,
    #[prost(string, tag="3")]
    pub bucket: ::prost::alloc::string::String,
    #[prost(string, tag="4")]
    pub key: ::prost::alloc::string::String,
    #[prost(bool, tag="5")]
    pub multipart: bool,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct FinishPresignedUploadResponse {
    ///  If the upload finished successfully.
    #[prost(bool, tag="1")]
    pub ok: bool,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct Range {
    #[prost(int64, tag="1")]
    pub start: i64,
    #[prost(int64, tag="2")]
    pub end: i64,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CreatePresignedDownloadRequest {
    #[prost(message, optional, tag="1")]
    pub location: ::core::option::Option<Location>,
    #[prost(bool, tag="2")]
    pub is_public: bool,
    ///  optional Range
    #[prost(message, optional, tag="3")]
    pub range: ::core::option::Option<Range>,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CreatePresignedDownloadResponse {
    ///  The presigned URL to download the file to.
    #[prost(string, tag="1")]
    pub url: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CreateBucketRequest {
    #[prost(string, tag="1")]
    pub bucket_name: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, Eq, ::prost::Message)]
pub struct CreateBucketResponse {
}
///  Enum to support multiple target Locations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum LocationType {
    Unspecified = 0,
    S3 = 1,
    File = 2,
}
impl LocationType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            LocationType::Unspecified => "LOCATION_TYPE_UNSPECIFIED",
            LocationType::S3 => "LOCATION_TYPE_S3",
            LocationType::File => "LOCATION_TYPE_FILE",
        }
    }
}
/// Generated client implementations.
pub mod internal_proxy_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct InternalProxyServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl InternalProxyServiceClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> InternalProxyServiceClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InternalProxyServiceClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            InternalProxyServiceClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        pub async fn init_presigned_upload(
            &mut self,
            request: impl tonic::IntoRequest<super::InitPresignedUploadRequest>,
        ) -> Result<tonic::Response<super::InitPresignedUploadResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/aruna.api.internal.v1.InternalProxyService/InitPresignedUpload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_presigned_upload_url(
            &mut self,
            request: impl tonic::IntoRequest<super::CreatePresignedUploadUrlRequest>,
        ) -> Result<
            tonic::Response<super::CreatePresignedUploadUrlResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/aruna.api.internal.v1.InternalProxyService/CreatePresignedUploadUrl",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn finish_presigned_upload(
            &mut self,
            request: impl tonic::IntoRequest<super::FinishPresignedUploadRequest>,
        ) -> Result<
            tonic::Response<super::FinishPresignedUploadResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/aruna.api.internal.v1.InternalProxyService/FinishPresignedUpload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_presigned_download(
            &mut self,
            request: impl tonic::IntoRequest<super::CreatePresignedDownloadRequest>,
        ) -> Result<
            tonic::Response<super::CreatePresignedDownloadResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/aruna.api.internal.v1.InternalProxyService/CreatePresignedDownload",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn create_bucket(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateBucketRequest>,
        ) -> Result<tonic::Response<super::CreateBucketResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/aruna.api.internal.v1.InternalProxyService/CreateBucket",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod internal_proxy_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with InternalProxyServiceServer.
    #[async_trait]
    pub trait InternalProxyService: Send + Sync + 'static {
        async fn init_presigned_upload(
            &self,
            request: tonic::Request<super::InitPresignedUploadRequest>,
        ) -> Result<tonic::Response<super::InitPresignedUploadResponse>, tonic::Status>;
        async fn create_presigned_upload_url(
            &self,
            request: tonic::Request<super::CreatePresignedUploadUrlRequest>,
        ) -> Result<
            tonic::Response<super::CreatePresignedUploadUrlResponse>,
            tonic::Status,
        >;
        async fn finish_presigned_upload(
            &self,
            request: tonic::Request<super::FinishPresignedUploadRequest>,
        ) -> Result<
            tonic::Response<super::FinishPresignedUploadResponse>,
            tonic::Status,
        >;
        async fn create_presigned_download(
            &self,
            request: tonic::Request<super::CreatePresignedDownloadRequest>,
        ) -> Result<
            tonic::Response<super::CreatePresignedDownloadResponse>,
            tonic::Status,
        >;
        async fn create_bucket(
            &self,
            request: tonic::Request<super::CreateBucketRequest>,
        ) -> Result<tonic::Response<super::CreateBucketResponse>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct InternalProxyServiceServer<T: InternalProxyService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: InternalProxyService> InternalProxyServiceServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>>
    for InternalProxyServiceServer<T>
    where
        T: InternalProxyService,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/aruna.api.internal.v1.InternalProxyService/InitPresignedUpload" => {
                    #[allow(non_camel_case_types)]
                    struct InitPresignedUploadSvc<T: InternalProxyService>(pub Arc<T>);
                    impl<
                        T: InternalProxyService,
                    > tonic::server::UnaryService<super::InitPresignedUploadRequest>
                    for InitPresignedUploadSvc<T> {
                        type Response = super::InitPresignedUploadResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::InitPresignedUploadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).init_presigned_upload(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = InitPresignedUploadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/aruna.api.internal.v1.InternalProxyService/CreatePresignedUploadUrl" => {
                    #[allow(non_camel_case_types)]
                    struct CreatePresignedUploadUrlSvc<T: InternalProxyService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: InternalProxyService,
                    > tonic::server::UnaryService<super::CreatePresignedUploadUrlRequest>
                    for CreatePresignedUploadUrlSvc<T> {
                        type Response = super::CreatePresignedUploadUrlResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::CreatePresignedUploadUrlRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_presigned_upload_url(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreatePresignedUploadUrlSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/aruna.api.internal.v1.InternalProxyService/FinishPresignedUpload" => {
                    #[allow(non_camel_case_types)]
                    struct FinishPresignedUploadSvc<T: InternalProxyService>(pub Arc<T>);
                    impl<
                        T: InternalProxyService,
                    > tonic::server::UnaryService<super::FinishPresignedUploadRequest>
                    for FinishPresignedUploadSvc<T> {
                        type Response = super::FinishPresignedUploadResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::FinishPresignedUploadRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).finish_presigned_upload(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = FinishPresignedUploadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/aruna.api.internal.v1.InternalProxyService/CreatePresignedDownload" => {
                    #[allow(non_camel_case_types)]
                    struct CreatePresignedDownloadSvc<T: InternalProxyService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: InternalProxyService,
                    > tonic::server::UnaryService<super::CreatePresignedDownloadRequest>
                    for CreatePresignedDownloadSvc<T> {
                        type Response = super::CreatePresignedDownloadResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::CreatePresignedDownloadRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_presigned_download(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreatePresignedDownloadSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/aruna.api.internal.v1.InternalProxyService/CreateBucket" => {
                    #[allow(non_camel_case_types)]
                    struct CreateBucketSvc<T: InternalProxyService>(pub Arc<T>);
                    impl<
                        T: InternalProxyService,
                    > tonic::server::UnaryService<super::CreateBucketRequest>
                    for CreateBucketSvc<T> {
                        type Response = super::CreateBucketResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::CreateBucketRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_bucket(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateBucketSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: InternalProxyService> Clone for InternalProxyServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: InternalProxyService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: InternalProxyService> tonic::server::NamedService
    for InternalProxyServiceServer<T> {
        const NAME: &'static str = "aruna.api.internal.v1.InternalProxyService";
    }
}
