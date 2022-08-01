#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEventStreamingGroupRequest {
    #[prost(enumeration="create_event_streaming_group_request::EventResources", tag="1")]
    pub resource: i32,
    #[prost(string, tag="2")]
    pub resource_id: ::prost::alloc::string::String,
    #[prost(bool, tag="3")]
    pub include_subresource: bool,
    #[prost(string, tag="7")]
    pub stream_group_id: ::prost::alloc::string::String,
    #[prost(oneof="create_event_streaming_group_request::StreamType", tags="4, 5, 6")]
    pub stream_type: ::core::option::Option<create_event_streaming_group_request::StreamType>,
}
/// Nested message and enum types in `CreateEventStreamingGroupRequest`.
pub mod create_event_streaming_group_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum EventResources {
        Unspecified = 0,
        CollectionResource = 1,
        AllResource = 5,
    }
    impl EventResources {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EventResources::Unspecified => "EVENT_RESOURCES_UNSPECIFIED",
                EventResources::CollectionResource => "EVENT_RESOURCES_COLLECTION_RESOURCE",
                EventResources::AllResource => "EVENT_RESOURCES_ALL_RESOURCE",
            }
        }
    }
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum StreamType {
        #[prost(message, tag="4")]
        StreamAll(super::StreamAll),
        #[prost(message, tag="5")]
        StreamFromDate(super::StreamFromDate),
        #[prost(message, tag="6")]
        StreamFromSequence(super::StreamFromSequence),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CreateEventStreamingGroupResponse {
    #[prost(string, tag="1")]
    pub stream_group_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotificationStreamGroupRequest {
    #[prost(bool, tag="3")]
    pub close: bool,
    #[prost(oneof="notification_stream_group_request::StreamAction", tags="1, 2")]
    pub stream_action: ::core::option::Option<notification_stream_group_request::StreamAction>,
}
/// Nested message and enum types in `NotificationStreamGroupRequest`.
pub mod notification_stream_group_request {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum StreamAction {
        #[prost(message, tag="1")]
        Init(super::NotificationStreamInit),
        #[prost(message, tag="2")]
        Ack(super::NotficationStreamAck),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotificationStreamInit {
    #[prost(string, tag="1")]
    pub stream_group_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotficationStreamAck {
    #[prost(string, repeated, tag="1")]
    pub ack_chunk_id: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotificationStreamGroupResponse {
    #[prost(message, repeated, tag="1")]
    pub notification: ::prost::alloc::vec::Vec<NotificationStreamResponse>,
    #[prost(string, tag="2")]
    pub ack_chunk_id: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamFromSequence {
    #[prost(uint64, tag="1")]
    pub sequence: u64,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamFromDate {
    #[prost(message, optional, tag="1")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StreamAll {
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NotificationStreamResponse {
    #[prost(message, optional, tag="1")]
    pub message: ::core::option::Option<EventNotificationMessage>,
    #[prost(uint64, tag="2")]
    pub sequence: u64,
    #[prost(message, optional, tag="3")]
    pub timestamp: ::core::option::Option<::prost_types::Timestamp>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct EventNotificationMessage {
    #[prost(enumeration="event_notification_message::EventResources", tag="1")]
    pub resource: i32,
    #[prost(string, tag="2")]
    pub resource_id: ::prost::alloc::string::String,
    #[prost(enumeration="event_notification_message::UpdateType", tag="3")]
    pub updated_type: i32,
}
/// Nested message and enum types in `EventNotificationMessage`.
pub mod event_notification_message {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum UpdateType {
        Unspecified = 0,
        Created = 1,
        Available = 2,
        Updated = 3,
        MetadataUpdated = 4,
        Deleted = 5,
    }
    impl UpdateType {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                UpdateType::Unspecified => "UPDATE_TYPE_UNSPECIFIED",
                UpdateType::Created => "UPDATE_TYPE_CREATED",
                UpdateType::Available => "UPDATE_TYPE_AVAILABLE",
                UpdateType::Updated => "UPDATE_TYPE_UPDATED",
                UpdateType::MetadataUpdated => "UPDATE_TYPE_METADATA_UPDATED",
                UpdateType::Deleted => "UPDATE_TYPE_DELETED",
            }
        }
    }
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum EventResources {
        Unspecified = 0,
        CollectionResource = 1,
        AllResource = 5,
    }
    impl EventResources {
        /// String value of the enum field names used in the ProtoBuf definition.
        ///
        /// The values are not transformed in any way and thus are considered stable
        /// (if the ProtoBuf definition does not change) and safe for programmatic use.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                EventResources::Unspecified => "EVENT_RESOURCES_UNSPECIFIED",
                EventResources::CollectionResource => "EVENT_RESOURCES_COLLECTION_RESOURCE",
                EventResources::AllResource => "EVENT_RESOURCES_ALL_RESOURCE",
            }
        }
    }
}
/// Generated client implementations.
pub mod update_notification_service_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    #[derive(Debug, Clone)]
    pub struct UpdateNotificationServiceClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl UpdateNotificationServiceClient<tonic::transport::Channel> {
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
    impl<T> UpdateNotificationServiceClient<T>
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
        ) -> UpdateNotificationServiceClient<InterceptedService<T, F>>
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
            UpdateNotificationServiceClient::new(
                InterceptedService::new(inner, interceptor),
            )
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
        pub async fn create_event_streaming_group(
            &mut self,
            request: impl tonic::IntoRequest<super::CreateEventStreamingGroupRequest>,
        ) -> Result<
            tonic::Response<super::CreateEventStreamingGroupResponse>,
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
                "/aruna.api.notification.services.v1.UpdateNotificationService/CreateEventStreamingGroup",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
        pub async fn notification_stream_group(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::NotificationStreamGroupRequest,
            >,
        ) -> Result<
            tonic::Response<
                tonic::codec::Streaming<super::NotificationStreamGroupResponse>,
            >,
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
                "/aruna.api.notification.services.v1.UpdateNotificationService/NotificationStreamGroup",
            );
            self.inner.streaming(request.into_streaming_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod update_notification_service_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with UpdateNotificationServiceServer.
    #[async_trait]
    pub trait UpdateNotificationService: Send + Sync + 'static {
        async fn create_event_streaming_group(
            &self,
            request: tonic::Request<super::CreateEventStreamingGroupRequest>,
        ) -> Result<
            tonic::Response<super::CreateEventStreamingGroupResponse>,
            tonic::Status,
        >;
        ///Server streaming response type for the NotificationStreamGroup method.
        type NotificationStreamGroupStream: futures_core::Stream<
                Item = Result<super::NotificationStreamGroupResponse, tonic::Status>,
            >
            + Send
            + 'static;
        async fn notification_stream_group(
            &self,
            request: tonic::Request<
                tonic::Streaming<super::NotificationStreamGroupRequest>,
            >,
        ) -> Result<tonic::Response<Self::NotificationStreamGroupStream>, tonic::Status>;
    }
    #[derive(Debug)]
    pub struct UpdateNotificationServiceServer<T: UpdateNotificationService> {
        inner: _Inner<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
    }
    struct _Inner<T>(Arc<T>);
    impl<T: UpdateNotificationService> UpdateNotificationServiceServer<T> {
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
    for UpdateNotificationServiceServer<T>
    where
        T: UpdateNotificationService,
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
                "/aruna.api.notification.services.v1.UpdateNotificationService/CreateEventStreamingGroup" => {
                    #[allow(non_camel_case_types)]
                    struct CreateEventStreamingGroupSvc<T: UpdateNotificationService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: UpdateNotificationService,
                    > tonic::server::UnaryService<
                        super::CreateEventStreamingGroupRequest,
                    > for CreateEventStreamingGroupSvc<T> {
                        type Response = super::CreateEventStreamingGroupResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::CreateEventStreamingGroupRequest,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).create_event_streaming_group(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = CreateEventStreamingGroupSvc(inner);
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
                "/aruna.api.notification.services.v1.UpdateNotificationService/NotificationStreamGroup" => {
                    #[allow(non_camel_case_types)]
                    struct NotificationStreamGroupSvc<T: UpdateNotificationService>(
                        pub Arc<T>,
                    );
                    impl<
                        T: UpdateNotificationService,
                    > tonic::server::StreamingService<
                        super::NotificationStreamGroupRequest,
                    > for NotificationStreamGroupSvc<T> {
                        type Response = super::NotificationStreamGroupResponse;
                        type ResponseStream = T::NotificationStreamGroupStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::NotificationStreamGroupRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).notification_stream_group(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = NotificationStreamGroupSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.streaming(method, req).await;
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
    impl<T: UpdateNotificationService> Clone for UpdateNotificationServiceServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: UpdateNotificationService> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: UpdateNotificationService> tonic::server::NamedService
    for UpdateNotificationServiceServer<T> {
        const NAME: &'static str = "aruna.api.notification.services.v1.UpdateNotificationService";
    }
}
