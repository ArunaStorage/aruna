use crate::data_middleware::data_middlware::DataMiddleware;
use crate::data_middleware::empty_middleware::EmptyMiddleware;
use crate::storage_backend::s3_backend::S3Backend;
use crate::storage_backend::storage_backend::StorageBackend;
use actix_web::middleware::Logger;
use actix_web::web::Data;
use actix_web::{put, web, App, Error, HttpRequest, HttpResponse, HttpServer};
use async_channel::Sender;
use env_logger::Env;
use futures::{join, StreamExt};

const UPLOAD_CHUNK_SIZE: usize = 6291456;

pub struct DataServer {
    s3_client: Box<dyn StorageBackend + Send + Sync>,
}

pub async fn serve() -> std::io::Result<()> {
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    let s3_handler = S3Backend::new().await;
    let server: Data<DataServer> = Data::new(DataServer {
        s3_client: Box::new(s3_handler),
    });

    HttpServer::new(move || {
        App::new()
            .service(single_upload)
            .app_data(Data::clone(&server))
            .wrap(Logger::default())
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}

#[put("/objects/upload/single/{bucket}/{object_id}/{revision_number}")]
async fn single_upload(
    req: HttpRequest,
    server: web::Data<DataServer>,
    payload: web::Payload,
    path: web::Path<(String, String, String)>,
) -> Result<HttpResponse, Error> {
    let content_len = req.headers().get("Content-Length").unwrap();
    let content_len_string = std::str::from_utf8(content_len.as_bytes()).unwrap();
    let content_len = content_len_string.parse::<i64>().unwrap();

    let (payload_sender, data_middleware_recv) = async_channel::bounded(10);
    let (data_middleware_sender, object_handler_recv) = async_channel::bounded(10);

    let middleware = EmptyMiddleware::new(data_middleware_sender, data_middleware_recv).await;
    let payload_handler = handle_payload(payload, payload_sender);
    let middleware_handler = middleware.handle_stream();
    let s3_handler = server.s3_client.upload_object(
        object_handler_recv,
        "test".to_string(),
        "test/key".to_string(),
        content_len,
    );

    join!(payload_handler, middleware_handler, s3_handler);

    return Ok(HttpResponse::Ok().finish());
}

async fn handle_payload(mut payload: web::Payload, sender: Sender<bytes::Bytes>) {
    let mut count = 0;
    let mut count_2 = 0;
    let mut bytes = web::BytesMut::new();
    while let Some(item) = payload.next().await {
        count = count + 1;
        let item = item.unwrap();
        bytes.extend_from_slice(&item);
        if bytes.len() > UPLOAD_CHUNK_SIZE {
            count_2 = count_2 + 1;
            let bytes_for_send = bytes::Bytes::from(bytes);

            sender.send(bytes_for_send).await.unwrap();
            bytes = web::BytesMut::new();
        }
    }

    let bytes_for_send = bytes::Bytes::from(bytes);
    sender.send(bytes_for_send).await.unwrap();
}
