use std::fmt::Debug;

use async_channel::{Receiver, Sender};
use async_trait::async_trait;

use aruna_rust_api::api::internal::v1::{Location, PartETag, Range};

/// A generic backend API for storing and retrieving objects
/// Represents a very simple object storage API
/// Data is always read and written in chunks and send via channels following a CSP style pattern
#[async_trait]
pub trait StorageBackend: Debug + Send + Sync {
    /// Uploads the given object from the receiver and stores it in the provided location
    /// # Arguments
    ///
    /// * `recv` - The receiver from which to load the objects data chunks
    /// * `location` - The location of the object which to load
    /// * `content_len` - The size of the uploaded object
    async fn upload_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        location: Location,
        content_len: i64,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;

    /// Downloads the given object from storage and put it into the sender
    /// # Arguments
    ///
    /// * `location` - The location of the object which to load
    /// * `ranges` - Optional: Set of ranges which to load from a larger file; work like HTTP range requests
    /// * `chunk_size` - Size of the individual chunks which are send
    /// * `sender` - The target for the individual chunks of data
    async fn download(
        &self,
        location: Location,
        range: Option<Range>,
        sender: Sender<bytes::Bytes>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;

    /// Initiates a multipart upload.
    /// Returns the UploadID of the multipart upload
    /// This is modelled after other multipart upload mechanisms like from S3
    /// This should be comptible with FS based multipart uploads as well
    /// # Arguments
    ///
    /// * `location` - The location of the object which to load
    async fn init_multipart_upload(
        &self,
        location: Location,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>>;

    /// Uploads one part of an object in a multipart uploads
    /// Returns the ETag of the uploaded object
    /// # Arguments
    ///
    /// * `recv` - The receiver from which to load the objects data chunks; the chunks only represent a single part
    /// * `location` - The location of the object
    /// * `upload_id` - The upload id of the multipart uploads
    /// * `content_len` - The size of the uploaded object
    /// * `part_number` - The number of the uploaded part in the final sequence
    async fn upload_multi_object(
        &self,
        recv: Receiver<Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync + 'static>>>,
        location: Location,
        upload_id: String,
        content_len: i64,
        part_number: i32,
    ) -> Result<String, Box<dyn std::error::Error + Send + Sync + 'static>>;

    /// Finishes multipart uploads
    /// # Arguments
    ///
    /// * `location` - The location of the object
    /// * `parts` - The sequence of all uploaded parts that contain their part_number and their ETag
    /// * `upload_id` - The upload id of the multipart uploads
    async fn finish_multipart_upload(
        &self,
        location: Location,
        parts: Vec<PartETag>,
        upload_id: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;

    /// Creates a bucket or the storage system equivalent
    /// # Arguments
    ///
    /// * `bucket` - Name of the bucket to create
    async fn create_bucket(
        &self,
        bucket: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync + 'static>>;
}
