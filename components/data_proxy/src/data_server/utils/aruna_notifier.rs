use aruna_rust_api::api::internal::v1::{
    internal_proxy_notifier_service_client::InternalProxyNotifierServiceClient,
    GetOrCreateEncryptionKeyRequest, GetOrCreateObjectByPathRequest,
    GetOrCreateObjectByPathResponse, Location,
};
use s3s::{
    auth::{Credentials, SecretKey},
    s3_error, S3Error,
};
use tonic::transport::Channel;

use super::{
    settings::ServiceSettings,
    utils::{
        construct_path, create_location_from_hash, create_stage_object, validate_and_check_hashes,
    },
};

pub struct ArunaNotifier {
    client: InternalProxyNotifierServiceClient<Channel>,
    settings: ServiceSettings,
    credentials: Credentials,
    get_object_response: Option<GetOrCreateObjectByPathResponse>,
    path: Option<String>,
    valid_md5: Option<String>,
    valid_sha: Option<String>,
    encryption_key: Option<Vec<u8>>,
}

impl ArunaNotifier {
    pub fn new(
        client: InternalProxyNotifierServiceClient<Channel>,
        settings: ServiceSettings,
    ) -> Self {
        ArunaNotifier {
            client,
            settings,
            credentials: Credentials {
                access_key: "".to_string(),
                secret_key: SecretKey::from(""),
            },
            get_object_response: None,
            path: None,
            valid_md5: None,
            valid_sha: None,
            encryption_key: None,
        }
    }

    pub fn set_credentials(&mut self, creds: Option<Credentials>) -> Result<(), S3Error> {
        // Get the credentials
        match creds {
            Some(cred) => {
                self.credentials = cred;
                Ok(())
            }
            None => {
                log::error!("{}", "Not identified PutObjectRequest");
                return Err(s3_error!(NotSignedUp, "Your account is not signed up"));
            }
        }
    }

    pub async fn get_object(
        &mut self,
        bucket: &str,
        key: &str,
        content_len: i64,
    ) -> Result<(), S3Error> {
        self.path = Some(construct_path(bucket, key));

        let get_obj_req = GetOrCreateObjectByPathRequest {
            path: self
                .path
                .clone()
                .ok_or_else(|| s3_error!(InternalError, "Path not found"))?,
            access_key: self.credentials.access_key.to_string(),
            object: Some(create_stage_object(key, content_len)),
            get_only: false,
        };

        // Get or create object by path
        self.get_object_response = Some(
            self.client
                .get_or_create_object_by_path(get_obj_req)
                .await
                .map_err(|e| {
                    log::error!("{}", e);
                    if e.message() == "Record not found" {
                        s3_error!(NoSuchBucket, "Bucket not found")
                    } else {
                        s3_error!(InternalError, "Internal notifier error")
                    }
                })?
                .into_inner(),
        );
        Ok(())
    }

    pub fn validate_hashes(
        &mut self,
        md5: Option<String>,
        sha256: Option<String>,
    ) -> Result<(), S3Error> {
        // Check / get hashes
        (self.valid_md5, self.valid_sha) = validate_and_check_hashes(
            md5,
            sha256,
            self.get_object_response
                .clone()
                .ok_or_else(|| s3_error!(InternalError, "Internal notifier error"))?
                .hashes,
        )?;
        Ok(())
    }

    pub async fn get_encryption_key(&mut self) -> Result<(), S3Error> {
        // Get the encryption key from backend
        let enc_key = self
            .client
            .get_or_create_encryption_key(GetOrCreateEncryptionKeyRequest {
                path: self
                    .path
                    .clone()
                    .ok_or_else(|| s3_error!(InternalError, "Path not found"))?,
                endpoint_id: self.settings.endpoint_id.to_string(),
                hash: self.valid_sha.clone().ok_or_else(|| {
                    s3_error!(
                        SignatureDoesNotMatch,
                        "ArunaNotifier signature does not exist"
                    )
                })?,
            })
            .await
            .map_err(|e| {
                log::error!("{}", e);
                s3_error!(InternalError, "Internal notifier error")
            })?
            .into_inner()
            .encryption_key
            .as_bytes()
            .to_vec();

        self.encryption_key = Some(enc_key);
        Ok(())
    }

    pub fn get_location(&self) -> Result<(Location, bool), S3Error> {
        // Create a target location (may be a temp location)
        Ok(create_location_from_hash(
            &self.valid_sha.clone().ok_or_else(|| {
                s3_error!(
                    InternalError,
                    "Internal notifier error (get_loc / no_valid_sha)"
                )
            })?,
            &self
                .get_object_response
                .clone()
                .ok_or_else(|| {
                    s3_error!(
                        InternalError,
                        "Internal notifier error (get_loc / no_get_obj_response)"
                    )
                })?
                .object_id,
            &self
                .get_object_response
                .clone()
                .ok_or_else(|| {
                    s3_error!(
                        InternalError,
                        "Internal notifier error (get_loc / no_get_obj_response)"
                    )
                })?
                .collection_id,
            self.settings.encrypting,
            self.settings.compressing,
            String::from_utf8_lossy(
                self.encryption_key
                    .clone()
                    .ok_or_else(|| s3_error!(InternalError, "Internal notifier error"))?
                    .as_ref(),
            )
            .into(),
        ))
    }

    pub fn retrieve_enc_key(&self) -> Result<Vec<u8>, S3Error> {
        self.encryption_key.clone().ok_or_else(|| {
            s3_error!(
                InternalError,
                "Internal notifier error unable to retreive enc_key"
            )
        })
    }

    pub fn test_final_hashes(&self, md5: &str, sha: &str) -> Result<(), S3Error> {
        let got_md5 = self
            .valid_md5
            .clone()
            .ok_or_else(|| s3_error!(InvalidDigest, "Invalid or inconsistent MD5 digest"))?;

        let got_sha = self
            .valid_sha
            .clone()
            .ok_or_else(|| s3_error!(InvalidDigest, "Invalid or inconsistent SHA256 digest"))?;

        if !got_md5.is_empty() && !md5.is_empty() && md5 != got_md5 {
            return Err(s3_error!(
                InvalidDigest,
                "Invalid or inconsistent MD5 digest"
            ));
        }
        if !got_sha.is_empty() && !sha.is_empty() && got_sha != sha {
            return Err(s3_error!(
                InvalidDigest,
                "Invalid or inconsistent SHA256 digest"
            ));
        }

        Ok(())
    }

    pub fn get_col_obj(&self) -> Result<(String, String), S3Error> {
        let unwrapped = self
            .get_object_response
            .clone()
            .ok_or_else(|| s3_error!(InternalError, "Unable to retrieve object + collection_id"))?;
        Ok((unwrapped.object_id, unwrapped.collection_id))
    }

    pub fn get_path(&self) -> Result<String, S3Error> {
        self.path
            .clone()
            .ok_or_else(|| s3_error!(InternalError, "Path not found"))
    }

    pub fn get_revision_string(&self) -> Result<String, S3Error> {
        Ok(format!(
            "{}",
            self.get_object_response
                .clone()
                .ok_or_else(|| s3_error!(InternalError, "Unable to retrieve version_id"))?
                .revision_number
        ))
    }
}
