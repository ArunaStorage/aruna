use anyhow::anyhow;
use anyhow::Result;
use aruna_rust_api::api::storage::models::v1::Hash;
use aruna_rust_api::api::storage::models::v1::Hashalgorithm;
use aruna_rust_api::api::{
    internal::v1::Location,
    storage::{models::v1::DataClass, services::v1::StageObject},
};
use s3s::s3_error;
use s3s::S3Error;

pub fn construct_path(bucket: &str, key: &str) -> String {
    format!("s3://{bucket}/{key}")
}

pub fn create_stage_object(key: &str, content_len: i64) -> StageObject {
    let (fname, sub_path) = extract_filename_path(key);
    StageObject {
        filename: fname,
        content_len,
        source: None,
        dataclass: DataClass::Private as i32,
        labels: Vec::new(),
        hooks: Vec::new(),
        sub_path,
    }
}

pub fn extract_filename_path(path: &str) -> (String, String) {
    let mut splits: Vec<&str> = path.split('/').collect();
    (String::from(splits.pop().unwrap_or("")), splits.join("/"))
}

#[allow(clippy::too_many_arguments)]
pub fn create_location_from_hash(
    sha256_hash: &str,
    object_id: &str,
    collection_id: &str,
    encrypting: bool,
    compressing: bool,
    encryption_key: String,
    endpoint_id: String,
    exists: bool,
) -> (Location, bool) {
    if sha256_hash.is_empty() {
        (
            // For now we do not compress temp values
            Location {
                bucket: format!("{}-temp", endpoint_id.to_lowercase()),
                is_compressed: false,
                is_encrypted: encrypting,
                encryption_key,
                endpoint_id,
                path: format!("{}/{}", collection_id, object_id),
                ..Default::default()
            },
            true,
        )
    } else {
        // If not existing -> Write directly to the expected location,
        if !exists {
            (
                Location {
                    bucket: format!("{}-{}", endpoint_id.to_lowercase(), &sha256_hash[0..2]),
                    path: sha256_hash[2..].to_string(),
                    is_compressed: compressing,
                    is_encrypted: encrypting,
                    encryption_key,
                    endpoint_id,
                    ..Default::default()
                },
                false,
            )
        // Otherwise do not try to overwrite the expected location
        } else {
            (
                // For now we do not compress temp values
                Location {
                    bucket: format!("{}-temp", endpoint_id.to_lowercase()),
                    is_compressed: false,
                    is_encrypted: encrypting,
                    encryption_key,
                    endpoint_id,
                    path: format!("{}/{}", collection_id, object_id),
                    ..Default::default()
                },
                true,
            )
        }
    }
}

pub fn validate_and_check_hashes(
    s3_md5_hash: Option<String>,
    s3_sha256_hash: Option<String>,
    backend_hashes: Vec<Hash>,
) -> Result<(Option<String>, Option<String>), S3Error> {
    let mut hash_md5 = match s3_md5_hash {
        Some(h) => h,
        None => String::new(),
    };
    let mut hash_sha256 = match s3_sha256_hash {
        Some(h) => h,
        None => String::new(),
    };

    for hash in backend_hashes {
        match Hashalgorithm::from_i32(hash.alg) {
            Some(Hashalgorithm::Md5) => {
                if !hash_md5.is_empty() && !hash.hash.is_empty() && hash.hash != hash_md5 {
                    return Err(s3_error!(
                        InvalidDigest,
                        "Invalid or inconsistent MD5 digest"
                    ));
                }
                hash_md5 = hash.hash;
            }
            Some(Hashalgorithm::Sha256) => {
                if !hash_sha256.is_empty() && !hash.hash.is_empty() && hash.hash != hash_sha256 {
                    return Err(s3_error!(
                        InvalidDigest,
                        "Invalid or inconsistent SHA256 digest"
                    ));
                }
                hash_sha256 = hash.hash;
            }
            _ => {}
        }
    }

    if !hash_md5.is_empty() && hash_md5.len() != 32 {
        return Err(s3_error!(
            InvalidDigest,
            "Invalid or inconsistent MD5 digest"
        ));
    }

    if !hash_sha256.is_empty() && hash_sha256.len() != 64 {
        return Err(s3_error!(
            InvalidDigest,
            "Invalid or inconsistent SHA256 digest"
        ));
    }

    Ok((Some(hash_md5), Some(hash_sha256)))
}

pub fn validate_expected_hashes(expected: Option<Vec<Hash>>, got: &[Hash]) -> Result<()> {
    match expected {
        Some(hashes) => {
            if got.iter().all(|got_hash| {
                hashes
                    .iter()
                    .any(|exp_hash| exp_hash.clone().hash == got_hash.clone().hash)
            }) {
                Ok(())
            } else {
                Err(anyhow!("Got hash {:?}, expected: {:?}", hashes, got))
            }
        }
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn extract_filename_path_test() {
        use super::*;
        let test = "";

        assert_eq!(
            extract_filename_path(test),
            ("".to_string(), "".to_string())
        );

        let test = "a/a";
        assert_eq!(
            extract_filename_path(test),
            ("a".to_string(), "a".to_string())
        );

        let test = "/a";
        assert_eq!(
            extract_filename_path(test),
            ("a".to_string(), "".to_string())
        );
    }
}
