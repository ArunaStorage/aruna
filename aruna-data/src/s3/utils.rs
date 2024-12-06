use aruna_server::{
    models::{
        models::{Audience, Hash, HashAlgorithm, ResourceVariant},
        requests::RegisterDataRequest,
    },
    transactions::user,
};
use chrono::Utc;
use itertools::Itertools;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use opendal::{services, Operator};
use s3s::{auth::Credentials, s3_error, S3Error};
use std::sync::Arc;
use tracing::error;
use ulid::Ulid;

use crate::{
    client::ServerClient, config::Backend, error::ProxyError, lmdbstore::LmdbStore,
    structs::StorageLocation, CONFIG,
};

// Create an increasing list of "permutated" paths
// bucket: foo key: bar/baz/bat
// Returns: ["foo/bar", "foo/bar/baz", "foo/bar/baz/bat"]
// All subpaths for a given path

pub fn permute_path(bucket: &str, path: &str) -> Vec<String> {
    let parts = path.split('/').collect::<Vec<&str>>();

    let mut current = String::from(bucket);

    let mut result = Vec::new();
    for part in parts {
        current.push_str("/");
        current.push_str(part);
        result.push(current.clone());
    }

    result
}

pub fn token_from_credentials(creds: Option<&Credentials>) -> Result<String, ProxyError> {
    sign_user_token(
        CONFIG.proxy.get_encoding_key()?,
        CONFIG.proxy.endpoint_id,
        creds.map(|c| c.access_key.clone()).ok_or_else(|| {
            error!("Access key is missing");
            ProxyError::InvalidAccessKey
        })?,
    )
}

pub fn sign_user_token(
    proxy_secret: EncodingKey,
    proxy_id: Ulid,
    access_key: String,
) -> Result<String, ProxyError> {
    // Gets the signing key -> if this returns a poison error this should also panic
    // We dont want to allow poisoned / malformed encoding keys and must crash at this point

    let Some((user_id, token_idx)) = access_key.split(".").collect_tuple() else {
        return Err(ProxyError::InvalidAccessKey);
    };

    let user_id = Ulid::from_string(user_id).map_err(|_| ProxyError::InvalidAccessKey)?;
    let token_idx = token_idx
        .parse()
        .map_err(|_| ProxyError::InvalidAccessKey)?;

    let claims = aruna_server::models::models::ArunaTokenClaims {
        iss: proxy_id.to_string(),
        sub: user_id.to_string(),
        exp: (Utc::now().timestamp() as u64) + 315360000,
        info: Some((0u8, token_idx)),
        scope: None,
        aud: Some(Audience::String("aruna".to_string())),
    };

    let header = Header {
        kid: Some(format!("{}", proxy_id)),
        alg: Algorithm::EdDSA,
        ..Default::default()
    };

    Ok(encode(&header, &claims, &proxy_secret).map_err(|_| ProxyError::InvalidAccessKey)?)
}

pub async fn ensure_parts_exists(
    storage: &Arc<LmdbStore>,
    client: &ServerClient,
    parts: Vec<String>,
    mut parent_id: Ulid,
    user_token: &str,
    bucket: &str,
    key: &str,
) -> Result<(Ulid, StorageLocation), S3Error> {
    let parts_len = parts.len() - 1;
    let project_id = parent_id;
    let location = crate::structs::StorageLocation::S3 {
        bucket: "aruna".to_string(),
        key: format!("{}/{}", bucket, key),
    };
    // Query all the parts of the path
    for (i, part) in parts.into_iter().enumerate() {
        parent_id = if let Some(exists) = storage.get_object_id(&part) {
            exists
        } else {
            let variant = if i != parts_len {
                ResourceVariant::Folder
            } else {
                ResourceVariant::Object
            };
            let name = part.split("/").last().ok_or_else(|| {
                error!("Invalid path");
                s3_error!(InternalError, "Invalid path")
            })?;
            let id = client
                .create_object(name, variant.clone(), parent_id, &user_token)
                .await?;

            if variant == ResourceVariant::Object {
                storage.put_object(
                    id,
                    &part,
                    crate::structs::ObjectInfo::Object {
                        location: location.clone(),
                        storage_format: crate::structs::StorageFormat::Raw,
                        project: project_id,
                        revision_number: 0,
                        public: false,
                    },
                )?;
            } else {
                storage.put_key(id, &part)?;
            }
            id
        };
    }
    Ok((parent_id, location))
}

pub fn get_operator(location: StorageLocation) -> Result<Operator, S3Error> {
    let Backend::S3 {
        host: Some(host),
        access_key: Some(access_key),
        secret_key: Some(secret_key),
        ..
    } = &CONFIG.backend
    else {
        return Err(s3_error!(InternalError, "Invalid backend"));
    };

    let builder = services::S3::default()
        .bucket("aruna")
        .endpoint(host)
        .access_key_id(access_key)
        .secret_access_key(secret_key);

    // Init an operator
    Ok(Operator::new(builder).map_err(ProxyError::from)?.finish())
}

pub async fn finish_data_upload(
    client: &ServerClient,
    object_id: Ulid,
    md5_final: String,
    sha_final: String,
    token: &str,
) -> Result<(), ProxyError> {
    client
        .add_data(
            object_id,
            RegisterDataRequest {
                object_id,
                component_id: CONFIG.proxy.endpoint_id,
                hashes: vec![
                    Hash {
                        algorithm: HashAlgorithm::MD5,
                        value: md5_final.clone(),
                    },
                    Hash {
                        algorithm: HashAlgorithm::Sha256,
                        value: sha_final.clone(),
                    },
                ],
            },
            token,
        )
        .await
}
