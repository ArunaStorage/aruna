use aruna_server::{models::models::Audience, transactions::user};
use chrono::Utc;
use itertools::Itertools;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use ulid::Ulid;

use crate::error::ProxyError;

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
