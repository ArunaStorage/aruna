use hmac::{Hmac, Mac};
use rand::Rng;
use sha2::Sha256;
use std::str;

// Create alias for HMAC-SHA256
type HmacSha256 = Hmac<Sha256>;

pub async fn sign_url(mut url: url::Url, sign_query_params: Vec<(&str, &str)>) -> url::Url {
    let secret = "fii";

    let salt = rand::thread_rng().gen::<[u8; 32]>();
    let salt_string = base64::encode(salt);

    let signature_url = create_sign_url(
        url.host_str().unwrap(),
        url.scheme(),
        url.path(),
        salt_string.as_str(),
        sign_query_params,
    )
    .await;

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(signature_url.as_str().as_bytes());
    let result = mac.finalize();
    let signature = result.into_bytes();
    let signature_base64 = base64::encode(signature);

    url.query_pairs_mut()
        .append_pair("salt", salt_string.as_str());

    url.query_pairs_mut()
        .append_pair("sha265-hmac-sign", signature_base64.as_str());

    return url;
}

pub async fn verify_url(
    url: url::Url,
    hmac_sign_base64: &str,
    salt: &str,
    sign_query_params: Vec<(&str, &str)>,
) -> bool {
    let secret = "fii";
    let sign_url = create_sign_url(
        url.host_str().unwrap(),
        url.scheme(),
        url.path(),
        salt,
        sign_query_params,
    )
    .await;
    let sign_url_string = sign_url.as_str();

    let hmac_sign = base64::decode(hmac_sign_base64).unwrap();

    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
    mac.update(sign_url_string.as_bytes());

    match mac.verify_slice(hmac_sign.as_slice()) {
        Ok(_) => return true,
        Err(_) => return false,
    }
}

async fn create_sign_url(
    hostname: &str,
    schema: &str,
    path: &str,
    salt: &str,
    sign_query_params: Vec<(&str, &str)>,
) -> url::Url {
    let joined_url = format!("{}://{}", schema, hostname);
    let mut signature_url = url::Url::parse(joined_url.as_str()).unwrap();
    signature_url.set_path(path);

    signature_url.query_pairs_mut().append_pair("salt", salt);

    for (key, value) in sign_query_params {
        signature_url.query_pairs_mut().append_pair(key, value);
    }

    return signature_url;
}

#[tokio::test]
async fn sign_test() {
    let mut url = url::Url::parse("https://example.com/foo/baa").unwrap();
    url.query_pairs_mut().append_pair("testkey", "testvalue");
    url.query_pairs_mut().append_pair("expires", "500");
    let params = vec![("expires", "500")];

    let signed_url = sign_url(url.clone(), params).await;
    let mut salt = "".to_string();
    let mut expires = "".to_string();
    let mut hmac_sign_base64 = "".to_string();
    for (key, value) in signed_url.clone().query_pairs() {
        match key.to_string().as_str() {
            "salt" => salt = value.to_string(),
            "expires" => expires = value.to_string(),
            "sha265-hmac-sign" => hmac_sign_base64 = value.to_string(),
            _ => {}
        }
    }

    let sign_query_params = vec![("expires", expires.as_str())];

    let verified = verify_url(
        url.clone(),
        hmac_sign_base64.as_str(),
        salt.as_str(),
        sign_query_params.clone(),
    )
    .await;

    assert_eq!(verified, true);

    let verified = verify_url(
        url.clone(),
        hmac_sign_base64.as_str(),
        "fooo",
        sign_query_params.clone(),
    )
    .await;

    assert_eq!(verified, false);

    let verified = verify_url(url.clone(), "foo", salt.as_str(), sign_query_params.clone()).await;

    assert_eq!(verified, false);
}
