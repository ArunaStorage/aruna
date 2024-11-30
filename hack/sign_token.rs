use aruna_server::{models::models::Audience, transactions::user};
use chrono::Utc;
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use ulid::Ulid;

pub fn sign_user_token(
    proxy_secret: EncodingKey,
    proxy_id: Ulid,
    user_id: Ulid,
    token_idx: u16,
) -> String {
    // Gets the signing key -> if this returns a poison error this should also panic
    // We dont want to allow poisoned / malformed encoding keys and must crash at this point

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

    encode(&header, &claims, &proxy_secret).unwrap()
}

fn main() {
    let private_key = "MC4CAQAwBQYDK2VwBCIEIM/FI+bYw+auSKGyGqeISRIEjofvZV/lbK7QL1wkuCey";
    let pubkey = "MCowBQYDK2VwAyEAnouQBh4GHPCD/k85VIzPyCdOijVg2qlzt2TELwTMy4c=";
    let proxy_id = Ulid::from_string("01JDWY8MN7JDSJ7E3WZ957TQJS").unwrap();

    // let user_id = Ulid::from_string("01JDWY8MN7JDSJ7E3WZ957TQJS").unwrap();
    let user_id = Ulid::from_string("01JDWXJ6V34MH1XGZW8WS7Q22Z").unwrap();
    let token_idx = 1;

    let private_pem = format!(
        "-----BEGIN PRIVATE KEY-----{}-----END PRIVATE KEY-----",
        private_key
    );

    let encoding_key = EncodingKey::from_ed_pem(private_pem.as_bytes()).unwrap();

    let token = sign_user_token(encoding_key, proxy_id, user_id, token_idx);

    println!("Token: {}", token);
}
