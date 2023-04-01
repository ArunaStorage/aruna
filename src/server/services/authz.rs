use crate::config::ArunaServerConfig;
use crate::database::connection::Database;
use crate::database::models::auth::{ApiToken, PubKey};
use crate::database::models::enums::{Resources, UserRights};
use crate::error::GrpcNotFoundError;
use crate::error::{ArunaError, AuthorizationError};

use anyhow::Result;
use chrono::prelude::*;
use dotenv::dotenv;
use http::Method;
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use openssl::pkey::PKey;
use reqsign::credential::{Credential, CredentialLoad};
use reqsign::{AwsCredentialLoader, AwsV4Signer};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env::{self, VarError};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use time::Duration;
use tokio::sync::RwLock;
use tonic::metadata::MetadataMap;
use url::Url;

/// This is the main struct for the Authz handling
///
/// It is okay if the `pub_keys` field does not always contain all of the most recent pubkeys
/// if a user uses an unknown pubkey an automated refresh will be triggered.
/// "Old" or invalidated pubkeys should not get any results when the token is checked against the database
/// because they should get deleted if the corresponding pubkey is delete
///
/// ## Fields:
///
/// pub_keys: HashMap<i64, DecodingKey> -> Contains all existing pubkeys
/// db: Arc<Database> -> Database access
/// oidc_realminfo: String -> The URL to query the oidc pubkey
///
pub struct Authz {
    pub_keys: Arc<RwLock<HashMap<i64, DecodingKey>>>,
    signing_key: Arc<Mutex<(i64, EncodingKey, DecodingKey)>>,
    db: Arc<Database>,
    oidc_realminfo: String,
}

/// This contains claims for ArunaTokens
/// containing two fields
///
/// - tid: UUID from the specific token
/// - exp: When this token expires (by default very large number)
///
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    exp: usize,
}

/// This is a helper struct to deserialize the JSON response from Keycloak
/// Uses Serde and reqwest to deserialize the needed public_key
#[derive(Deserialize, Debug)]
struct KeyCloakResponse {
    #[serde(alias = "realm")]
    _realm: String,
    public_key: String,
    #[serde(alias = "token-service")]
    _token_service: String,
    #[serde(alias = "account-service")]
    _account_service: String,
    #[serde(alias = "tokens-not-before")]
    _tokens_not_before: i64,
}

/// Format PublicKey to PEM format and return it as String
impl KeyCloakResponse {
    fn to_pem(&self) -> String {
        format!(
            "{}\n{}\n{}",
            "-----BEGIN PUBLIC KEY-----", self.public_key, "-----END PUBLIC KEY-----"
        )
    }
}

/// This struct represents a request "Context" it is used to specify the
/// accessed resource_type and id as well as the needed permissions
pub struct Context {
    pub user_right: UserRights,
    pub resource_type: Resources,
    pub resource_id: uuid::Uuid,
    // These requests need admin rights
    // For this the user needs to be part of a projects with the admin flag 1
    pub admin: bool,

    // Some requests can only be authorized for personal access
    // for example querying or modifying personal tokens
    pub personal: bool,

    // Some requests can be authorized using only oidc tokens
    // If oidc_context is true and the token is an oidc token this request will succeed
    // All other fields will be ignored, this should only be used for
    // register and the initial creation / deletion of private access tokens
    pub oidc_context: bool,
}

#[derive(Debug)]
pub struct CustomLoader {
    pub access_key: String,
    pub secret_key: String,
}

impl CustomLoader {
    pub fn new(access_key: &str, secret_key: &str) -> Self {
        CustomLoader {
            access_key: access_key.to_string(),
            secret_key: secret_key.to_string(),
        }
    }
}

impl CredentialLoad for CustomLoader {
    fn load_credential(&self) -> Result<Option<Credential>> {
        Ok(Some(Credential::new(&self.access_key, &self.secret_key)))
    }
}

/// Implementations for the Authz struct contain methods to create and check
/// authorizations for the database
impl Authz {
    /// Create new Authz object, this should only be initialized once in the beginning.
    /// This will pre-populate Authz::pub_keys with DecodingKeys for all existing pubkeys
    ///
    /// ## Arguments
    ///
    /// * db `Arc<Database>` an atomic reference to the database used to query existing pubkeys
    ///
    /// ## Result
    ///
    /// * `Authz` an instance of this struct.
    ///
    pub async fn new(db: Arc<Database>, config: ArunaServerConfig) -> Authz {
        dotenv().ok();
        // Get the realinfo from config, this is used to query the pubkey from oidc
        let realminfo = config.config.oauth_realminfo;
        // Query the required signing key environment variable
        let signing_key_result = env::var("SIGNING_KEY");
        // Check if the signing key is set, if yes continue with this key
        // otherwise generate a new ed25519 private key
        // This requires openssl on the system
        let signing_key = match signing_key_result {
            Ok(key) => key.as_bytes().to_vec(),
            Err(err) => {
                if err == VarError::NotPresent {
                    let privatekey =
                        PKey::generate_ed25519().expect("Unable to generate new signing key");
                    privatekey
                        .private_key_to_pem_pkcs8()
                        .expect("Unable to convert signing key to pem")
                } else {
                    panic!("Unable to parse SIGNING KEY: {}", err)
                }
            }
        };

        // Parse the returned Vec<u8> (private) signing key to pem format
        let priv_key_ossl =
            PKey::private_key_from_pem(&signing_key).expect("Unable to parse private key to pem");
        // Create an associated pubkey for this private key
        let pubkey = priv_key_ossl
            .public_key_to_pem()
            .expect("Cant convert pkey to pem");
        // Convert the pubkey to string
        let pub_key_string = String::from_utf8_lossy(&pubkey).to_string();
        // Query the database for this pubkey, either return the queried id or add this pubkey as new key to the database
        let serial = db
            .get_or_add_pub_key(pub_key_string, None)
            .expect("Error in get or add signing key");

        // Add endpoint keys

        db.get_or_add_pub_key(
            config.config.default_endpoint.endpoint_pubkey,
            Some(config.config.default_endpoint.endpoint_serial),
        )
        .expect("Error in get or add signing key");

        // Query databse for all existing pubkeys
        let keys = db.get_pub_keys().expect("Unable to query signing pubkeys");
        // Store them in a local hashmap cache
        let result = Authz::convert_pubkey_to_decoding_key(keys)
            .await
            .expect("Error in decoding pubkeys");

        let decoding_key = result
            .get(&serial)
            .expect("Current decoding key not found")
            .clone();

        // return the Authz struct
        Authz {
            pub_keys: Arc::new(RwLock::new(result)),
            db,
            oidc_realminfo: realminfo,
            signing_key: Arc::new(Mutex::new((
                serial,
                EncodingKey::from_ed_pem(&signing_key)
                    .expect("Unable to create EncodingKey from pem"),
                decoding_key,
            ))),
        }
    }
    /// Converts a Database pubkey to the correct HashMap for the Authz struct.
    ///
    /// ## Arguments
    ///
    /// * pubkey `Vec<PubKey>` Vector with database pubkey objects. At least 1 pubkey must be present in the database.
    ///
    /// ## Result
    ///
    /// * `Result<HashMap<i64, DecodingKey>, ArunaError>` Resulting Hashmap with key = pubkey serial and values = decoding key to validate JWT tokens
    ///
    async fn convert_pubkey_to_decoding_key(
        pubkey: Vec<PubKey>,
    ) -> Result<HashMap<i64, DecodingKey>, ArunaError> {
        pubkey
            .into_iter()
            .map(
                |pubkey| match DecodingKey::from_ed_pem(pubkey.pubkey.as_bytes()) {
                    Ok(e) => Ok((pubkey.id, e)),
                    Err(_) => Err(ArunaError::AuthorizationError(
                        AuthorizationError::PERMISSIONDENIED,
                    )),
                },
            )
            .collect::<Result<HashMap<_, _>, _>>()
    }
    /// Renews the internal pubkey struct. It is okay if this happens infrequently.
    /// Apitokens should be deleted when a corresponding pub / privkey gets deleted.
    ///
    /// ## Result
    ///
    /// * Result<_, ArunaError> Only an error is returned
    ///
    async fn renew_pubkeys(&self) -> Result<(), ArunaError> {
        let mut _pub_keys = self.pub_keys.write().await.deref();
        _pub_keys = &mut Authz::convert_pubkey_to_decoding_key(self.db.get_pub_keys()?).await?;
        Ok(())
    }

    /// The `authorize` method is used to check if the supplied user token has enough permissions
    /// to fullfill the gRPC request the `db.get_checked_user_id_from_token()` method will check if the token and its
    /// associated permissions permissions are sufficient enough to execute the request
    ///
    /// ## Arguments
    ///
    /// - Metadata of the request containing a token
    /// - Context that specifies which ressource is accessed and which permissions are requested
    ///
    /// ## Return
    ///
    /// This returns an Result<UUID> or an Error
    /// If it returns an Error the authorization failed otherwise
    /// the uuid is the user_id of the user that owns the token
    /// this user_id will for example be used to specify the "created_by" field in the database
    ///
    pub async fn authorize(
        &self,
        metadata: &MetadataMap,
        context: &Context,
    ) -> Result<uuid::Uuid, ArunaError> {
        let oidc_user = self.check_if_oidc(metadata).await?;

        match oidc_user {
            Some(u) => {
                if context.personal {
                    Ok(u)
                } else {
                    Err(ArunaError::AuthorizationError(
                        AuthorizationError::PERMISSIONDENIED,
                    ))
                }
            }
            None => {
                let token_uuid = self.validate_and_query_token_from_md(metadata).await?;
                Ok(self
                    .db
                    .get_checked_user_id_from_token(&token_uuid, context)?
                    .0)
            }
        }
    }

    /// The `authorize_verbose` method is used to check if the supplied user token has enough permissions
    /// to fullfill the gRPC request the `db.get_checked_user_id_from_token()` method will check if the token and its
    /// associated permissions permissions are sufficient enough to execute the request.
    ///
    /// ## Arguments
    ///
    /// - Metadata of the request containing a token
    /// - Context that specifies which ressource is accessed and which permissions are requested
    ///
    /// ## Return
    ///
    /// This returns an Result<(UUID, Option<ApiToken>)> or an Error.
    /// If it returns an Error the authorization failed otherwise.
    /// The uuid is the user_id of the user that owns the token.
    /// This user_id will for example be used to specify the "created_by" field in the database.
    /// The ApiToken only additionally returns if no OIDC token was used for authorization.
    ///
    pub async fn authorize_verbose(
        &self,
        metadata: &MetadataMap,
        context: &Context,
    ) -> Result<(uuid::Uuid, Option<ApiToken>), ArunaError> {
        let oidc_user = self.check_if_oidc(metadata).await?;

        match oidc_user {
            Some(u) => {
                if context.personal {
                    Ok((u, None))
                } else {
                    Err(ArunaError::AuthorizationError(
                        AuthorizationError::PERMISSIONDENIED,
                    ))
                }
            }
            None => {
                let token_uuid = self.validate_and_query_token_from_md(metadata).await?;
                let (creator_uuid, api_token) = self
                    .db
                    .get_checked_user_id_from_token(&token_uuid, context)?;

                Ok((creator_uuid, Some(api_token)))
            }
        }
    }

    /// This is a wrapper that runs the authorize function with a `personal` context
    /// a convenience function if this request is `personal` scoped
    pub async fn personal_authorize(
        &self,
        metadata: &MetadataMap,
    ) -> Result<uuid::Uuid, ArunaError> {
        self.authorize(
            metadata,
            &(Context {
                user_right: UserRights::READ,
                resource_type: Resources::PROJECT,
                resource_id: uuid::Uuid::default(),
                admin: false,
                personal: true,
                oidc_context: false,
            }),
        )
        .await
    }

    /// This is a wrapper that runs the authorize function with an `admin` context
    /// a convenience function if this request is `admin` scoped
    pub async fn admin_authorize(&self, metadata: &MetadataMap) -> Result<uuid::Uuid, ArunaError> {
        self.authorize(
            metadata,
            &(Context {
                user_right: UserRights::READ,
                resource_type: Resources::PROJECT,
                resource_id: uuid::Uuid::default(),
                admin: true,
                personal: false,
                oidc_context: false,
            }),
        )
        .await
    }

    /// This is a wrapper that runs the authorize function with an `collection` context
    /// a convenience function if this request is `collection` scoped
    pub async fn collection_authorize(
        &self,
        metadata: &MetadataMap,
        collection_id: uuid::Uuid,
        user_right: UserRights,
    ) -> Result<uuid::Uuid, ArunaError> {
        self.authorize(
            metadata,
            &(Context {
                user_right,
                resource_type: Resources::COLLECTION,
                resource_id: collection_id,
                admin: false,
                personal: false,
                oidc_context: false,
            }),
        )
        .await
    }

    /// This is a wrapper that runs the authorize function with an `project` context
    /// a convenience function if this request is `project` scoped
    pub async fn project_authorize(
        &self,
        metadata: &MetadataMap,
        project_id: uuid::Uuid,
        user_right: UserRights,
    ) -> Result<uuid::Uuid, ArunaError> {
        self.authorize(
            metadata,
            &(Context {
                user_right,
                resource_type: Resources::PROJECT,
                resource_id: project_id,
                admin: false,
                personal: false,
                oidc_context: false,
            }),
        )
        .await
    }

    /// This is a wrapper that runs the authorize function with an `project` context
    /// a convenience function if this request is `project` scoped
    /// this uses a collection_id to determine the associated project
    pub async fn project_authorize_by_collectionid(
        &self,
        metadata: &MetadataMap,
        collection_id: uuid::Uuid,
        user_right: UserRights,
    ) -> Result<uuid::Uuid, ArunaError> {
        let project_id = self.db.get_project_id_by_collection_id(collection_id)?;
        self.authorize(
            metadata,
            &(Context {
                user_right,
                resource_type: Resources::PROJECT,
                resource_id: project_id,
                admin: false,
                personal: false,
                oidc_context: false,
            }),
        )
        .await
    }

    pub async fn check_if_oidc(
        &self,
        metadata: &MetadataMap,
    ) -> Result<Option<uuid::Uuid>, ArunaError> {
        // If this token is OIDC
        if Authz::is_oidc_from_metadata(metadata).await? {
            let subject = self.validate_oidc_only(metadata).await?;

            match self.db.get_oidc_user(&subject)? {
                Some(u) => Ok(Some(u)),
                None => Err(ArunaError::AuthorizationError(
                    AuthorizationError::UNREGISTERED,
                )),
            }
            // Could be an arunatoken
        } else {
            Ok(None)
        }
    }

    pub async fn validate_and_query_token_from_md(
        &self,
        metadata: &MetadataMap,
    ) -> Result<uuid::Uuid, ArunaError> {
        let token_string = get_token_from_md(metadata)?;
        self.validate_and_query_token(&token_string).await
    }

    pub async fn validate_and_query_token(
        &self,
        token_secret: &String,
    ) -> Result<uuid::Uuid, ArunaError> {
        let header = decode_header(token_secret.as_str())?;

        let kid = header.kid.ok_or(AuthorizationError::PERMISSIONDENIED)?;

        let hashmap = self.pub_keys.clone();
        let index = kid
            .parse::<i64>()
            .map_err(|_| AuthorizationError::PERMISSIONDENIED)?;
        let guard = hashmap.read().await;

        let dec_map = guard.clone();
        drop(guard);
        let option_key = dec_map.get(&index);

        if option_key.is_none() {
            self.renew_pubkeys().await?;
        }

        let guard = hashmap.read().await;

        let key = (if option_key.is_some() {
            Ok(option_key.unwrap())
        } else {
            guard
                .get(&index)
                .ok_or(AuthorizationError::PERMISSIONDENIED)
        })?;

        let token_data = decode::<Claims>(
            token_secret.as_str(),
            key,
            &Validation::new(Algorithm::EdDSA),
        )
        .map_err(|e| match e.into_kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthorizationError::TOKENEXPIRED,
            _ => AuthorizationError::PERMISSIONDENIED,
        })?;

        Ok(uuid::Uuid::parse_str(token_data.claims.sub.as_str())?)
    }

    pub async fn validate_oidc_only(&self, metadata: &MetadataMap) -> Result<String, ArunaError> {
        let token_string = get_token_from_md(metadata)?;

        let header = decode_header(&token_string)?;

        // Process as keycloak token
        let pem_token = self.get_token_realminfo().await?;

        // Validate key

        let token_data = decode::<Claims>(
            &token_string,
            &DecodingKey::from_rsa_pem(pem_token.as_bytes())?,
            &Validation::new(header.alg),
        )
        .map_err(|e| match e.into_kind() {
            jsonwebtoken::errors::ErrorKind::ExpiredSignature => AuthorizationError::TOKENEXPIRED,
            _ => AuthorizationError::PERMISSIONDENIED,
        })?;

        let subject = token_data.claims.sub;
        Ok(subject)
    }

    async fn get_token_realminfo(&self) -> Result<String, ArunaError> {
        let resp = reqwest::get(&self.oidc_realminfo).await?;
        log::debug!("Realm info response: {:#?}", resp);
        let mapped = resp.json::<KeyCloakResponse>().await?;
        log::debug!("KeyCloak response: {:#?}", mapped);
        Ok(mapped.to_pem())
    }

    pub async fn get_decoding_key(&self) -> DecodingKey {
        // This unwrap is ok, poison errors should crash the whole application
        self.signing_key.lock().unwrap().2.clone()
    }

    pub async fn get_decoding_serial(&self) -> i64 {
        // This unwrap is ok, poison errors should crash the whole application
        self.signing_key.lock().unwrap().0
    }

    pub async fn sign_new_token(
        &self,
        token_id: &str,
        expires_at: Option<prost_types::Timestamp>,
    ) -> Result<String, ArunaError> {
        // Gets the signing key / mutex -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_key.lock().unwrap();

        let claim = Claims {
            sub: token_id.to_string(),
            exp: if expires_at.is_none() {
                // Add 10 years to token lifetime
                (Utc::now().timestamp() as usize) + 315360000
            } else {
                expires_at.unwrap().seconds as usize
            },
        };

        let header = Header {
            kid: Some(format!("{}", signing_key.0)),
            alg: Algorithm::EdDSA,
            ..Default::default()
        };

        Ok(encode(&header, &claim, &signing_key.1)?)
    }

    /// This gets the TokenType from a gRPC metadata
    ///
    /// Oidc token -> "true"
    /// Or Aruna token -> "false"
    ///
    pub async fn is_oidc_from_metadata(metadata: &MetadataMap) -> Result<bool, ArunaError> {
        let token_string = get_token_from_md(metadata)?;

        let header = decode_header(&token_string)?;

        if header.alg != Algorithm::EdDSA {
            return Ok(true);
        }
        Ok(false)
    }
}

/// Creates a fully customized presigned S3 url.
///
/// ## Arguments:
///
/// * `method: http::Method` - Http method the request is valid for
/// * `access_key: &String` - Secret key id
/// * `secret_key: &String` - Secret key for access
/// * `ssl: bool` - Flag if the endpoint is accessible via ssl
/// * `multipart: bool` - Flag if the request is for a specific multipart part upload
/// * `part_number: i32` - Specific part number if multipart: true
/// * `upload_id: &String` - Multipart upload id if multipart: true
/// * `bucket: &String` - Bucket name
/// * `key: &String` - Full path of object in bucket
/// * `endpoint: &String` - Full path of object in bucket
/// * `duration: i64` - Full path of object in bucket
/// *
///
/// ## Returns:
///
/// * `` -
///
pub fn sign_url(
    method: http::Method,
    access_key: &str,
    secret_key: &str,
    ssl: bool,
    multipart: bool,
    part_number: i32,
    upload_id: &str,
    bucket: &str,
    key: &str,
    endpoint: &str,
    duration: i64,
) -> Result<String> {
    // Signer will load region and credentials from environment by default.
    let cloder = reqsign::AwsConfigLoader::with_loaded();
    cloder.set_region("RegionOne");

    let signer = AwsV4Signer::builder()
        .config_loader(cloder.clone())
        .credential_loader(
            AwsCredentialLoader::new(cloder).with_customed_credential_loader(Arc::new(
                CustomLoader::new(access_key, secret_key),
            )),
        )
        .service("s3")
        .build()?;

    // Set protocol depending if ssl
    let protocol = if ssl { "https://" } else { "http://" };

    // Remove http:// or https:// from beginning of endpoint url if present
    let endpoint_sanitized = if endpoint.starts_with("https://") {
        endpoint[8..].to_string()
    } else if endpoint.starts_with("http://") {
        endpoint[7..].to_string()
    } else {
        endpoint.to_string()
    };

    // Construct request
    let url = if multipart {
        Url::parse(&format!(
            "{}{}.{}/{}?partNumber={}&uploadId={}",
            protocol, bucket, endpoint_sanitized, key, part_number, upload_id
        ))?
    } else {
        Url::parse(&format!(
            "{}{}.{}/{}",
            protocol, bucket, endpoint_sanitized, key
        ))?
    };

    let mut req = reqwest::Request::new(method, url);

    // Signing request with Signer
    signer.sign_query(&mut req, Duration::seconds(duration))?;
    Ok(req.url().to_string())
}

/// Convenience wrapper function for sign_url(...) to reduce unused parameters for download url.
pub fn sign_download_url(
    access_key: &str,
    secret_key: &str,
    ssl: bool,
    bucket: &str,
    key: &str,
    endpoint: &str,
) -> Result<String> {
    sign_url(
        Method::GET,
        access_key,
        secret_key,
        ssl,
        false,
        0,
        "",
        bucket,
        key,
        endpoint,
        604800, //Note: Default 1 week until requests allow custom duration
    )
}

pub fn get_token_from_md(md: &MetadataMap) -> Result<String, ArunaError> {
    let token_string = md
        .get("Authorization")
        .ok_or(ArunaError::GrpcNotFoundError(
            GrpcNotFoundError::METADATATOKEN,
        ))?
        .to_str()?;

    let splitted = token_string.split(' ').collect::<Vec<_>>();

    if splitted.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            splitted.len()
        );
        return Err(ArunaError::AuthorizationError(
            AuthorizationError::AUTHFLOWERROR,
        ));
    }

    if splitted[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid Tokentype, expected: Bearer, got: {:?}",
            splitted[0]
        );
        return Err(ArunaError::AuthorizationError(
            AuthorizationError::AUTHFLOWERROR,
        ));
    }

    if splitted[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid Tokenlength, expected: >0, got: {:?}",
            splitted[1].len()
        );
        return Err(ArunaError::AuthorizationError(
            AuthorizationError::AUTHFLOWERROR,
        ));
    }

    Ok(splitted[1].to_string())
}
