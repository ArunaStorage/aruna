use crate::database::connection::Database;
use crate::database::models::auth::PubKey;
use crate::database::models::enums::{Resources, UserRights};
use crate::error::GrpcNotFoundError;
use crate::error::{ArunaError, AuthorizationError};
use chrono::prelude::*;
use dotenv::dotenv;
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use openssl::pkey::PKey;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env::{self, VarError};
use std::ops::Deref;
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tonic::metadata::MetadataMap;

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
    pub async fn new(db: Arc<Database>) -> Authz {
        dotenv().ok();
        // Get the realinfo from config, this is used to query the pubkey from oidc
        let realminfo = env::var("OAUTH_REALMINFO").expect("OAUTH_REALMINFO must be set");
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
            .get_or_add_pub_key(pub_key_string)
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
        context: Context,
    ) -> Result<uuid::Uuid, ArunaError> {
        let token = self.validate_and_query_token(metadata).await?;
        self.db.get_checked_user_id_from_token(token, context)
    }

    /// This is a wrapper that runs the authorize function with a `personal` context
    /// a convenience function if this request is `personal` scoped
    pub async fn personal_authorize(
        &self,
        metadata: &MetadataMap,
    ) -> Result<uuid::Uuid, ArunaError> {
        self.authorize(
            metadata,
            Context {
                user_right: UserRights::READ,
                resource_type: Resources::PROJECT,
                resource_id: uuid::Uuid::default(),
                admin: false,
                personal: true,
                oidc_context: false,
            },
        )
        .await
    }

    /// This is a wrapper that runs the authorize function with an `admin` context
    /// a convenience function if this request is `admin` scoped
    pub async fn admin_authorize(&self, metadata: &MetadataMap) -> Result<uuid::Uuid, ArunaError> {
        self.authorize(
            metadata,
            Context {
                user_right: UserRights::READ,
                resource_type: Resources::PROJECT,
                resource_id: uuid::Uuid::default(),
                admin: true,
                personal: false,
                oidc_context: false,
            },
        )
        .await
    }

    pub async fn validate_and_query_token(
        &self,
        metadata: &MetadataMap,
    ) -> Result<uuid::Uuid, ArunaError> {
        let token_string = metadata
            .get("Bearer")
            .ok_or(ArunaError::GrpcNotFoundError(
                GrpcNotFoundError::METADATATOKEN,
            ))?
            .to_str()?;

        let header = decode_header(token_string)?;

        let kid = header.kid.ok_or(AuthorizationError::PERMISSIONDENIED)?;

        if Authz::is_oidc_from_metadata(metadata).await? {
            let subject = self.validate_oidc_only(metadata).await?;

            match self.db.get_oidc_user(subject)? {
                Some(u) => return Ok(u),
                None => {
                    return Err(ArunaError::AuthorizationError(
                        AuthorizationError::UNREGISTERED,
                    ))
                }
            }
        }

        // --------------- This section only applies if the token is an "aruna" token -------------------------

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

        let key = if option_key.is_some() {
            Ok(option_key.unwrap())
        } else {
            guard
                .get(&index)
                .ok_or(AuthorizationError::PERMISSIONDENIED)
        }?;

        let token_data = decode::<Claims>(token_string, key, &Validation::new(Algorithm::EdDSA))?;

        Ok(uuid::Uuid::parse_str(token_data.claims.sub.as_str())?)
    }

    pub async fn validate_oidc_only(&self, metadata: &MetadataMap) -> Result<String, ArunaError> {
        let token_string = metadata
            .get("Bearer")
            .ok_or(ArunaError::GrpcNotFoundError(
                GrpcNotFoundError::METADATATOKEN,
            ))?
            .to_str()?;

        let header = decode_header(token_string)?;

        // Process as keycloak token
        let pem_token = self.get_token_realminfo().await?;

        // Validate key

        let token_data = decode::<Claims>(
            token_string,
            &DecodingKey::from_rsa_pem(pem_token.as_bytes())?,
            &Validation::new(header.alg),
        )?;

        let subject = token_data.claims.sub;
        Ok(subject)
    }

    async fn get_token_realminfo(&self) -> Result<String, ArunaError> {
        let resp = reqwest::get(&self.oidc_realminfo)
            .await?
            .json::<HashMap<String, String>>()
            .await?;

        let pub_key = resp
            .get("public_key")
            .ok_or(AuthorizationError::AUTHFLOWERROR)?;

        Ok(format!(
            "{}{}{}",
            "-----BEGIN RSA PUBLIC KEY-----\n", pub_key, "\n-----END RSA PUBLIC KEY-----"
        ))
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
        token_id: String,
        expires_at: Option<prost_types::Timestamp>,
    ) -> Result<String, ArunaError> {
        // Gets the signing key / mutex -> if this returns a poison error this should also panic
        // We dont want to allow poisoned / malformed encoding keys and must crash at this point
        let signing_key = self.signing_key.lock().unwrap();

        let claim = Claims {
            sub: token_id,
            exp: if expires_at.is_none() {
                // Add 10 years to token lifetime
                Utc::now().second() as usize + 315360000
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
        let token_string = metadata
            .get("Bearer")
            .ok_or(ArunaError::GrpcNotFoundError(
                GrpcNotFoundError::METADATATOKEN,
            ))?
            .to_str()?;

        let header = decode_header(token_string)?;

        if header.alg != Algorithm::EdDSA {
            return Ok(true);
        }
        Ok(false)
    }
}
