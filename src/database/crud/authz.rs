use crate::database::connection::Database;
use crate::database::models::auth::{PubKey, PubKeyInsert, User};
use crate::error::{ArunaError, AuthorizationError};
use diesel::insert_into;
use diesel::prelude::*;

impl Database {
    /// Method to query all public keys from the Database
    ///
    /// ## Arguments
    ///
    /// ## Result
    ///
    /// * `Vec<PubKey>` - Vector with public keys
    ///
    pub fn get_pub_keys(&self) -> Result<Vec<PubKey>, ArunaError> {
        use crate::database::schema::pub_keys::dsl::*;
        use diesel::result::Error as dError;
        Ok(self
            .pg_connection
            .get()?
            .transaction::<Vec<PubKey>, dError, _>(|conn| pub_keys.load::<PubKey>(conn))?)
    }

    /// Method to query a specific pubkey and add it to the database if it not exists
    ///
    /// ## Arguments
    ///
    /// - pubkey String
    /// - serial: Option<i64> // Filter for this exact serial
    ///
    /// ## Result
    ///
    /// * `Result<i64, ArunaError>` -> Returns the queried or inserted serial number
    ///
    pub fn get_or_add_pub_key(
        &self,
        pub_key: String,
        serial: Option<i64>,
    ) -> Result<i64, ArunaError> {
        use crate::database::schema::pub_keys::dsl::*;
        use diesel::result::Error as dError;
        let result = self
            .pg_connection
            .get()?
            .transaction::<i64, dError, _>(|conn| {
                let pkey = pub_keys
                    .filter(pubkey.eq(pub_key.clone()))
                    .first::<PubKey>(conn)
                    .optional()?;
                if let Some(pkey_noption) = pkey {
                    Ok(pkey_noption.id)
                } else {
                    let new_pkey = PubKeyInsert {
                        pubkey: pub_key,
                        id: serial,
                    };

                    Ok(insert_into(pub_keys)
                        .values(&new_pkey)
                        .returning(id)
                        .get_result::<i64>(conn)?)
                }
            })?;
        Ok(result)
    }

    pub fn get_oidc_user(
        &self,
        oidc_id: &str,
    ) -> Result<Option<diesel_ulid::DieselUlid>, ArunaError> {
        use crate::database::schema::users::dsl::*;
        use diesel::result::Error;

        let result = self
            .pg_connection
            .get()?
            .transaction::<_, Error, _>(|conn| {
                users
                    .filter(external_id.eq(oidc_id))
                    .first::<User>(conn)
                    .optional()
            })?;

        match result {
            Some(u) => {
                if !u.active {
                    Err(ArunaError::AuthorizationError(
                        AuthorizationError::NOTACTIVATED,
                    ))
                } else {
                    Ok(Some(u.id))
                }
            }
            None => Ok(None),
        }
    }
}
