use crate::database::connection::Database;
use crate::database::models::auth::{ApiToken, PubKey, UserPermission};
use crate::database::models::enums::{Resources, UserRights};
use crate::error::ArunaError;
use crate::server::services::authz::Context;
use diesel::{prelude::*, sql_query, sql_types::Uuid};

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

    /// This method checks if the user has the correct permissions
    /// It will only return an uuid if the permissions are granted
    ///
    /// ## Arguments
    ///  
    /// - `context_token` -> The token string from the request metadata
    /// - `requested_ctx` -> The context that is requested (must be either PROJECT or COLLECTION)
    ///
    /// ## Result:
    ///
    /// - `Result<uuid::Uuid, Error>` -> This will either return the user uuid or error
    ///
    /// ## Behaviour
    ///
    /// The following section describes what happens in all different variants of the following cases.
    ///
    /// The ApiToken (api_token) can have three different "scopes":
    /// - Project           (proj)
    /// - Collection        (coll)
    /// - Personal          (pers)
    ///
    /// The Requested Context (req_ctx) can also have three different scopes:
    /// - Admin             (admi)
    /// - Project           (proj)
    /// - Collection        (coll)
    ///
    /// Yielding the following possible combinations:
    ///
    /// 1.  req_ctx == admi
    ///     -> check if token is personal and user is part of an admin project and return
    ///        no additional permission checks are necessary because global admins have no specific user_permissions (for now)
    /// 2.  req_ctx == coll && api_token == coll
    ///     -> check if both uuid are the same and check if the api_token permission is greater or equal the
    ///        requested permission
    /// 3.  req_ctx == coll && api_token == proj
    ///     -> check if context_collection is in project and and check if the api_token permission is greater or
    ///        equal the requested permissio;n
    /// 4.  req_ctx == proj && api_token == proj
    ///     -> check if context_project equals the apitoken project and the api_token permission is greater or  
    ///        equal the requested permission
    ///
    /// (req_ctx == coll && api_token == proj does not exist because the req_ctx must always be greater than the api_token scope)
    ///
    /// These cases all require the api_token to be "scoped" to a specific context. The next cases occur when
    /// the token is a personal token of a specific user.
    ///
    /// 5.  req_ctx == coll && api_token == pers
    ///     -> check for associated project and validate if the user has enough permissions
    /// 6.  req_ctx == proj && api_token == pers
    ///     -> check if the user has a user_permission for this specific project and if this permission is >= the req_ctx permission
    ///
    pub fn get_checked_user_id_from_token(
        &self,
        ctx_token: uuid::Uuid,
        req_ctx: Context,
    ) -> Result<uuid::Uuid, ArunaError> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::collections::dsl::*;
        //use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error as dError;

        let creator_uid =
            self.pg_connection
                .get()?
                .transaction::<Option<uuid::Uuid>, dError, _>(|conn| {
                    // Get the API token, if this errors -> no corresponding database token object could be found
                    let api_token = api_tokens
                        .filter(crate::database::schema::api_tokens::id.eq(ctx_token))
                        .first::<ApiToken>(conn)?;

                    // Case 1:
                    // This is checked first because all other checks can be omitted if this succeeds
                    // Check if Token is "personal" and if the request is admin scoped
                    // Check if the user has admin permissions and return

                    if api_token.collection_id.is_none()
                        && api_token.project_id.is_none()
                        && req_ctx.admin
                    {
                        let admin_user_perm = sql_query(
                            "SELECT uperm.id, uperm.user_id, uperm.user_right, uperm.project_id 
                               FROM user_permissions AS uperm 
                               JOIN projects AS p 
                               ON p.id = uperm.project_id 
                               WHERE uperm.user_id == ?
                               AND p.flag & 1 == 1
                               LIMIT 1",
                        )
                        .bind::<Uuid, _>(api_token.creator_user_id)
                        .get_result::<UserPermission>(conn)
                        .optional()?;

                        // If an associated admin_user_perm is found, this can return a new context
                        // for the admin scope
                        if admin_user_perm.is_some() {
                            return Ok(Some(api_token.creator_user_id));
                        }
                    }

                    // If the requested context / scope is of type COLLECTION
                    if req_ctx.resource_type == Resources::COLLECTION {
                        // Case 2:
                        // If api_token.collection_id == context_collection_id
                        // And user_right != None && api_token.collection_id != None && context_collection_id != None
                        // This will return Some(Context) otherwise this will return None
                        if api_token.collection_id.is_some() {
                            let collection_ctx = option_uuid_helper(
                                api_token.collection_id,
                                Some(req_ctx.resource_id),
                                Resources::COLLECTION,
                                req_ctx.user_right,
                                api_token.user_right,
                            );

                            // If apitoken.collection_id == context_collection_id
                            // We can return early here -> The ApiToken is "scoped" to this specific collection
                            // in case the response is None -> just continue
                            match collection_ctx {
                                Some(_) => return Ok(Some(api_token.creator_user_id)),
                                _ => (),
                            }
                        }

                        // Case 3:
                        // When the request is a collection_id that does not directly match
                        // apitoken.collection_id or apitoken.project_id but api_token is project scoped
                        // It might be possible that the collection is part of the "scoped" project
                        // This checks if the collection is part of the "scoped" project
                        // and returns early
                        if api_token.project_id.is_some() {
                            let is_collection_in_project = collections
                                .filter(
                                    crate::database::schema::collections::dsl::id
                                        .eq(req_ctx.resource_id),
                                )
                                .filter(
                                    crate::database::schema::collections::dsl::project_id
                                        .eq(api_token.project_id.unwrap_or_default()),
                                )
                                .select(crate::database::schema::collections::dsl::id)
                                .first::<uuid::Uuid>(conn)
                                .optional()?;

                            let col_in_proj_context = option_uuid_helper(
                                Some(req_ctx.resource_id),
                                is_collection_in_project,
                                Resources::COLLECTION,
                                req_ctx.user_right,
                                api_token.user_right,
                            );

                            match col_in_proj_context {
                                Some(_) => return Ok(Some(api_token.creator_user_id)),
                                _ => (),
                            }
                        }

                        // Case 5:
                        // This is the case when the request is Collection scoped but the ApiToken is "personal"
                        // -> no collection_id or project_id is specified
                        // in this case it needs to be checked if the user_permission for the collections project exists
                        if api_token.collection_id.is_none() && api_token.project_id.is_none() {
                            // SELECT * from userpermissions INNER JOIN collections on project_id;
                            let user_permission_option: Option<UserPermission> = user_permissions
                                .inner_join(collections.on(
                                    crate::database::schema::collections::dsl::project_id.eq(
                                        crate::database::schema::user_permissions::dsl::project_id,
                                    ),
                                ))
                                .filter(
                                    crate::database::schema::collections::dsl::id
                                        .eq(req_ctx.resource_id),
                                )
                                .select(UserPermission::as_select())
                                .first::<UserPermission>(conn)
                                .optional()?;

                            if user_permission_option.is_some() {
                                let col_in_proj_ctx2 = option_uuid_helper(
                                    Some(req_ctx.resource_id),
                                    Some(req_ctx.resource_id),
                                    Resources::COLLECTION,
                                    req_ctx.user_right,
                                    Some(user_permission_option.unwrap().user_right), // This unwrap is ok safe because project_valid.is_some()
                                );

                                match col_in_proj_ctx2 {
                                    Some(_) => return Ok(Some(api_token.creator_user_id)),
                                    _ => (),
                                }
                            }
                        }
                    }

                    if req_ctx.resource_type == Resources::PROJECT {
                        // Case 4:
                        // If api_token.project_id == context_project_id
                        // And user_right != None && api_token.project_id != None && context_project_id != None
                        // This will return Some(Context) otherwise this will return None
                        if api_token.project_id.is_some() {
                            let project_ctx = option_uuid_helper(
                                api_token.project_id,
                                Some(req_ctx.resource_id),
                                Resources::PROJECT,
                                req_ctx.user_right,
                                api_token.user_right,
                            );

                            // If apitoken.collection_id == context_collection_id
                            // We can return early here -> The ApiToken is "scoped" to this specific collection
                            match project_ctx {
                                Some(_) => return Ok(Some(api_token.creator_user_id)),
                                _ => (),
                            }
                        }

                        // Case 6:
                        // If context is user_scoped check if the user has the correct project permissions
                        // This checks for the permissions in the user_permissions table which already contains a project_id
                        if api_token.project_id.is_none() && api_token.collection_id.is_none() {
                            let user_permissions_option = user_permissions
                                .filter(user_id.eq(api_token.creator_user_id))
                                .filter(
                                    crate::database::schema::user_permissions::dsl::project_id
                                        .eq(req_ctx.resource_id),
                                )
                                .first::<UserPermission>(conn)
                                .optional()?;
                            if user_permissions_option.is_some() {
                                let col_in_proj_ctx = option_uuid_helper(
                                    Some(user_permissions_option.as_ref().unwrap().project_id), // This unwrap is ok safe because project_valid.is_some()
                                    Some(req_ctx.resource_id),
                                    Resources::PROJECT,
                                    req_ctx.user_right,
                                    Some(user_permissions_option.as_ref().unwrap().user_right), // This unwrap is ok safe because project_valid.is_some()
                                );

                                match col_in_proj_ctx {
                                    Some(_) => return Ok(Some(api_token.creator_user_id)),
                                    _ => (),
                                }
                            }
                        }
                    }

                    Ok(None)
                })?;

        match creator_uid {
            Some(uid) => Ok(uid),
            None => Err(ArunaError::PERMISSIONDENIED),
        }
    }
}

/// This function is a helper method to automatically bubble up options for ids and userrights
/// and to compare if the users actual rights are sufficient for the request
///
/// ## Arguments
///
/// - id1: Optional uuid -> Uuid of the requested ressource
/// - id2: Optional uuid -> Uuid of the resource from the db response
/// - res_type: Resources -> The Type of the resource needed to build the result context
/// - req_user_right -> The requested right
/// - actual_user_right -> The actual right returned by the DB
///
/// ## Results
///
/// This will return an Option<Context> if the request succeeds
/// otherwise this will return None indicating a None variant for the inputs
/// or a failed check.
///  
fn option_uuid_helper(
    id1: Option<uuid::Uuid>,
    id2: Option<uuid::Uuid>,
    res_type: Resources,
    req_user_right: UserRights,
    actual_user_right: Option<UserRights>,
) -> Option<Context> {
    let id1_value = id1?;
    let id2_value = id2?;

    // If ids are the same and the actual_user_rights are gr/eq the requested rights
    // Return a context
    if id1_value == id2_value && req_user_right <= actual_user_right? {
        return Some(Context {
            user_right: actual_user_right?,
            resource_type: res_type,
            resource_id: id1_value,
            admin: false,
            oidc_context: false,
        });
    }
    // Otherwise return None
    None
}

#[cfg(test)]
mod tests {

    use super::*;
    #[test]
    fn option_uuid_helper_test() {
        let uuid_a = Some(uuid::Uuid::new_v4());
        let uuid_b = Some(uuid::Uuid::new_v4());

        // This should return none because both uuids are different
        assert!(option_uuid_helper(
            uuid_a,
            uuid_b,
            Resources::PROJECT, // Does not matter
            UserRights::READ,
            Some(UserRights::WRITE),
        )
        .is_none());

        // This should return some because both uuids are the same
        // And WRITE permissions are greater equal READ permissions
        assert!(option_uuid_helper(
            uuid_a,
            uuid_a,
            Resources::PROJECT, // Does not matter
            UserRights::READ,
            Some(UserRights::WRITE),
        )
        .is_some());

        // This should return none because the requested WRITE permissions
        // are not met by the actual READ permissions
        assert!(option_uuid_helper(
            uuid_a,
            uuid_a,
            Resources::PROJECT, // Does not matter
            UserRights::WRITE,
            Some(UserRights::READ),
        )
        .is_none());

        // Equal permissions should return a value
        assert!(option_uuid_helper(
            uuid_a,
            uuid_a,
            Resources::PROJECT, // Does not matter
            UserRights::WRITE,
            Some(UserRights::WRITE),
        )
        .is_some());

        // This should return none because one of both uuids is None
        assert!(option_uuid_helper(
            None,
            uuid_a,
            Resources::PROJECT, // Does not matter
            UserRights::WRITE,
            Some(UserRights::READ),
        )
        .is_none());

        // This should return None because of a missing comparable user_right
        assert!(option_uuid_helper(
            uuid_a,
            uuid_a,
            Resources::PROJECT, // Does not matter
            UserRights::WRITE,
            None,
        )
        .is_none());
    }
}
