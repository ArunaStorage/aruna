use std::io::ErrorKind;

use crate::{
    database::{
        connection::Database,
        models::{
            auth::{ApiToken, UserPermission},
            enums::{Resources, UserRights},
        },
    },
    server::services::authz::Context,
};
use diesel::{prelude::*, sql_query, sql_types::Uuid};

impl Database {
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
    /// These cases descripe what happens in all different variants of the following cases.
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
    ///
    /// 0.  req_ctx == admi
    ///     -> check if token is personal and user is part of an admin project and return
    ///        no additional permission checks are necessary because global admins have no specific user_permissions (for now)
    /// 1.  req_ctx == coll && api_token == coll
    ///     -> check if both uuid are the same and check if the api_token permission is greater or equal the
    ///        requested permission
    /// 2.  req_ctx == coll && api_token == proj
    ///     -> check if context_collection is in project and and check if the api_token permission is greater or
    ///        equal the requested permission
    /// 3.  req_ctx == proj && api_token == proj
    ///     -> check if context_project equals the apitoken project and the api_token permission is greater or  
    ///        equal the requested permission
    ///
    /// (req_ctx == coll && api_token == proj does not exist because the req_ctx must always be greater than the api_token)
    ///
    /// These cases all require the api_token to be "scoped" to a specific context. The next cases occur when
    /// the token is a personal token of a specific user.
    ///
    /// 4   req_ctx == proj && api_token == pers
    ///     -> check if the user has a user_permission for this specific project and if this permission is >= the req_ctx permission
    /// 5.  req_ctx == coll && api_token == pers
    ///     -> check for associated project and validate if the user has enough permissions

    pub fn get_user_right_from_token(
        &self,
        ctx_token: &str,
        req_ctx: Context,
    ) -> Result<uuid::Uuid, Box<dyn std::error::Error>> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::collections::dsl::*;
        //use crate::database::schema::projects::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error;

        let (ctx, creator_uid) = self
            .pg_connection
            .get()?
            .transaction::<(Option<Context>, uuid::Uuid), Error, _>(|conn| {
                // Get the API token
                let api_token = api_tokens
                    .filter(token.eq(ctx_token))
                    .first::<ApiToken>(conn)?;

                // Case 4:
                // This is checked first because all other checks can be omitted if this succeeds
                // Check if Token is "personal" and if the request is admin scoped
                // Check if the user has admin permissions and return

                if api_token.collection_id.is_none()
                    && api_token.project_id.is_none()
                    && requested_ctx.admin
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

                    if admin_user_perm.is_some() {
                        return Ok((
                            Some(Context {
                                user_right: admin_user_perm.as_ref().unwrap().user_right,
                                resource_type: Resources::PROJECT,
                                resource_id: admin_user_perm.as_ref().unwrap().project_id,
                                admin: true,
                            }),
                            api_token.creator_user_id,
                        ));
                    }
                }

                // If the requested context / scope is of type COLLECTION
                if requested_ctx.resource_type == Resources::COLLECTION {
                    // Case 1:
                    // If api_token.collection_id == context_collection_id
                    // And user_right != None && api_token.collection_id != None && context_collection_id != None
                    // This will return Some(Context) otherwise this will return None
                    if api_token.collection_id.is_some() {
                        let collection_ctx = option_uuid_helper(
                            api_token.collection_id,
                            Some(requested_ctx.resource_id),
                            Resources::COLLECTION,
                            api_token.user_right,
                        );

                        // If apitoken.collection_id == context_collection_id
                        // We can return early here -> The ApiToken is "scoped" to this specific collection
                        match collection_ctx {
                            Some(_) => return Ok((collection_ctx, api_token.creator_user_id)),
                            _ => (),
                        }
                    }

                    // Case 2.1:
                    // When the request is a collection_id that does not directly match
                    // apitoken.collection_id or apitoken.project_id but api_token is project scoped
                    // It might be possible that the collection is part of the "scoped" project
                    // This checks if the collection is part of the "scoped" project
                    // and returns early
                    if api_token.project_id.is_some() {
                        let is_collection_in_project = collections
                            .filter(
                                crate::database::schema::collections::dsl::id
                                    .eq(requested_ctx.resource_id),
                            )
                            .filter(
                                crate::database::schema::collections::dsl::project_id
                                    .eq(api_token.project_id.unwrap_or_default()),
                            )
                            .select(crate::database::schema::collections::dsl::id)
                            .first::<uuid::Uuid>(conn)
                            .optional()?;

                        let col_in_proj_context = option_uuid_helper(
                            Some(requested_ctx.resource_id),
                            is_collection_in_project,
                            Resources::COLLECTION,
                            api_token.user_right,
                        );

                        match col_in_proj_context {
                            Some(_) => return Ok((col_in_proj_context, api_token.creator_user_id)),
                            _ => (),
                        }
                    }
                }

                if requested_ctx.resource_type == Resources::PROJECT {
                    // Case 2:
                    // If api_token.project_id == context_project_id
                    // And user_right != None && api_token.project_id != None && context_project_id != None
                    // This will return Some(Context) otherwise this will return None
                    if api_token.project_id.is_some() {
                        let project_ctx = option_uuid_helper(
                            api_token.project_id,
                            Some(requested_ctx.resource_id),
                            Resources::PROJECT,
                            api_token.user_right,
                        );

                        // If apitoken.collection_id == context_collection_id
                        // We can return early here -> The ApiToken is "scoped" to this specific collection
                        match project_ctx {
                            Some(_) => return Ok((project_ctx, api_token.creator_user_id)),
                            _ => (),
                        }
                    }
                }

                // Case 3.1:
                // If context is user_scoped check if the user has the correct project permissions
                // This checks for the permissions in the user_permissions table which already contains a project_id
                if context_project_id.is_some() {
                    let project_valid = user_permissions
                        .filter(user_id.eq(api_token.creator_user_id))
                        .filter(
                            crate::database::schema::user_permissions::dsl::project_id
                                .eq(context_project_id.unwrap_or_default()),
                        )
                        .first::<UserPermission>(conn)
                        .optional()?;
                    if project_valid.is_some() {
                        let col_in_proj_ctx = option_uuid_helper(
                            context_project_id,
                            Some(project_valid.as_ref().unwrap().project_id), // This unwrap is ok safe because project_valid.is_some()
                            Resources::PROJECT,
                            Some(project_valid.as_ref().unwrap().user_right), // This unwrap is ok safe because project_valid.is_some()
                        );

                        match col_in_proj_ctx {
                            Some(_) => return Ok((col_in_proj_ctx, api_token.creator_user_id)),
                            _ => (),
                        }
                    }
                }

                // Case 3.2
                // If the request is collection scoped and the token is a personal user_token
                // it needs to be checked if the user has permissions in the collection associated project
                if context_collection_id.is_some() {
                    let collection_valid: Option<UserPermission> = user_permissions
                        .inner_join(
                            collections.on(crate::database::schema::collections::dsl::project_id
                                .eq(crate::database::schema::user_permissions::dsl::project_id)),
                        )
                        .filter(
                            crate::database::schema::collections::dsl::id
                                .eq(context_collection_id.unwrap_or_default()),
                        )
                        .select(UserPermission::as_select())
                        .first::<UserPermission>(conn)
                        .optional()?;

                    if collection_valid.is_some() {
                        let col_in_proj_ctx2 = option_uuid_helper(
                            context_collection_id,
                            context_collection_id,
                            Resources::COLLECTION,
                            Some(collection_valid.unwrap().user_right), // This unwrap is ok safe because project_valid.is_some()
                        );

                        match col_in_proj_ctx2 {
                            Some(_) => return Ok((col_in_proj_ctx2, api_token.creator_user_id)),
                            _ => (),
                        }
                    }
                }

                Ok((None, api_token.creator_user_id))
            })?;

        // Convert the optional Context to an error if Option == None
        let ctx = ctx.ok_or_else(|| std::io::Error::new(ErrorKind::Other, "Unauthorized"))?;

        return Ok(creator_uid);
    }
}

fn option_uuid_helper(
    id1: Option<uuid::Uuid>,
    id2: Option<uuid::Uuid>,
    res_type: Resources,
    user_right: Option<UserRights>,
) -> Option<Context> {
    let id1_value = id1?;
    let id2_value = id2?;

    if id1_value == id2_value {
        return Some(Context {
            user_right: user_right?,
            resource_type: res_type,
            resource_id: id1_value,
            admin: false,
        });
    }
    None
}
