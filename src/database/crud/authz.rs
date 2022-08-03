use crate::{
    database::{
        connection::Database,
        models::{
            self,
            auth::{ApiToken, UserPermission},
            enums::{Resources, UserRights},
        },
    },
    server::services::authz::Context,
};
use diesel::prelude::*;

impl Database {
    /// This method gets the user_right context based on the provided APIToken
    /// It will only return context if one of the following (in order) checks succeed:
    ///
    /// 1.  The ApiToken is explicitly "scoped" to the requested context_collection_id
    ///     This means the collection_id specified in the ApiToken is the same as in the requested api_call
    ///     Returning a context with user_rights equivalent to ApiToken userright
    /// 2.  The ApiToken is explicitly "scoped" to the requested context_project_id
    ///     The procedure here is equivalent to 1
    /// 2.1 A special case occurs when the requested context_id is a collection_id but the token is "project scoped"
    ///     For this it needs to be checked if the collection_id is part of the project and then the user_right is returned
    /// 3.  When no project_id or collection_id is present in the token, the token is "personal"
    ///     Personal tokens inherit all permissions by the user specified in `creator_user_id`
    ///     To check the validity first the `user_permissions` need to be queried from the db
    ///     When the requested context is a project and there is an entry which matches `user_id` and `project_id`,
    ///     the userpermission is returned otherwise if a context_collection_id is requested, first the collection
    ///     project_id is determined and the check is executed based on this project_id

    pub fn get_user_right_from_token(
        &self,
        token: String,
        context_collection_id: Option<uuid::Uuid>,
        context_project_id: Option<uuid::Uuid>,
    ) -> Result<Context, Box<dyn std::error::Error>> {
        use crate::database::schema::api_tokens::dsl::*;
        use crate::database::schema::collections::dsl::*;
        use crate::database::schema::user_permissions::dsl::*;
        use diesel::result::Error;

        let result = self
            .pg_connection
            .get()?
            .transaction::<Context, Error, _>(|conn| {
                // Get the API token
                let api_token = api_tokens.filter(token.eq(token)).first::<ApiToken>(conn)?;

                // If api_token.collection_id == context_collection_id
                // And user_right != None && api_token.collection_id != None && context_collection_id != None
                // This will return Some(Context) otherwise this will return None
                let collection_ctx = option_uuid_helper(
                    api_token.collection_id,
                    context_collection_id,
                    Resources::COLLECTION,
                    api_token.user_right,
                );

                // If apitoken.collection_id == context_collection_id
                // We can return early here -> The ApiToken is "scoped" to this specific collection
                match collection_ctx {
                    Some(ctx) => return Ok(ctx),
                    _ => (),
                }

                // If api_token.project_id == context_project_id
                // And user_right != None && api_token.project_id != None && context_project_id != None
                // This will return Some(Context) otherwise this will return None
                let project_ctx = option_uuid_helper(
                    api_token.project_id,
                    context_project_id,
                    Resources::PROJECT,
                    api_token.user_right,
                );

                // If apitoken.collection_id == context_collection_id
                // We can return early here -> The ApiToken is "scoped" to this specific collection
                match project_ctx {
                    Some(ctx) => return Ok(ctx),
                    _ => (),
                }

                // When the request is a collection_id that does not directly match
                // apitoken.collection_id or apitoken.project_id but api_token is project scoped
                // It might be possible that the collection is part of the "scoped" project
                if context_collection_id.is_some() && api_token.project_id.is_some() {
                    let is_collection_in_project = collections
                        .select(crate::database::schema::collections::dsl::project_id)
                        .filter(
                            crate::database::schema::collections::dsl::id
                                .eq(context_collection_id.unwrap_or_default()),
                        )
                        .filter(
                            crate::database::schema::collections::dsl::project_id
                                .eq(api_token.project_id.unwrap_or_default()),
                        )
                        .first::<uuid::Uuid>(conn)
                        .optional()?;
                    if is_collection_in_project.is_some() {
                        return Ok(Context {
                            user_right: todo!(),
                            resource_type: todo!(),
                            resource_id: todo!(),
                        });
                    }
                }

                // If context_user
                let userperm;
                if api_token.project_id.is_none() || api_token.collection_id.is_none() {
                    userperm = user_permissions
                        .filter(user_id.eq(api_token.creator_user_id))
                        .load::<UserPermission>(conn)
                        .optional()?;
                }
                Ok((api_token, userperm))
            })?;

        Ok(result)
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
        });
    }
    None
}
