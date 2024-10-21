use crate::{
    error::ArunaError,
    logerr,
    models::{
        AddGroupRequest, CreateGroupRequest, CreateProjectRequest, CreateRealmRequest,
        CreateResourceRequest, GetGroupRequest, GetRealmRequest, GetResourceRequest, Permission,
    },
    requests::controller::Get,
};
use ulid::Ulid;

pub enum Context {
    Public,
    UserOnly,
    GlobalAdmin,
    Permission {
        min_permission: Permission,
        // Source for finding a path to the user
        source: Ulid,
        // target can only be known after the token is serialized in get_token()
    },
    Permissions {
        // Source for finding a path to the user
        first_source: Ulid,
        first_min_permission: Permission,
        // For modify relations two paths to the same target
        // need to be checked
        second_min_permission: Permission,
        second_source: Ulid,
    },
}

#[async_trait::async_trait]
pub trait GetContext: Send + Sync {
    async fn get_context<G: Get + Send + Sync>(
        &self,
        controller: &G,
    ) -> Result<Context, ArunaError>;
}

#[async_trait::async_trait]
impl GetContext for GetResourceRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        controller: &G,
    ) -> Result<Context, ArunaError> {
        let res = controller
            .get(self.id)
            .await?
            .ok_or_else(|| ArunaError::NotFound(self.id.to_string()))
            .inspect_err(logerr!())?;
        match res {
            crate::models::NodeVariantValue::Resource(res) => match res.visibility {
                crate::models::VisibilityClass::Public => return Ok(Context::Public),
                _ => Ok(Context::Permission {
                    min_permission: Permission::Read,
                    source: self.id,
                }),
            },
            _ => return Err(ArunaError::NotFound(self.id.to_string())),
        }
    }
}

#[async_trait::async_trait]
impl GetContext for CreateResourceRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::Permission {
            min_permission: Permission::Append,
            source: self.parent_id,
        })
    }
}

#[async_trait::async_trait]
impl GetContext for CreateProjectRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::Permission {
            min_permission: Permission::Append,
            source: self.group_id,
        })
    }
}

#[async_trait::async_trait]
impl GetContext for AddGroupRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::Permission {
            min_permission: Permission::Admin,
            source: self.realm_id,
        })
    }
}

#[async_trait::async_trait]
impl GetContext for CreateRealmRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::UserOnly)
    }
}

#[async_trait::async_trait]
impl GetContext for CreateGroupRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::UserOnly)
    }
}

#[async_trait::async_trait]
impl GetContext for GetRealmRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::Public)
    }
}

#[async_trait::async_trait]
impl GetContext for GetGroupRequest {
    async fn get_context<G: Get + Sync + Send>(
        &self,
        _controller: &G,
    ) -> Result<Context, ArunaError> {
        Ok(Context::Permission {
            min_permission: Permission::Read,
            source: self.id,
        })
    }
}
