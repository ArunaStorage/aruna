use diesel_ulid::DieselUlid;

use crate::structs::{AccessKeyPermissions, Object, ObjectLocation, PubKey, User};

use super::persistence::{GenericBytes, Table, WithGenericBytes};

impl TryFrom<GenericBytes<i16>> for PubKey {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<i16>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?)
    }
}

impl TryInto<GenericBytes<i16>> for PubKey {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<i16>, Self::Error> {
        let data = bincode::serialize(&self).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(GenericBytes {
            id: self.id,
            data,
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<i16> for PubKey {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::PubKeys
    }
}

impl TryFrom<GenericBytes<DieselUlid>> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<DieselUlid>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?)
    }
}

impl TryInto<GenericBytes<DieselUlid>> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<DieselUlid>, Self::Error> {
        let data = bincode::serialize(&self).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(GenericBytes {
            id: self.id,
            data,
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<DieselUlid> for Object {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::Objects
    }
}

impl TryFrom<GenericBytes<DieselUlid>> for ObjectLocation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<DieselUlid>) -> Result<Self, Self::Error> {
        Ok(bincode::deserialize(&value.data).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?)
    }
}

impl TryInto<GenericBytes<DieselUlid>> for ObjectLocation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<DieselUlid>, Self::Error> {
        let data = bincode::serialize(&self).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(GenericBytes {
            id: self.id,
            data,
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<DieselUlid> for ObjectLocation {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::ObjectLocations
    }
}

impl WithGenericBytes<DieselUlid> for User {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::Users
    }
}

impl TryFrom<GenericBytes<DieselUlid>> for User {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<DieselUlid>) -> Result<Self, Self::Error> {
        let user: User = bincode::deserialize(&value.data).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(user)
    }
}

impl TryInto<GenericBytes<DieselUlid>> for User {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<DieselUlid>, Self::Error> {
        let user = self;
        let data = bincode::serialize(&user).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(GenericBytes {
            id: user.user_id,
            data,
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<String> for AccessKeyPermissions {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::Permissions
    }
}

impl TryFrom<GenericBytes<String>> for AccessKeyPermissions {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<String>) -> Result<Self, Self::Error> {
        let access_key_perm: AccessKeyPermissions =
            bincode::deserialize(&value.data).map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
        Ok(access_key_perm)
    }
}

impl TryInto<GenericBytes<String>> for AccessKeyPermissions {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<String>, Self::Error> {
        let access_key_perm = self;
        let data = bincode::serialize(&access_key_perm).map_err(|e| {
            tracing::error!(error = ?e, msg = e.to_string());
            e
        })?;
        Ok(GenericBytes {
            id: access_key_perm.access_key,
            data,
            table: Self::get_table(),
        })
    }
}
