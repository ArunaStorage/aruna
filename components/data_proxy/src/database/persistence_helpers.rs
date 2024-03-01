use diesel_ulid::DieselUlid;
use postgres_types::Json;

use crate::structs::{AccessKeyPermissions, Object, ObjectLocation, PubKey, User};

use super::persistence::{GenericBytes, Table, WithGenericBytes};

impl TryFrom<GenericBytes<i16, PubKey>> for PubKey {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<i16, PubKey>) -> Result<Self, Self::Error> {
        Ok(value.data.0)
    }
}

impl TryInto<GenericBytes<i16, PubKey>> for PubKey {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<i16, PubKey>, Self::Error> {
        Ok(GenericBytes {
            id: self.id,
            data: Json(self),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<i16, PubKey> for PubKey {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::PubKeys
    }
}

impl TryFrom<GenericBytes<DieselUlid, Self>> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<DieselUlid, Self>) -> Result<Self, Self::Error> {
        Ok(value.data.0)
    }
}

impl TryInto<GenericBytes<DieselUlid, Self>> for Object {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<DieselUlid, Self>, Self::Error> {
        Ok(GenericBytes {
            id: self.id,
            data: Json(self),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<DieselUlid, Self> for Object {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::Objects
    }
}

impl TryFrom<GenericBytes<DieselUlid, Self>> for ObjectLocation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<DieselUlid, Self>) -> Result<Self, Self::Error> {
        Ok(value.data.0)
    }
}

impl TryInto<GenericBytes<DieselUlid, Self>> for ObjectLocation {
    type Error = anyhow::Error;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<DieselUlid, Self>, Self::Error> {
        Ok(GenericBytes {
            id: self.id,
            data: Json(self),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<DieselUlid, Self> for ObjectLocation {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::ObjectLocations
    }
}

impl WithGenericBytes<DieselUlid, Self> for User {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::Users
    }
}

impl TryFrom<GenericBytes<DieselUlid, Self>> for User {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<DieselUlid, Self>) -> Result<Self, Self::Error> {
        Ok(value.data.0)
    }
}

impl TryInto<GenericBytes<DieselUlid, Self>> for User {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<DieselUlid, Self>, Self::Error> {
        let user = self;

        Ok(GenericBytes {
            id: user.user_id,
            data: Json(user),
            table: Self::get_table(),
        })
    }
}

impl WithGenericBytes<String, Self> for AccessKeyPermissions {
    #[tracing::instrument(level = "trace", skip())]
    fn get_table() -> Table {
        Table::Permissions
    }
}

impl TryFrom<GenericBytes<String, Self>> for AccessKeyPermissions {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: GenericBytes<String, Self>) -> Result<Self, Self::Error> {
        Ok(value.data.0)
    }
}

impl TryInto<GenericBytes<String, Self>> for AccessKeyPermissions {
    type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
    #[tracing::instrument(level = "trace", skip(self))]
    fn try_into(self) -> Result<GenericBytes<String, Self>, Self::Error> {
        Ok(GenericBytes {
            id: self.access_key.to_string(),
            data: Json(self),
            table: Self::get_table(),
        })
    }
}
