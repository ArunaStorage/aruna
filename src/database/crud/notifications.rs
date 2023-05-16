use crate::{
    database::{
        connection::Database,
        models::{enums::Resources, notifications::NotificationStreamGroup},
    },
    error::ArunaError,
};

use crate::database::schema::notification_stream_groups::dsl::notification_stream_groups;
use crate::database::schema::object_groups::dsl::object_groups;
use crate::database::schema::objects::dsl::objects;

use diesel::{delete, insert_into, prelude::*};

impl Database {
    /// Inserts a new notification stream group in the database.
    ///
    /// ## Arguments:
    ///
    /// * `stream_group_ulid` - A unique ULID for the new notification stream group.
    /// * `resource_id` - The unique id of the resource associated with the notification stream group
    /// * `resource_type` - The resource type of the associated resource
    /// * `include_sub_resources` - Flag to include subresources in the notifications
    ///
    /// ## Returns:
    ///
    /// * `Result<NotificationStreamGroup, ArunaError>` -
    /// The db model notification stream group which was inserted in the database; Error else.
    ///
    pub fn create_notification_stream_group(
        &self,
        stream_group_ulid: diesel_ulid::DieselUlid,
        resource_id: diesel_ulid::DieselUlid,
        resource_type: Resources,
        include_sub_resources: bool,
    ) -> Result<NotificationStreamGroup, ArunaError> {
        // Insert NotificationStreamGroup in database
        let stream_group = self
            .pg_connection
            .get()?
            .transaction::<NotificationStreamGroup, ArunaError, _>(|conn| {
                // Prepare NotificationStreamGroup for insert
                let stream_group_insert = NotificationStreamGroup {
                    id: stream_group_ulid,
                    subject: "".to_string(), //ToDo: waiting for API change
                    /*
                    generate_consumer_subject(
                        conn,
                        resource_type,
                        resource_hierarchy, // ToDo ...
                        include_sub_resources,
                    )?,
                    */
                    resource_id: resource_id,
                    resource_type: resource_type,
                    notify_on_sub_resources: include_sub_resources,
                };

                insert_into(notification_stream_groups)
                    .values(&stream_group_insert)
                    .execute(conn)?;

                Ok(stream_group_insert)
            })?;

        Ok(stream_group)
    }

    /// Fetches the notification stream associated with the provided
    /// notification stream group id from the database.
    ///
    /// ## Arguments:
    ///
    /// * `stream_group_ulid` - Unique ULID of the notification stream group
    ///
    /// ## Returns:
    ///
    /// * `Result<NotificationStreamGroup, ArunaError>` -
    /// The db model notification stream group; Error else.
    ///
    pub fn get_notification_stream_group(
        &self,
        stream_group_ulid: diesel_ulid::DieselUlid,
    ) -> Result<NotificationStreamGroup, ArunaError> {
        use crate::database::schema::notification_stream_groups::dsl as stream_group_dsl;

        // Fetch NotificationStreamGroup from
        let db_stream_group = self
            .pg_connection
            .get()?
            .transaction::<NotificationStreamGroup, ArunaError, _>(|conn| {
                Ok(notification_stream_groups
                    .filter(stream_group_dsl::id.eq(&stream_group_ulid))
                    .first::<NotificationStreamGroup>(conn)?)
            })?;

        Ok(db_stream_group)
    }

    /// Deletes the notification stream associated with the provided
    /// notification stream group id from the database.
    ///
    /// ## Arguments:
    ///
    /// * `stream_group_ulid` - Unique ULID of the notification stream group
    ///
    /// ## Returns:
    ///
    /// * `Result<(), ArunaError` -
    /// An empty Ok result signals a successful deletion ; Error else.
    ///
    pub fn delete_notification_stream_group(
        &self,
        stream_group_ulid: diesel_ulid::DieselUlid,
    ) -> Result<(), ArunaError> {
        use crate::database::schema::notification_stream_groups::dsl as stream_group_dsl;

        // Fetch NotificationStreamGroup from
        self.pg_connection
            .get()?
            .transaction::<_, ArunaError, _>(|conn| {
                delete(notification_stream_groups)
                    .filter(stream_group_dsl::id.eq(&stream_group_ulid))
                    .execute(conn)?;

                Ok(())
            })?;

        Ok(())
    }

    /// Fetches the shared revision id of the resource associated with the provided
    /// resource id from the database.
    ///
    /// ## Arguments:
    ///
    /// * `resource_id` - The unique ULID of the resource
    /// * `resource_type` - The resource type (Only object and object groups have shared revision ids)
    ///  
    /// ## Returns:
    ///
    /// * `Result<diesel_ulid::DieselUlid, ArunaError>` -
    /// The shared revision id of the associated with the provided resource id; Error else.
    pub fn get_resource_shared_revision(
        &self,
        resource_id: diesel_ulid::DieselUlid,
        resource_type: Resources,
    ) -> Result<diesel_ulid::DieselUlid, ArunaError> {
        use crate::database::schema::object_groups::dsl as object_groups_dsl;
        use crate::database::schema::objects::dsl as objects_dsl;

        // Fetch NotificationStreamGroup from
        let shared_revision_ulid = self
            .pg_connection
            .get()?
            .transaction::<diesel_ulid::DieselUlid, ArunaError, _>(|conn| {
                // Fetch resource with provided resource id depending on resource type
                let shared_revision_ulid = match resource_type {
                    Resources::OBJECT => objects
                        .filter(objects_dsl::id.eq(&resource_id))
                        .select(objects_dsl::shared_revision_id)
                        .first::<diesel_ulid::DieselUlid>(conn)?,
                    Resources::OBJECTGROUP => object_groups
                        .filter(object_groups_dsl::id.eq(&resource_id))
                        .select(object_groups_dsl::shared_revision_id)
                        .first::<diesel_ulid::DieselUlid>(conn)?,
                    _ => {
                        return Err(ArunaError::InvalidRequest(
                            "Only object or object groups are supported".to_string(),
                        ))
                    }
                };

                // Return shared revision id of resource
                Ok(shared_revision_ulid)
            })?;

        Ok(shared_revision_ulid)
    }
}
