use super::enums::*;
use crate::database::schema::*;
use uuid;

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct NotificationStreamGroup {
    pub id: uuid::Uuid,
    pub subject: String,
    pub resource_id: uuid::Uuid,
    pub resource_type: Resources,
    pub notify_on_sub_resources: bool,
}
