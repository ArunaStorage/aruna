use super::enums::*;
use crate::database::schema::*;

#[derive(Queryable, Insertable, Identifiable, Debug)]
pub struct NotificationStreamGroup {
    pub id: diesel_ulid::DieselUlid,
    pub subject: String,
    pub resource_id: diesel_ulid::DieselUlid,
    pub resource_type: Resources,
    pub notify_on_sub_resources: bool,
}
