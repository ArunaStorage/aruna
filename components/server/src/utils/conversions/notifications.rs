use crate::database::{
    dsls::persistent_notification_dsl::{NotificationReference, PersistentNotification},
    enums::NotificationReferenceType,
};
use aruna_rust_api::api::storage::services::v2::{
    PersonalNotification, PersonalNotificationVariant, Reference, ReferenceType,
};

impl From<PersistentNotification> for PersonalNotification {
    fn from(value: PersistentNotification) -> Self {
        let variant: PersonalNotificationVariant = value.notification_variant.into();
        let refs = value
            .refs
            .0
             .0
            .into_iter()
            .map(|r| r.into())
            .collect::<Vec<_>>();

        PersonalNotification {
            id: value.id.to_string(),
            variant: variant as i32,
            message: value.message,
            refs,
        }
    }
}
impl From<NotificationReference> for Reference {
    fn from(value: NotificationReference) -> Self {
        Reference {
            ref_type: match value.reference_type {
                NotificationReferenceType::User => ReferenceType::User,
                NotificationReferenceType::Resource => ReferenceType::Resource,
            } as i32,
            ref_name: value.reference_name,
            ref_value: value.reference_value,
        }
    }
}
