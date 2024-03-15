use aruna_rust_api::api::storage::services::v2::{
    PartialReplicateDataRequest, ReplicateProjectDataRequest,
};

pub enum ReplicationVariant {
    Full(ReplicateProjectDataRequest),
    Partial(PartialReplicateDataRequest),
}
