use crate::database::enums::ObjectMapping;
use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::services::v2::clone_object_request::Parent;
use aruna_rust_api::api::storage::services::v2::CloneObjectRequest;
use diesel_ulid::DieselUlid;
use std::str::FromStr;

pub struct CloneObject(pub CloneObjectRequest);

impl CloneObject {
    pub fn get_object_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }
    pub fn get_parent(&self) -> Result<(DieselUlid, ObjectMapping<DieselUlid>)> {
        if let Some(p) = &self.0.parent {
            Ok(match &p {
                Parent::ProjectId(id) => {
                    let id = DieselUlid::from_str(id)?;
                    (id, ObjectMapping::PROJECT(id))
                }
                Parent::CollectionId(id) => {
                    let id = DieselUlid::from_str(id)?;
                    (id, ObjectMapping::COLLECTION(id))
                }
                Parent::DatasetId(id) => {
                    let id = DieselUlid::from_str(id)?;
                    (id, ObjectMapping::DATASET(id))
                }
            })
        } else {
            Err(anyhow!("No parent specified"))
        }
    }
}
