use anyhow::Result;
use std::str::FromStr;

use aruna_rust_api::api::storage::services::v2::FinishObjectStagingRequest;
use aws_sdk_s3::types::CompletedPart;
use diesel_ulid::DieselUlid;
use itertools::Itertools;

use crate::database::dsls::object_dsl::{Hash, Hashes};

pub struct FinishRequest(pub FinishObjectStagingRequest);

impl FinishRequest {
    pub fn get_object_id(&self) -> Result<DieselUlid> {
        Ok(DieselUlid::from_str(&self.0.object_id)?)
    }

    pub fn get_content_len(&self) -> i64 {
        self.0.content_len
    }

    pub fn get_hashes(&self) -> Result<Hashes> {
        let mut hashes = Vec::new();
        for h in &self.0.hashes {
            hashes.push(Hash {
                alg: h.alg.try_into()?,
                hash: h.hash.clone(),
            })
        }

        Ok(Hashes(hashes))
    }

    pub fn get_parts(&self) -> Vec<CompletedPart> {
        self.0
            .completed_parts
            .iter()
            .map(|part| {
                CompletedPart::builder()
                    .e_tag(part.etag.clone())
                    .part_number(part.part as i32)
                    .build()
            })
            .collect_vec()
    }

    pub fn get_upload_id(&self) -> &str {
        self.0.upload_id.as_ref()
    }
}
