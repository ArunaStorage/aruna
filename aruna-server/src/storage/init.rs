use std::collections::HashMap;

use ahash::RandomState;
use heed::{
    types::{SerdeBincode, Str},
    Database,
};
use jsonwebtoken::DecodingKey;
use milli::BEU32;

use crate::{
    constants::const_relations,
    error::ArunaError,
    logerr,
    models::models::{Issuer, IssuerType, RelationInfo},
};

use super::store::{DecodingKeyIdentifier, IssuerInfo};

#[tracing::instrument(level = "trace", skip(key_id, decoding_key, write_txn))]
pub(super) fn init_issuer(
    mut write_txn: &mut heed::RwTxn,
    issuers: &Database<Str, SerdeBincode<Issuer>>,
    key_id: &u32,
    decoding_key: &DecodingKey,
) -> Result<HashMap<DecodingKeyIdentifier, IssuerInfo, RandomState>, ArunaError> {
    issuers
        .put(
            &mut write_txn,
            "aruna",
            &Issuer {
                issuer_name: "aruna".to_string(),
                pubkey_endpoint: None,
                audiences: vec!["aruna".to_string()],
                issuer_type: IssuerType::ARUNA,
            },
        )
        .inspect_err(logerr!())?;

    // TODO: Read existing issuers
    // Query the endpoint for the decoding key -> Add to hashmap
    //todo!();

    let mut iss: HashMap<DecodingKeyIdentifier, IssuerInfo, RandomState> = HashMap::default();
    iss.insert(
        ("aruna".to_string(), key_id.to_string()),
        (
            IssuerType::ARUNA,
            decoding_key.clone(),
            vec!["aruna".to_string()],
        ),
    );

    Ok(iss)
}

#[tracing::instrument(level = "trace", skip(write_txn))]
pub(super) fn init_relations(
    mut write_txn: &mut heed::RwTxn,
    relation_infos: &Database<BEU32, SerdeBincode<RelationInfo>>,
) -> Result<(), ArunaError> {
    const_relations().iter().try_for_each(|info| {
        relation_infos
            .put(&mut write_txn, &info.idx, info)
            .inspect_err(logerr!())
    })?;
    Ok(())
}