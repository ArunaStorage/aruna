use std::{collections::HashMap, sync::RwLock};

use ahash::RandomState;
use heed::{
    types::{SerdeBincode, Str},
    Database, Unspecified,
};
use milli::BEU32;

use crate::{
    constants::const_relations,
    error::ArunaError,
    logerr,
    models::models::{IssuerKey, IssuerType, RelationInfo},
};

use super::{
    store::single_entry_names,
    utils::{config_into_keys, SigningInfoCodec},
};

#[tracing::instrument(level = "trace", skip(write_txn))]
pub(super) fn init_encoding_keys(
    mut write_txn: &mut heed::RwTxn,
    key_config: &(u32, String, String),
    single_entry_db: &Database<Unspecified, Unspecified>,
) -> Result<(), ArunaError> {
    let key_config = config_into_keys(key_config).inspect_err(logerr!())?;

    let single_entry_decode = single_entry_db.remap_types::<Str, SigningInfoCodec>();
    single_entry_decode
        .put(
            &mut write_txn,
            single_entry_names::SIGNING_KEYS,
            &key_config,
        )
        .inspect_err(logerr!())?;
    Ok(())
}

#[tracing::instrument(level = "trace", skip(write_txn))]
pub(super) fn init_issuers(
    mut write_txn: &mut heed::RwTxn,
    key_config: &(u32, String, String),
    single_entry_db: &Database<Unspecified, Unspecified>,
) -> Result<(), ArunaError> {
    let config_into_keys = config_into_keys(key_config)?;

    let issuer_single_entry_db = single_entry_db.remap_types::<Str, SerdeBincode<Vec<IssuerKey>>>();

    let current_aruna_issuer_key = IssuerKey {
        key_id: format!("{}", config_into_keys.0),
        issuer_name: "aruna".to_string(),
        issuer_endpoint: None,
        issuer_type: IssuerType::ARUNA,
        decoding_key: config_into_keys.2,
        audiences: vec!["aruna".to_string()],
    };

    match issuer_single_entry_db
        .get(&write_txn, single_entry_names::ISSUER_KEYS)
        .inspect_err(logerr!())?
    {
        Some(current_keys) if current_keys.contains(&current_aruna_issuer_key) => {
            return Ok(());
        }
        Some(mut current_keys) if !current_keys.contains(&current_aruna_issuer_key) => {
            current_keys.push(current_aruna_issuer_key);
            issuer_single_entry_db
                .put(
                    &mut write_txn,
                    single_entry_names::ISSUER_KEYS,
                    &current_keys,
                )
                .inspect_err(logerr!())?;
            return Ok(());
        }
        _ => {}
    }
    issuer_single_entry_db
        .put(
            &mut write_txn,
            single_entry_names::ISSUER_KEYS,
            &vec![current_aruna_issuer_key],
        )
        .inspect_err(logerr!())?;
    Ok(())
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
