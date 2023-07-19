use anyhow::{anyhow, Result};
use aruna_rust_api::api::storage::models::v2::{ExternalRelation, Hash, KeyValue};
use tonic::metadata::MetadataMap;

use crate::database::{
    enums::{DataClass, ObjectStatus},
    object_dsl::{
        Algorithm, DefinedVariant, ExternalRelation as DBExternalRelation, ExternalRelations,
        Hash as DBHash, Hashes, KeyValue as DBKeyValue, KeyValueVariant, KeyValues,
    },
};

pub fn get_token_from_md(md: &MetadataMap) -> Result<String> {
    let token_string = md
        .get("Authorization")
        .ok_or(anyhow!("Metadata token not found"))?
        .to_str()?;

    let splitted = token_string.split(' ').collect::<Vec<_>>();

    if splitted.len() != 2 {
        log::debug!(
            "Could not get token from metadata: Wrong length, expected: 2, got: {:?}",
            splitted.len()
        );
        return Err(anyhow!("Auhtorization flow error"));
    }

    if splitted[0] != "Bearer" {
        log::debug!(
            "Could not get token from metadata: Invalid Tokentype, expected: Bearer, got: {:?}",
            splitted[0]
        );

        return Err(anyhow!("Auhtorization flow error"));
    }

    if splitted[1].is_empty() {
        log::debug!(
            "Could not get token from metadata: Invalid Tokenlength, expected: >0, got: {:?}",
            splitted[1].len()
        );

        return Err(anyhow!("Auhtorization flow error"));
    }

    Ok(splitted[1].to_string())
}

impl TryFrom<Vec<KeyValue>> for KeyValues {
    type Error = anyhow::Error;
    fn try_from(key_val: Vec<KeyValue>) -> Result<Self> {
        let mut key_vals: Vec<DBKeyValue> = Vec::new();
        for kv in key_val {
            let kv = kv.try_into()?;
            key_vals.push(kv);
        }
        Ok(KeyValues(key_vals))
    }
}

impl TryFrom<KeyValue> for DBKeyValue {
    type Error = anyhow::Error;
    fn try_from(key_val: KeyValue) -> Result<Self> {
        Ok(DBKeyValue {
            key: key_val.key,
            value: key_val.value,
            variant: key_val.variant.try_into()?,
        })
    }
}

impl TryFrom<i32> for KeyValueVariant {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<Self> {
        match var {
            1 => Ok(KeyValueVariant::LABEL),
            2 => Ok(KeyValueVariant::STATIC_LABEL),
            3 => Ok(KeyValueVariant::HOOK),
            _ => return Err(anyhow!("KeyValue variant not defined.")),
        }
    }
}

impl TryFrom<Vec<ExternalRelation>> for ExternalRelations {
    type Error = anyhow::Error;
    fn try_from(ex_rels: Vec<ExternalRelation>) -> Result<Self> {
        let mut relations: Vec<DBExternalRelation> = Vec::new();
        for r in ex_rels {
            let rs = r.try_into()?;
            relations.push(rs);
        }
        Ok(ExternalRelations(relations))
    }
}

impl TryFrom<ExternalRelation> for DBExternalRelation {
    type Error = anyhow::Error;
    fn try_from(ex_rel: ExternalRelation) -> Result<Self> {
        let (defined_variant, custom_variant) = match ex_rel.defined_variant {
            1 => (DefinedVariant::URL, None),
            2 => (DefinedVariant::IDENTIFIER, None),
            3 => (DefinedVariant::CUSTOM, ex_rel.custom_variant),
            _ => return Err(anyhow!("Relation variant not defined.")),
        };
        Ok(DBExternalRelation {
            identifier: ex_rel.identifier,
            defined_variant,
            custom_variant,
        })
    }
}

// impl TryFrom<ExternalVariant> for RelationVariantVariant {
//     type Error = anyhow::Error;
//     fn try_from(var: ExternalVariant) -> Result<Self> {
//         match var {
//             ExternalVariant::DefinedVariant(v) => {
//                 let def_var = v.try_into()?;
//                 Ok(RelationVariantVariant::DEFINED(def_var))
//             }
//             ExternalVariant::CustomVariant(s) => Ok(RelationVariantVariant::CUSTOM(s)),
//         }
//     }
// }
//
// impl TryFrom<i32> for RelationVariant {
//     type Error = anyhow::Error;
//     fn try_from(var: i32) -> Result<Self> {
//         match var {
//             1 => Ok(RelationVariant::URL),
//             2 => Ok(RelationVariant::IDENTIFIER),
//             _ => return Err(anyhow!("Not defined.")),
//         }
//     }
// }

impl TryFrom<i32> for DataClass {
    type Error = anyhow::Error;
    fn try_from(var: i32) -> Result<Self> {
        match var {
            1 => Ok(DataClass::PUBLIC),
            2 => Ok(DataClass::PRIVATE),
            4 => Ok(DataClass::WORKSPACE),
            5 => Ok(DataClass::CONFIDENTIAL),
            _ => return Err(anyhow!("Not defined.")),
        }
    }
}
impl From<DataClass> for i32 {
    fn from(var: DataClass) -> Self {
        match var {
            DataClass::PUBLIC => 1,
            DataClass::PRIVATE => 2,
            DataClass::WORKSPACE => 4,
            DataClass::CONFIDENTIAL => 5,
        }
    }
}
impl From<ObjectStatus> for i32 {
    fn from(var: ObjectStatus) -> Self {
        match var {
            ObjectStatus::INITIALIZING => 1,
            ObjectStatus::VALIDATING => 2,
            ObjectStatus::AVAILABLE => 3,
            ObjectStatus::UNAVAILABLE => 4,
            ObjectStatus::ERROR => 5,
            ObjectStatus::DELETED => 6,
        }
    }
}
impl From<KeyValues> for Vec<KeyValue> {
    fn from(keyval: KeyValues) -> Self {
        keyval
            .0
            .into_iter()
            .map(|kv| KeyValue {
                key: kv.key,
                value: kv.value,
                variant: match kv.variant {
                    KeyValueVariant::LABEL => 1,
                    KeyValueVariant::STATIC_LABEL => 2,
                    KeyValueVariant::HOOK => 3,
                },
            })
            .collect()
    }
}

impl From<DBExternalRelation> for ExternalRelation {
    fn from(r: DBExternalRelation) -> Self {
        let (defined_variant, custom_variant) = match r.defined_variant {
            DefinedVariant::CUSTOM => (3, r.custom_variant),
            DefinedVariant::IDENTIFIER => (2, None),
            DefinedVariant::URL => (1, None),
        };
        ExternalRelation {
            identifier: r.identifier,
            defined_variant,
            custom_variant,
        }
    }
}

impl TryFrom<Vec<Hash>> for Hashes {
    type Error = anyhow::Error;
    fn try_from(h: Vec<Hash>) -> Result<Self> {
        let mut hashes = Vec::new();
        for h in h {
            hashes.push(DBHash {
                alg: h.alg.try_into()?,
                hash: h.hash,
            })
        }

        Ok(Hashes(hashes))
    }
}

impl TryFrom<i32> for Algorithm {
    type Error = anyhow::Error;
    fn try_from(a: i32) -> Result<Self> {
        match a {
            1 => Ok(Algorithm::MD5),
            2 => Ok(Algorithm::SHA256),
            _ => Err(anyhow!("Hash algorithm conversion error.")),
        }
    }
}

impl From<Hashes> for Vec<Hash> {
    fn from(hashes: Hashes) -> Self {
        hashes
            .0
            .into_iter()
            .map(|h| Hash {
                alg: match h.alg {
                    Algorithm::MD5 => 1,
                    Algorithm::SHA256 => 2,
                },
                hash: h.hash,
            })
            .collect()
    }
}
