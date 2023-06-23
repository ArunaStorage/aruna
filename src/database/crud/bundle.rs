use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use crate::{
    database::{
        connection::Database,
        crud::utils::naivedatetime_to_prost_time,
        models::{
            bundle::Bundle,
            collection::CollectionObject,
            object::{EncryptionKey, Endpoint, Object, ObjectLocation},
        },
    },
    error::ArunaError,
    server::services::utils::create_bundle_id,
};
use aruna_rust_api::api::{
    bundler::services::v1::{CreateBundleRequest, DeleteBundleRequest, DeleteBundleResponse},
    internal::v1::{
        Bundle as ProtoBundle, GetBundlesRequest, GetBundlesResponse, Location, ObjectRef,
    },
};
use chrono::NaiveDateTime;
use diesel::{
    delete, insert_into, Connection, ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl,
};
use diesel_ulid::DieselUlid;
use prost_types::Timestamp;
use rusty_ulid::DecodingError;

impl Database {
    pub fn create_bundle(
        &self,
        request: CreateBundleRequest,
    ) -> Result<(String, Endpoint, Option<ProtoBundle>), ArunaError> {
        use crate::database::schema::bundles::dsl as bundle_dsl;
        use crate::database::schema::collection_objects::dsl as col_obj;
        use crate::database::schema::encryption_keys::dsl as enc_dsl;
        use crate::database::schema::endpoints::dsl::*;
        use crate::database::schema::object_locations::dsl as obj_loc;
        use crate::database::schema::objects::dsl::*;

        let endpoint_id: Option<DieselUlid> = if request.endpoint_id.is_empty() {
            None
        } else {
            Some(DieselUlid::from_str(&request.endpoint_id)?)
        };

        let objects_as_ulid = request
            .object_ids
            .iter()
            .map(|o| DieselUlid::from_str(o).map_err(DecodingError::into))
            .collect::<Result<Vec<DieselUlid>, ArunaError>>()?;

        let collection_id = DieselUlid::from_str(&request.collection_id)?;

        let bundle_id = create_bundle_id(&objects_as_ulid, &collection_id)?;

        self.pg_connection
            .get()?
            .transaction::<(String, Endpoint, Option<ProtoBundle>), ArunaError, _>(|conn| {
                let endpoint = match endpoint_id {
                    Some(eid) => endpoints
                        .filter(crate::database::schema::endpoints::id.eq(&eid))
                        .first::<Endpoint>(conn)?,
                    None => endpoints.first::<Endpoint>(conn)?,
                };

                let all_col_objs: Vec<CollectionObject> = col_obj::collection_objects
                    .filter(col_obj::object_id.eq_any(&objects_as_ulid))
                    .filter(col_obj::collection_id.eq(collection_id))
                    .load::<CollectionObject>(conn)?;

                if all_col_objs.len() != objects_as_ulid.len() {
                    return Err(ArunaError::InvalidRequest(
                        "Not all object_ids are part of collection".to_string(),
                    ));
                }

                let exists: Option<Bundle> = bundle_dsl::bundles
                    .filter(bundle_dsl::bundle_id.eq(&bundle_id))
                    .first::<Bundle>(conn)
                    .optional()?;

                if exists.is_some() {
                    return Ok((bundle_id, endpoint, None));
                }

                let all_objects: Vec<Object> = objects
                    .filter(crate::database::schema::objects::id.eq_any(&objects_as_ulid))
                    .load::<Object>(conn)?;

                let object_locs: Vec<ObjectLocation> = obj_loc::object_locations
                    .filter(obj_loc::endpoint_id.eq(&endpoint.id))
                    .filter(obj_loc::object_id.eq_any(&objects_as_ulid))
                    .load::<ObjectLocation>(conn)?;

                let mut db_bundles = Vec::with_capacity(all_objects.len());
                let mut proxy_refs = Vec::with_capacity(all_objects.len());

                let expiry = request
                    .expires_at
                    .clone()
                    .and_then(|t| NaiveDateTime::from_timestamp_opt(t.seconds, t.nanos as u32));

                'outer: for a_obj in all_objects.iter() {
                    for a_o_loc in object_locs.iter() {
                        if a_obj.id == a_o_loc.object_id {
                            db_bundles.push(Bundle {
                                id: DieselUlid::generate(),
                                bundle_id: bundle_id.to_string(),
                                object_id: a_obj.id,
                                endpoint_id: endpoint.id,
                                collection_id,
                                expires_at: expiry,
                            });

                            let get_enc_key: EncryptionKey = enc_dsl::encryption_keys
                                .filter(enc_dsl::hash.eq(Some(format!(
                                    "{}{}",
                                    a_o_loc.bucket.split('-').collect::<Vec<&str>>()[1],
                                    a_o_loc.path
                                ))))
                                .first::<EncryptionKey>(conn)?;

                            proxy_refs.push(ObjectRef {
                                object_location: Some(Location {
                                    r#type: 1,
                                    bucket: a_o_loc.bucket.to_string(),
                                    path: a_o_loc.path.to_string(),
                                    endpoint_id: a_o_loc.endpoint_id.to_string(),
                                    is_compressed: a_o_loc.is_compressed,
                                    is_encrypted: a_o_loc.is_encrypted,
                                    encryption_key: get_enc_key.encryption_key,
                                }),
                                object_info: Some(
                                    aruna_rust_api::api::storage::models::v1::Object {
                                        id: a_obj.id.to_string(),
                                        filename: a_obj.filename.to_string(),
                                        created: Some(naivedatetime_to_prost_time(
                                            a_obj.created_at,
                                        )?),
                                        content_len: a_obj.content_len,
                                        ..Default::default()
                                    },
                                ),
                                sub_path: "".to_string(),
                            });

                            continue 'outer;
                        }
                    }
                }

                insert_into(crate::database::schema::bundles::dsl::bundles)
                    .values(&db_bundles)
                    .execute(conn)?;

                let bundle = ProtoBundle {
                    bundle_id: bundle_id.to_string(),
                    object_refs: proxy_refs,
                    expires_at: request.expires_at,
                };

                Ok((bundle_id, endpoint, Some(bundle)))
            })
    }

    pub fn delete_bundle(
        &self,
        request: DeleteBundleRequest,
    ) -> Result<(DeleteBundleResponse, Endpoint), ArunaError> {
        use crate::database::schema::bundles::dsl as bdsl;
        use crate::database::schema::endpoints::dsl as edsl;

        self.pg_connection
            .get()?
            .transaction::<(DeleteBundleResponse, Endpoint), ArunaError, _>(|conn| {
                let bundl: Bundle = bdsl::bundles
                    .filter(bdsl::bundle_id.eq(&request.bundle_id))
                    .first::<Bundle>(conn)?;

                let endpoint: Endpoint = edsl::endpoints
                    .filter(edsl::id.eq(bundl.endpoint_id))
                    .first::<Endpoint>(conn)?;

                delete(bdsl::bundles)
                    .filter(bdsl::bundle_id.eq(request.bundle_id))
                    .execute(conn)?;
                Ok((DeleteBundleResponse {}, endpoint))
            })
    }

    pub fn get_all_bundles(
        &self,
        request: GetBundlesRequest,
    ) -> Result<GetBundlesResponse, ArunaError> {
        use crate::database::schema::bundles::dsl as bdsl;
        use crate::database::schema::encryption_keys::dsl as enc_dsl;
        use crate::database::schema::object_locations::dsl as obj_loc;
        use crate::database::schema::objects::dsl as ob_dsl;

        let ep = DieselUlid::from_str(&request.endpoint_id)?;

        self.pg_connection
            .get()?
            .transaction::<GetBundlesResponse, ArunaError, _>(|conn| {
                let all_bundles: Vec<Bundle> = bdsl::bundles
                    .filter(bdsl::endpoint_id.eq(&ep))
                    .load::<Bundle>(conn)?;

                let oids = all_bundles
                    .iter()
                    .map(|e| e.object_id)
                    .collect::<HashSet<DieselUlid>>();

                let all_objects = ob_dsl::objects
                    .filter(ob_dsl::id.eq_any(&oids))
                    .load::<Object>(conn)?;

                let object_locs: Vec<ObjectLocation> = obj_loc::object_locations
                    .filter(obj_loc::object_id.eq_any(&oids))
                    .filter(obj_loc::endpoint_id.eq(&ep))
                    .load::<ObjectLocation>(conn)?;

                let mut ob_refs = Vec::with_capacity(all_objects.len());

                'outer: for a_obj in all_objects.iter() {
                    for a_o_loc in object_locs.iter() {
                        if a_obj.id == a_o_loc.object_id {
                            let get_enc_key: EncryptionKey = enc_dsl::encryption_keys
                                .filter(enc_dsl::hash.eq(Some(format!(
                                    "{}{}",
                                    a_o_loc.bucket.split('-').collect::<Vec<&str>>()[1],
                                    a_o_loc.path
                                ))))
                                .first::<EncryptionKey>(conn)?;

                            ob_refs.push(ObjectRef {
                                object_location: Some(Location {
                                    r#type: 1,
                                    bucket: a_o_loc.bucket.to_string(),
                                    path: a_o_loc.path.to_string(),
                                    endpoint_id: a_o_loc.endpoint_id.to_string(),
                                    is_compressed: a_o_loc.is_compressed,
                                    is_encrypted: a_o_loc.is_encrypted,
                                    encryption_key: get_enc_key.encryption_key,
                                }),
                                object_info: Some(
                                    aruna_rust_api::api::storage::models::v1::Object {
                                        id: a_obj.id.to_string(),
                                        filename: a_obj.filename.to_string(),
                                        created: Some(naivedatetime_to_prost_time(
                                            a_obj.created_at,
                                        )?),
                                        content_len: a_obj.content_len,
                                        ..Default::default()
                                    },
                                ),
                                sub_path: "".to_string(),
                            });
                            continue 'outer;
                        }
                    }
                }

                let mut bundle_map: HashMap<String, Vec<ObjectRef>> = HashMap::new();
                let mut expires_map: HashMap<String, Option<Timestamp>> = HashMap::new();

                'out_loop: for bundle_entry in all_bundles {
                    for oref in &ob_refs {
                        if bundle_entry.object_id.to_string()
                            == oref.object_info.clone().unwrap_or_default().id
                        {
                            let entry = bundle_map
                                .entry(bundle_entry.bundle_id.to_string())
                                .or_default();
                            expires_map.insert(
                                bundle_entry.bundle_id.to_string(),
                                bundle_entry
                                    .expires_at
                                    .and_then(|e| naivedatetime_to_prost_time(e).ok()),
                            );
                            entry.push(oref.clone());
                            continue 'out_loop;
                        }
                    }
                }

                let proto_bundles = bundle_map
                    .into_iter()
                    .map(|(k, v)| ProtoBundle {
                        bundle_id: k.to_string(),
                        expires_at: expires_map.remove(&k).flatten(),
                        object_refs: v,
                    })
                    .collect::<Vec<ProtoBundle>>();

                Ok(GetBundlesResponse {
                    bundles: proto_bundles,
                })
            })
    }
}
