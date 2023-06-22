use std::str::FromStr;

use diesel::insert_into;
use diesel::prelude::*;
use diesel::result::Error;

use crate::config::DefaultEndpoint;
use crate::database;
use crate::database::connection::Database;
use crate::database::models::auth::PubKeyInsert;
use crate::database::models::enums::EndpointType;
use crate::database::models::object::HostConfigs;
use crate::database::models::object::{Endpoint, ObjectLocation};
use crate::database::schema::endpoints::dsl::*;
use crate::database::schema::object_locations::dsl::*;
use crate::database::schema::pub_keys::dsl::pub_keys;
use crate::error::ArunaError;
use aruna_rust_api::api::storage::models::v1::Endpoint as ProtoEndpoint;
use aruna_rust_api::api::storage::services::v1::{
    AddEndpointRequest, GetObjectEndpointsRequest, GetObjectEndpointsResponse,
};

impl Database {
    /// This is a helper method to ensure that at least the endpoint defined in the config exists in the
    /// database on startup of the server. This method does not check if the endpoint is available or if
    /// the endpoint values are valid to establish a connection.
    ///
    /// ## Arguments
    ///
    /// - `database`: A reference to the established database connection
    ///
    /// ## Result:
    ///
    ///  `uuid:Uuid` - The unique identifier of the endpoint defined in the config
    ///
    /// ## Behaviour:
    ///
    /// If an endpoint with the values from the given DefaultEndpoint already exists in the database its unique uuid
    /// will be returned; else an endpoint with the values from the config will be inserted in the
    /// database and the newly generated uuid will be returned.
    ///

    pub fn init_default_endpoint(
        &self,
        default_endpoint: DefaultEndpoint,
    ) -> Result<Endpoint, ArunaError> {
        // Check if endpoint defined in the config already exists in the database
        let all_endpoints_result = self
            .pg_connection
            .get()?
            .transaction::<Vec<Endpoint>, Error, _>(|conn| {
                let result = endpoints.load::<Endpoint>(conn)?;
                Ok(result)
            });

        // Return endpoint if already exists; else insert endpoint into database and then return
        match all_endpoints_result {
            Ok(eps) => {
                for ep in eps {
                    if ep.host_config.configs == default_endpoint.endpoint_host_config {
                        return Ok(ep);
                    }
                }
                let endpoint = Endpoint {
                    id: diesel_ulid::DieselUlid::generate(),
                    endpoint_type: default_endpoint.ep_type,
                    name: default_endpoint.endpoint_name.to_string(),
                    documentation_path: default_endpoint.endpoint_docu,
                    is_public: default_endpoint.endpoint_public,
                    status: database::models::enums::EndpointStatus::AVAILABLE,
                    is_bundler: default_endpoint.endpoint_bundler,
                    host_config: HostConfigs {
                        configs: default_endpoint.endpoint_host_config.clone(),
                    },
                };

                self.pg_connection
                    .get()?
                    .transaction::<_, Error, _>(|conn| {
                        diesel::insert_into(endpoints)
                            .values(&endpoint)
                            .execute(conn)?;
                        Ok(())
                    })?;

                Ok(endpoint)
            }
            Err(_) => {
                let endpoint = Endpoint {
                    id: diesel_ulid::DieselUlid::generate(),
                    endpoint_type: default_endpoint.ep_type,
                    name: default_endpoint.endpoint_name.to_string(),
                    documentation_path: default_endpoint.endpoint_docu,
                    is_public: default_endpoint.endpoint_public,
                    status: database::models::enums::EndpointStatus::AVAILABLE,
                    is_bundler: default_endpoint.endpoint_bundler,
                    host_config: HostConfigs {
                        configs: default_endpoint.endpoint_host_config,
                    },
                };
                self.pg_connection
                    .get()?
                    .transaction::<_, Error, _>(|conn| {
                        diesel::insert_into(endpoints)
                            .values(&endpoint)
                            .execute(conn)?;
                        Ok(())
                    })?;

                Ok(endpoint)
            }
        }
    }

    /// Add a new data proxy endpoint to the database.
    ///
    /// ## Arguments:
    ///
    /// * `request`: A gRPC request containing all the needed information to create a new endpoint.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<AddEndpointResponse>, Status>`:
    ///   - **On success**: Database endpoint model
    ///   - **On failure**: Aruna error with failure details
    ///

    pub fn add_endpoint(
        &self,
        request: &AddEndpointRequest,
    ) -> Result<(Endpoint, i64), ArunaError> {
        let mut temp_conf = Vec::new();

        for ep_config in request.host_configs.clone() {
            temp_conf.push(ep_config.into())
        }

        let db_endpoint = Endpoint {
            id: diesel_ulid::DieselUlid::generate(),
            endpoint_type: EndpointType::from_i32(request.ep_type)?,
            name: request.name.to_string(),
            documentation_path: match request.documentation_path.is_empty() {
                true => None,
                false => Some(request.documentation_path.to_string()),
            },
            is_public: request.is_public,
            status: database::models::enums::EndpointStatus::AVAILABLE,
            is_bundler: request.is_bundler,
            host_config: HostConfigs { configs: temp_conf },
        };

        let db_pubkey = PubKeyInsert {
            id: None,
            pubkey: request.pubkey.to_string(),
        };

        let pubkey_serial = self
            .pg_connection
            .get()?
            .transaction::<i64, Error, _>(|conn| {
                use crate::database::schema::pub_keys::dsl as pub_keys_dsl;

                // Insert endpoint into db
                insert_into(endpoints).values(&db_endpoint).execute(conn)?;

                // Insert public key of endpoint into database
                insert_into(pub_keys)
                    .values(&db_pubkey)
                    .returning(pub_keys_dsl::id)
                    .get_result::<i64>(conn)
            })?;

        Ok((db_endpoint, pubkey_serial))
    }

    /// Get a data proxy endpoint from the database specified by its id.
    ///
    /// ## Arguments:
    ///
    /// * `endpoint_uuid`: The unique endpoint id
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<GetEndpointResponse>, Status>`:
    ///   - **On success**: Database endpoint model
    ///   - **On failure**: Aruna error with failure details
    ///

    pub fn get_endpoint(
        &self,
        endpoint_uuid: &diesel_ulid::DieselUlid,
    ) -> Result<Endpoint, ArunaError> {
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let endpoint: Endpoint = endpoints
                    .filter(database::schema::endpoints::id.eq(&endpoint_uuid))
                    .first::<Endpoint>(conn)?;

                Ok(endpoint)
            })?;

        Ok(endpoint)
    }

    /// Get all registered public endpoints from the database.
    ///
    /// ## Returns:
    ///
    /// * `Result<Vec<Endpoint>, Status>`:
    ///   - **On success**: A vector of database endpoint models
    ///   - **On failure**: Aruna error with failure details
    ///

    pub fn get_endpoints(&self) -> Result<Vec<Endpoint>, ArunaError> {
        let pub_endpoints = self
            .pg_connection
            .get()?
            .transaction::<Vec<Endpoint>, Error, _>(|conn| {
                let pub_endpoints: Vec<Endpoint> = endpoints
                    .filter(database::schema::endpoints::is_public.eq(true))
                    .load::<Endpoint>(conn)?;

                Ok(pub_endpoints)
            })?;

        Ok(pub_endpoints)
    }

    /// Get a data proxy endpoint from the database specified by its name.
    ///
    /// ## Arguments:
    ///
    /// * `endpoint_name`: The endpoint name
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<Endpoint>, Status>`:
    ///   - **On success**: Database endpoint model
    ///   - **On failure**: Aruna error with failure details
    ///

    pub fn get_endpoint_by_name(&self, endpoint_name: &str) -> Result<Endpoint, ArunaError> {
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let endpoint: Endpoint = endpoints
                    .filter(database::schema::endpoints::name.eq(&endpoint_name))
                    .first::<Endpoint>(conn)?;

                Ok(endpoint)
            })?;

        Ok(endpoint)
    }

    /// Get all data proxy endpoints associated with a specific object.
    ///
    /// ## Arguments:
    ///
    /// * `request`: The request containing the specific object id.
    ///
    /// ## Returns:
    ///
    /// * `Result<Response<Endpoint>, Status>`:
    ///   - **On success**: Response containing a vector of proto endpoints
    ///   - **On failure**: Aruna error with failure details
    ///

    pub fn get_object_endpoints(
        &self,
        request: GetObjectEndpointsRequest,
    ) -> Result<GetObjectEndpointsResponse, ArunaError> {
        let parsed_object_id = diesel_ulid::DieselUlid::from_str(&request.object_id)?;
        // Transaction time
        let obj_eps = self
            .pg_connection
            .get()?
            .transaction::<(Vec<Endpoint>, diesel_ulid::DieselUlid), Error, _>(|conn| {
                // Get collection_object association of original object
                let locations = object_locations
                    .filter(database::schema::object_locations::object_id.eq(&parsed_object_id))
                    .load::<ObjectLocation>(conn)?;
                let endpoint_ids = locations.iter().map(|e| e.endpoint_id).collect::<Vec<_>>();

                let default_id = locations.iter().find(|e| e.is_primary).unwrap();

                Ok((
                    endpoints
                        .filter(database::schema::endpoints::id.eq_any(&endpoint_ids))
                        .load::<Endpoint>(conn)?,
                    default_id.endpoint_id,
                ))
            })?;

        let mapped_ep = obj_eps
            .0
            .iter()
            .map(|ep| {
                let is_default = ep.id == obj_eps.1;

                ProtoEndpoint {
                    id: ep.id.to_string(),
                    ep_type: ep.endpoint_type as i32,
                    name: ep.name.to_string(),
                    is_bundler: ep.is_bundler,
                    documentation_path: ep
                        .documentation_path
                        .as_ref()
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                    is_public: ep.is_public,
                    is_default,
                    status: ep.status as i32,
                    host_configs: ep.host_config.clone().into(),
                }
            })
            .collect::<Vec<_>>();

        Ok(GetObjectEndpointsResponse {
            endpoints: mapped_ep,
        })
    }
}
