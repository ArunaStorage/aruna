use diesel::insert_into;
use diesel::prelude::*;
use diesel::result::Error;

use crate::config::DefaultEndpoint;
use crate::database;
use crate::database::connection::Database;
use crate::database::models::auth::PubKeyInsert;
use crate::database::models::enums::EndpointType;
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
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let result = endpoints
                    .filter(
                        database::schema::endpoints::endpoint_type.eq(&default_endpoint.ep_type),
                    )
                    .filter(
                        database::schema::endpoints::proxy_hostname
                            .eq(&default_endpoint.endpoint_proxy),
                    )
                    .filter(
                        database::schema::endpoints::internal_hostname
                            .eq(&default_endpoint.endpoint_host),
                    )
                    .filter(
                        database::schema::endpoints::is_public.eq(default_endpoint.endpoint_public),
                    )
                    .first::<Endpoint>(conn)?;

                Ok(result)
            });

        // Return endpoint if already exists; else insert endpoint into database and then return
        match endpoint {
            Ok(endpoint) => Ok(endpoint),
            Err(_) => {
                let endpoint = Endpoint {
                    id: uuid::Uuid::new_v4(),
                    endpoint_type: default_endpoint.ep_type,
                    proxy_hostname: default_endpoint.endpoint_proxy,
                    name: default_endpoint.endpoint_name,
                    internal_hostname: default_endpoint.endpoint_host,
                    documentation_path: default_endpoint.endpoint_docu,
                    is_public: default_endpoint.endpoint_public,
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
        let db_endpoint = Endpoint {
            id: uuid::Uuid::new_v4(),
            endpoint_type: EndpointType::from_i32(request.ep_type)?,
            name: request.name.to_string(),
            proxy_hostname: request.proxy_hostname.to_string(),
            internal_hostname: request.internal_hostname.to_string(),
            documentation_path: match request.documentation_path.is_empty() {
                true => None,
                false => Some(request.documentation_path.to_string()),
            },
            is_public: request.is_public,
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
                Ok(insert_into(pub_keys)
                    .values(&db_pubkey)
                    .returning(pub_keys_dsl::id)
                    .get_result::<i64>(conn)?)
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
    pub fn get_endpoint(&self, endpoint_uuid: &uuid::Uuid) -> Result<Endpoint, ArunaError> {
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
        let parsed_object_id = uuid::Uuid::parse_str(&request.object_id)?;
        // Transaction time
        let obj_eps = self
            .pg_connection
            .get()?
            .transaction::<(Vec<Endpoint>, uuid::Uuid), Error, _>(|conn| {
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
                    proxy_hostname: ep.proxy_hostname.to_string(),
                    internal_hostname: ep.internal_hostname.to_string(),
                    documentation_path: ep
                        .documentation_path
                        .as_ref()
                        .map(|e| e.to_string())
                        .unwrap_or_default(),
                    is_public: ep.is_public,
                    is_default,
                }
            })
            .collect::<Vec<_>>();

        Ok(GetObjectEndpointsResponse {
            endpoints: mapped_ep,
        })
    }
}
