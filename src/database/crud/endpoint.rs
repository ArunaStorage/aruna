use diesel::prelude::*;
use diesel::result::Error;

use crate::api::aruna::api::storage::models::v1::Endpoint as ProtoEndpoint;
use crate::api::aruna::api::storage::services::v1::{
    GetObjectEndpointsRequest, GetObjectEndpointsResponse,
};
use crate::config::DefaultEndpoint;
use crate::database;
use crate::database::connection::Database;
use crate::database::models::object::{Endpoint, ObjectLocation};
use crate::database::schema::endpoints::dsl::*;
use crate::database::schema::object_locations::dsl::*;
use crate::error::ArunaError;

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

    ///ToDo: Rust Doc
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

    ///ToDo: Rust Doc
    pub fn get_location_endpoint(&self, location: &ObjectLocation) -> Result<Endpoint, ArunaError> {
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let endpoint: Endpoint = endpoints
                    .filter(database::schema::endpoints::id.eq(&location.endpoint_id))
                    .first::<Endpoint>(conn)?;

                Ok(endpoint)
            })?;

        Ok(endpoint)
    }

    /// ToDo: Rust Doc
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
