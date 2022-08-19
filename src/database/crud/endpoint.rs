use diesel::result::Error;
use diesel::prelude::*;

use crate::config::DefaultEndpoint;

use crate::database;
use crate::database::schema::endpoints::dsl::*;
use crate::database::connection::Database;
use crate::database::models::object::Endpoint;

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
    pub fn init_default_endpoint(&self, default_endpoint: DefaultEndpoint) -> Result<Endpoint, ArunaError> {
        // Check if endpoint defined in the config already exists in the database
        let endpoint = self
            .pg_connection
            .get()?
            .transaction::<Endpoint, Error, _>(|conn| {
                let result = endpoints
                    .filter(database::schema::endpoints::endpoint_type.eq(&default_endpoint.ep_type))
                    .filter(database::schema::endpoints::proxy_hostname.eq(&default_endpoint.endpoint_proxy))
                    .filter(database::schema::endpoints::internal_hostname.eq(&default_endpoint.endpoint_host))
                    .filter(database::schema::endpoints::is_public.eq(default_endpoint.endpoint_public))
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
                    is_public: default_endpoint.endpoint_public
                };

                self.pg_connection
                    .get()?
                    .transaction::<_, Error, _>(|conn| {
                        diesel::insert_into(endpoints).values(&endpoint).execute(conn)?;
                        Ok(())
                    })?;

                Ok(endpoint)
            }
        }
    }
}