use crate::database::models::{enums::EndpointType, object::HostConfig};
use std::env::VarError;
use std::fs;

use crate::error::{ArunaError, TypeConversionError};
use serde::Deserialize;

/// This struct contains all parsed configuration parameters of the ArunaServer

#[derive(Clone, Debug, Deserialize)]
pub struct ArunaServerConfig {
    pub config: Config,
    pub env_config: EnvConfig,
}

/// This struct contains all values collected from environmental variables

#[derive(Clone, Debug, Deserialize)]
pub struct EnvConfig {}

/// This struct is used as template to parse the ArunaServer config.toml file

#[derive(Clone, Debug, Deserialize)]
pub struct Config {
    pub database_url: String,
    pub oauth_realminfo: String,
    pub default_endpoint: DefaultEndpoint,
    pub event_notifications: EventNotifications,
    pub loc_version: LocationVersion,
}

#[derive(Clone, Debug, Deserialize)]
pub struct EventNotifications {
    pub emitter_host: String,
    pub emitter_token: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct DefaultEndpoint {
    pub ep_type: EndpointType,
    pub endpoint_name: String,
    pub endpoint_host_config: Vec<HostConfig>,
    pub endpoint_bundler: bool,
    pub endpoint_public: bool,
    pub endpoint_docu: Option<String>,
    pub endpoint_serial: i64,
    pub endpoint_pubkey: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SemVer {
    pub major: i32,
    pub minor: i32,
    pub patch: i32,
    pub labels: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ComponentVersion {
    pub component_name: String,
    pub semver: SemVer,
}

#[derive(Clone, Debug, Deserialize)]
pub struct LocationVersion {
    pub location: String,
    pub components: Vec<ComponentVersion>,
}

///
impl ArunaServerConfig {
    pub fn new() -> Self {
        let config =
            ArunaServerConfig::load_config_file().expect("Reading/Parsing config file failed");
        let env_config =
            ArunaServerConfig::load_config_env().expect("Loading config env vars failed");

        ArunaServerConfig { config, env_config }
    }

    /// This method tries to read the ArunaServer config file and parse its content.
    ///
    /// ## Returns:
    ///
    /// * `Result<Config, ArunaError>` - The Config contains all the parameter values from the config file
    ///
    /// ## Behaviour:
    ///
    /// Tries to read the ArunaServer configuration file and then parse its content.
    ///
    /// This function will return an error if path of the configuration file does not already exist.
    /// Other errors may also be returned according to OpenOptions::open. It will also return
    /// an error if it encounters while reading an error of a kind other
    /// than io::ErrorKind::Interrupted, or if the contents of the file are not valid UTF-8.
    ///
    /// It will also return an error if the deserialization of the content into the Config struct fails.
    ///

    fn load_config_file() -> Result<Config, ArunaError> {
        // Read config file content
        let content = fs::read_to_string("./config/config.toml")
            .map_err(|_| TypeConversionError::PARSECONFIG)?;

        //Parse config file content
        toml::from_str(content.as_str())
            .map_err(|_| ArunaError::TypeConversionError(TypeConversionError::PARSECONFIG))
    }

    /// This method loads the config parameters stored in environmental variables.
    ///
    /// ## Returns:
    ///
    /// * Result<EnvConfig, VarError> - The EnvConfig contains all parameters loaded from environmental variables
    ///
    /// ## Behaviour:
    ///
    /// Tries to load all specified environmental variables specified in the function body.
    /// If the parameter is mandatory any error stops the execution. Optional parameters are
    /// filled with a specified default value if not set.
    ///
    fn load_config_env() -> Result<EnvConfig, VarError> {
        //let env_var = env::var("ENV_VAR")?; //Note: Mandatory

        /* Note: Optional
        let env_var = match env::var("ENV_VAR") {
            Ok(value) => value,
            Err(_) => "Default_Value"
        };
        */

        Ok(EnvConfig {})
    }
}

impl Default for ArunaServerConfig {
    fn default() -> Self {
        Self::new()
    }
}
