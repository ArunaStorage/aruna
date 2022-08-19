use std::fs;
use std::env::VarError;
use crate::database::models::enums::EndpointType;

use serde::Deserialize;
use toml::de::Error;
use tonic::codegen::Body;
use crate::error::{ArunaError, TypeConversionError};


/// This struct contains all parsed configuration parameters of the ArunaServer
pub struct ArunaServerConfig {
    pub config: Config,
    pub env_config: EnvConfig
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
}
#[derive(Clone, Debug, Deserialize)]
pub struct DefaultEndpoint {
    pub ep_type: EndpointType,
    pub endpoint_name: String,
    pub endpoint_host: String,
    pub endpoint_proxy: String,
    pub endpoint_public: bool,
    pub endpoint_docu: Option<String>,
}

///
impl ArunaServerConfig {
    pub fn new() -> Self {
        //let config_content = ArunaServerConfig::read_config_file().expect("Reading config file failed");
        //let config = ArunaServerConfig::parse_config_content(config_content).expect("Parsing config file failed");
        let config = ArunaServerConfig::test().expect("Reading/Parsing config file failed");
        let env_config = ArunaServerConfig::load_config_env().expect("Loading config env vars failed");

        ArunaServerConfig {
            config,
            env_config
        }
    }

    /// This method tries to read the ArunaServer config file.
    ///
    /// ## Returns:
    ///
    /// * `std::io::Result<String>` - The String contains the complete configuration file content
    ///
    /// ## Behaviour:
    ///
    /// Tries to read the ArunaServer configuration file.
    ///
    /// This function will return an error if path does not already exist.
    /// Other errors may also be returned according to OpenOptions::open. It will also return
    /// an error if it encounters while reading an error of a kind other
    /// than io::ErrorKind::Interrupted, or if the contents of the file are not valid UTF-8.
    ///
    fn read_config_file() -> std::io::Result<String> {
        // Read config file
        fs::read_to_string("./config.toml")
    }

    /// This method tries to parse the ArunaServer config file.
    ///
    /// ## Returns:
    ///
    /// * `Result<Config, ArunaError>` - The config object contains all parameters loaded from the config file
    ///
    /// ## Behaviour:
    ///
    /// Tries to read the ArunaServer configuration file.
    fn parse_config_content(content: String) -> Result<Config, Error> {
        // Parse config file content
        toml::from_str(content.as_str())
    }

    fn test() -> Result<Config, ArunaError> {
        // Read
        let content = fs::read_to_string("./config.toml").map_err(|_| TypeConversionError::PARSECONFIG)?;

        toml::from_str(content.as_str()).map_err(|_| ArunaError::TypeConversionError(TypeConversionError::PARSECONFIG))
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
    /// If the parameter is mandatory the
    ///
    fn load_config_env() -> Result<EnvConfig, VarError> {
        //ToDo: Implement loading of mandatory/optional environmental variables
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
