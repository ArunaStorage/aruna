use crate::config::Config;
use anyhow::Result;
use aruna_server::models::models::Audience;
use chrono::Utc;
use error::ProxyError;
use jsonwebtoken::{encode, Algorithm, Header};
use lazy_static::lazy_static;
use lmdbstore::LmdbStore;
use regex::Regex;
use s3::server::run_server;
use std::backtrace::Backtrace;
use std::panic;
use std::sync::Arc;
use tracing::trace;
use tracing_subscriber::EnvFilter;

mod client;
mod config;
mod error;
mod lmdbstore;
mod s3;
mod structs;

lazy_static! {
    static ref CONFIG: Config = {
        dotenvy::from_filename(".env").ok();
        let config_file = dotenvy::var("CONFIG").unwrap_or("config.toml".to_string());
        let mut config: Config =
            toml::from_str(std::fs::read_to_string(config_file).unwrap().as_str()).unwrap();
        config.validate().unwrap();
        config
    };
    static ref CORS_REGEX: Option<Regex> = {
        if let Some(cors_regex) = &CONFIG.frontend.cors_exception {
            return Some(Regex::new(cors_regex).expect("CORS exception regex invalid"));
        }
        None
    };
}

#[tracing::instrument(level = "trace", skip())]
#[tokio::main]
async fn main() -> Result<()> {
    panic::set_hook(Box::new(|info| {
        //let stacktrace = Backtrace::capture();
        let stacktrace = Backtrace::force_capture();
        println!("Got panic. @info:{}\n@stackTrace:{}", info, stacktrace);
        std::process::abort();
    }));

    dotenvy::from_filename(".env").ok();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_data=trace".parse()?);

    let subscriber = tracing_subscriber::fmt()
        //.with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
        // Use a more compact, abbreviated log format
        .compact()
        // Set LOG_LEVEL to
        .with_env_filter(filter)
        // Display source code file paths
        .with_file(true)
        // Display source code line numbers
        .with_line_number(true)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber)?;

    let self_token = sign_self_token()?;

    let store = Arc::new(LmdbStore::new(&CONFIG.proxy.lmdb_path)?);
    let client = client::ServerClient::new(store.clone(), self_token).await?;

    trace!("init s3 server");
    run_server(store, client).await?;

    Ok(())
}


pub fn sign_self_token() -> Result<String, ProxyError> {
    // Gets the signing key -> if this returns a poison error this should also panic
    // We dont want to allow poisoned / malformed encoding keys and must crash at this point

    let self_id = CONFIG.proxy.endpoint_id;

    let claims = aruna_server::models::models::ArunaTokenClaims {
        iss: self_id.to_string(),
        sub: self_id.to_string(),
        exp: (Utc::now().timestamp() as u64) + 315360000,
        info: Some((0u8, 0u16)),
        scope: None,
        aud: Some(Audience::String("aruna".to_string())),
    };

    let header = Header {
        kid: Some(format!("{}", self_id)),
        alg: Algorithm::EdDSA,
        ..Default::default()
    };

    Ok(encode(&header, &claims, &CONFIG.proxy.get_encoding_key()?).map_err(|_| ProxyError::InvalidAccessKey)?)
}
