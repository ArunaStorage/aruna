use anyhow::anyhow;
use anyhow::Result;
use lazy_static::lazy_static;
use regex::Regex;
use std::panic;
use std::{net::SocketAddr, sync::Arc};
use tokio::try_join;
use tonic::transport::Server;
use tracing::error;
use tracing::info_span;
use tracing::trace;
use tracing::Instrument;
use tracing_subscriber::EnvFilter;

mod auth;
mod config;
mod lmdbstore;
mod s3;
mod error;

use crate::config::Config;
use std::backtrace::Backtrace;
use std::time::Duration;

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
        .add_directive("data_proxy=trace".parse()?);

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

    trace!("init s3 server");

    Ok(())
}
