mod error;
mod poll;
mod task;

use std::{backtrace::Backtrace, panic};
use error::ComputeError;
use poll::State;
use task::start_server;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

#[tracing::instrument(level = "trace", skip())]
#[tokio::main]
async fn main() -> Result<(), ComputeError> {
    panic::set_hook(Box::new(|info| {
        //let stacktrace = Backtrace::capture();
        let stacktrace = Backtrace::force_capture();
        println!("Got panic. @info:{}\n@stackTrace:{}", info, stacktrace);
        std::process::abort();
    }));

    dotenvy::from_filename(".env").ok();

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("aruna_compute=trace".parse()?);

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

    let token = dotenvy::var("TOKEN".to_string()).unwrap();
    let frequency = dotenvy::var("POLL_FREQUENCY".to_string()).unwrap().parse().unwrap();
    let realm_id = Ulid::from_string(&dotenvy::var("REALM_ID".to_string()).unwrap()).unwrap();

    let state = State::new(token).await;

    tokio::spawn(async move {start_server(state, frequency, realm_id)});

    Ok(())
}

