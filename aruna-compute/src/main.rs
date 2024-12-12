mod error;
mod poll;
mod trigger;

use std::{backtrace::Backtrace, panic, time::Duration};
use error::ComputeError;
use poll::State;
use tracing_subscriber::EnvFilter;

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

    let mut state = State::new(token).await;

    loop {
        state.poll_notifications().await?;
        tokio::time::sleep(Duration::from_secs(frequency)).await;
    }
}

