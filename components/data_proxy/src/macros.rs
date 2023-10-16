#[macro_export]
macro_rules! required {
    ($option:expr) => {
        match $option {
            Some(value) => value,
            None => return Err(tonic::Status::invalid_argument("Missing required field")),
        }
    };
}

#[macro_export]
macro_rules! log_received {
    ($request:expr) => {
        log::info!("Received {}", $crate::structs::type_name_of($request));
        log::debug!("{:?}", $request);
    };
}
