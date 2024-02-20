#[macro_export]
macro_rules! required {
    ($option:expr) => {
        match $option {
            Some(value) => value,
            None => return Err(tonic::Status::invalid_argument("Missing required field")),
        }
    };
}
