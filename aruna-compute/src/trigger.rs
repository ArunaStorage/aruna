use crate::error::ComputeError;

pub struct TriggerState {
    ep: String,
    token: String,
}

impl TriggerState {
    pub fn new() -> Self {
        TriggerState {
            ep: "http://localhost:1234".to_string(),
            token: "my secret token".to_string(),
        }
    }
    pub fn trigger_event(&self) -> Result<(), ComputeError> {
        todo!()
    }
}
