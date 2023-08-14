#[derive(Clone, Debug)]
pub struct ImpersonatingClient {
    self_endpoint_id: String,
    aruna_url: Option<String>,
}

impl ImpersonatingClient {
    pub fn new(self_endpoint_id: String, aruna_url: Option<String>) -> Self {
        Self {
            self_endpoint_id,
            aruna_url,
        }
    }

    pub fn get_endpoint(&self) -> String {
        self.self_endpoint_id.clone()
    }
}
