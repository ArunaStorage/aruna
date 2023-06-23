use anyhow::Result;
use aruna_rust_api::api::internal::v1::{
    internal_bundler_backchannel_service_client, Bundle, GetBundlesRequest, GetBundlesResponse,
};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tonic::{transport::Channel, Response};

#[derive(Debug)]
pub struct Bundler {
    pub bundle_store: HashMap<String, Option<Bundle>>,
}

impl Bundler {
    pub async fn new(
        internal_bundler_channel_addr: String,
        endpoint_id: String,
    ) -> Result<Arc<Mutex<Self>>> {
        let endpoint = Channel::from_shared(internal_bundler_channel_addr).unwrap();
        let channel = endpoint.connect().await.unwrap();
        let mut back_channel_client =
            internal_bundler_backchannel_service_client::InternalBundlerBackchannelServiceClient::new(channel.clone());
        let response: Response<GetBundlesResponse> = back_channel_client
            .get_bundles(tonic::Request::new(GetBundlesRequest { endpoint_id }))
            .await?;
        let bundles = response.into_inner().bundles;
        let mut bundle_store = HashMap::new();

        for bundle in bundles {
            bundle_store.insert(bundle.bundle_id.to_string(), Some(bundle));
        }
        Ok(Arc::new(Mutex::new(Bundler { bundle_store })))
    }
}
