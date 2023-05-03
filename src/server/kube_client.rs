use anyhow::anyhow;
use kube::{
    api::{Api, PostParams},
    client::Client,
    CustomResource,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "BucketCert",
    group = "aruna-storage.org",
    version = "v1alpha1",
    namespaced
)]
#[kube(status = "BucketCertStatus", shortname = "bcert")]
pub struct BucketCertSpec {
    pub bucket: String,
}
/// The status object of `Document`
#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct BucketCertStatus {
    pub compacted: bool,
    pub created: bool,
}

pub struct KubeClient {
    _client: Client,
    api: Api<BucketCert>,
}

impl KubeClient {
    pub async fn new() -> anyhow::Result<Self> {
        let client = Client::try_default().await?;
        let api: Api<BucketCert> = Api::default_namespaced(client.clone());
        Ok(KubeClient {
            _client: client,
            api,
        })
    }

    pub async fn create_bucket(&self, name: &str) -> anyhow::Result<BucketCert> {
        self.api
            .create(
                &PostParams::default(),
                &BucketCert::new(
                    name,
                    BucketCertSpec {
                        bucket: name.to_string(),
                    },
                ),
            )
            .await
            .map_err(|e| anyhow!("Unable to create bucket err: {e}"))
    }
}
