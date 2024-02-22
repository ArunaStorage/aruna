use diesel_ulid::DieselUlid;
use serde::{Serialize, Deserialize};
use anyhow::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    proxy: Proxy,
    persistence: Persistence,
    frontend: Frontend,
    rules: Vec<Rule>,
}

impl Config {
    pub fn validate(&mut self) -> Result<()> {
        let Config {
            proxy,
            persistence,
            frontend,
            rules,
        } = self;
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Proxy {
    endpoint_id: DieselUlid,
    private_key: Option<String>,
    serial: Option<u64>,
    remote_synced: bool,
    enable_ingest: bool,
    grpc_server: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Persistence {
    Postgres{
        host: String,
        port: u16,
        user: String,
        password: Option<String>,
        database: String,
        schema: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Frontend {
    server: String,
    hostname: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    S3 {
        host: String,
        access_key: Option<String>,
        secret_key: Option<String>,
        encryption: bool,
        compression: bool,
        deduplication: bool,
        dropbox_bucket: Option<String>,
        backend_scheme: String,
    },
    FileSystem {
        root_path: String,
        encryption: bool,
        compression: bool,
        dropbox_folder: Option<String>,
        backend_scheme: String,
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RuleTarget {
    ALL,
    OBJECT,
    BUNDLE,
    REPLICATION,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Rule {
    target: RuleTarget,
    rule: String,
}