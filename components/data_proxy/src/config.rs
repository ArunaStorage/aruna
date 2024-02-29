use anyhow::Result;
use diesel_ulid::DieselUlid;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub proxy: Proxy,
    pub persistence: Option<Persistence>,
    pub frontend: Option<Frontend>,
    pub backend: Backend,
    pub rules: Vec<Rule>,
}

impl Config {
    pub fn validate(&mut self) -> Result<()> {
        let Config {
            proxy,
            persistence,
            backend,
            ..
        } = self;

        proxy.validate()?;
        if let Some(persistence) = persistence {
            persistence.validate()?;
        }
        backend.validate()?;
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Proxy {
    pub endpoint_id: DieselUlid,
    pub private_key: Option<String>,
    pub serial: i32,
    pub remote_synced: bool,
    pub enable_ingest: bool,
    pub aruna_url: Option<String>,
    pub grpc_server: String,
}

impl Proxy {
    pub fn validate(&mut self) -> Result<()> {
        let Proxy {
            endpoint_id,
            private_key,
            serial,
            remote_synced,
            enable_ingest,
            grpc_server,
            ..
        } = self;

        if let Some(private_key) = private_key {
            if private_key.len() < 32 {
                return Err(anyhow::anyhow!(
                    "private_key must be at least 32 characters long"
                ));
            }
        } else {
            let env_var = dotenvy::var("PROXY_PRIVATE_KEY").map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            *private_key = Some(env_var);
        }

        if *serial < 1 {
            return Err(anyhow::anyhow!("serial must be at least 1"));
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Persistence {
    Postgres {
        host: String,
        port: u16,
        user: String,
        password: Option<String>,
        database: String,
        schema: String,
    },
}

impl Persistence {
    fn validate(&mut self) -> Result<()> {
        let Persistence::Postgres {
            host,
            port,
            user,
            password,
            database,
            schema,
        } = self
        else {
            return Ok(());
        };

        if host.is_empty() {
            return Err(anyhow::anyhow!("host cannot be empty"));
        }

        if *port < 1 {
            return Err(anyhow::anyhow!("port must be at least 1"));
        }

        if user.is_empty() {
            return Err(anyhow::anyhow!("user cannot be empty"));
        }

        if database.is_empty() {
            return Err(anyhow::anyhow!("database cannot be empty"));
        }

        if schema.is_empty() {
            return Err(anyhow::anyhow!("schema cannot be empty"));
        }

        if let None = password {
            let env_var = dotenvy::var("POSTGRES_PASSWORD").map_err(|e| {
                tracing::error!(error = ?e, msg = e.to_string());
                e
            })?;
            *password = Some(env_var);
        }
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Frontend {
    pub server: String,
    pub hostname: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    S3 {
        host: Option<String>,
        access_key: Option<String>,
        secret_key: Option<String>,
        encryption: bool,
        compression: bool,
        deduplication: bool,
        dropbox_bucket: Option<String>,
        backend_scheme: String,
        tmp: Option<String>,
    },
    FileSystem {
        root_path: String,
        encryption: bool,
        compression: bool,
        dropbox_folder: Option<String>,
        backend_scheme: String,
        tmp: Option<String>, // Will default to /tmp
    },
}

impl Backend {
    fn validate(&mut self) -> Result<()> {
        match self {
            Self::S3 {
                access_key,
                secret_key,
                host,
                ..
            } => {
                if let None = host {
                    let env_var = dotenvy::var("AWS_S3_HOST").map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string());
                        e
                    })?;
                    *host = Some(env_var);
                }

                if let None = access_key {
                    let env_var = dotenvy::var("AWS_ACCESS_KEY_ID").map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string());
                        e
                    })?;
                    *access_key = Some(env_var);
                }

                if let None = secret_key {
                    let env_var = dotenvy::var("AWS_SECRET_ACCESS").map_err(|e| {
                        tracing::error!(error = ?e, msg = e.to_string());
                        e
                    })?;
                    *secret_key = Some(env_var);
                }

                Ok(())
            }
            Self::FileSystem { .. } => Ok(()),
        }
    }

    pub fn get_tmp(&self) -> Option<String> {
        match self {
            Self::S3 { tmp, .. } => tmp.clone(),
            Self::FileSystem { tmp, .. } => tmp.clone(),
        }
    }

    pub fn is_encrypted(&self) -> bool {
        match self {
            Self::S3 { encryption, .. } => *encryption,
            Self::FileSystem { encryption, .. } => *encryption,
        }
    }

    pub fn is_compressed(&self) -> bool {
        match self {
            Self::S3 { compression, .. } => *compression,
            Self::FileSystem { compression, .. } => *compression,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub enum RuleTarget {
    ROOT, // Info
    OBJECT,
    OBJECTPACKAGE,
    BUNDLE,
    REPLICATIONIN,
    REPLICATIONOUT,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Rule {
    pub target: RuleTarget,
    pub rule: String,
}
