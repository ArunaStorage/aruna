use aws_sdk_s3::types::{builders::CorsRuleBuilder, CorsRule};
use chrono::{DateTime, Utc};
use pithos_lib::helpers::footer_parser::Footer;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

#[derive(Debug, Serialize, Clone, Deserialize)]
pub enum StorageLocation {
    S3 { bucket: String, key: String },
    FileSystem { path: String },
    Temp { upload_id: String }, // A temporary location for uploads
}

#[derive(Debug, Serialize, Deserialize)]
pub enum StorageFormat {
    Pithos(Footer),
    Encrypted([u8; 32]),
    Compressed,
    Raw,
    Uploading, // Still uploading
}

#[derive(Debug, Serialize, Deserialize)]
pub enum ObjectInfo {
    Object {
        location: StorageLocation,
        storage_format: StorageFormat,
        project: Ulid,
        revision_number: u64,
        public: bool,
    },
    Project {
        cors: Vec<CorsRuleAruna>,
        public: bool,
    },
    Bundle {
        vec: Vec<Ulid>,
        expiry: DateTime<Utc>,
        //password: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CorsRuleAruna {
    /// <p>Unique identifier for the rule. The value cannot be longer than 255 characters.</p>
    pub id: Option<String>,
    /// <p>Headers that are specified in the <code>Access-Control-Request-Headers</code> header. These headers are allowed in a preflight OPTIONS request. In response to any preflight OPTIONS request, Amazon S3 returns any requested headers that are allowed.</p>
    pub allowed_headers: Option<Vec<String>>,
    /// <p>An HTTP method that you allow the origin to execute. Valid values are <code>GET</code>, <code>PUT</code>, <code>HEAD</code>, <code>POST</code>, and <code>DELETE</code>.</p>
    pub allowed_methods: Vec<String>,
    /// <p>One or more origins you want customers to be able to access the bucket from.</p>
    pub allowed_origins: Vec<String>,
    /// <p>One or more headers in the response that you want customers to be able to access from their applications (for example, from a JavaScript <code>XMLHttpRequest</code> object).</p>
    pub expose_headers: Option<Vec<String>>,
    /// <p>The time in seconds that your browser is to cache the preflight response for the specified resource.</p>
    pub max_age_seconds: Option<i32>,
}

impl TryFrom<CorsRuleAruna> for CorsRule {
    type Error = anyhow::Error;
    fn try_from(c: CorsRuleAruna) -> anyhow::Result<Self> {
        let mut builder = CorsRuleBuilder::default();

        if let Some(id) = c.id {
            builder = builder.id(id);
        }

        builder = builder.set_allowed_headers(c.allowed_headers);
        if !c.allowed_methods.is_empty() {
            builder = builder.set_allowed_methods(Some(c.allowed_methods));
        }

        if !c.allowed_origins.is_empty() {
            builder = builder.set_allowed_origins(Some(c.allowed_origins));
        }

        builder = builder.set_expose_headers(c.expose_headers);

        if let Some(max_age_seconds) = c.max_age_seconds {
            builder = builder.max_age_seconds(max_age_seconds);
        }

        Ok(builder.build()?)
    }
}

impl From<CorsRule> for CorsRuleAruna {
    fn from(c: CorsRule) -> Self {
        CorsRuleAruna {
            id: c.id,
            allowed_headers: c.allowed_headers,
            allowed_methods: c.allowed_methods,
            allowed_origins: c.allowed_origins,
            expose_headers: c.expose_headers,
            max_age_seconds: c.max_age_seconds,
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Hash)]
pub struct UploadPart {
    pub object_id: Ulid,
    //pub upload_id: String,
    pub part_number: u64,
    pub raw_size: u64,
    pub size: u64,
}
