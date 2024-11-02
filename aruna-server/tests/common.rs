use aruna_rust_api::v3::aruna::api::v3::group_service_client::GroupServiceClient;
use aruna_rust_api::v3::aruna::api::v3::realm_service_client::RealmServiceClient;
use aruna_rust_api::v3::aruna::api::v3::resource_service_client::ResourceServiceClient;
use aruna_rust_api::v3::aruna::api::v3::user_service_client::UserServiceClient;
use aruna_server::{start_server, Config};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::atomic::AtomicU16;
use std::sync::Arc;
use std::sync::Once;
use std::time::Duration;
use tokio::fs;
use tokio::sync::Notify;
use tokio::time::sleep;
use tonic::codegen::InterceptedService;
use tonic::metadata::{AsciiMetadataKey, AsciiMetadataValue};
use tonic::transport::Channel;
use tracing_subscriber::EnvFilter;
use ulid::Ulid;

pub const TEST_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkJHVDFUWVY4UVo3WkZaS1hQNERRS05OIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjE3MzU2MzIyODQsImluZm8iOlswLDBdfQ.lCvp27T537Ygm8bCt3TPl7hLwaLObHqNkFwet-J19-QoT_YmbO7tJ9O_aWNXT1KLnmbxq8WJUlvROecxXf0UDA";
pub static SUBSCRIBERS: AtomicU16 = AtomicU16::new(0);
static INIT_TRACING: Once = Once::new();
const MAX_RETRIES: u8 = 50;

// Create a client interceptor which always adds the specified api token to the request header
#[derive(Clone)]
pub struct ClientInterceptor {
    api_token: String,
}
// Implement a request interceptor which always adds
//  the authorization header with a specific API token to all requests
impl tonic::service::Interceptor for ClientInterceptor {
    fn call(&mut self, request: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let mut mut_req: tonic::Request<()> = request;
        let metadata = mut_req.metadata_mut();
        metadata.append(
            AsciiMetadataKey::from_bytes("Authorization".as_bytes()).unwrap(),
            AsciiMetadataValue::try_from(format!("Bearer {}", self.api_token.as_str())).unwrap(),
        );

        return Ok(mut_req);
    }
}

pub struct Clients {
    pub realm_client: RealmServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub group_client: GroupServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub user_client: UserServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub resource_client: ResourceServiceClient<InterceptedService<Channel, ClientInterceptor>>,
}
pub async fn init_test(offset: u16) -> Clients {
    INIT_TRACING.call_once(init_tracing);

    // Start server and get port
    let (port, notify) = init_testing_server(offset).await;
    notify.notified().await;
    // Create connection to the Aruna instance via gRPC
    let api_token = TEST_TOKEN;
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{port}")).unwrap();

    let mut retries = MAX_RETRIES;
    let channel = loop {
        retries -= 1;
        if retries == 0 {
            panic!()
        }
        sleep(Duration::from_millis(10)).await;
        match endpoint.connect().await {
            Ok(channel) => break channel,
            Err(e) => {
                dbg!(e);
                sleep(Duration::from_millis(100)).await;
            }
        }
    };
    //let channel = endpoint.connect().await.unwrap();
    let interceptor = ClientInterceptor {
        api_token: api_token.to_string(),
    };

    // Create the individual client services
    let realm_client = RealmServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    let group_client = GroupServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    let user_client = UserServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    let resource_client =
        ResourceServiceClient::with_interceptor(channel.clone(), interceptor.clone());
    Clients {
        realm_client,
        group_client,
        user_client,
        resource_client,
    }
}

async fn init_testing_server(offset: u16) -> (u16, Arc<Notify>) {
    // Create notifier
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();

    // Copy & create db
    let node_id = Ulid::new();
    let test_path = format!("/dev/shm/{node_id}");
    fs::create_dir_all(format!("{test_path}/events"))
        .await
        .unwrap();
    fs::create_dir_all(format!("{test_path}/store"))
        .await
        .unwrap();
    fs::copy(
        "./tests/test_db/events/data.mdb",
        &format!("{test_path}/events/data.mdb"),
    )
    .await
    .unwrap();

    fs::copy(
        "./tests/test_db/store/data.mdb",
        &format!("{test_path}/store/data.mdb"),
    )
    .await
    .unwrap();

    // Create server config with unused ports
    let subscriber_count = SUBSCRIBERS.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + offset;
    let node_serial = subscriber_count;
    let grpc_port = 50050 + subscriber_count;
    let consensus_port = 60050 + subscriber_count;
    let socket_addr = format!("0.0.0.0:{consensus_port}");
    let rest_port = 8080 + subscriber_count;

    // Spawn server
    tokio::spawn(async move {
        start_server(
            Config {
                node_id,
                grpc_port,
                rest_port,
                node_serial,
                database_path: test_path,
                members: Vec::new(),
                key_config: (
                    1,
                    "MC4CAQAwBQYDK2VwBCIEICHl/V9wxvENDJKePwusDhnC7xgaHYV6iHLb0ENJZndj".to_string(),
                    "MCowBQYDK2VwAyEA2YfYTgb8Y0LTFr+2Rm2Fkdu38eJTfnsMDH2iZHErBH0=".to_string(),
                ),
                socket_addr: SocketAddr::from_str(&socket_addr).unwrap(),
            },
            Some(notify_clone),
        )
        .await
    });

    // Return grpc port
    (grpc_port, notify)
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or("none".into())
        .add_directive("h2=off".parse().unwrap())
        .add_directive("tower=off".parse().unwrap())
        .add_directive("hyper=off".parse().unwrap())
        .add_directive("tonic=off".parse().unwrap());
    //.add_directive("synevi_core=trace".parse().unwrap());

    tracing_subscriber::fmt().with_env_filter(filter).init();
}
