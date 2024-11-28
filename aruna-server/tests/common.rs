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

// USERS
//
// ADMIN:
//  {
//      "id":"01JDPGX26PDFD4Y3771K28MPN5",
//      "first_name":"aruna",
//      "last_name":"admin",
//      "email":"admin@test.com",
//      "identifiers":"",
//      "global_admin":false
//  }
// {
//    "token": {
//      "id": 0,
//      "user_id": "01JDPGX26PDFD4Y3771K28MPN5",
//      "name": "TESTTOKEN",
//      "expires_at": "2050-11-27T10:04:08.438Z",
//      "constraints": null
//    },
//    "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQR1gyNlBERkQ0WTM3NzFLMjhNUE41IiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTYyNDgsImluZm8iOlswLDBdfQ.BWU1NZntUcQn0LY0qQRL9ixS_m3vqz4JWlmG_IzxN_y1SrQdmgg8G7SNbwK4wYs8bCbi8Et35cCH-tngEmT9DA"
//  }
pub const ADMIN_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQR1gyNlBERkQ0WTM3NzFLMjhNUE41IiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTYyNDgsImluZm8iOlswLDBdfQ.BWU1NZntUcQn0LY0qQRL9ixS_m3vqz4JWlmG_IzxN_y1SrQdmgg8G7SNbwK4wYs8bCbi8Et35cCH-tngEmT9DA";

// REGULAR
// {
//      "id":"01JDPGYPQ2G3MDS0ZH20FQNNM4",
//      "first_name":"regular",
//      "last_name":"user",
//      "email":"regular@test.com",
//      "identifiers":"",
//      "global_admin":false
//  }
// {
//   "token": {
//     "id": 0,
//     "user_id": "01JDPGYPQ2G3MDS0ZH20FQNNM4",
//     "name": "TESTTOKEN",
//     "expires_at": "2050-11-27T10:07:15.417Z",
//     "constraints": null
//   },
//   "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQR1lQUTJHM01EUzBaSDIwRlFOTk00IiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTY0MzUsImluZm8iOlswLDBdfQ._eaf52a_Lnxnh3yVu2aWRKkU8XZ4fwRomlXI_rnx-5xh9rHS6sdXIEVh22NyXNnCza7qomC5tUsKqTkBH2QfCQ"
// }
pub const REGULAR_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQR1lQUTJHM01EUzBaSDIwRlFOTk00IiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTY0MzUsImluZm8iOlswLDBdfQ._eaf52a_Lnxnh3yVu2aWRKkU8XZ4fwRomlXI_rnx-5xh9rHS6sdXIEVh22NyXNnCza7qomC5tUsKqTkBH2QfCQ";

// REGULAR2
// {
//      "id":"01JDPH096816PAXTQRBTZHXJE0",
//      "first_name":"regular2",
//      "last_name":"user",
//      "email":"regular2@test.com",
//      "identifiers":"",
//      "global_admin":false
//  }
//{
//   "token": {
//     "id": 1,
//     "user_id": "01JDPH096816PAXTQRBTZHXJE0",
//     "name": "TESTTOKEN",
//     "expires_at": "2050-11-27T10:08:37.559Z",
//     "constraints": null
//   },
//   "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQSDA5NjgxNlBBWFRRUkJUWkhYSkUwIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTY1MTcsImluZm8iOlswLDFdfQ.vts4apGmkGyl4amQ_CJXyufb6lgChOFZRJ8DqXym4IOEJR3Lf4Shd0ViPVsEm9JJtakas0SFbP1feGmfXj3OBw"
// }
#[allow(unused)]
pub const REGULAR2_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQSDA5NjgxNlBBWFRRUkJUWkhYSkUwIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTY1MTcsImluZm8iOlswLDFdfQ.vts4apGmkGyl4amQ_CJXyufb6lgChOFZRJ8DqXym4IOEJR3Lf4Shd0ViPVsEm9JJtakas0SFbP1feGmfXj3OBw";

// WEBTEST
//
// {
//      "id":"01JDPH1QAJQRAMKNGTF5YC09CC",
//      "first_name":"webtest",
//      "last_name":"user",
//      "email":"webtest@test.com",
//      "identifiers":"",
//      "global_admin":false
//  }
//{
//  "token": {
//    "id": 0,
//    "user_id": "01JDPH1QAJQRAMKNGTF5YC09CC",
//    "name": "TESTTOKEN",
//    "expires_at": "2050-11-27T10:10:27.550Z",
//    "constraints": null
//  },
//  "secret": "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQSDFRQUpRUkFNS05HVEY1WUMwOUNDIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTY2MjcsImluZm8iOlswLDBdfQ.xDDlapkpV2XJNQ8eBuyp-CbJpOlfmXyRk7mM3i7cB0gNdpUUOVOMLyW5sv4vp3pTUgswBNorbVLFbLeRPgNkDg"
//}
#[allow(unused)]
pub const WEBTEST_TOKEN: &str = "eyJ0eXAiOiJKV1QiLCJhbGciOiJFZERTQSIsImtpZCI6IjEifQ.eyJpc3MiOiJhcnVuYSIsInN1YiI6IjAxSkRQSDFRQUpRUkFNS05HVEY1WUMwOUNDIiwiYXVkIjoiYXJ1bmEiLCJleHAiOjI1NTMxNTY2MjcsImluZm8iOlswLDBdfQ.xDDlapkpV2XJNQ8eBuyp-CbJpOlfmXyRk7mM3i7cB0gNdpUUOVOMLyW5sv4vp3pTUgswBNorbVLFbLeRPgNkDg";


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

#[allow(unused)]
pub struct Clients {
    pub realm_client: RealmServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub group_client: GroupServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub user_client: UserServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub resource_client: ResourceServiceClient<InterceptedService<Channel, ClientInterceptor>>,
    pub rest_endpoint: String,
}
pub async fn init_test(offset: u16) -> Clients {
    INIT_TRACING.call_once(init_tracing);

    // Start server and get port
    let (grpc_port, rest_port, notify) = init_testing_server(offset).await;
    notify.notified().await;
    // Create connection to the Aruna instance via gRPC
    let api_token = ADMIN_TOKEN;
    let endpoint = Channel::from_shared(format!("http://0.0.0.0:{grpc_port}")).unwrap();

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
        rest_endpoint: format!("http://localhost:{}", rest_port),
    }
}

async fn init_testing_server(offset: u16) -> (u16, u16, Arc<Notify>) {
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
                //members: Vec::new(),
                key_config: (
                    1,
                    "MC4CAQAwBQYDK2VwBCIEICHl/V9wxvENDJKePwusDhnC7xgaHYV6iHLb0ENJZndj".to_string(),
                    "MCowBQYDK2VwAyEA2YfYTgb8Y0LTFr+2Rm2Fkdu38eJTfnsMDH2iZHErBH0=".to_string(),
                ),
                socket_addr: SocketAddr::from_str(&socket_addr).unwrap(),
                init_node: None,
            },
            Some(notify_clone),
        )
        .await
    });

    // Return grpc port
    (grpc_port, rest_port, notify)
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
