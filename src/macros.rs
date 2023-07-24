/// This macro creates a gRPC struct and implements the new method
///
/// ## Behaviour:
///
/// By default this macro expands to a struct that contains the following fields:
///
/// - database: Arc<Database>,
/// - authz: Arc<Authz>,
///
/// It also includes an (optional) list of n additional fields that can be specified with its type.
///
/// ## Usage:
///
/// ```
/// # use aruna_server::*;
/// # use std::sync::Arc;
/// # use aruna_server::database::connection::Database;
/// # use aruna_server::middlelayer::db_handler::DatabaseHandler;
/// # use crate::middlelayer::db_handler::DatabaseHandler
/// # use aruna_cache::notifications::NotificationCache;
/// # use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
///
/// // Without any additional argument (only name)
/// impl_grpc_server!(MyServiceImpl);
/// // Or with n additional fields
/// impl_grpc_server!(MyFieldsServiceImpl, variable1:String, variable2:String);
///
/// ```
///
/// The later will expand to the following code:
///
/// ```
/// # use aruna_server::*;
/// # use std::sync::Arc;
/// # use std::sync::Mutex;
/// # use aruna_cache::notifications::NotificationCache;
/// # use aruna_policy::ape::policy_evaluator::PolicyEvaluator;
/// # use aruna_server::middlelayer::db_handler::DatabaseHandler;
/// # use crate::middlelayer::db_handler::DatabaseHandler
///
/// pub struct MyFieldsServiceImpl {
///     pub database_handler: DatabaseHandler;
///     pub authorizer: Arc<PolicyEvaluator>,
///     pub cache: Arc<NotificationCache>,
///     pub variable1: String,
///     pub variable2: String,
/// }
///
/// impl MyFieldsServiceImpl {
///     pub async fn new(database_handler: DatabaseHandler, authorizer: Arc<PolicyEvaluator>, cache: Arc<NotificationCache>, variable1:String, variable2: String) -> Self {
///         MyFieldsServiceImpl {
///             database_handler,
///             authorizer,
///             cache,
///             variable1,
///             variable2,
///         }
///     }
/// }
/// ```
///
#[macro_export]
macro_rules! impl_grpc_server {
    ($struct_name:ident $(, $variable_name:ident:$variable_type:ty )*) => {
        pub struct $struct_name {
            pub database_handler: DatabaseHandler,
            pub authorizer: Arc<PolicyEvaluator>,
            pub cache: Arc<NotificationCache>,
            $(
                pub $variable_name:$variable_type,
            )*
        }

        impl $struct_name {
            pub async fn new(database_handler: DatabaseHandler, authorizer: Arc<PolicyEvaluator>, cache: Arc<NotificationCache>, $($variable_name:$variable_type,)*) -> Self {
                $struct_name {
                    database_handler,
                    authorizer,
                    cache,
                    $(
                        $variable_name,
                    )*
                }
            }
        }
    };
}

#[macro_export]
macro_rules! tonic_internal {
    ($result:expr, $message:expr) => {
        $result.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::internal($message)
        })?
    };
}

#[macro_export]
macro_rules! tonic_invalid {
    ($result:expr, $message:expr) => {
        $result.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::invalid_argument($message)
        })?
    };
}

#[macro_export]
macro_rules! tonic_auth {
    ($result:expr, $message:expr) => {
        $result.map_err(|e| {
            log::error!("{}", e);
            tonic::Status::unauthenticated($message)
        })?
    };
}

#[macro_export]
macro_rules! log_received {
    ($request:expr) => {
        log::info!("Recieved {}", crate::utils::type_name_of($request));
        log::debug!("{:?}", $request);
    };
}
