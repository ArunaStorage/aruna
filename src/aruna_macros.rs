/// This macro creates a gRPC struct and implements the new method
///
/// ## Behaviour:
///
/// By default this macro contains the following fields:
///
/// database: Arc<Database>,
/// authz: Arc<Authz>,
///
/// It also includes a list of additional fields that can be specified.
///
/// ## Usage:
///
/// ```
/// # use aruna_server::*;
/// # use std::sync::Arc;
/// # use aruna_server::database::connection::Database;
/// # use aruna_server::server::services::authz::Authz;
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
/// # use aruna_server::database::connection::Database;
/// # use aruna_server::server::services::authz::Authz;
///
/// pub struct MyFieldsServiceImpl {
///     pub database: Arc<Database>,
///     pub authz: Arc<Authz>,
///     pub variable1: String,
///     pub variable2: String,
/// }
///
/// impl MyFieldsServiceImpl {
///     pub async fn new(database: Arc<Database>, authz: Arc<Authz>, variable1:String, variable2: String) -> Self {
///         MyFieldsServiceImpl {
///             database,
///             authz,
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

            pub database: Arc<Database>,
            pub authz: Arc<Authz>,
            $(
                pub $variable_name:$variable_type,
            )*
        }

        impl $struct_name {
            pub async fn new(database: Arc<Database>, authz: Arc<Authz>, $($variable_name:$variable_type,)*) -> Self {
                $struct_name {
                    database,
                    authz,
                    $(
                        $variable_name,
                    )*
                }
            }
        }
    };
}
