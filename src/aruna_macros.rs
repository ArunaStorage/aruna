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
