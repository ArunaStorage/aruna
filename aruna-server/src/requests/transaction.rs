use super::{controller::Controller, request::SerializedResponse};
use crate::{
    error::ArunaError,
    requests::{realm::CreateRealmRequestTx, request::WriteRequest},
};
use serde::{Deserialize, Serialize};
use synevi::{SyneviError, Transaction};
use tracing::debug;
use ulid::Ulid;

#[derive(Debug, Clone)]
pub struct ArunaTransaction(pub Vec<u8>);

impl ArunaTransaction {
    pub fn get_index(&self) -> Result<u16, ArunaError> {
        Ok(u16::from_be_bytes(self.0[0..2].try_into().map_err(
            |_| ArunaError::DeserializeError("Transaction index does not match".to_string()),
        )?))
    }
}

impl Transaction for ArunaTransaction {
    type TxErr = ArunaError;
    type TxOk = SerializedResponse;

    fn as_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized,
    {
        Ok(Self(bytes))
    }
}

pub const CREATE_REALM: u16 = 0;


impl_write_request!(
    CreateRealmRequestTx, 
    


)

impl Controller {
    pub async fn process_transaction(
        &self,
        transaction: ArunaTransaction,
    ) -> Result<SerializedResponse, ArunaError> {
        debug!("Started transaction");

        let index = transaction.get_index()?;

        match index {
            CREATE_REALM => {
                CreateRealmRequestTx::from_bytes(transaction.0)?
                    .execute(self)
                    .await
            }
            _ => unimplemented!(),
        }
    }
}


#[macro_export]
macro_rules! impl_write_request {
    ( $( $request:ident ),* ) => {
        $(
            impl IntoResponse<$request> for TransactionOk {
                fn into_response(self) -> Result<Response<$request>, tonic::Status> {
                    match self {
                        TransactionOk::$request(resp) => Ok(Response::new(resp)),
                        _ => Err(tonic::Status::internal("Invalid response type")),
                    }
                }
            }
        )*
    };
}
