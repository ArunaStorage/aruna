use std::sync::Arc;

use super::{controller::Controller, request::SerializedResponse};
use crate::{error::ArunaError, requests::request::WriteRequest};
use synevi::{SyneviError, Transaction};
use tracing::debug;

#[derive(Debug, Clone)]
pub struct ArunaTransaction(pub Vec<u8>);

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

impl Controller {
    pub async fn process_transaction(
        &self,
        transaction: ArunaTransaction,
    ) -> Result<SerializedResponse, ArunaError> {
        debug!("Started transaction");
        let tx: Box<dyn WriteRequest> = bincode::deserialize(&transaction.0)?;
        tx.execute(self).await
    }
}
