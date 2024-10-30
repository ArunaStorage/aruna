use super::{controller::Controller, request::SerializedResponse};
use crate::{error::ArunaError, logerr, transactions::request::WriteRequest};
use serde::Serialize;
use synevi::{SyneviError, Transaction};

#[derive(Debug, Clone, Serialize)]
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
        id: u128,
        transaction: ArunaTransaction,
    ) -> Result<SerializedResponse, ArunaError> {
        tracing::trace!(?id, "Deserializing transaction");
        tracing::debug!(transaction = ?transaction.0.len());
        let tx: Box<dyn WriteRequest> =
            bincode::deserialize(&transaction.0).inspect_err(logerr!())?;
        tx.execute(id, self).await
    }
}
