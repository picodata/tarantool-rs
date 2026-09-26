use std::{fmt, result::Result as StdResult, time::Duration};

use async_trait::async_trait;

use rmpv::Value;
use tracing::debug;

use super::{Connection, ExecutorExt, Stream};
use crate::{
    Error, Executor, Result,
    codec::{
        consts::TransactionIsolationLevel,
        request::{Begin, Commit, EncodedRequest, Request, Rollback},
    },
    errors::TransactionError,
};

/// Started transaction ([docs](https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-streams)).
///
/// If tranasction have a timeout and no requests made for that time, tranasction is automatically
/// rolled back.
///
/// On drop the transaction is rolled back, unless the server answered its
/// COMMIT or ROLLBACK, with success or an error, or the connection it began
/// on was lost: the server then discarded it together with that
/// connection's session.
pub struct Transaction {
    stream: Stream,
    /// Set once the server answered COMMIT or ROLLBACK: the transaction
    /// sends nothing more, not even on drop.
    finished: bool,
}

impl Transaction {
    async fn new(
        conn: Connection,
        timeout_secs: Option<f64>,
        isolation_level: TransactionIsolationLevel,
    ) -> Result<Self> {
        let this = Self {
            stream: Stream::new(conn),
            finished: false,
        };
        this.begin(isolation_level, timeout_secs).await?;
        Ok(this)
    }

    async fn begin(
        &self,
        transaction_isolation_level: TransactionIsolationLevel,
        timeout_secs: Option<f64>,
    ) -> Result<()> {
        debug!("Beginning tranasction on stream {}", self.stream.id);
        self.send_request(Begin::new(timeout_secs, transaction_isolation_level))
            .await
            .map(drop)
    }

    /// Commit tranasction.
    /// # Errors
    ///
    /// Every failure returns a [`TransactionError`] with the transaction in
    /// it. A server error ([`Error::Response`]) finishes the transaction: from
    /// then on every request through it, a retried COMMIT included, fails with
    /// [`Error::Other`] without being sent.
    ///
    /// Any other failure of an open transaction, such as [`Error::Timeout`] or
    /// [`Error::ConnectionClosed`], hands the transaction back unfinished, but
    /// the outcome of the COMMIT is unknown. It may have been applied, may
    /// have failed on the server, or may never have been sent. A retried
    /// COMMIT and a ROLLBACK, the one sent on drop included, succeed in every
    /// one of these cases, because Tarantool treats COMMIT and ROLLBACK
    /// without an active transaction as successful no-ops. So neither proves
    /// what happened. After a timeout, check the data or make the operation
    /// idempotent. After a lost connection the transaction is stale: a retry
    /// fails with [`Error::ConnectionReset`], and dropping it sends nothing.
    ///
    /// After [`Error::ConnectionReset`] the transaction is already gone with
    /// the connection it began on.
    pub async fn commit(self) -> StdResult<(), TransactionError> {
        debug!("Commiting tranasction on stream {}", self.stream.id);
        self.finish(Commit::default()).await
    }

    /// Rollback tranasction.
    /// # Errors
    ///
    /// Every failure returns a [`TransactionError`] with the transaction in
    /// it. A server error ([`Error::Response`]) finishes the transaction: from
    /// then on every request through it, a retried ROLLBACK included, fails
    /// with [`Error::Other`] without being sent.
    ///
    /// Any other failure of an open transaction, such as [`Error::Timeout`] or
    /// [`Error::ConnectionClosed`], hands the transaction back unfinished. The
    /// ROLLBACK may have been applied, or may never have been sent. Roll it
    /// back again, or drop it to roll it back: either succeeds in both cases,
    /// because Tarantool treats a ROLLBACK without an active transaction as a
    /// successful no-op. After a lost connection the transaction is stale and
    /// rolled back either way: a retry fails with [`Error::ConnectionReset`],
    /// and dropping it sends nothing.
    ///
    /// After [`Error::ConnectionReset`] the transaction is already gone with
    /// the connection it began on.
    pub async fn rollback(self) -> StdResult<(), TransactionError> {
        debug!("Rolling back tranasction on stream {}", self.stream.id);
        self.finish(Rollback::default()).await
    }

    /// Send COMMIT or ROLLBACK. Success and a server error finish the
    /// transaction; any other error hands the transaction back unfinished.
    async fn finish<R: Request>(mut self, request: R) -> StdResult<(), TransactionError> {
        match self.send_request(request).await {
            Ok(_) => {
                self.finished = true;
                Ok(())
            }
            Err(error) => {
                // The server answered, so the transaction is over either way.
                if matches!(error, Error::Response(_)) {
                    self.finished = true;
                }
                Err(TransactionError {
                    error,
                    transaction: self,
                })
            }
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        // The connection the transaction began on was lost: the server already
        // discarded the transaction together with that session.
        if self.stream.check_generation().is_err() {
            debug!(
                "Transaction on stream {} was lost with its connection, not rolling back",
                self.stream.id
            );
            return;
        }
        debug!(
            "Rolling back tranasction on stream {} (on drop)",
            self.stream.id
        );
        self.stream.conn.send_request_sync_and_forget(
            &Rollback::default(),
            Some(self.stream.id),
            Some(self.stream.generation),
        );
    }
}

#[async_trait]
impl Executor for Transaction {
    async fn send_encoded_request(&self, request: EncodedRequest) -> Result<Value> {
        if self.finished {
            // Only the transaction inside a `TransactionError` for a server
            // error gets here. Sent, a retried COMMIT would succeed on a stream
            // without a transaction, and any other request would run outside
            // one.
            return Err(Error::Other(anyhow::anyhow!(
                "Transaction is already finished"
            )));
        }
        self.stream.send_encoded_request(request).await
    }

    // TODO: do we need to repeat this in all ConnetionLike implementations?
    fn stream(&self) -> Stream {
        self.stream.conn.stream()
    }

    fn transaction_builder(&self) -> TransactionBuilder {
        self.stream.conn.transaction_builder()
    }

    async fn transaction(&self) -> Result<Transaction> {
        self.stream.conn.transaction().await
    }

    async fn get_cached_sql_statement_id(&self, statement: &str) -> Option<u64> {
        self.stream.get_cached_sql_statement_id(statement).await
    }
}

impl fmt::Debug for Transaction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("stream", &self.stream)
            .field("finished", &self.finished)
            .finish()
    }
}

/// Build transaction.
pub struct TransactionBuilder {
    connection: Connection,
    timeout_secs: Option<f64>,
    isolation_level: TransactionIsolationLevel,
}

impl TransactionBuilder {
    pub(crate) fn new(
        connection: Connection,
        timeout_secs: Option<f64>,
        isolation_level: TransactionIsolationLevel,
    ) -> Self {
        Self {
            connection,
            timeout_secs,
            isolation_level,
        }
    }

    pub fn timeout(&mut self, timeout: impl Into<Option<Duration>>) -> &mut Self {
        self.timeout_secs = timeout.into().as_ref().map(Duration::as_secs_f64);
        self
    }

    pub fn isolation_level(&mut self, isolation_level: TransactionIsolationLevel) -> &mut Self {
        self.isolation_level = isolation_level;
        self
    }

    /// # Errors
    ///
    /// Returns an error if the transaction could not be started.
    pub async fn begin(&self) -> Result<Transaction> {
        Transaction::new(
            self.connection.clone(),
            self.timeout_secs,
            self.isolation_level,
        )
        .await
    }
}
