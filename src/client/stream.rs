use std::fmt;

use async_trait::async_trait;

use rmpv::Value;

use super::{Connection, Transaction, TransactionBuilder};
use crate::{Executor, Result, codec::request::EncodedRequest};

/// Abstraction, providing sequential processing of requests.
///
/// With streams there is a guarantee that the server instance will not handle the next request in a stream until it has completed the previous one ([docs](https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#binary-protocol-streams)).
///
/// A stream does not survive a reconnect of the underlying [`Connection`]:
/// its state lived only on the old server session, and any request made
/// through it after the connection it was created on is lost fails with
/// [`Error::ConnectionReset`][crate::Error::ConnectionReset] instead of
/// silently continuing on the new connection.
///
/// # Example
///
/// ```rust,compile
/// use tarantool_rs::{Connection, Executor, ExecutorExt};
/// # use futures::FutureExt;
/// # use rmpv::Value;
///
/// # async fn async_wrapper() {
/// let connection = Connection::builder().build("localhost:3301").await.unwrap();
///
/// // This will print 'fast' and then 'slow'
/// let eval_slow_fut = connection
///     .eval("fiber = require('fiber'); fiber.sleep(0.5); return ...;", ("slow", ))
///     .inspect(|res| println!("{:?}", res));
/// let eval_fast_fut = connection
///     .eval("return ...;", ("fast", ))
///     .inspect(|res| println!("{:?}", res));
/// let _ = tokio::join!(eval_slow_fut, eval_fast_fut);
///
/// // This will print 'slow' and then 'fast', since slow request was created first and have smaller sync
/// let stream = connection.stream();
/// let eval_slow_fut = stream
///     .eval("fiber = require('fiber'); fiber.sleep(0.5); return ...;", ("slow", ))
///     .inspect(|res| println!("{:?}", res));
/// let eval_fast_fut = stream
///     .eval("return ...;", ("fast", ))
///     .inspect(|res| println!("{:?}", res));
/// let _ = tokio::join!(eval_slow_fut, eval_fast_fut);
/// # }
/// ```

#[derive(Clone)]
pub struct Stream {
    conn: Connection,
    id: u32,
    generation: u64,
}

// TODO: convert stream to transaction and back
impl Stream {
    pub(crate) fn new(conn: Connection) -> Self {
        let id = conn.next_stream_id();
        let generation = conn.generation();
        Self {
            conn,
            id,
            generation,
        }
    }
}

#[async_trait]
impl Executor for Stream {
    async fn send_encoded_request(&self, mut request: EncodedRequest) -> Result<Value> {
        self.conn.check_generation(self.generation)?;
        request.stream_id = Some(self.id);
        self.conn
            .send_with_generation(request, Some(self.generation))
            .await
    }

    fn stream(&self) -> Stream {
        self.conn.stream()
    }

    fn transaction_builder(&self) -> TransactionBuilder {
        self.conn.transaction_builder()
    }

    async fn transaction(&self) -> Result<Transaction> {
        self.conn.transaction().await
    }

    async fn get_cached_sql_statement_id(&self, statement: &str) -> Option<u64> {
        self.conn.get_cached_sql_statement_id(statement).await
    }
}

impl fmt::Debug for Stream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Stream")
            .field("id", &self.id)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
