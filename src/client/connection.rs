use std::{
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use rmpv::Value;
use tokio::time::timeout;
use tracing::{debug, trace};

use crate::{
    ExecutorExt, Result,
    builder::ConnectionBuilder,
    client::{Executor, Stream, Transaction, TransactionBuilder, sql::SqlStatementCache},
    codec::{
        consts::TransactionIsolationLevel,
        request::{EncodedRequest, Request},
        response::ResponseBody,
    },
    transport::{DispatcherSender, RequestSender},
};

/// Connection to Tarantool instance.
///
/// This type doesn't represent single TCP connection, but rather an abstraction
/// for interaction with Tarantool instance.
///
/// Underling implemenation could reconnect automatically (depending on builder configuration),
/// and could do pooling in the future (not yet implemented!).
///
/// Plain requests go to whichever connection is current, so they survive a
/// reconnect. A request that depends on server session state does not: a
/// [`Stream`] or [`Transaction`] created before the connection was lost fails
/// with [`Error::ConnectionReset`][crate::Error::ConnectionReset] instead of
/// silently rebinding to the new connection, and a statement from
/// [`ExecutorExt::prepare_sql`] fails with the server's error (see
/// [`PreparedSqlStatement`][crate::PreparedSqlStatement]).
/// [`ExecutorExt::execute_sql`] handles its own statement cache and needs
/// nothing from the caller.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    dispatcher_sender: DispatcherSender,
    // TODO: change how stream id assigned when dispatcher have more than one connection
    next_stream_id: AtomicU32,
    /// Sync of the next request, 64-bit as IPROTO carries it. The counter
    /// never wraps in practice, so a sync is never handed to a new request
    /// while an older request with the same sync may still run on the server.
    next_sync: AtomicU64,
    timeout: Option<Duration>,
    transaction_timeout_secs: Option<f64>,
    transaction_isolation_level: TransactionIsolationLevel,
    async_rt_handle: tokio::runtime::Handle,
    sql_statement_cache: Option<SqlStatementCache>,
}

impl Connection {
    /// Create new [`ConnectionBuilder`].
    #[must_use]
    pub fn builder() -> ConnectionBuilder {
        ConnectionBuilder::default()
    }

    pub(crate) fn new(
        dispatcher_sender: DispatcherSender,
        timeout: Option<Duration>,
        transaction_timeout: Option<Duration>,
        transaction_isolation_level: TransactionIsolationLevel,
        sql_statement_cache_capacity: usize,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectionInner {
                dispatcher_sender,
                // TODO: check if 0 is valid value
                next_stream_id: AtomicU32::new(1),
                next_sync: AtomicU64::new(1),
                timeout,
                transaction_timeout_secs: transaction_timeout.as_ref().map(Duration::as_secs_f64),
                transaction_isolation_level,
                // NOTE: Safety: this method can be called only in async tokio context (because it
                // is called only from ConnectionBuilder).
                async_rt_handle: tokio::runtime::Handle::current(),
                sql_statement_cache: NonZeroUsize::new(sql_statement_cache_capacity)
                    .map(SqlStatementCache::new),
            }),
        }
    }

    /// Send a request from a background task and drop its response.
    ///
    /// The task holds only the dispatcher's request handle and the request
    /// timeout, never a `Connection` and never the liveness receiver, so it
    /// cannot keep the dispatcher alive: once the last client handle is gone,
    /// the dispatcher closes the connection and the request gets
    /// `ConnectionClosed`.
    #[allow(clippy::let_underscore_future)]
    pub(crate) fn send_request_sync_and_forget(
        &self,
        body: &impl Request,
        stream_id: Option<u32>,
        generation: Option<u64>,
    ) {
        let req = EncodedRequest::new(body, stream_id).map(|mut req| {
            *req.sync_mut() = self.next_sync();
            req
        });
        let requests = self.inner.dispatcher_sender.requests().clone();
        let request_timeout = self.inner.timeout;
        let _ = self.inner.async_rt_handle.spawn(async move {
            let res = match req {
                Ok(req) => send_with_timeout(&requests, req, generation, request_timeout).await,
                Err(err) => Err(err.into()),
            };
            debug!("Response for background request: {:?}", res);
        });
    }

    // TODO: maybe other Ordering??
    pub(crate) fn next_stream_id(&self) -> u32 {
        let next = self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed);
        if next != 0 {
            next
        } else {
            self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed)
        }
    }

    /// Allocate the sync for the next request.
    pub(crate) fn next_sync(&self) -> u64 {
        self.inner.next_sync.fetch_add(1, Ordering::Relaxed)
    }

    /// Send `request` with a client-assigned sync.
    ///
    /// `generation` is the value a `Stream` or `Transaction` captured at
    /// creation, `None` for plain requests. The transport answers a request
    /// whose generation is no longer current with
    /// [`Error::ConnectionReset`][crate::Error::ConnectionReset] instead of
    /// sending it, which also covers requests that were already waiting in the
    /// dispatcher queue when a reconnect happened.
    ///
    /// Dropping the returned future once the request is queued, for example
    /// on the request timeout, cancels the request in the transport.
    pub(crate) async fn send_with_generation(
        &self,
        mut request: EncodedRequest,
        generation: Option<u64>,
    ) -> Result<Value> {
        *request.sync_mut() = self.next_sync();
        send_with_timeout(
            self.inner.dispatcher_sender.requests(),
            request,
            generation,
            self.inner.timeout,
        )
        .await
    }

    /// Generation of the underlying transport connection.
    pub(crate) fn generation(&self) -> u64 {
        self.inner.dispatcher_sender.generation()
    }

    pub(crate) fn stream(&self) -> Stream {
        Stream::new(self.clone())
    }

    /// Create transaction, overriding default connection's parameters.
    pub(crate) fn transaction_builder(&self) -> TransactionBuilder {
        TransactionBuilder::new(
            self.clone(),
            self.inner.transaction_timeout_secs,
            self.inner.transaction_isolation_level,
        )
    }

    /// Create transaction.
    pub(crate) async fn transaction(&self) -> Result<Transaction> {
        self.transaction_builder().begin().await
    }

    /// Get prepared statement id from cache (if it is enabled).
    ///
    /// If statement not present in cache, then prepare statement and put it
    /// to cache.
    ///
    /// Only one statement can be prepared at the time. All other will immediately
    /// return None, when there is already a statement being prepared. Eventually
    /// all statements should be allowed to prepare.
    // The prepare lock is only taken with `try_lock`, so holding it across
    // `await` never blocks other tasks.
    #[allow(clippy::await_holding_lock)]
    async fn get_cached_sql_statement_id_inner(&self, statement: &str) -> Option<u64> {
        let cache = self.inner.sql_statement_cache.as_ref()?;
        let generation = self.generation();
        if let Some(stmt_id) = cache.get(statement, generation) {
            return Some(stmt_id);
        }

        // Only one statement is prepared at a time; the other callers send
        // their text.
        let _preparing = cache.preparing.try_lock()?;
        let stmt_id = match self.prepare_sql(statement).await {
            Ok(x) => {
                let stmt_id = x.stmt_id();
                trace!(statement, "Statement prepared with id {stmt_id}");
                stmt_id
            }
            Err(err) => {
                debug!("Failed to prepare statement for cache: {:#}", err);
                return None;
            }
        };
        // The id belongs to the session the PREPARE ran on. If the connection
        // was lost meanwhile, the id is useless: send the text instead.
        if self.generation() != generation {
            return None;
        }
        cache.put(statement, stmt_id, generation).then_some(stmt_id)
    }
}

/// Queue `request` and wait for its response body, at most `request_timeout`.
/// A timeout drops the request's future, which cancels the request.
async fn send_with_timeout(
    requests: &RequestSender,
    request: EncodedRequest,
    generation: Option<u64>,
    request_timeout: Option<Duration>,
) -> Result<Value> {
    let fut = requests.send(request, generation);
    let resp = match request_timeout {
        Some(x) => timeout(x, fut).await??,
        None => fut.await?,
    };
    match resp.body {
        ResponseBody::Ok(x) => Ok(x),
        ResponseBody::Error(x) => Err(x.into()),
    }
}

#[async_trait]
impl Executor for Connection {
    async fn send_encoded_request(&self, request: EncodedRequest) -> Result<Value> {
        self.send_with_generation(request, None).await
    }

    fn stream(&self) -> Stream {
        self.stream()
    }

    fn transaction_builder(&self) -> TransactionBuilder {
        self.transaction_builder()
    }

    async fn transaction(&self) -> Result<Transaction> {
        self.transaction().await
    }

    async fn get_cached_sql_statement_id(&self, statement: &str) -> Option<u64> {
        self.get_cached_sql_statement_id_inner(statement).await
    }

    fn evict_cached_sql_statement(&self, statement: &str, stmt_id: u64) {
        if let Some(cache) = &self.inner.sql_statement_cache {
            cache.evict(statement, stmt_id);
        }
    }
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connection")
    }
}

#[cfg(test)]
mod tests {
    use std::{
        pin::pin,
        task::{Context, Waker},
    };

    use parking_lot::Mutex;
    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        codec::consts::RequestType,
        codec::consts::error_codes::{ER_SQL_EXECUTE, ER_WRONG_QUERY_ID},
        codec::response::Response,
        errors::{Error, ErrorResponse, TransactionError},
        transport::ClientRequest,
    };

    fn test_connection(
        tx: mpsc::Sender<ClientRequest>,
        generation: Arc<AtomicU64>,
        request_timeout: Option<Duration>,
    ) -> Connection {
        test_connection_with_cancels(tx, generation, request_timeout).0
    }

    /// [`test_connection`] that also returns the receiver of the cancels its
    /// requests send when they are dropped.
    fn test_connection_with_cancels(
        tx: mpsc::Sender<ClientRequest>,
        generation: Arc<AtomicU64>,
        request_timeout: Option<Duration>,
    ) -> (Connection, mpsc::UnboundedReceiver<u64>) {
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();
        let conn = Connection::new(
            DispatcherSender::new_for_test(tx, cancel_tx, generation),
            request_timeout,
            None,
            TransactionIsolationLevel::default(),
            10,
        );
        (conn, cancel_rx)
    }

    fn ok_body(_request_type: u8) -> ResponseBody {
        ResponseBody::Ok(Value::Map(Vec::new()))
    }

    /// OK body of a PREPARE reply with statement id `id`.
    fn stmt_id_body(id: u64) -> ResponseBody {
        ResponseBody::Ok(Value::Map(vec![(
            Value::from(crate::codec::consts::keys::SQL_STMT_ID),
            Value::from(id),
        )]))
    }

    /// Answers every request OK; a `Prepare` request gets a fixed
    /// `SQL_STMT_ID` so the response decodes into a prepared statement.
    fn prepare_ok_body(request_type: u8) -> ResponseBody {
        if request_type == RequestType::Prepare as u8 {
            stmt_id_body(42)
        } else {
            ok_body(request_type)
        }
    }

    /// Reply for the eviction tests: PREPARE gets id 42, the `failing`th
    /// EXECUTE gets Tarantool error `code`, everything else is OK.
    fn fail_nth_execute(failing: usize, code: u32) -> impl FnMut(u8) -> ResponseBody + Send {
        let mut executes = 0;
        move |request_type| {
            if request_type == RequestType::Execute as u8 {
                executes += 1;
                if executes == failing {
                    return ResponseBody::Error(ErrorResponse::new(code, "rejected".into(), None));
                }
            }
            prepare_ok_body(request_type)
        }
    }

    /// The PREPARE and EXECUTE requests the fake dispatcher saw, in order.
    fn sql_requests(seen: &Mutex<Vec<Seen>>) -> Vec<&'static str> {
        seen.lock()
            .iter()
            .filter_map(|&(request_type, ..)| {
                if request_type == RequestType::Prepare as u8 {
                    Some("PREPARE")
                } else if request_type == RequestType::Execute as u8 {
                    Some("EXECUTE")
                } else {
                    None
                }
            })
            .collect()
    }

    /// Answer `request`, a PREPARE, with statement id `id`.
    fn answer_prepare(request: ClientRequest, id: u64) {
        assert_eq!(
            request.request.request_type as u8,
            RequestType::Prepare as u8
        );
        let _ = request.responder.send(Ok(Response {
            sync: request.request.sync,
            schema_version: 1,
            body: stmt_id_body(id),
        }));
    }

    fn reject_commit_and_rollback(request_type: u8) -> ResponseBody {
        if request_type == RequestType::Commit as u8 || request_type == RequestType::Rollback as u8
        {
            ResponseBody::Error(ErrorResponse::new(1, "rejected".into(), None))
        } else {
            ok_body(request_type)
        }
    }

    /// `(request_type, stream_id, sync, generation)` of one request the fake
    /// dispatcher received.
    type Seen = (u8, Option<u32>, u64, Option<u64>);

    /// Fake dispatcher: answers every request with `reply(request_type)` and
    /// records every request it receives. `reply` may keep state, so one test
    /// can answer the same request type differently over time.
    fn spawn_fake_dispatcher(
        mut rx: mpsc::Receiver<ClientRequest>,
        mut reply: impl FnMut(u8) -> ResponseBody + Send + 'static,
    ) -> Arc<Mutex<Vec<Seen>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_by_task = seen.clone();
        tokio::spawn(async move {
            while let Some(ClientRequest {
                request,
                generation,
                responder,
            }) = rx.recv().await
            {
                let request_type = request.request_type as u8;
                seen_by_task.lock().push((
                    request_type,
                    request.stream_id,
                    request.sync,
                    generation,
                ));
                let _ = responder.send(Ok(Response {
                    sync: request.sync,
                    schema_version: 1,
                    body: reply(request_type),
                }));
            }
        });
        seen
    }

    /// `(request_type, stream_id)` of every request the fake dispatcher saw.
    fn kinds(seen: &Mutex<Vec<Seen>>) -> Vec<(u8, Option<u32>)> {
        seen.lock()
            .iter()
            .map(|&(request_type, stream_id, ..)| (request_type, stream_id))
            .collect()
    }

    #[tokio::test]
    async fn failed_commit_finishes_the_transaction() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, reject_commit_and_rollback);

        let transaction = conn.transaction().await.unwrap();
        let res = transaction.commit().await;
        assert!(
            matches!(
                res,
                Err(TransactionError {
                    error: Error::Response(_),
                    ..
                })
            ),
            "{res:?}"
        );

        drop(res);
        // Give a drop-time rollback, if any, the chance to reach the dispatcher.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            kinds(&seen),
            vec![
                (RequestType::Begin as u8, Some(1)),
                (RequestType::Commit as u8, Some(1)),
            ]
        );
    }

    #[tokio::test]
    async fn failed_rollback_finishes_the_transaction() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, reject_commit_and_rollback);

        let transaction = conn.transaction().await.unwrap();
        let res = transaction.rollback().await;
        assert!(
            matches!(
                res,
                Err(TransactionError {
                    error: Error::Response(_),
                    ..
                })
            ),
            "{res:?}"
        );

        drop(res);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            kinds(&seen),
            vec![
                (RequestType::Begin as u8, Some(1)),
                (RequestType::Rollback as u8, Some(1)),
            ]
        );
    }

    /// `(request_type, stream_id)` of the next queued request, left
    /// unanswered.
    async fn next_request(rx: &mut mpsc::Receiver<ClientRequest>) -> (u8, Option<u32>) {
        let ClientRequest { request, .. } = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("no request within 1 s")
            .expect("the queue is closed");
        (request.request_type as u8, request.stream_id)
    }

    /// Answer the next queued request OK and return its
    /// `(request_type, stream_id)`.
    async fn answer_next(rx: &mut mpsc::Receiver<ClientRequest>) -> (u8, Option<u32>) {
        let ClientRequest {
            request, responder, ..
        } = timeout(Duration::from_secs(1), rx.recv())
            .await
            .expect("no request within 1 s")
            .expect("the queue is closed");
        let _ = responder.send(Ok(Response {
            sync: request.sync,
            schema_version: 1,
            body: ok_body(0),
        }));
        (request.request_type as u8, request.stream_id)
    }

    /// A transaction on stream 1 of a connection with a 50 ms request timeout
    /// and a queue of `capacity` that only the test reads; the test answered
    /// its BEGIN.
    async fn begun_transaction(
        capacity: usize,
    ) -> (Connection, Transaction, mpsc::Receiver<ClientRequest>) {
        let (tx, mut rx) = mpsc::channel(capacity);
        let conn = test_connection(tx, Arc::default(), Some(Duration::from_millis(50)));
        let (transaction, begin) = tokio::join!(conn.transaction(), answer_next(&mut rx));
        assert_eq!(begin, (RequestType::Begin as u8, Some(1)));
        (conn, transaction.unwrap(), rx)
    }

    const COMMIT: (u8, Option<u32>) = (RequestType::Commit as u8, Some(1));
    const ROLLBACK: (u8, Option<u32>) = (RequestType::Rollback as u8, Some(1));

    #[tokio::test]
    async fn commit_timed_out_while_queued_rolls_back_when_the_error_drops() {
        let (_conn, transaction, mut rx) = begun_transaction(8).await;

        let err = transaction.commit().await.unwrap_err();
        assert!(matches!(err.error, Error::Timeout), "{:?}", err.error);
        // The COMMIT sits in the queue, unanswered.
        assert_eq!(next_request(&mut rx).await, COMMIT);

        drop(err);
        assert_eq!(next_request(&mut rx).await, ROLLBACK);
    }

    #[tokio::test]
    async fn commit_timed_out_while_queued_can_be_committed_again() {
        let (_conn, transaction, mut rx) = begun_transaction(8).await;
        let err = transaction.commit().await.unwrap_err();
        assert_eq!(next_request(&mut rx).await, COMMIT);

        let (res, retried) = tokio::join!(err.transaction.commit(), answer_next(&mut rx));
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(retried, COMMIT);
        // Committed: nothing follows, not even a drop-time ROLLBACK.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "a request followed the commit");
    }

    #[tokio::test]
    async fn commit_timed_out_waiting_for_queue_capacity_rolls_back_when_the_error_drops() {
        let (conn, transaction, mut rx) = begun_transaction(1).await;
        // Another request takes the only slot of the queue.
        let mut filler = conn.ping();
        assert!(futures::poll!(filler.as_mut()).is_pending());

        let err = transaction.commit().await.unwrap_err();
        assert!(matches!(err.error, Error::Timeout), "{:?}", err.error);
        drop(err);

        // Once capacity frees up, the ROLLBACK follows; the COMMIT never
        // entered the queue.
        assert_eq!(next_request(&mut rx).await, (RequestType::Ping as u8, None));
        assert_eq!(next_request(&mut rx).await, ROLLBACK);
        drop(filler);
    }

    #[tokio::test]
    async fn commit_timed_out_waiting_for_queue_capacity_can_be_committed_again() {
        let (conn, transaction, mut rx) = begun_transaction(1).await;
        let mut filler = conn.ping();
        assert!(futures::poll!(filler.as_mut()).is_pending());
        let err = transaction.commit().await.unwrap_err();

        let (res, (first, retried)) = tokio::join!(err.transaction.commit(), async {
            (next_request(&mut rx).await, answer_next(&mut rx).await)
        });
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(first, (RequestType::Ping as u8, None));
        assert_eq!(retried, COMMIT);
        drop(filler);
    }

    #[tokio::test]
    async fn rollback_timed_out_while_queued_rolls_back_when_the_error_drops() {
        let (_conn, transaction, mut rx) = begun_transaction(8).await;

        let err = transaction.rollback().await.unwrap_err();
        assert!(matches!(err.error, Error::Timeout), "{:?}", err.error);
        assert_eq!(next_request(&mut rx).await, ROLLBACK);

        drop(err);
        assert_eq!(next_request(&mut rx).await, ROLLBACK);
    }

    #[tokio::test]
    async fn rollback_timed_out_while_queued_can_be_rolled_back_again() {
        let (_conn, transaction, mut rx) = begun_transaction(8).await;
        let err = transaction.rollback().await.unwrap_err();
        assert_eq!(next_request(&mut rx).await, ROLLBACK);

        let (res, retried) = tokio::join!(err.transaction.rollback(), answer_next(&mut rx));
        assert!(res.is_ok(), "{res:?}");
        assert_eq!(retried, ROLLBACK);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(rx.try_recv().is_err(), "a request followed the rollback");
    }

    #[tokio::test]
    async fn transaction_error_converted_into_error_rolls_back() {
        let (_conn, transaction, mut rx) = begun_transaction(8).await;
        let err = transaction.commit().await.unwrap_err();
        assert_eq!(next_request(&mut rx).await, COMMIT);

        let err: Error = err.into();
        assert!(matches!(err, Error::Timeout), "{err:?}");
        assert_eq!(next_request(&mut rx).await, ROLLBACK);
    }

    #[tokio::test]
    async fn dropped_open_transaction_rolls_back() {
        let (_conn, transaction, mut rx) = begun_transaction(8).await;
        drop(transaction);
        assert_eq!(next_request(&mut rx).await, ROLLBACK);
    }

    #[tokio::test]
    async fn transaction_finished_by_a_server_error_sends_nothing_more() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, reject_commit_and_rollback);

        let transaction = conn.transaction().await.unwrap();
        let err = transaction.commit().await.unwrap_err();
        assert!(matches!(err.error, Error::Response(_)), "{:?}", err.error);

        // Sent, a retried COMMIT would succeed on a stream without a
        // transaction, and a ping would run outside one: both fail unsent.
        let retried = err.transaction.commit().await.unwrap_err();
        assert!(
            matches!(retried.error, Error::Other(_)),
            "{:?}",
            retried.error
        );
        let res = retried.transaction.ping().await;
        assert!(matches!(res, Err(Error::Other(_))), "{res:?}");
        drop(retried);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            kinds(&seen),
            vec![(RequestType::Begin as u8, Some(1)), COMMIT]
        );
    }

    #[tokio::test]
    async fn transaction_finished_by_a_server_error_sends_no_sql() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, |request_type| {
            if request_type == RequestType::Commit as u8 {
                reject_commit_and_rollback(request_type)
            } else {
                prepare_ok_body(request_type)
            }
        });

        let transaction = conn.transaction().await.unwrap();
        let err = transaction.commit().await.unwrap_err();
        assert!(matches!(err.error, Error::Response(_)), "{:?}", err.error);

        // Neither call may reach the statement cache, whose lookup would
        // send a PREPARE for a transaction that can do nothing.
        let res = err.transaction.execute_sql("SELECT 1", ()).await;
        assert!(matches!(res, Err(Error::Other(_))), "{res:?}");
        let res = err.transaction.prepare_sql("SELECT 1").await;
        assert!(matches!(res, Err(Error::Other(_))), "{res:?}");
        drop(err);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            kinds(&seen),
            vec![(RequestType::Begin as u8, Some(1)), COMMIT]
        );
    }

    #[tokio::test]
    async fn requests_carry_client_assigned_syncs() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, ok_body);

        conn.ping().await.unwrap();
        conn.ping().await.unwrap();

        let syncs: Vec<u64> = seen.lock().iter().map(|&(_, _, sync, _)| sync).collect();
        assert_eq!(syncs, vec![1, 2]);
    }

    #[tokio::test]
    async fn next_sync_is_64_bit() {
        let (tx, _rx) = mpsc::channel(1);
        let conn = test_connection(tx, Arc::default(), None);
        conn.inner
            .next_sync
            .store(u64::from(u32::MAX), Ordering::Relaxed);
        assert_eq!(conn.next_sync(), u64::from(u32::MAX));
        // Past u32::MAX the counter keeps counting instead of wrapping to 0.
        assert_eq!(conn.next_sync(), u64::from(u32::MAX) + 1);
    }

    #[tokio::test]
    async fn timed_out_request_cancels_its_sync() {
        let (tx, mut rx) = mpsc::channel(8);
        let (conn, mut cancels) =
            test_connection_with_cancels(tx, Arc::default(), Some(Duration::from_millis(50)));

        let dispatcher = async {
            // Hold the request, and with it its responder, so it never
            // completes.
            let request = rx.recv().await.expect("expected the request first");
            let cancelled = timeout(Duration::from_secs(1), cancels.recv())
                .await
                .expect("no cancel after the timeout");
            (request.request.sync, cancelled)
        };
        let (res, (sent, cancelled)) = tokio::join!(conn.ping(), dispatcher);

        assert!(matches!(res, Err(Error::Timeout)), "{res:?}");
        assert_eq!(cancelled, Some(sent));
    }

    #[tokio::test]
    async fn dropped_request_future_cancels_its_sync() {
        let (tx, mut rx) = mpsc::channel(8);
        let (conn, mut cancels) = test_connection_with_cancels(tx, Arc::default(), None);

        let mut ping = conn.ping();
        assert!(futures::poll!(ping.as_mut()).is_pending());
        let request = rx.try_recv().expect("the request was queued");
        drop(ping);

        assert_eq!(cancels.try_recv(), Ok(request.request.sync));
    }

    #[tokio::test]
    async fn future_dropped_while_waiting_for_queue_capacity_sends_no_cancel() {
        let (tx, mut rx) = mpsc::channel(1);
        let (conn, mut cancels) = test_connection_with_cancels(tx, Arc::default(), None);

        // The first request takes the only slot of the queue.
        let mut first = conn.ping();
        assert!(futures::poll!(first.as_mut()).is_pending());
        let mut second = conn.ping();
        assert!(futures::poll!(second.as_mut()).is_pending());
        drop(second);

        assert!(
            cancels.try_recv().is_err(),
            "a request that never entered the queue was cancelled"
        );
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_err(), "the second request was queued");
        drop(first);
    }

    #[tokio::test]
    async fn stream_requests_carry_their_generation() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::new(AtomicU64::new(3)), None);
        let seen = spawn_fake_dispatcher(rx, ok_body);

        conn.stream().ping().await.unwrap();
        conn.ping().await.unwrap();

        // The stream request carries the generation it was issued under; the
        // plain request carries none and is never rejected for staleness.
        let generations: Vec<(bool, Option<u64>)> = seen
            .lock()
            .iter()
            .map(|&(_, stream_id, _, generation)| (stream_id.is_some(), generation))
            .collect();
        assert_eq!(generations, vec![(true, Some(3)), (false, None)]);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn sql_statement_is_not_prepared_while_another_is_in_flight() {
        let (tx, mut rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);

        let preparing = conn
            .inner
            .sql_statement_cache
            .as_ref()
            .unwrap()
            .preparing
            .lock();
        let res = timeout(
            Duration::from_secs(1),
            conn.get_cached_sql_statement_id_inner("SELECT 1"),
        )
        .await
        .expect("did not return immediately while another statement is being prepared");
        drop(preparing);

        assert_eq!(res, None);
        assert!(rx.try_recv().is_err(), "PREPARE request was sent");
    }

    #[tokio::test]
    async fn stale_stream_fails_with_connection_reset() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, ok_body);

        let stream = conn.stream();
        stream.ping().await.unwrap();
        generation.fetch_add(1, Ordering::SeqCst);

        let res = stream.ping().await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        assert_eq!(seen.lock().len(), 1, "the stale request was sent");
    }

    #[tokio::test]
    async fn stale_transaction_fails_and_skips_drop_rollback() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, ok_body);

        let transaction = conn.transaction().await.unwrap();
        generation.fetch_add(1, Ordering::SeqCst);

        let res = transaction.ping().await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        drop(transaction);
        // Give a drop-time rollback, if any, the chance to reach the dispatcher.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(kinds(&seen), vec![(RequestType::Begin as u8, Some(1))]);
    }

    #[tokio::test]
    async fn stale_stream_sends_no_sql() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, prepare_ok_body);
        let stream = conn.stream();
        generation.fetch_add(1, Ordering::SeqCst);

        // The cache lookup would send a PREPARE on the new session.
        let res = stream.execute_sql("SELECT 1", ()).await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        let res = stream.prepare_sql("SELECT 1").await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(kinds(&seen), vec![]);
    }

    #[tokio::test]
    async fn stale_transaction_sends_no_sql() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, prepare_ok_body);
        let transaction = conn.transaction().await.unwrap();
        generation.fetch_add(1, Ordering::SeqCst);

        let res = transaction.execute_sql("SELECT 1", ()).await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        let res = transaction.prepare_sql("SELECT 1").await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        drop(transaction);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(kinds(&seen), vec![(RequestType::Begin as u8, Some(1))]);
    }

    #[tokio::test]
    async fn plain_request_ignores_generation() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, ok_body);

        generation.fetch_add(1, Ordering::SeqCst);

        conn.ping().await.unwrap();
        assert_eq!(kinds(&seen), vec![(RequestType::Ping as u8, None)]);
    }

    #[tokio::test]
    async fn cached_sql_statement_id_is_invalidated_after_reconnect() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, prepare_ok_body);
        let prepares = || {
            seen.lock()
                .iter()
                .filter(|(request_type, ..)| *request_type == RequestType::Prepare as u8)
                .count()
        };

        let stmt_id = conn
            .get_cached_sql_statement_id_inner("SELECT 1")
            .await
            .expect("first lookup prepares and caches the statement");
        assert_eq!(prepares(), 1);

        // Cached: a second lookup for the same text does not send PREPARE
        // again while the generation is unchanged.
        assert_eq!(
            conn.get_cached_sql_statement_id_inner("SELECT 1").await,
            Some(stmt_id)
        );
        assert_eq!(prepares(), 1);

        // A reconnect invalidates every cached id (Tarantool checks
        // prepared-statement ids per session), so the same statement text
        // triggers a new PREPARE instead of reusing the stale id.
        generation.fetch_add(1, Ordering::SeqCst);
        conn.get_cached_sql_statement_id_inner("SELECT 1")
            .await
            .expect("statement is re-prepared after a reconnect");
        assert_eq!(prepares(), 2, "cached statement id survived a reconnect");
    }

    /// Poll a lookup of "SELECT 1" once and check that it sent a PREPARE and
    /// waits for the reply. A lookup that returns at once fails the check
    /// with `reason`, which says what went wrong.
    fn assert_next_lookup_prepares(
        conn: &Connection,
        rx: &mut mpsc::Receiver<ClientRequest>,
        reason: &str,
    ) {
        let mut cx = Context::from_waker(Waker::noop());
        let mut lookup = pin!(conn.get_cached_sql_statement_id_inner("SELECT 1"));
        let poll = lookup.as_mut().poll(&mut cx);
        assert!(poll.is_pending(), "{reason}: the lookup returned {poll:?}");
        let request = rx.try_recv().expect("the pending lookup sent nothing");
        assert_eq!(
            request.request.request_type as u8,
            RequestType::Prepare as u8
        );
    }

    /// V03: lookup A sends PREPARE at generation 0, and the connection is
    /// lost before A resumes. With `concurrent_lookup`, task B looks up
    /// another text in between and relabels the cache (the persistent form);
    /// without it, A resumes first (the one-shot variant).
    async fn assert_id_prepared_on_a_lost_session_is_not_used(concurrent_lookup: bool) {
        let (tx, mut rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let mut cx = Context::from_waker(Waker::noop());

        // Task A looks up "SELECT 1" at generation 0 and sends PREPARE; the
        // old session answers 42, which wakes A without polling it.
        let mut a = pin!(conn.get_cached_sql_statement_id_inner("SELECT 1"));
        assert!(a.as_mut().poll(&mut cx).is_pending());
        answer_prepare(rx.try_recv().unwrap(), 42);
        // The connection is lost.
        generation.fetch_add(1, Ordering::SeqCst);

        if concurrent_lookup {
            // Task B relabels the cache, finds the prepare lock taken and
            // sends nothing.
            assert_eq!(
                conn.get_cached_sql_statement_id_inner("SELECT 2").await,
                None
            );
            assert!(rx.try_recv().is_err());
        }

        // A neither returns nor caches the id of the lost session.
        assert_eq!(a.await, None);
        assert_next_lookup_prepares(&conn, &mut rx, "the stale id 42 was served");
    }

    #[tokio::test]
    async fn id_prepared_on_a_lost_session_is_not_cached() {
        assert_id_prepared_on_a_lost_session_is_not_used(true).await;
    }

    #[tokio::test]
    async fn id_prepared_on_a_lost_session_is_not_returned() {
        assert_id_prepared_on_a_lost_session_is_not_used(false).await;
    }

    /// Three `execute_sql` calls of one text, the second EXECUTE rejected
    /// with `code`: the second call returns that error after a single
    /// EXECUTE, and the third prepares the statement again.
    async fn assert_rejected_statement_is_evicted_without_a_retry(code: u32) {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, fail_nth_execute(2, code));

        conn.execute_sql("SELECT 1", ()).await.unwrap();
        let res = conn.execute_sql("SELECT 1", ()).await;
        assert!(
            matches!(res, Err(Error::Response(ref err)) if err.code == code),
            "{res:?}"
        );
        conn.execute_sql("SELECT 1", ()).await.unwrap();

        assert_eq!(
            sql_requests(&seen),
            ["PREPARE", "EXECUTE", "EXECUTE", "PREPARE", "EXECUTE"]
        );
    }

    #[tokio::test]
    async fn statement_rejected_with_sql_execute_is_evicted_without_a_retry() {
        assert_rejected_statement_is_evicted_without_a_retry(ER_SQL_EXECUTE).await;
    }

    #[tokio::test]
    async fn statement_rejected_with_wrong_query_id_is_evicted_without_a_retry() {
        assert_rejected_statement_is_evicted_without_a_retry(ER_WRONG_QUERY_ID).await;
    }

    #[tokio::test]
    async fn unrelated_server_error_keeps_the_cached_statement() {
        // 3 is ER_TUPLE_FOUND.
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, fail_nth_execute(2, 3));

        conn.execute_sql("SELECT 1", ()).await.unwrap();
        assert!(conn.execute_sql("SELECT 1", ()).await.is_err());
        conn.execute_sql("SELECT 1", ()).await.unwrap();

        assert_eq!(
            sql_requests(&seen),
            ["PREPARE", "EXECUTE", "EXECUTE", "EXECUTE"]
        );
    }

    #[tokio::test]
    async fn statement_evicted_through_a_stream_leaves_the_connection_cache() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, fail_nth_execute(2, ER_SQL_EXECUTE));
        let stream = conn.stream();

        conn.execute_sql("SELECT 1", ()).await.unwrap();
        assert!(stream.execute_sql("SELECT 1", ()).await.is_err());
        conn.execute_sql("SELECT 1", ()).await.unwrap();

        assert_eq!(
            sql_requests(&seen),
            ["PREPARE", "EXECUTE", "EXECUTE", "PREPARE", "EXECUTE"]
        );
    }

    #[tokio::test]
    async fn statement_prepared_on_a_stream_fails_with_connection_reset_after_the_loss() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, prepare_ok_body);
        let stream = conn.stream();
        let statement = stream.prepare_sql("SELECT ?").await.unwrap();

        generation.fetch_add(1, Ordering::SeqCst);

        let res = statement.execute((1,)).await;
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
        assert_eq!(sql_requests(&seen), ["PREPARE"]);
    }

    #[tokio::test]
    async fn lookup_dropped_while_preparing_releases_the_prepare_lock() {
        let (tx, mut rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let mut cx = Context::from_waker(Waker::noop());

        {
            // A caller's timeout drops the lookup while its PREPARE is in
            // flight.
            let mut lookup = pin!(conn.get_cached_sql_statement_id_inner("SELECT 1"));
            assert!(lookup.as_mut().poll(&mut cx).is_pending());
        }
        let abandoned = rx.try_recv().expect("the first lookup sent PREPARE");
        drop(abandoned);

        // The next lookup prepares again instead of sending the text.
        assert_next_lookup_prepares(&conn, &mut rx, "the prepare lock stayed taken");
    }

    #[tokio::test]
    async fn stream_created_after_reconnect_is_not_stale() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let _seen = spawn_fake_dispatcher(rx, ok_body);

        generation.fetch_add(1, Ordering::SeqCst);

        conn.stream().ping().await.unwrap();
        let transaction = conn.transaction().await.unwrap();
        transaction.ping().await.unwrap();
        transaction.commit().await.unwrap();
    }
}
