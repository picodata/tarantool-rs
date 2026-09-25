use std::{
    fmt,
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use rmpv::Value;
use tokio::time::timeout;
use tracing::{debug, trace};

use crate::{
    Error, ExecutorExt, Result,
    builder::ConnectionBuilder,
    client::{Executor, Stream, Transaction, TransactionBuilder},
    codec::{
        consts::TransactionIsolationLevel,
        request::{ConnectionFeatures, EncodedRequest, Request},
        response::ResponseBody,
    },
    transport::DispatcherSender,
};

/// Connection to Tarantool instance.
///
/// This type doesn't represent single TCP connection, but rather an abstraction
/// for interaction with Tarantool instance.
///
/// Underling implemenation could reconnect automatically (depending on builder configuration),
/// and could do pooling in the future (not yet implemented!).
///
/// Automatic reconnect only ever affects plain, stateless requests sent
/// directly through a `Connection`. A [`Stream`] or [`Transaction`] created
/// from it does not survive a reconnect: its state lived only on the old
/// server session, so a request made through it afterwards fails with
/// [`Error::ConnectionReset`] instead of silently rebinding to the new
/// connection.
#[derive(Clone)]
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    dispatcher_sender: DispatcherSender,
    /// Written by the transport after every `IPROTO_ID` handshake.
    features: Arc<RwLock<ConnectionFeatures>>,
    // TODO: change how stream id assigned when dispatcher have more than one connection
    next_stream_id: AtomicU32,
    /// Sync of the next request. IPROTO only needs syncs to be unique among
    /// requests in flight on one TCP connection, which a single wrapping
    /// counter shared by all generations satisfies.
    next_sync: AtomicU32,
    timeout: Option<Duration>,
    transaction_timeout_secs: Option<f64>,
    transaction_isolation_level: TransactionIsolationLevel,
    async_rt_handle: tokio::runtime::Handle,
    // TODO: tests
    // TODO: move sql statement cache to separate type
    sql_statement_cache: Option<Mutex<LruCache<String, u64>>>,
    sql_statement_cache_update_lock: Mutex<()>,
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
        features: Arc<RwLock<ConnectionFeatures>>,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectionInner {
                dispatcher_sender,
                features,
                // TODO: check if 0 is valid value
                next_stream_id: AtomicU32::new(1),
                next_sync: AtomicU32::new(1),
                timeout,
                transaction_timeout_secs: transaction_timeout.as_ref().map(Duration::as_secs_f64),
                transaction_isolation_level,
                // NOTE: Safety: this method can be called only in async tokio context (because it
                // is called only from ConnectionBuilder).
                async_rt_handle: tokio::runtime::Handle::current(),
                sql_statement_cache: NonZeroUsize::new(sql_statement_cache_capacity)
                    .map(|x| Mutex::new(LruCache::new(x))),
                sql_statement_cache_update_lock: Mutex::new(()),
            }),
        }
    }

    /// Synchronously send request to channel and drop response.
    #[allow(clippy::let_underscore_future)]
    pub(crate) fn send_request_sync_and_forget(
        &self,
        body: &impl Request,
        stream_id: Option<u32>,
        generation: Option<u64>,
    ) {
        let this = self.clone();
        let req = EncodedRequest::new(body, stream_id);
        let _ = self.inner.async_rt_handle.spawn(async move {
            let res = match req {
                Ok(req) => this.send_with_generation(req, generation).await,
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
    pub(crate) fn next_sync(&self) -> u32 {
        // `fetch_add` on atomics wraps around on overflow.
        self.inner.next_sync.fetch_add(1, Ordering::Relaxed)
    }

    /// Send `request` with a client-assigned sync.
    ///
    /// `generation` is the value a `Stream` or `Transaction` captured at
    /// creation, `None` for plain requests. The transport answers a request
    /// whose generation is no longer current with [`Error::ConnectionReset`]
    /// instead of sending it, which also covers requests that were already
    /// waiting in the dispatcher queue when a reconnect happened.
    pub(crate) async fn send_with_generation(
        &self,
        mut request: EncodedRequest,
        generation: Option<u64>,
    ) -> Result<Value> {
        let sync = self.next_sync();
        *request.sync_mut() = sync;
        let fut = self.inner.dispatcher_sender.send(request, generation);
        let resp = match self.inner.timeout {
            Some(x) => match timeout(x, fut).await {
                Ok(resp) => resp?,
                Err(elapsed) => {
                    // Nobody will read the response any more; let the
                    // transport forget the in-flight entry.
                    self.inner.dispatcher_sender.cancel(sync);
                    return Err(elapsed.into());
                }
            },
            None => fut.await?,
        };
        match resp.body {
            ResponseBody::Ok(x) => Ok(x),
            ResponseBody::Error(x) => Err(x.into()),
        }
    }

    /// Features negotiated by the most recent `IPROTO_ID` handshake.
    pub(crate) fn features(&self) -> ConnectionFeatures {
        self.inner.features.read().clone()
    }

    /// Generation of the underlying transport connection.
    pub(crate) fn generation(&self) -> u64 {
        self.inner.dispatcher_sender.generation()
    }

    /// Fail with [`Error::ConnectionReset`] if the transport connection was
    /// re-established since `captured` was read.
    ///
    /// Fast path only: the transport repeats the comparison when it accepts
    /// the request (see [`Self::send_with_generation`]), which catches the
    /// requests that pass this check and then wait out a reconnect in the
    /// dispatcher queue.
    pub(crate) fn check_generation(&self, captured: u64) -> Result<()> {
        if self.generation() == captured {
            Ok(())
        } else {
            Err(Error::ConnectionReset)
        }
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
    // Update lock is only taken with `try_lock`, so holding it across `await`
    // never blocks other tasks.
    #[allow(clippy::await_holding_lock)]
    async fn get_cached_sql_statement_id_inner(&self, statement: &str) -> Option<u64> {
        // Lock cache mutex (if cache is not None) and check
        // if statement present in cache.
        let cache = self.inner.sql_statement_cache.as_ref()?;
        if let Some(stmt_id) = cache.lock().get(statement) {
            return Some(*stmt_id);
        }

        // If statement not found, try to lock update lock mutex.
        // If successful, proceed with preparing SQL statement,
        // otherwise return None.
        let update_lock = self.inner.sql_statement_cache_update_lock.try_lock()?;
        let stmt_id = {
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
            let _ = cache.lock().put(statement.into(), stmt_id);
            stmt_id
        };
        drop(update_lock);

        Some(stmt_id)
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
}

impl fmt::Debug for Connection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Connection")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use tokio::sync::mpsc;

    use super::*;
    use crate::{
        codec::consts::RequestType,
        codec::response::Response,
        errors::ErrorResponse,
        transport::{ClientRequest, DispatcherMessage},
    };

    fn test_connection(
        tx: mpsc::Sender<DispatcherMessage>,
        generation: Arc<AtomicU64>,
        request_timeout: Option<Duration>,
    ) -> Connection {
        Connection::new(
            DispatcherSender::new_for_test(tx, generation),
            request_timeout,
            None,
            TransactionIsolationLevel::default(),
            10,
            Arc::new(RwLock::new(ConnectionFeatures::default())),
        )
    }

    fn ok_body(_request_type: u8) -> ResponseBody {
        ResponseBody::Ok(Value::Map(Vec::new()))
    }

    fn reject_commit_and_rollback(request_type: u8) -> ResponseBody {
        if request_type == RequestType::Commit as u8 || request_type == RequestType::Rollback as u8
        {
            ResponseBody::Error(ErrorResponse::new(1, "rejected".into(), None))
        } else {
            ok_body(request_type)
        }
    }

    /// Fake dispatcher: answers every request with `reply(request_type)` and
    /// records the `(request_type, stream_id)` of every request it receives.
    /// Cancel messages are ignored.
    #[allow(clippy::type_complexity)]
    fn spawn_fake_dispatcher(
        mut rx: mpsc::Receiver<DispatcherMessage>,
        reply: fn(u8) -> ResponseBody,
    ) -> Arc<Mutex<Vec<(u8, Option<u32>)>>> {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_by_task = seen.clone();
        tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let DispatcherMessage::Request(ClientRequest {
                    request, responder, ..
                }) = message
                else {
                    continue;
                };
                let request_type = request.request_type as u8;
                seen_by_task.lock().push((request_type, request.stream_id));
                let _ = responder.send(Ok(Response {
                    sync: request.sync,
                    schema_version: 1,
                    body: reply(request_type),
                }));
            }
        });
        seen
    }

    #[tokio::test]
    async fn failed_commit_finishes_the_transaction() {
        let (tx, rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);
        let seen = spawn_fake_dispatcher(rx, reject_commit_and_rollback);

        let transaction = conn.transaction().await.unwrap();
        let res = transaction.commit().await;
        assert!(matches!(res, Err(Error::Response(_))), "{res:?}");

        // Give a drop-time rollback, if any, the chance to reach the dispatcher.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *seen.lock(),
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
        assert!(matches!(res, Err(Error::Response(_))), "{res:?}");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            *seen.lock(),
            vec![
                (RequestType::Begin as u8, Some(1)),
                (RequestType::Rollback as u8, Some(1)),
            ]
        );
    }

    #[tokio::test]
    async fn requests_carry_client_assigned_syncs() {
        let (tx, mut rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);

        let dispatcher = async {
            let mut syncs = Vec::new();
            for _ in 0..2 {
                let Some(DispatcherMessage::Request(ClientRequest {
                    request, responder, ..
                })) = rx.recv().await
                else {
                    panic!("expected a request");
                };
                syncs.push(request.sync);
                let _ = responder.send(Ok(Response {
                    sync: request.sync,
                    schema_version: 1,
                    body: ok_body(0),
                }));
            }
            syncs
        };
        let pings = async {
            conn.ping().await.unwrap();
            conn.ping().await.unwrap();
        };
        let ((), syncs) = tokio::join!(pings, dispatcher);

        assert_eq!(syncs, vec![1, 2]);
    }

    #[tokio::test]
    async fn next_sync_wraps_around() {
        let (tx, _rx) = mpsc::channel(1);
        let conn = test_connection(tx, Arc::default(), None);
        conn.inner.next_sync.store(u32::MAX, Ordering::Relaxed);
        assert_eq!(conn.next_sync(), u32::MAX);
        assert_eq!(conn.next_sync(), 0);
    }

    #[tokio::test]
    async fn timed_out_request_cancels_its_sync() {
        let (tx, mut rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), Some(Duration::from_millis(50)));

        let dispatcher = async {
            let Some(DispatcherMessage::Request(ClientRequest {
                request, responder, ..
            })) = rx.recv().await
            else {
                panic!("expected the request first");
            };
            // Hold the responder so the request never completes.
            let message = timeout(Duration::from_secs(1), rx.recv())
                .await
                .expect("no cancel after the timeout");
            let Some(DispatcherMessage::Cancel(cancelled)) = message else {
                panic!("expected a cancel after the timeout");
            };
            drop(responder);
            (request.sync, cancelled)
        };
        let (res, (sent, cancelled)) = tokio::join!(conn.ping(), dispatcher);

        assert!(matches!(res, Err(Error::Timeout)), "{res:?}");
        assert_eq!(sent, cancelled);
    }

    #[tokio::test]
    async fn timeout_with_full_dispatcher_queue_drops_the_cancel() {
        let (tx, mut rx) = mpsc::channel(1);
        let conn = test_connection(tx.clone(), Arc::default(), Some(Duration::from_millis(50)));
        // Fill the queue so neither the request nor its cancel fits.
        assert!(tx.try_send(DispatcherMessage::Cancel(u32::MAX)).is_ok());

        let res = timeout(Duration::from_secs(1), conn.ping())
            .await
            .expect("a timed-out request blocked on its cancel");

        assert!(matches!(res, Err(Error::Timeout)), "{res:?}");
        assert!(matches!(
            rx.try_recv(),
            Ok(DispatcherMessage::Cancel(u32::MAX))
        ));
        assert!(rx.try_recv().is_err(), "a message was queued past capacity");
    }

    #[tokio::test]
    async fn stream_requests_carry_their_generation() {
        let (tx, mut rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::new(AtomicU64::new(3)), None);

        let dispatcher = async {
            let mut seen = Vec::new();
            for _ in 0..2 {
                let Some(DispatcherMessage::Request(ClientRequest {
                    request,
                    generation,
                    responder,
                })) = rx.recv().await
                else {
                    panic!("expected a request");
                };
                seen.push((request.stream_id.is_some(), generation));
                let _ = responder.send(Ok(Response {
                    sync: request.sync,
                    schema_version: 1,
                    body: ok_body(0),
                }));
            }
            seen
        };
        let requests = async {
            conn.stream().ping().await.unwrap();
            conn.ping().await.unwrap();
        };
        let ((), seen) = tokio::join!(requests, dispatcher);

        // The stream request carries the generation it was issued under; the
        // plain request carries none and is never rejected for staleness.
        assert_eq!(seen, vec![(true, Some(3)), (false, None)]);
    }

    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn sql_statement_is_not_prepared_while_another_is_in_flight() {
        let (tx, mut rx) = mpsc::channel(8);
        let conn = test_connection(tx, Arc::default(), None);

        let update_lock = conn.inner.sql_statement_cache_update_lock.lock();
        let res = timeout(
            Duration::from_secs(1),
            conn.get_cached_sql_statement_id_inner("SELECT 1"),
        )
        .await
        .expect("did not return immediately while another statement is being prepared");
        drop(update_lock);

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
        assert_eq!(*seen.lock(), vec![(RequestType::Begin as u8, Some(1))]);
    }

    #[tokio::test]
    async fn plain_request_ignores_generation() {
        let (tx, rx) = mpsc::channel(8);
        let generation = Arc::new(AtomicU64::new(0));
        let conn = test_connection(tx, generation.clone(), None);
        let seen = spawn_fake_dispatcher(rx, ok_body);

        generation.fetch_add(1, Ordering::SeqCst);

        conn.ping().await.unwrap();
        assert_eq!(*seen.lock(), vec![(RequestType::Ping as u8, None)]);
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
