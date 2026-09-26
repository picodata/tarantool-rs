use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, ready},
    time::Duration,
};

use tokio::{
    net::ToSocketAddrs,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error};

use super::connection::Connection;
use crate::{
    Error, ReconnectInterval,
    codec::{request::EncodedRequest, response::Response},
};

/// Request from the client together with the channel its response goes to.
pub(crate) struct ClientRequest {
    pub(crate) request: EncodedRequest,
    /// Generation the issuing `Stream` or `Transaction` captured at
    /// creation; `None` for plain stateless requests. The transport answers
    /// a request whose generation is no longer current with
    /// `Error::ConnectionReset` instead of sending it.
    pub(crate) generation: Option<u64>,
    pub(crate) responder: DispatcherResponseSender,
}

/// Channel the response to one request, or its error, goes to.
#[repr(transparent)]
pub(crate) struct DispatcherResponseSender(pub(super) oneshot::Sender<Result<Response, Error>>);

impl DispatcherResponseSender {
    #[inline]
    pub(crate) fn send(
        self,
        value: Result<Response, Error>,
    ) -> Result<(), Result<Response, Error>> {
        self.0.send(value)
    }

    #[inline]
    pub(crate) fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// Cloneable handle that sends requests to the dispatcher.
///
/// It holds the request queue and the cancel channel, never the liveness
/// receiver, so a clone kept by a background task cannot keep the dispatcher
/// alive once the last client handle is gone.
#[derive(Clone)]
pub(crate) struct RequestSender {
    requests: mpsc::Sender<ClientRequest>,
    cancels: mpsc::UnboundedSender<u64>,
}

impl RequestSender {
    /// Queue `request` and wait for its response.
    ///
    /// Dropping the returned future after the request was queued and before
    /// its response arrived sends the request's sync on the cancel channel,
    /// so the transport forgets it: a timeout, a lost `select!` branch and an
    /// aborted task all cancel. A request that never entered the queue sends
    /// nothing.
    pub(crate) async fn send(
        &self,
        request: EncodedRequest,
        generation: Option<u64>,
    ) -> Result<Response, Error> {
        let sync = request.sync;
        let (tx, rx) = oneshot::channel();
        // A failed send means the dispatcher task is gone, which is permanent.
        if self
            .requests
            .send(ClientRequest {
                request,
                generation,
                responder: DispatcherResponseSender(tx),
            })
            .await
            .is_err()
        {
            return Err(Error::ConnectionClosed);
        }
        PendingResponse {
            rx,
            sync,
            cancels: &self.cancels,
            answered: false,
        }
        .await
    }
}

/// Response to a queued request. Dropped before the response arrived, it
/// sends the request's sync on the cancel channel.
struct PendingResponse<'a> {
    rx: oneshot::Receiver<Result<Response, Error>>,
    sync: u64,
    cancels: &'a mpsc::UnboundedSender<u64>,
    answered: bool,
}

impl Future for PendingResponse<'_> {
    type Output = Result<Response, Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let response = ready!(Pin::new(&mut self.rx).poll(cx));
        self.answered = true;
        Poll::Ready(response.unwrap_or(Err(Error::ConnectionClosed)))
    }
}

impl Drop for PendingResponse<'_> {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        // Closed first: if `run` reads the request only after the cancel, it
        // finds the responder closed and skips the request.
        self.rx.close();
        // Fails only once the dispatcher is gone, and then nothing is left to
        // cancel.
        let _ = self.cancels.send(self.sync);
    }
}

pub(crate) struct DispatcherSender {
    requests: RequestSender,
    generation: Arc<AtomicU64>,
    /// Dropped together with the last client handle; the dispatcher awaits
    /// that through [`Dispatcher::client_liveness`].
    _liveness: oneshot::Receiver<()>,
}

impl DispatcherSender {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        requests: mpsc::Sender<ClientRequest>,
        cancels: mpsc::UnboundedSender<u64>,
        generation: Arc<AtomicU64>,
    ) -> Self {
        let (_client_liveness, liveness) = oneshot::channel();
        Self {
            requests: RequestSender { requests, cancels },
            generation,
            _liveness: liveness,
        }
    }

    /// Generation of the transport connection; see [`Dispatcher::generation`].
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    /// The request handle, which does not carry the liveness receiver.
    pub(crate) fn requests(&self) -> &RequestSender {
        &self.requests
    }
}

type ConnectDynFuture = dyn Future<Output = Result<Connection, Error>> + Send;
type ConnFactory = Box<dyn Fn() -> Pin<Box<ConnectDynFuture>> + Send + Sync>;

/// Dispatching messages from client to connection.
///
/// Owns the reconnect loop and advances the shared generation counter as soon
/// as a connection is lost, before the first reconnect attempt. It closes the
/// connection and exits as soon as the last client handle is dropped, whether
/// a connection is running or a reconnect is in progress. Schema reloading and
/// pooling are not implemented yet.
pub(crate) struct Dispatcher {
    rx: mpsc::Receiver<ClientRequest>,
    /// Syncs of the requests whose callers gave up. Unbounded, so a cancel is
    /// never lost to a full request queue.
    cancel_rx: mpsc::UnboundedReceiver<u64>,
    conn: Option<Connection>,
    conn_factory: ConnFactory,
    reconnect_interval: Option<ReconnectInterval>,
    /// Advanced as soon as a connection is lost, before the first reconnect
    /// attempt. Streams and transactions compare it with the value they
    /// captured at creation; each `Connection::run` gets the value of its own
    /// connection.
    generation: Arc<AtomicU64>,
    /// Its `closed()` resolves once every client handle is gone; both
    /// `Connection::run` and `reconnect` race it.
    client_liveness: oneshot::Sender<()>,
}

impl Dispatcher {
    pub(crate) async fn prepare<A>(
        addr: A,
        user: Option<&str>,
        password: Option<&str>,
        connect_timeout: Option<Duration>,
        reconnect_interval: Option<ReconnectInterval>,
        internal_simultaneous_requests_threshold: usize,
    ) -> Result<(impl Future<Output = ()> + use<A>, DispatcherSender), Error>
    where
        A: ToSocketAddrs + Display + Clone + Send + Sync + 'static,
    {
        let user: Option<String> = user.map(Into::into);
        let password: Option<String> = password.map(Into::into);
        let conn_factory: ConnFactory = Box::new(move || {
            let addr = addr.clone();
            let user = user.clone();
            let password = password.clone();
            let connect_timeout = connect_timeout;
            Box::pin(async move {
                Connection::new(
                    addr,
                    user.as_deref(),
                    password.as_deref(),
                    connect_timeout,
                    internal_simultaneous_requests_threshold,
                )
                .await
            }) as Pin<Box<ConnectDynFuture>>
        });

        let conn = conn_factory().await?;

        let (dispatcher, sender) = Self::new(
            conn_factory,
            Some(conn),
            reconnect_interval,
            internal_simultaneous_requests_threshold,
        );
        Ok((dispatcher.run(), sender))
    }

    /// Wire a dispatcher to the sender the client uses.
    fn new(
        conn_factory: ConnFactory,
        conn: Option<Connection>,
        reconnect_interval: Option<ReconnectInterval>,
        queue_size: usize,
    ) -> (Self, DispatcherSender) {
        let (tx, rx) = mpsc::channel(queue_size);
        let (cancel_tx, cancel_rx) = mpsc::unbounded_channel();
        let generation = Arc::new(AtomicU64::new(0));
        let (client_liveness, liveness) = oneshot::channel();
        (
            Self {
                rx,
                cancel_rx,
                conn,
                conn_factory,
                reconnect_interval,
                generation: generation.clone(),
                client_liveness,
            },
            DispatcherSender {
                requests: RequestSender {
                    requests: tx,
                    cancels: cancel_tx,
                },
                generation,
                _liveness: liveness,
            },
        )
    }

    /// Connect again, retrying with the reconnect interval. `None` means that
    /// every client handle was dropped and the dispatcher should stop.
    async fn reconnect(&mut self) -> Option<Connection> {
        let mut reconn_int_state = self
            .reconnect_interval
            .as_ref()
            .map(ReconnectIntervalState::from);
        loop {
            // Liveness first, here and in the backoff below: once the last
            // client handle is gone, no further connect attempt starts.
            let attempt = tokio::select! {
                biased;
                () = self.client_liveness.closed() => return None,
                res = (self.conn_factory)() => res,
            };
            match attempt {
                Ok(conn) => return Some(conn),
                Err(err) => {
                    error!("Failed to reconnect to Tarantool: {:#}", err);
                    if let Some(ref mut x) = reconn_int_state {
                        tokio::select! {
                            biased;
                            () = self.client_liveness.closed() => return None,
                            () = tokio::time::sleep(x.next_timeout()) => {}
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn run(mut self) {
        debug!("Starting dispatcher");
        loop {
            let conn = if let Some(conn) = self.conn.take() {
                conn
            } else if let Some(conn) = self.reconnect().await {
                conn
            } else {
                debug!("All client handles dropped, stopping dispatcher");
                return;
            };
            let generation = self.generation.load(Ordering::Acquire);
            if conn
                .run(
                    &mut self.rx,
                    &mut self.cancel_rx,
                    &mut self.client_liveness,
                    generation,
                )
                .await
                .is_ok()
            {
                return;
            }
            // Advance as soon as the loss is observed, before the first
            // reconnect attempt: stale streams and transactions fail fast
            // during the outage, and the ones created during it carry the
            // generation of the next connection.
            self.generation.fetch_add(1, Ordering::AcqRel);
        }
    }
}

/// Get interval before next reconnect attempt.
#[derive(Debug)]
enum ReconnectIntervalState {
    Fixed(Duration),
    ExponentialBackoff {
        current: Duration,
        max: Duration,
        randomization_factor: f64,
        multiplier: f64,
    },
}

impl ReconnectIntervalState {
    fn next_timeout(&mut self) -> Duration {
        match self {
            ReconnectIntervalState::Fixed(x) => *x,

            ReconnectIntervalState::ExponentialBackoff {
                current,
                max,
                randomization_factor,
                multiplier,
            } => {
                // Mirrors `backoff::ExponentialBackoff` with unlimited elapsed
                // time: the returned interval is the current one randomized
                // within [1 - randomization_factor, 1 + randomization_factor],
                // after which the current interval grows by multiplier, capped
                // at max (the randomized value itself may exceed max, exactly
                // as in the original crate).
                let delta = current.mul_f64(*randomization_factor);
                let low = current.saturating_sub(delta);
                let jittered = low + (delta + delta).mul_f64(fastrand::f64());
                // The f64 comparison guards `mul_f64` against overflowing
                // `Duration` with a huge multiplier.
                *current = if current.as_secs_f64() * *multiplier >= max.as_secs_f64() {
                    *max
                } else {
                    current.mul_f64(*multiplier)
                };
                jittered
            }
        }
    }
}

impl From<&ReconnectInterval> for ReconnectIntervalState {
    fn from(value: &ReconnectInterval) -> Self {
        match value {
            ReconnectInterval::Fixed(x) => Self::Fixed(*x),
            ReconnectInterval::ExponentialBackoff {
                min,
                max,
                randomization_factor,
                multiplier,
            } => Self::ExponentialBackoff {
                current: *min,
                max: *max,
                // Clamp so that `Duration::mul_f64` in `next_timeout` cannot
                // panic on a negative or NaN factor.
                randomization_factor: if randomization_factor.is_nan() {
                    0.0
                } else {
                    randomization_factor.clamp(0.0, 1.0)
                },
                multiplier: if multiplier.is_nan() {
                    1.0
                } else {
                    multiplier.max(0.0)
                },
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::{AtomicBool, AtomicUsize};

    use parking_lot::Mutex;
    use tokio::{io::AsyncWriteExt, net::TcpListener, task::JoinHandle, time::timeout};

    use super::super::connection::tests::{
        echo_sync, fake_greeting, id_response_body, ok_body_for, ok_response, read_request,
        spawn_fake_server,
    };
    use crate::{
        ExecutorExt, TransactionIsolationLevel,
        codec::{consts::RequestType, request::Eval},
    };

    /// Dispatcher with no live connection whose factory connects to `addr`.
    fn dispatcher_for(
        addr: String,
        reconnect_interval: Option<ReconnectInterval>,
    ) -> (Dispatcher, DispatcherSender) {
        Dispatcher::new(
            Box::new(move || {
                let addr = addr.clone();
                Box::pin(async move { Connection::new(addr, None, None, None, 16).await })
                    as Pin<Box<ConnectDynFuture>>
            }),
            None,
            reconnect_interval,
            4,
        )
    }

    /// Address nothing listens on: connects to it are refused.
    async fn dead_endpoint() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap().to_string()
    }

    /// Fake server that completes the handshake of every connection, then
    /// holds the socket and never reads from it again.
    async fn spawn_non_reading_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    sock.write_all(&fake_greeting()).await.unwrap();
                    let (request_type, sync) = read_request(&mut sock).await.unwrap();
                    assert_eq!(request_type, RequestType::Id as u8);
                    sock.write_all(&ok_response(sync, &id_response_body()))
                        .await
                        .unwrap();
                    std::future::pending::<()>().await;
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn dispatcher_exits_when_last_handle_drops_while_the_peer_does_not_read() {
        let addr = spawn_non_reading_server().await;
        let (dispatcher, sender) = dispatcher_for(
            addr,
            Some(ReconnectInterval::fixed(Duration::from_millis(10))),
        );
        let dispatcher = tokio::spawn(dispatcher.run());

        // Requests of 1 MiB fill the socket buffers, the writer queue (16)
        // and the dispatcher queue (4); the rest wait for queue capacity.
        let sender = Arc::new(sender);
        let expr = "x".repeat(1024 * 1024);
        let mut requests = Vec::new();
        for sync in 1..=40 {
            let mut request = EncodedRequest::new(&Eval::new(&expr, ()), None).unwrap();
            *request.sync_mut() = sync;
            let sender = sender.clone();
            requests.push(tokio::spawn(async move {
                sender.requests().send(request, None).await
            }));
        }
        tokio::time::sleep(Duration::from_millis(300)).await;

        // The application drops every handle, its pending requests included.
        for request in &requests {
            request.abort();
        }
        drop(sender);

        timeout(Duration::from_secs(1), dispatcher)
            .await
            .expect("the dispatcher outlived the last client handle")
            .unwrap();
    }

    #[tokio::test]
    async fn dispatcher_exits_when_last_client_handle_is_dropped_during_reconnect() {
        // `None` is the hot loop: no backoff sleep to race against.
        for reconnect_interval in [
            Some(ReconnectInterval::fixed(Duration::from_millis(10))),
            None,
        ] {
            let (dispatcher, sender) =
                dispatcher_for(dead_endpoint().await, reconnect_interval.clone());
            let handle = tokio::spawn(dispatcher.run());
            tokio::time::sleep(Duration::from_millis(50)).await;

            drop(sender);

            tokio::time::timeout(Duration::from_secs(2), handle)
                .await
                .unwrap_or_else(|_| {
                    panic!(
                        "dispatcher kept reconnecting after the last client handle was dropped \
                         (reconnect_interval = {reconnect_interval:?})"
                    )
                })
                .unwrap();
        }
    }

    #[tokio::test]
    async fn reconnect_returns_the_connection_and_leaves_the_generation() {
        let addr = spawn_fake_server(echo_sync).await;
        let (mut dispatcher, sender) = dispatcher_for(addr, None);

        assert!(dispatcher.reconnect().await.is_some());
        // `Dispatcher::run` advances the generation when a connection is lost,
        // not when a new one comes up.
        assert_eq!(sender.generation(), 0);
    }

    /// Fake server on one local address that can go down and come back.
    ///
    /// While up, it serves every connection: it sends the greeting, answers ID
    /// with an ID body and every other request with an OK response, except the
    /// request types in `silent`, which it never answers, and it records the
    /// type of every request it reads. Going down closes the live sockets;
    /// while down, it accepts each new connection and closes it at once,
    /// counting it as a reconnect attempt.
    struct FlakyServer {
        addr: String,
        up: Arc<AtomicBool>,
        attempts_while_down: Arc<AtomicUsize>,
        seen: Arc<Mutex<Vec<u8>>>,
        live: Arc<Mutex<Vec<JoinHandle<()>>>>,
    }

    impl FlakyServer {
        async fn start() -> Self {
            Self::start_with_silent(&[]).await
        }

        async fn start_with_silent(silent: &'static [RequestType]) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = Self {
                addr: listener.local_addr().unwrap().to_string(),
                up: Arc::new(AtomicBool::new(true)),
                attempts_while_down: Arc::default(),
                seen: Arc::default(),
                live: Arc::default(),
            };
            let up = server.up.clone();
            let attempts_while_down = server.attempts_while_down.clone();
            let seen = server.seen.clone();
            let live = server.live.clone();
            tokio::spawn(async move {
                while let Ok((mut sock, _)) = listener.accept().await {
                    if !up.load(Ordering::SeqCst) {
                        attempts_while_down.fetch_add(1, Ordering::SeqCst);
                        drop(sock);
                        continue;
                    }
                    let seen = seen.clone();
                    live.lock().push(tokio::spawn(async move {
                        if sock.write_all(&fake_greeting()).await.is_err() {
                            return;
                        }
                        while let Some((request_type, sync)) = read_request(&mut sock).await {
                            seen.lock().push(request_type);
                            if silent.iter().any(|x| *x as u8 == request_type) {
                                continue;
                            }
                            let response = ok_response(sync, &ok_body_for(request_type));
                            if sock.write_all(&response).await.is_err() {
                                return;
                            }
                        }
                    }));
                }
            });
            server
        }

        /// Close every live connection and refuse new ones until [`Self::up`].
        fn down(&self) {
            self.up.store(false, Ordering::SeqCst);
            for connection in self.live.lock().drain(..) {
                connection.abort();
            }
        }

        fn up(&self) {
            self.up.store(true, Ordering::SeqCst);
        }

        /// Types of the requests the server read, in order.
        fn seen(&self) -> Vec<u8> {
            self.seen.lock().clone()
        }

        /// Wait until the server read a request of `request_type`.
        async fn wait_for_request(&self, request_type: RequestType) {
            timeout(Duration::from_secs(2), async {
                while !self.seen().contains(&(request_type as u8)) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("no {request_type:?} within 2 s"));
        }

        /// Wait until the dispatcher tried to connect while the server was
        /// down.
        async fn wait_for_reconnect_attempt(&self) {
            timeout(Duration::from_secs(2), async {
                while self.attempts_while_down.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .expect("no reconnect attempt within 2 s");
        }
    }

    /// Client `Connection` over a fresh dispatcher to `server`, with a fixed
    /// 10 ms reconnect interval and `request_timeout`. Returns once the first
    /// connect succeeded, together with the dispatcher task.
    async fn connect(
        server: &FlakyServer,
        request_timeout: Option<Duration>,
    ) -> (crate::Connection, JoinHandle<()>) {
        let (dispatcher, sender) = Dispatcher::prepare(
            server.addr.clone(),
            None,
            None,
            None,
            Some(ReconnectInterval::fixed(Duration::from_millis(10))),
            16,
        )
        .await
        .unwrap();
        let dispatcher = tokio::spawn(dispatcher);
        let conn = crate::Connection::new(
            sender,
            request_timeout,
            None,
            TransactionIsolationLevel::default(),
            0,
        );
        (conn, dispatcher)
    }

    #[tokio::test]
    async fn lost_connection_advances_the_generation_before_reconnecting() {
        let server = FlakyServer::start().await;
        let (conn, _dispatcher) = connect(&server, None).await;
        assert_eq!(conn.generation(), 0);

        server.down();
        server.wait_for_reconnect_attempt().await;

        // The server is still down, and the generation already moved.
        assert_eq!(conn.generation(), 1);
    }

    #[tokio::test]
    async fn stream_created_before_the_loss_fails_fast_during_the_outage() {
        let server = FlakyServer::start().await;
        let (conn, _dispatcher) = connect(&server, None).await;
        let stream = conn.stream();
        stream.ping().await.unwrap();

        server.down();
        server.wait_for_reconnect_attempt().await;

        let res = timeout(Duration::from_secs(1), stream.ping())
            .await
            .expect("the stale stream waited out the outage");
        assert!(matches!(res, Err(Error::ConnectionReset)), "{res:?}");
    }

    #[tokio::test]
    async fn stream_and_transaction_created_during_the_outage_work_after_it() {
        let server = FlakyServer::start().await;
        let (conn, _dispatcher) = connect(&server, None).await;
        server.down();
        server.wait_for_reconnect_attempt().await;

        let stream = conn.stream();
        // Its BEGIN waits in the queue until the server is back.
        let transaction = tokio::spawn({
            let conn = conn.clone();
            async move { conn.transaction().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        server.up();

        timeout(Duration::from_secs(2), stream.ping())
            .await
            .expect("no reconnect within 2 s")
            .unwrap();
        let transaction = transaction.await.unwrap().unwrap();
        transaction.ping().await.unwrap();
        transaction.commit().await.unwrap();
    }

    #[tokio::test]
    async fn transaction_dropped_during_the_outage_rolls_nothing_back() {
        let server = FlakyServer::start().await;
        let (conn, dispatcher) = connect(&server, None).await;
        let transaction = conn.transaction().await.unwrap();
        server.down();
        server.wait_for_reconnect_attempt().await;

        // Stale: the server discarded it with the lost session.
        drop(transaction);
        server.up();
        timeout(Duration::from_secs(2), conn.ping())
            .await
            .expect("no reconnect within 2 s")
            .unwrap();
        // Give a stray ROLLBACK the time to reach the new session. It gets
        // there only if both the drop's staleness check and the transport's
        // generation check break.
        tokio::time::sleep(Duration::from_millis(50)).await;
        let seen = server.seen();
        assert!(!seen.contains(&(RequestType::Rollback as u8)), "{seen:?}");

        drop(conn);
        timeout(Duration::from_secs(2), dispatcher)
            .await
            .expect("the dispatcher outlived the last client handle")
            .unwrap();
    }

    #[tokio::test]
    async fn dispatcher_exits_during_the_outage_after_a_committed_transaction() {
        let server = FlakyServer::start().await;
        let (conn, dispatcher) = connect(&server, None).await;
        conn.transaction().await.unwrap().commit().await.unwrap();
        server.down();
        server.wait_for_reconnect_attempt().await;

        drop(conn);

        timeout(Duration::from_secs(2), dispatcher)
            .await
            .expect("the dispatcher outlived the last client handle")
            .unwrap();
    }

    /// Begin a transaction during the outage and give up on it after 100 ms:
    /// the transaction was already created, so its drop sends a ROLLBACK,
    /// which waits in the queue while the server is down.
    async fn cancel_a_begin_during_the_outage(conn: &crate::Connection) {
        assert!(
            timeout(Duration::from_millis(100), conn.transaction())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn begin_cancelled_during_the_outage_does_not_keep_the_dispatcher_alive() {
        let server = FlakyServer::start().await;
        let (conn, dispatcher) = connect(&server, None).await;
        server.down();
        server.wait_for_reconnect_attempt().await;
        cancel_a_begin_during_the_outage(&conn).await;

        drop(conn);

        timeout(Duration::from_secs(2), dispatcher)
            .await
            .expect("the drop-time rollback kept the dispatcher alive")
            .unwrap();
    }

    #[tokio::test]
    async fn dispatcher_exits_at_once_with_a_request_timeout() {
        let server = FlakyServer::start().await;
        let (conn, dispatcher) = connect(&server, Some(Duration::from_millis(500))).await;
        server.down();
        server.wait_for_reconnect_attempt().await;
        cancel_a_begin_during_the_outage(&conn).await;

        drop(conn);

        // Well before the ROLLBACK's 500 ms timeout would release it.
        timeout(Duration::from_millis(250), dispatcher)
            .await
            .expect("the dispatcher waited for the ROLLBACK's timeout")
            .unwrap();
    }

    #[tokio::test]
    async fn unanswered_drop_rollback_does_not_keep_the_dispatcher_alive() {
        let server = FlakyServer::start_with_silent(&[RequestType::Rollback]).await;
        let (conn, dispatcher) = connect(&server, None).await;
        let transaction = conn.transaction().await.unwrap();

        // Connected: the ROLLBACK is sent, and the server never answers it.
        drop(transaction);
        server.wait_for_request(RequestType::Rollback).await;
        drop(conn);

        timeout(Duration::from_secs(2), dispatcher)
            .await
            .expect("the unanswered ROLLBACK kept the dispatcher alive")
            .unwrap();
    }

    fn exp_backoff_state(
        min: Duration,
        max: Duration,
        randomization_factor: f64,
        multiplier: f64,
    ) -> ReconnectIntervalState {
        ReconnectIntervalState::from(&ReconnectInterval::ExponentialBackoff {
            min,
            max,
            randomization_factor,
            multiplier,
        })
    }

    #[test]
    fn fixed_interval_is_constant() {
        let mut state =
            ReconnectIntervalState::from(&ReconnectInterval::Fixed(Duration::from_millis(42)));
        for _ in 0..10 {
            assert_eq!(state.next_timeout(), Duration::from_millis(42));
        }
    }

    #[test]
    fn exponential_backoff_growth_without_jitter() {
        let mut state =
            exp_backoff_state(Duration::from_millis(1), Duration::from_secs(1), 0.0, 5.0);
        let expected = [1, 5, 25, 125, 625, 1000, 1000];
        for millis in expected {
            assert_eq!(state.next_timeout(), Duration::from_millis(millis));
        }
    }

    #[test]
    fn exponential_backoff_jitter_within_bounds() {
        let mut state = exp_backoff_state(
            Duration::from_millis(100),
            Duration::from_secs(100),
            0.5,
            1.0,
        );
        for _ in 0..1000 {
            let timeout = state.next_timeout();
            assert!(timeout >= Duration::from_millis(50), "{timeout:?}");
            assert!(timeout <= Duration::from_millis(150), "{timeout:?}");
        }
    }

    #[test]
    fn exponential_backoff_pathological_factors_do_not_panic() {
        for (randomization_factor, multiplier) in [
            (-1.0, -1.0),
            (f64::NAN, f64::NAN),
            (2.0, f64::INFINITY),
            (0.5, 1e300),
        ] {
            let mut state = exp_backoff_state(
                Duration::from_millis(1),
                Duration::from_secs(1),
                randomization_factor,
                multiplier,
            );
            for _ in 0..10 {
                assert!(state.next_timeout() <= Duration::from_secs(2));
            }
        }
    }
}
