use std::{
    fmt::Display,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use parking_lot::RwLock;
use tokio::{
    net::ToSocketAddrs,
    sync::{mpsc, oneshot},
};
use tokio_stream::wrappers::ReceiverStream;
use tracing::{debug, error};

use super::connection::Connection;
use crate::{
    Error, ReconnectInterval,
    codec::{
        request::{ConnectionFeatures, EncodedRequest},
        response::Response,
    },
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

/// Message from the client side to the dispatcher.
pub(crate) enum DispatcherMessage {
    /// Send the request and route its response back.
    Request(ClientRequest),
    /// The caller gave up on the request with this sync; forget it.
    Cancel(u64),
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

pub(crate) struct DispatcherSender {
    tx: mpsc::Sender<DispatcherMessage>,
    generation: Arc<AtomicU64>,
    /// Dropped together with the last client handle; the dispatcher awaits
    /// that through [`Dispatcher::client_liveness`].
    _liveness: oneshot::Receiver<()>,
}

impl DispatcherSender {
    #[cfg(test)]
    pub(crate) fn new_for_test(
        tx: mpsc::Sender<DispatcherMessage>,
        generation: Arc<AtomicU64>,
    ) -> Self {
        let (_client_liveness, liveness) = oneshot::channel();
        Self {
            tx,
            generation,
            _liveness: liveness,
        }
    }

    /// Generation of the transport connection; see [`Dispatcher::generation`].
    pub(crate) fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub(crate) async fn send(
        &self,
        request: EncodedRequest,
        generation: Option<u64>,
    ) -> Result<Response, Error> {
        let (tx, rx) = oneshot::channel();
        // A failed send means the dispatcher task is gone, which is permanent.
        if self
            .tx
            .send(DispatcherMessage::Request(ClientRequest {
                request,
                generation,
                responder: DispatcherResponseSender(tx),
            }))
            .await
            .is_err()
        {
            return Err(Error::ConnectionClosed);
        }
        rx.await.unwrap_or(Err(Error::ConnectionClosed))
    }

    /// Tell the dispatcher that nobody awaits the response for `sync` any more.
    ///
    /// Best-effort: if the channel is full or closed the message is dropped,
    /// and the entry lives until its response arrives or the connection is
    /// recycled.
    pub(crate) fn cancel(&self, sync: u64) {
        if let Err(err) = self.tx.try_send(DispatcherMessage::Cancel(sync)) {
            debug!("Failed to cancel sync {sync}: {err}");
        }
    }
}

type ConnectDynFuture = dyn Future<Output = Result<Connection, Error>> + Send;
type ConnFactory = Box<dyn Fn() -> Pin<Box<ConnectDynFuture>> + Send + Sync>;

/// How [`Dispatcher::reconnect`] ended.
enum ReconnectOutcome {
    /// A new connection is in `Dispatcher::conn`.
    Connected,
    /// Every client handle was dropped; the dispatcher should stop.
    ClientsGone,
}

/// Dispatching messages from client to connection.
///
/// Owns the reconnect loop, bumps the shared generation counter on every
/// successful reconnect, and exits once the last client handle is dropped.
/// Schema reloading and pooling are not implemented yet.
pub(crate) struct Dispatcher {
    rx: ReceiverStream<DispatcherMessage>,
    conn: Option<Connection>,
    conn_factory: ConnFactory,
    reconnect_interval: Option<ReconnectInterval>,
    /// Bumped after every successful reconnect, before requests flow on the
    /// new connection. Streams and transactions compare it with the value
    /// they captured at creation.
    generation: Arc<AtomicU64>,
    /// Its `closed()` resolves once every client handle is gone, even while
    /// no connection is reading `rx`.
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
        features: Arc<RwLock<ConnectionFeatures>>,
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
            let features = features.clone();
            Box::pin(async move {
                Connection::new(
                    addr,
                    user.as_deref(),
                    password.as_deref(),
                    connect_timeout,
                    internal_simultaneous_requests_threshold,
                    &features,
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
        let generation = Arc::new(AtomicU64::new(0));
        let (client_liveness, liveness) = oneshot::channel();
        (
            Self {
                rx: ReceiverStream::new(rx),
                conn,
                conn_factory,
                reconnect_interval,
                generation: generation.clone(),
                client_liveness,
            },
            DispatcherSender {
                tx,
                generation,
                _liveness: liveness,
            },
        )
    }

    async fn reconnect(&mut self) -> ReconnectOutcome {
        let mut reconn_int_state = self
            .reconnect_interval
            .as_ref()
            .map(ReconnectIntervalState::from);
        loop {
            let attempt = tokio::select! {
                res = (self.conn_factory)() => res,
                () = self.client_liveness.closed() => return ReconnectOutcome::ClientsGone,
            };
            match attempt {
                Ok(conn) => {
                    // The handshake already refreshed the shared features;
                    // bump before `run` lets requests onto the new connection.
                    self.generation.fetch_add(1, Ordering::AcqRel);
                    self.conn = Some(conn);
                    return ReconnectOutcome::Connected;
                }
                Err(err) => {
                    error!("Failed to reconnect to Tarantool: {:#}", err);
                    if let Some(ref mut x) = reconn_int_state {
                        tokio::select! {
                            () = tokio::time::sleep(x.next_timeout()) => {}
                            () = self.client_liveness.closed() => {
                                return ReconnectOutcome::ClientsGone;
                            }
                        }
                    }
                }
            }
        }
    }

    pub(crate) async fn run(mut self) {
        debug!("Starting dispatcher");
        loop {
            match self.conn.take() {
                Some(conn) => {
                    if conn.run(&mut self.rx, &self.generation).await.is_ok() {
                        return;
                    }
                }
                None => {
                    if let ReconnectOutcome::ClientsGone = self.reconnect().await {
                        debug!("All client handles dropped, stopping dispatcher");
                        return;
                    }
                }
            }
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

    use parking_lot::RwLock;
    use tokio::net::TcpListener;

    use super::super::connection::tests::{echo_sync, spawn_fake_server};
    use crate::codec::request::ConnectionFeatures;

    /// Dispatcher with no live connection whose factory connects to `addr`.
    fn dispatcher_for(
        addr: String,
        reconnect_interval: Option<ReconnectInterval>,
    ) -> (Dispatcher, DispatcherSender) {
        let features = Arc::new(RwLock::new(ConnectionFeatures::default()));
        Dispatcher::new(
            Box::new(move || {
                let addr = addr.clone();
                let features = features.clone();
                Box::pin(
                    async move { Connection::new(addr, None, None, None, 16, &features).await },
                ) as Pin<Box<ConnectDynFuture>>
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
    async fn reconnect_bumps_generation() {
        let addr = spawn_fake_server(echo_sync).await;
        let (mut dispatcher, sender) = dispatcher_for(addr, None);
        assert_eq!(sender.generation(), 0);

        assert!(matches!(
            dispatcher.reconnect().await,
            ReconnectOutcome::Connected
        ));

        assert!(dispatcher.conn.is_some());
        assert_eq!(sender.generation(), 1);
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
