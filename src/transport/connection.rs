use std::{
    collections::HashMap,
    fmt::Display,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use futures::{
    FutureExt, SinkExt, StreamExt, TryStreamExt,
    future::{Fuse, FusedFuture},
};

use tokio::{
    io::AsyncReadExt,
    net::{
        TcpStream, ToSocketAddrs,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    pin,
    sync::mpsc,
    task::JoinHandle,
};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::{
    codec::{FramedRead, FramedWrite},
    sync::CancellationToken,
};
use tracing::{debug, error, trace, warn};

use super::dispatcher::{ClientRequest, DispatcherMessage, DispatcherResponseSender};
use crate::{
    codec::{
        ClientCodec, Greeting,
        request::{Auth, EncodedRequest, Id, Request},
        response::{Response, ResponseBody},
    },
    errors::{CodecEncodeError, ConnectionError, EncodingError, Error},
};

/// Sync of the AUTH request of the handshake. The handshake finishes before
/// `run` starts, so the handshake syncs cannot collide with the
/// client-assigned syncs used afterwards.
const AUTH_SYNC: u64 = 0;
/// Sync of the ID request of the handshake; see [`AUTH_SYNC`].
const ID_SYNC: u64 = 1;

/// Encode one handshake request with its fixed sync.
fn handshake_request(body: &impl Request, sync: u64) -> Result<EncodedRequest, EncodingError> {
    let mut request = EncodedRequest::new(body, None)?;
    *request.sync_mut() = sync;
    Ok(request)
}

struct ConnectionData {
    in_flights: HashMap<u64, DispatcherResponseSender>,
}

impl Default for ConnectionData {
    fn default() -> Self {
        Self {
            in_flights: HashMap::with_capacity(5),
        }
    }
}

impl ConnectionData {
    /// Register the response sender for a request whose sync the client
    /// already assigned.
    ///
    /// A request carrying a generation other than `current_generation` was
    /// issued through a stream or transaction created on an earlier
    /// connection, typically while it waited in the dispatcher queue during a
    /// reconnect. Its server-side state is gone, so it is answered with
    /// `Error::ConnectionReset` and neither registered nor sent. Requests
    /// without a generation are never rejected here.
    ///
    /// `Err` means that message was not registered and should not be sent.
    /// This function also take care of reporting error through `tx`.
    #[inline]
    fn try_prepare_request(
        &mut self,
        request: &EncodedRequest,
        generation: Option<u64>,
        current_generation: u64,
        tx: DispatcherResponseSender,
    ) -> Result<(), ()> {
        if let Some(captured) = generation
            && captured != current_generation
        {
            debug!(
                "Rejecting request with sync {} from generation {}, connection is at {}",
                request.sync, captured, current_generation
            );
            if tx.send(Err(Error::ConnectionReset)).is_err() {
                debug!(
                    "Failed to pass ConnectionReset to sync {}, receiver dropped",
                    request.sync
                );
            }
            return Err(());
        }
        trace!(
            "Sending request with sync {}, stream_id {:?}",
            request.sync, request.stream_id
        );
        // TODO: replace with try_insert when stabilized
        // Safety net: syncs are unique among in-flight requests by
        // construction, but a colliding one is rejected rather than
        // overwriting the older entry.
        if let Some(old) = self.in_flights.insert(request.sync, tx) {
            let new = self
                .in_flights
                .insert(request.sync, old)
                .expect("Shouldn't panic, value was just inserted");
            if new.send(Err(Error::DuplicatedSync(request.sync))).is_err() {
                warn!(
                    "Failed to pass error to sync {}, receiver dropped",
                    request.sync
                );
            }
            return Err(());
        }
        Ok(())
    }

    /// Forget the request with `sync`: its caller timed out and dropped the
    /// receiver. A response arriving later is logged as unknown and dropped.
    #[inline]
    fn cancel(&mut self, sync: u64) {
        if self.in_flights.remove(&sync).is_some() {
            trace!("Cancelled request with sync {}", sync);
        }
    }

    /// Send result of processing request (by sync) to client.
    #[inline]
    fn respond_to_client(&mut self, sync: u64, response: Result<Response, Error>) {
        match self.in_flights.remove(&sync) {
            Some(tx) => {
                if tx.send(response).is_err() {
                    // Expected race: the caller timed out and dropped its
                    // receiver, and this response was read before the
                    // queued `Cancel(sync)` was processed.
                    debug!("Failed to pass response sync {}, receiver dropped", sync);
                }
            }
            _ => {
                // Expected for a response whose caller is gone (for example a
                // request cancelled after a timeout), so not worth a warning.
                debug!("Unknown sync {}", sync);
            }
        }
    }

    /// Send error to all in-flight requests and drop them.
    #[inline]
    fn send_error_to_all_in_flights(&mut self, err: &ConnectionError) {
        for (_, tx) in self.in_flights.drain() {
            let _ = tx.send(Err(Error::from(err.clone())));
        }
    }
}

// NOTE: here is weird logic, where task can be cancelld using token and when
// rx closed. Token is necessary to close task when it currently sending to socket.
async fn writer_task(
    mut rx: mpsc::Receiver<EncodedRequest>,
    mut stream: FramedWrite<OwnedWriteHalf, ClientCodec>,
    cancellation_token: CancellationToken,
) -> Result<(), (u64, CodecEncodeError)> {
    let mut result = Ok(());

    loop {
        // NOTE: waiting for the next request must also be interruptible by the
        // token: `writer_tx` lives in `Connection::run` until the very end of the
        // function, so `rx.recv()` on its own never returns `None` and `run`
        // would hang forever on `writer_task_handle.await`.
        // `None` means the token was cancelled, `Some(None)` that the queue was
        // closed; both end the loop.
        let Some(Some(x)) = cancellation_token.run_until_cancelled(rx.recv()).await else {
            break;
        };
        let sync = x.sync;
        match cancellation_token.run_until_cancelled(stream.send(x)).await {
            Some(Ok(())) => {}
            Some(Err(err)) => {
                result = Err((sync, err));
                break;
            }
            None => {
                // Do not set error since task was cancelled externally.
                // Should respond with ConnectionClosed in main task
                break;
            }
        }
    }

    cancellation_token.cancel();

    // TODO: reenable or pass strema back into main task
    // if let Err(err) = stream.into_inner().shutdown().await {
    //     warn!("Failed to shutdown TCP stream cleanly: {err}");
    // }

    result
}

type WriterTaskJoinHandle = JoinHandle<Result<(), (u64, CodecEncodeError)>>;

pub(crate) struct Connection {
    read_stream: FramedRead<OwnedReadHalf, ClientCodec>,
    writer_tx: mpsc::Sender<EncodedRequest>,
    writer_task_handle: WriterTaskJoinHandle,
    writer_task_cancellation_token: CancellationToken,
    data: ConnectionData,
}

impl Connection {
    async fn new_inner<A>(
        addr: A,
        user: Option<&str>,
        password: Option<&str>,
        internal_simultaneous_requests_threshold: usize,
    ) -> Result<Self, Error>
    where
        A: ToSocketAddrs + Display,
    {
        debug!("Starting connection to Tarantool {}", addr);
        let mut tcp = TcpStream::connect(&addr).await?;
        trace!("Connection established to {}", addr);

        let mut greeting_buffer = [0u8; Greeting::SIZE];
        tcp.read_exact(&mut greeting_buffer).await?;
        let greeting = Greeting::decode(greeting_buffer)?;
        debug!("Server: {}", greeting.server);
        trace!("Salt: {:?}", greeting.salt);

        let (read_tcp_stream, write_tcp_stream) = tcp.into_split();
        let mut read_stream = FramedRead::new(read_tcp_stream, ClientCodec::default());
        let mut write_stream = FramedWrite::new(write_tcp_stream, ClientCodec::default());

        let auth = user
            .map(|user| handshake_request(&Auth::new(user, password, &greeting.salt), AUTH_SYNC))
            .transpose()?;
        // TODO: add option to disable pre 2.10 features (ID request, streams, watchers)
        // Sent on every connection: a session enables exactly the features the
        // client lists, so a reconnected session gets them as well.
        let id = Id::default();
        debug!("Sending IPROTO_ID: {:?}", id);
        let (auth_body, id_body) = Self::handshake(
            &mut read_stream,
            &mut write_stream,
            auth,
            handshake_request(&id, ID_SYNC)?,
        )
        .await?;
        // When both fail, the AUTH error wins: it is the actionable one.
        if let Some(ResponseBody::Error(err)) = auth_body {
            return Err(Error::Auth(err));
        }
        match id_body {
            // The server's own version and features, not a negotiated set.
            ResponseBody::Ok(body) => {
                debug!("Server capabilities from the IPROTO_ID reply: {}", body);
            }
            ResponseBody::Error(err) => return Err(Error::Response(err)),
        }

        // TODO: review size of this queue
        // Make this queue slightly larger than queue between Client and Dispatcher
        let (writer_tx, writer_rx) = mpsc::channel(
            (internal_simultaneous_requests_threshold.saturating_mul(105) / 100).max(1),
        );
        let writer_task_cancellation_token = CancellationToken::new();
        let writer_task_handle = tokio::spawn(writer_task(
            writer_rx,
            write_stream,
            writer_task_cancellation_token.clone(),
        ));

        let this = Self {
            read_stream,
            writer_tx,
            writer_task_handle,
            writer_task_cancellation_token,
            data: ConnectionData::default(),
        };

        Ok(this)
    }

    pub(super) async fn new<A>(
        addr: A,
        user: Option<&str>,
        password: Option<&str>,
        timeout: Option<Duration>,
        internal_simultaneous_requests_threshold: usize,
    ) -> Result<Self, Error>
    where
        A: ToSocketAddrs + Display,
    {
        match timeout {
            Some(dur) => tokio::time::timeout(
                dur,
                Self::new_inner(
                    addr,
                    user,
                    password,
                    internal_simultaneous_requests_threshold,
                ),
            )
            .await
            .map_err(|_| Error::ConnectTimeout)
            .and_then(|x| x),
            None => {
                Self::new_inner(
                    addr,
                    user,
                    password,
                    internal_simultaneous_requests_threshold,
                )
                .await
            }
        }
    }

    /// Send AUTH (when `auth` is set) and `IPROTO_ID` in one write, then read
    /// one reply per request and return the bodies of the AUTH and ID replies.
    ///
    /// The server may run the two requests on different fibers, so the ID
    /// reply can come first; replies are matched by sync. A reply with an
    /// unknown or repeated sync fails the handshake.
    async fn handshake(
        read_stream: &mut FramedRead<OwnedReadHalf, ClientCodec>,
        write_stream: &mut FramedWrite<OwnedWriteHalf, ClientCodec>,
        auth: Option<EncodedRequest>,
        id: EncodedRequest,
    ) -> Result<(Option<ResponseBody>, ResponseBody), Error> {
        let auth_sync = auth.as_ref().map(|request| request.sync);
        let id_sync = id.sync;
        trace!("Sending handshake requests");
        for request in auth.into_iter().chain([id]) {
            write_stream.feed(request).await?;
        }
        write_stream.flush().await?;

        let unexpected = |sync: u64| {
            Error::Other(anyhow::anyhow!(
                "Unexpected sync {sync} in handshake response"
            ))
        };
        let mut auth_body = None;
        let id_body = loop {
            let response = Self::get_next_stream_value(read_stream).await?;
            if response.sync == id_sync {
                break response.body;
            }
            if Some(response.sync) != auth_sync || auth_body.is_some() {
                return Err(unexpected(response.sync));
            }
            auth_body = Some(response.body);
        };
        // The ID reply came first: the AUTH reply is still on its way.
        if auth_sync.is_some() && auth_body.is_none() {
            let response = Self::get_next_stream_value(read_stream).await?;
            if Some(response.sync) != auth_sync {
                return Err(unexpected(response.sync));
            }
            auth_body = Some(response.body);
        }
        Ok((auth_body, id_body))
    }

    #[inline]
    async fn get_next_stream_value(
        read_stream: &mut FramedRead<OwnedReadHalf, ClientCodec>,
    ) -> Result<Response, ConnectionError> {
        match read_stream.try_next().await {
            Ok(Some(x)) => Ok(x),
            Ok(None) => Err(ConnectionError::ConnectionClosed),
            Err(e) => Err(e.into()),
        }
    }

    #[inline]
    fn handle_response(connection_data: &mut ConnectionData, response: Response) {
        trace!(
            "Received response for sync {}, schema version {}",
            response.sync, response.schema_version
        );
        connection_data.respond_to_client(response.sync, Ok(response));
    }

    /// Run connection until it breaks of `rx` is closed.
    ///
    /// `Ok` means `rx` was closed and connection should not be restarted.
    /// `Err` means connection was dropped due to some error.
    pub(crate) async fn run(
        self,
        client_rx: &mut ReceiverStream<DispatcherMessage>,
        current_generation: &AtomicU64,
    ) -> Result<(), ()> {
        let Self {
            mut read_stream,
            writer_tx,
            writer_task_handle,
            writer_task_cancellation_token,
            mut data,
        } = self;

        let send_to_writer_future = Fuse::terminated();
        pin!(send_to_writer_future);

        let mut result = loop {
            tokio::select! {
                // Read value from TCP stream
                next = Connection::get_next_stream_value(&mut read_stream) => {
                    match next {
                        Ok(x) => Connection::handle_response(&mut data, x),
                        Err(err) => break Err(err),
                    }
                }

                // Read value from internal queue if nothing being sent to writer
                next = client_rx.next(), if send_to_writer_future.is_terminated() => {
                    match next {
                        Some(DispatcherMessage::Request(ClientRequest { request, generation, responder })) => {
                            // If failed to prepare request (stale generation,
                            // duplicate sync) or client already dropped
                            // oneshot - just go to next
                            if responder.is_closed() || data
                                .try_prepare_request(
                                    &request,
                                    generation,
                                    current_generation.load(Ordering::Acquire),
                                    responder,
                                )
                                .is_err()
                            {
                                continue;
                            }

                            send_to_writer_future.set(writer_tx.send(request).fuse());
                        }
                        Some(DispatcherMessage::Cancel(sync)) => data.cancel(sync),
                        None => {
                            // TODO: actually don't quit until all in-flights processed
                            debug!("All senders dropped");
                            break Ok(());
                        }
                    }
                }

                // Await sending request to writer.
                // NOTE: For some reason checking Fuse for termination makes code _slightly_ faster
                send_res = &mut send_to_writer_future, if !send_to_writer_future.is_terminated() => {
                    // Error means the writer queue is closed and the connection
                    // must be torn down. The request inside the error is already
                    // registered in `in_flights`, so the teardown below answers
                    // its caller with `ConnectionClosed`.
                    if send_res.is_err() {
                        break Err(ConnectionError::ConnectionClosed)
                    }
                }
            }
        };
        writer_task_cancellation_token.cancel();

        // Wait for writer task to finish
        match writer_task_handle.await {
            Err(err) => {
                error!("Failed to await writer task's handle: {err}");
                result = result.and(Err(err.into()));
            }
            Ok(Err((sync, err))) => data.respond_to_client(sync, Err(err.into())),
            Ok(Ok(())) => {}
        }

        // Every request registered in `in_flights` is answered here, whether it
        // was queued for the writer, being written, or awaiting its response.
        data.send_error_to_all_in_flights(
            &result
                .clone()
                .err()
                .unwrap_or(ConnectionError::ConnectionClosed),
        );

        result.map_err(drop)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpListener, time::timeout};
    use tracing_test::traced_test;

    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use rmpv::Value;
    use tokio::sync::oneshot;

    use crate::codec::consts::{RequestType, keys, response_codes::ERROR_RANGE_START};
    use crate::{codec::request::Ping, transport::DispatcherSender};

    /// Syntactically valid 128-byte greeting whose salt is 32 zero bytes.
    fn fake_greeting() -> [u8; Greeting::SIZE] {
        let mut buf = [b' '; Greeting::SIZE];
        let line1 = b"Tarantool 2.11.0 (Binary) fake-uuid";
        buf[..line1.len()].copy_from_slice(line1);
        buf[63] = b'\n';
        let mut salt = [b'A'; 44];
        salt[43] = b'=';
        buf[64..108].copy_from_slice(&salt);
        buf[127] = b'\n';
        buf
    }

    /// Read one request frame sent by the client and return its
    /// `(request_type, sync)`, or `None` once the client closed the socket.
    async fn read_request(sock: &mut TcpStream) -> Option<(u8, u64)> {
        // `ClientCodec` always writes the length as MP_UINT64: 0xcf + 8 bytes.
        let mut len_buf = [0u8; 9];
        sock.read_exact(&mut len_buf).await.ok()?;
        let len = u64::from_be_bytes(len_buf[1..].try_into().unwrap());
        let mut frame = vec![0u8; usize::try_from(len).unwrap()];
        sock.read_exact(&mut frame).await.ok()?;
        let header = rmpv::decode::read_value(&mut &frame[..]).unwrap();
        let field = |key: u64| {
            header
                .as_map()
                .unwrap()
                .iter()
                .find(|(k, _)| k.as_u64() == Some(key))
                .and_then(|(_, v)| v.as_u64())
                .unwrap()
        };
        Some((u8::try_from(field(0x00)).unwrap(), field(0x01)))
    }

    /// Response frame `{RESPONSE_CODE: code, SYNC: sync, SCHEMA_VERSION: 1}`
    /// followed by `body`.
    fn response_frame(code: u32, sync: u64, body: &Value) -> Vec<u8> {
        let mut payload = Vec::new();
        rmp::encode::write_map_len(&mut payload, 3).unwrap();
        rmp::encode::write_pfix(&mut payload, keys::RESPONSE_CODE).unwrap();
        rmp::encode::write_uint(&mut payload, u64::from(code)).unwrap();
        rmp::encode::write_pfix(&mut payload, keys::SYNC).unwrap();
        rmp::encode::write_uint(&mut payload, sync).unwrap();
        rmp::encode::write_pfix(&mut payload, keys::SCHEMA_VERSION).unwrap();
        rmp::encode::write_pfix(&mut payload, 1).unwrap();
        rmpv::encode::write_value(&mut payload, body).unwrap();
        let mut frame = Vec::new();
        rmp::encode::write_u32(&mut frame, u32::try_from(payload.len()).unwrap()).unwrap();
        frame.extend_from_slice(&payload);
        frame
    }

    /// Response frame `{RESPONSE_CODE: 0, SYNC: sync, SCHEMA_VERSION: 1}` + `body`.
    fn ok_response(sync: u64, body: &Value) -> Vec<u8> {
        response_frame(0, sync, body)
    }

    /// Error response frame for Tarantool error `code`, whose description is
    /// `message`.
    fn error_response(sync: u64, code: u32, message: &str) -> Vec<u8> {
        let body = Value::Map(vec![(Value::from(keys::ERROR_24), Value::from(message))]);
        response_frame(ERROR_RANGE_START + code, sync, &body)
    }

    /// Body of an `IPROTO_ID` response: `{VERSION: 3, FEATURES: [0, 1, 2]}`.
    fn id_response_body() -> Value {
        Value::Map(vec![
            (Value::from(0x54u8), Value::from(3u8)),
            (
                Value::from(0x55u8),
                Value::Array(vec![Value::from(0u8), Value::from(1u8), Value::from(2u8)]),
            ),
        ])
    }

    /// OK body for a request of `request_type`: an `IPROTO_ID` body for ID
    /// requests, an empty map for the rest.
    fn ok_body_for(request_type: u8) -> Value {
        if request_type == RequestType::Id as u8 {
            id_response_body()
        } else {
            Value::Map(Vec::new())
        }
    }

    /// `sync_for` that answers every request with its own sync.
    pub(crate) fn echo_sync(_request_type: u8, sync: u64) -> u64 {
        sync
    }

    /// Fake server: writes the greeting, then answers every request with an
    /// OK response (an `IPROTO_ID` one for ID requests). `sync_for` maps the
    /// request's `(request_type, sync)` to the sync put into the response, so
    /// tests can inject a mismatch.
    pub(crate) async fn spawn_fake_server(sync_for: fn(u8, u64) -> u64) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if sock.write_all(&fake_greeting()).await.is_err() {
                        return;
                    }
                    while let Some((request_type, sync)) = read_request(&mut sock).await {
                        let response =
                            ok_response(sync_for(request_type, sync), &ok_body_for(request_type));
                        if sock.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    /// Builds the reply frames from the `(request_type, sync)` pairs of the
    /// handshake requests.
    type HandshakeAnswer = fn(&[(u8, u64)]) -> Vec<Vec<u8>>;

    /// Fake server for one handshake: writes the greeting and reads
    /// `expected` requests before it answers any of them, then writes the
    /// frames `answer` builds. A client that waits for one reply before it
    /// sends the next request hangs here.
    async fn spawn_handshake_server(expected: usize, answer: HandshakeAnswer) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(&fake_greeting()).await.unwrap();
            let mut requests = Vec::new();
            for _ in 0..expected {
                requests.push(read_request(&mut sock).await.unwrap());
            }
            for frame in answer(&requests) {
                sock.write_all(&frame).await.unwrap();
            }
            // Keep the socket open until the client closes it.
            let _ = read_request(&mut sock).await;
        });
        addr
    }

    /// Replies to handshake requests in request order; request types listed
    /// in `failing` get an error reply.
    fn replies(requests: &[(u8, u64)], failing: &[RequestType]) -> Vec<Vec<u8>> {
        requests
            .iter()
            .map(|&(request_type, sync)| {
                if failing.iter().any(|x| *x as u8 == request_type) {
                    error_response(sync, 1, "rejected")
                } else {
                    ok_response(sync, &ok_body_for(request_type))
                }
            })
            .collect()
    }

    /// Handshake as "user" against `addr`, bounded, so a client that does not
    /// pipeline AUTH and ID fails instead of hanging.
    async fn handshake_as_user(addr: String) -> Result<Connection, Error> {
        timeout(
            Duration::from_secs(1),
            Connection::new_inner(addr, Some("user"), Some("pass"), 500),
        )
        .await
        .expect("the client waited for the AUTH reply before sending ID")
    }

    #[tokio::test]
    async fn auth_and_id_are_sent_before_any_reply() {
        let addr = spawn_handshake_server(2, |requests| replies(requests, &[])).await;
        assert!(handshake_as_user(addr).await.is_ok());
    }

    #[tokio::test]
    async fn id_reply_before_auth_reply_completes_the_handshake() {
        let addr = spawn_handshake_server(2, |requests| {
            let mut frames = replies(requests, &[]);
            frames.reverse();
            frames
        })
        .await;
        assert!(handshake_as_user(addr).await.is_ok());
    }

    #[tokio::test]
    async fn failed_auth_is_an_auth_error() {
        let addr =
            spawn_handshake_server(2, |requests| replies(requests, &[RequestType::Auth])).await;
        assert!(matches!(handshake_as_user(addr).await, Err(Error::Auth(_))));
    }

    #[tokio::test]
    async fn failed_id_is_a_response_error() {
        let addr =
            spawn_handshake_server(2, |requests| replies(requests, &[RequestType::Id])).await;
        assert!(matches!(
            handshake_as_user(addr).await,
            Err(Error::Response(_))
        ));
    }

    #[tokio::test]
    async fn auth_error_wins_when_auth_and_id_fail() {
        let addr = spawn_handshake_server(2, |requests| {
            replies(requests, &[RequestType::Auth, RequestType::Id])
        })
        .await;
        assert!(matches!(handshake_as_user(addr).await, Err(Error::Auth(_))));
    }

    #[tokio::test]
    async fn small_threshold_does_not_panic() {
        let addr = spawn_fake_server(echo_sync).await;
        let conn = Connection::new_inner(addr, None, None, 50).await;
        assert!(conn.is_ok());
    }

    /// Build a connection over a local socket pair, with a custom writer task.
    async fn connection_with_writer(
        writer: impl Future<Output = Result<(), (u64, CodecEncodeError)>> + Send + 'static,
    ) -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (read, _write) = client.into_split();
        let (writer_tx, _writer_rx) = mpsc::channel(1);
        let conn = Connection {
            read_stream: FramedRead::new(read, ClientCodec::default()),
            writer_tx,
            writer_task_handle: tokio::spawn(writer),
            writer_task_cancellation_token: CancellationToken::new(),
            data: ConnectionData::default(),
        };
        (conn, server)
    }

    #[tokio::test]
    async fn writer_task_panic_is_reported_as_error() {
        let (conn, _server) =
            connection_with_writer(async { panic!("writer task panicked") }).await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherMessage>(1);
        let mut client_rx = ReceiverStream::new(client_rx);
        drop(client_tx);
        assert!(conn.run(&mut client_rx, &AtomicU64::new(0)).await.is_err());
    }

    #[tokio::test]
    async fn writer_task_clean_exit_is_reported_as_ok() {
        let (conn, _server) = connection_with_writer(async { Ok(()) }).await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherMessage>(1);
        let mut client_rx = ReceiverStream::new(client_rx);
        drop(client_tx);
        assert!(conn.run(&mut client_rx, &AtomicU64::new(0)).await.is_ok());
    }

    #[tokio::test]
    async fn default_threshold_does_not_panic() {
        let addr = spawn_fake_server(echo_sync).await;
        let conn = Connection::new_inner(addr, None, None, 500).await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn auth_response_with_matching_sync_is_accepted() {
        let addr = spawn_fake_server(echo_sync).await;
        let conn = Connection::new_inner(addr, Some("user"), Some("pass"), 500).await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn auth_response_with_other_sync_is_rejected() {
        let addr = spawn_fake_server(|request_type, sync| {
            if request_type == RequestType::Auth as u8 {
                42
            } else {
                sync
            }
        })
        .await;
        let conn = Connection::new_inner(addr, Some("user"), Some("pass"), 500).await;
        assert!(matches!(conn, Err(Error::Other(_))));
    }

    #[tokio::test]
    async fn id_response_with_other_sync_is_rejected() {
        let addr = spawn_fake_server(|request_type, sync| {
            if request_type == RequestType::Id as u8 {
                42
            } else {
                sync
            }
        })
        .await;
        let conn = Connection::new_inner(addr, None, None, 500).await;
        assert!(matches!(conn, Err(Error::Other(_))));
    }

    #[tokio::test]
    #[traced_test]
    async fn teardown_answers_registered_request_without_unknown_sync() {
        // The writer queue's receiver is already gone, so handing the request
        // to the writer fails and the connection tears down while the request
        // is registered in `in_flights`.
        let (conn, _server) = connection_with_writer(async { Ok(()) }).await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherMessage>(1);
        let mut client_rx = ReceiverStream::new(client_rx);
        let sender = DispatcherSender::new_for_test(client_tx, Arc::default());
        // `join!` borrows across its whole body, so the generation needs a
        // binding rather than a temporary.
        let generation = AtomicU64::new(0);

        // `join!` keeps both futures inside the test's tracing span, which
        // `logs_contain` needs; a spawned task would log outside it.
        let (run_res, send_res) = tokio::join!(
            conn.run(&mut client_rx, &generation),
            sender.send(EncodedRequest::new(&Ping {}, None).unwrap(), None),
        );

        assert!(run_res.is_err());
        assert!(
            matches!(send_res, Err(Error::ConnectionClosed)),
            "{send_res:?}"
        );
        assert!(!logs_contain("Unknown sync"));
    }

    fn ping_with_sync(sync: u64) -> EncodedRequest {
        let mut request = EncodedRequest::new(&Ping {}, None).unwrap();
        *request.sync_mut() = sync;
        request
    }

    /// A ping issued through a stream (stream id 1).
    fn stream_ping_with_sync(sync: u64) -> EncodedRequest {
        let mut request = EncodedRequest::new(&Ping {}, Some(1)).unwrap();
        *request.sync_mut() = sync;
        request
    }

    fn client_request(
        request: EncodedRequest,
        generation: Option<u64>,
        tx: oneshot::Sender<Result<Response, Error>>,
    ) -> DispatcherMessage {
        DispatcherMessage::Request(ClientRequest {
            request,
            generation,
            responder: DispatcherResponseSender(tx),
        })
    }

    /// Connection over a local socket pair with the real writer task and no
    /// handshake; the returned stream is the server side of the socket.
    async fn connected_pair() -> (Connection, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (read, write) = client.into_split();
        let (writer_tx, writer_rx) = mpsc::channel(8);
        let token = CancellationToken::new();
        let conn = Connection {
            read_stream: FramedRead::new(read, ClientCodec::default()),
            writer_tx,
            writer_task_handle: tokio::spawn(writer_task(
                writer_rx,
                FramedWrite::new(write, ClientCodec::default()),
                token.clone(),
            )),
            writer_task_cancellation_token: token,
            data: ConnectionData::default(),
        };
        (conn, server)
    }

    #[test]
    fn cancel_removes_in_flight_entry() {
        let mut data = ConnectionData::default();
        let (tx, mut rx) = oneshot::channel();
        assert!(
            data.try_prepare_request(&ping_with_sync(7), None, 0, DispatcherResponseSender(tx))
                .is_ok()
        );

        data.cancel(7);

        assert!(data.in_flights.is_empty());
        assert!(matches!(
            rx.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
    }

    #[test]
    fn stale_generation_is_answered_without_registering() {
        let mut data = ConnectionData::default();

        // Issued through a stream at generation 0; the connection is at 1.
        let (tx, mut rx) = oneshot::channel();
        assert!(
            data.try_prepare_request(
                &stream_ping_with_sync(8),
                Some(0),
                1,
                DispatcherResponseSender(tx)
            )
            .is_err()
        );
        assert!(data.in_flights.is_empty());
        assert!(matches!(rx.try_recv(), Ok(Err(Error::ConnectionReset))));

        // Control: the same request without a generation is registered.
        let (tx, _rx) = oneshot::channel();
        assert!(
            data.try_prepare_request(
                &stream_ping_with_sync(8),
                None,
                1,
                DispatcherResponseSender(tx)
            )
            .is_ok()
        );
        assert!(data.in_flights.contains_key(&8));
    }

    #[tokio::test]
    async fn stale_stream_request_queued_across_reconnect_is_rejected() {
        let (conn, mut server) = connected_pair().await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherMessage>(4);
        let mut client_rx = ReceiverStream::new(client_rx);
        // The dispatcher already reconnected: this connection is generation 1.
        let generation = AtomicU64::new(1);
        let (stale_tx, stale_rx) = oneshot::channel();
        let (plain_tx, plain_rx) = oneshot::channel();

        let script = async move {
            // Both requests were queued before the reconnect finished: one
            // issued through a stream at generation 0, the same one without a
            // generation as the control.
            client_tx
                .send(client_request(stream_ping_with_sync(10), Some(0), stale_tx))
                .await
                .unwrap();
            client_tx
                .send(client_request(stream_ping_with_sync(11), None, plain_tx))
                .await
                .unwrap();

            // Only the control reaches the server.
            assert_eq!(read_request(&mut server).await.unwrap().1, 11);
            server
                .write_all(&ok_response(11, &Value::Map(Vec::new())))
                .await
                .unwrap();
            let plain = plain_rx.await;

            drop(client_tx);
            (server, stale_rx.await, plain)
        };
        let (run_res, (_server, stale, plain)) =
            tokio::join!(conn.run(&mut client_rx, &generation), script);

        assert!(run_res.is_ok());
        assert!(matches!(stale, Ok(Err(Error::ConnectionReset))));
        assert!(matches!(plain, Ok(Ok(_))));
    }

    #[tokio::test]
    #[traced_test]
    async fn late_response_for_cancelled_sync_is_ignored() {
        let (conn, mut server) = connected_pair().await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherMessage>(4);
        let mut client_rx = ReceiverStream::new(client_rx);
        let generation = AtomicU64::new(0);
        let (tx5, rx5) = oneshot::channel();

        let script = async move {
            client_tx
                .send(client_request(ping_with_sync(5), None, tx5))
                .await
                .unwrap();
            assert_eq!(read_request(&mut server).await.unwrap().1, 5);

            // The caller times out: it drops its receiver and cancels.
            drop(rx5);
            client_tx.send(DispatcherMessage::Cancel(5)).await.unwrap();

            // Request 6 reaching the server proves Cancel(5), queued before
            // it, was processed.
            let (tx6, rx6) = oneshot::channel();
            client_tx
                .send(client_request(ping_with_sync(6), None, tx6))
                .await
                .unwrap();
            assert_eq!(read_request(&mut server).await.unwrap().1, 6);

            // The late response for 5 is read before the one for 6.
            server
                .write_all(&ok_response(5, &Value::Map(Vec::new())))
                .await
                .unwrap();
            server
                .write_all(&ok_response(6, &Value::Map(Vec::new())))
                .await
                .unwrap();
            assert!(matches!(rx6.await, Ok(Ok(_))));

            drop(client_tx);
            server
        };
        let (run_res, _server) = tokio::join!(conn.run(&mut client_rx, &generation), script);

        assert!(run_res.is_ok());
        assert!(logs_contain("Unknown sync 5"));
        assert!(!logs_contain("receiver dropped"));
    }

    #[tokio::test]
    async fn sync_above_u32_reaches_its_caller() {
        let (conn, mut server) = connected_pair().await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherMessage>(4);
        let mut client_rx = ReceiverStream::new(client_rx);
        let generation = AtomicU64::new(0);
        let sync = u64::from(u32::MAX) + 5;
        let (tx, rx) = oneshot::channel();

        let script = async move {
            client_tx
                .send(client_request(ping_with_sync(sync), None, tx))
                .await
                .unwrap();
            // The server sees the whole 64-bit sync and echoes it back.
            assert_eq!(read_request(&mut server).await.unwrap().1, sync);
            server
                .write_all(&ok_response(sync, &Value::Map(Vec::new())))
                .await
                .unwrap();
            let response = rx.await;
            drop(client_tx);
            (server, response)
        };
        let (run_res, (_server, response)) =
            tokio::join!(conn.run(&mut client_rx, &generation), script);

        assert!(run_res.is_ok());
        assert!(
            matches!(response, Ok(Ok(ref r)) if r.sync == sync),
            "{response:?}"
        );
    }
}
