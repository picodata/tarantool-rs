use std::{collections::HashMap, fmt::Display, time::Duration};

use futures::{
    FutureExt, SinkExt, StreamExt, TryStreamExt,
    future::{Fuse, FusedFuture},
};

use parking_lot::RwLock;
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

use super::dispatcher::{DispatcherRequest, DispatcherResponse, DispatcherResponseSender};
use crate::{
    codec::{
        ClientCodec, Greeting,
        request::{Auth, ConnectionFeatures, EncodedRequest, Id},
        response::{Response, ResponseBody},
    },
    errors::{CodecEncodeError, ConnectionError, Error},
};

struct ConnectionData {
    in_flights: HashMap<u32, DispatcherResponseSender>,
    next_sync: u32,
}

impl Default for ConnectionData {
    fn default() -> Self {
        Self {
            in_flights: HashMap::with_capacity(5),
            next_sync: 0,
        }
    }
}

impl ConnectionData {
    #[inline]
    fn next_sync(&mut self) -> u32 {
        let next = self.next_sync;
        self.next_sync = self.next_sync.wrapping_add(1);
        next
    }

    /// Prepare request for sending to server.
    ///
    /// Set `sync` value and attempt to store this message in in-flight storage.
    ///
    /// `Err` means that message was not prepared and should not be sent.
    /// This function also take care of reporting error through `tx`.
    #[inline]
    fn try_prepare_request(
        &mut self,
        request: &mut EncodedRequest,
        tx: DispatcherResponseSender,
    ) -> Result<(), ()> {
        let sync = self.next_sync();
        *request.sync_mut() = sync;
        trace!(
            "Sending request with sync {}, stream_id {:?}",
            request.sync, request.stream_id
        );
        // TODO: replace with try_insert when stabilized
        // If sync already assigned to another request, return an error
        // for current request
        if let Some(old) = self.in_flights.insert(request.sync, tx) {
            let new = self
                .in_flights
                .insert(request.sync, old)
                .expect("Shouldn't panic, value was just inserted");
            if new.send(Error::DuplicatedSync(request.sync)).is_err() {
                warn!(
                    "Failed to pass error to sync {}, receiver dropped",
                    request.sync
                );
            }
            return Err(());
        }
        Ok(())
    }

    /// Send result of processing request (by sync) to client.
    #[inline]
    fn respond_to_client(&mut self, sync: u32, response: impl Into<DispatcherResponse>) {
        match self.in_flights.remove(&sync) {
            Some(tx) => {
                if tx.send(response).is_err() {
                    warn!("Failed to pass response sync {}, receiver dropped", sync);
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
            let _ = tx.send(Error::from(err.clone()));
        }
    }
}

// NOTE: here is weird logic, where task can be cancelld using token and when
// rx closed. Token is necessary to close task when it currently sending to socket.
async fn writer_task(
    mut rx: mpsc::Receiver<EncodedRequest>,
    mut stream: FramedWrite<OwnedWriteHalf, ClientCodec>,
    cancellation_token: CancellationToken,
) -> Result<(), (u32, CodecEncodeError)> {
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

type WriterTaskJoinHandle = JoinHandle<Result<(), (u32, CodecEncodeError)>>;

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
        features: &RwLock<ConnectionFeatures>,
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

        let mut conn_data = ConnectionData::default();

        if let Some(user) = user {
            Self::auth(
                &mut read_stream,
                &mut write_stream,
                conn_data.next_sync(),
                user,
                password,
                &greeting.salt,
            )
            .await?;
        }

        // TODO: add option to disable pre 2.10 features (ID request, streams, watchers)
        // Runs on every connection, so a reconnect re-negotiates features.
        let negotiated =
            Self::id(&mut read_stream, &mut write_stream, conn_data.next_sync()).await?;
        debug!("Negotiated features: {:?}", negotiated);
        *features.write() = negotiated;

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
            data: conn_data,
        };

        Ok(this)
    }

    pub(super) async fn new<A>(
        addr: A,
        user: Option<&str>,
        password: Option<&str>,
        timeout: Option<Duration>,
        internal_simultaneous_requests_threshold: usize,
        features: &RwLock<ConnectionFeatures>,
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
                    features,
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
                    features,
                )
                .await
            }
        }
    }

    async fn auth(
        read_stream: &mut FramedRead<OwnedReadHalf, ClientCodec>,
        write_stream: &mut FramedWrite<OwnedWriteHalf, ClientCodec>,
        sync: u32,
        user: &str,
        password: Option<&str>,
        salt: &[u8],
    ) -> Result<(), Error> {
        let mut request = EncodedRequest::new(&Auth::new(user, password, salt), None).unwrap();
        *request.sync_mut() = sync;

        trace!("Sending auth request");
        write_stream.send(request).await?;

        let resp = Self::get_next_stream_value(read_stream).await?;
        if resp.sync != sync {
            return Err(Error::Other(anyhow::anyhow!(
                "Unexpected sync {} in auth response, expected {}",
                resp.sync,
                sync
            )));
        }
        match resp.body {
            ResponseBody::Ok(_x) => Ok(()),
            ResponseBody::Error(err) => Err(Error::Auth(err)),
        }
    }

    /// Send `IPROTO_ID` with the features this crate supports and return what
    /// the server agreed to. Same raw-stream pattern as [`Self::auth`].
    async fn id(
        read_stream: &mut FramedRead<OwnedReadHalf, ClientCodec>,
        write_stream: &mut FramedWrite<OwnedWriteHalf, ClientCodec>,
        sync: u32,
    ) -> Result<ConnectionFeatures, Error> {
        let mut request = EncodedRequest::new(&Id::default(), None)?;
        *request.sync_mut() = sync;

        trace!("Sending ID request");
        write_stream.send(request).await?;

        let resp = Self::get_next_stream_value(read_stream).await?;
        if resp.sync != sync {
            return Err(Error::Other(anyhow::anyhow!(
                "Unexpected sync {} in ID response, expected {}",
                resp.sync,
                sync
            )));
        }
        match resp.body {
            ResponseBody::Ok(body) => Ok(ConnectionFeatures::decode(&body)?),
            ResponseBody::Error(err) => Err(Error::Response(err)),
        }
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
        client_rx: &mut ReceiverStream<DispatcherRequest>,
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
                    if let Some((mut request, tx)) = next {
                        // If failed to prepare request or client already
                        // dropped oneshot - just go to next
                        if tx.is_closed() || data
                            .try_prepare_request(&mut request, tx)
                            .is_err()
                        {
                            continue;
                        }

                        send_to_writer_future.set(writer_tx.send(request).fuse());
                    } else {
                        // TODO: actually don't quit until all in-flights processed
                        debug!("All senders dropped");
                        break Ok(());
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
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tracing_test::traced_test;

    use std::sync::Arc;

    use parking_lot::RwLock;
    use rmpv::Value;

    use crate::codec::consts::RequestType;
    use crate::codec::request::ConnectionFeatures;
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
    async fn read_request(sock: &mut TcpStream) -> Option<(u8, u32)> {
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
        Some((
            u8::try_from(field(0x00)).unwrap(),
            u32::try_from(field(0x01)).unwrap(),
        ))
    }

    /// Response frame `{RESPONSE_CODE: 0, SYNC: sync, SCHEMA_VERSION: 1}` + `body`.
    fn ok_response(sync: u32, body: &Value) -> Vec<u8> {
        let mut payload = Vec::new();
        rmp::encode::write_map_len(&mut payload, 3).unwrap();
        rmp::encode::write_pfix(&mut payload, 0x00).unwrap();
        rmp::encode::write_pfix(&mut payload, 0x00).unwrap();
        rmp::encode::write_pfix(&mut payload, 0x01).unwrap();
        rmp::encode::write_u32(&mut payload, sync).unwrap();
        rmp::encode::write_pfix(&mut payload, 0x05).unwrap();
        rmp::encode::write_pfix(&mut payload, 0x01).unwrap();
        rmpv::encode::write_value(&mut payload, body).unwrap();
        let mut frame = Vec::new();
        rmp::encode::write_u32(&mut frame, u32::try_from(payload.len()).unwrap()).unwrap();
        frame.extend_from_slice(&payload);
        frame
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

    /// `sync_for` that answers every request with its own sync.
    pub(crate) fn echo_sync(_request_type: u8, sync: u32) -> u32 {
        sync
    }

    /// Fake server: writes the greeting, then answers every request with an
    /// OK response (an `IPROTO_ID` one for ID requests). `sync_for` maps the
    /// request's `(request_type, sync)` to the sync put into the response, so
    /// tests can inject a mismatch.
    pub(crate) async fn spawn_fake_server(sync_for: fn(u8, u32) -> u32) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if sock.write_all(&fake_greeting()).await.is_err() {
                        return;
                    }
                    while let Some((request_type, sync)) = read_request(&mut sock).await {
                        let body = if request_type == RequestType::Id as u8 {
                            id_response_body()
                        } else {
                            Value::Map(Vec::new())
                        };
                        let response = ok_response(sync_for(request_type, sync), &body);
                        if sock.write_all(&response).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        addr
    }

    #[test]
    fn next_sync_wraps_around() {
        let mut data = ConnectionData {
            next_sync: u32::MAX,
            ..ConnectionData::default()
        };
        assert_eq!(data.next_sync(), u32::MAX);
        assert_eq!(data.next_sync(), 0);
    }

    #[tokio::test]
    async fn small_threshold_does_not_panic() {
        let addr = spawn_fake_server(echo_sync).await;
        let conn = Connection::new_inner(addr, None, None, 50, &RwLock::default()).await;
        assert!(conn.is_ok());
    }

    /// Build a connection over a local socket pair, with a custom writer task.
    async fn connection_with_writer(
        writer: impl Future<Output = Result<(), (u32, CodecEncodeError)>> + Send + 'static,
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
        let (client_tx, client_rx) = mpsc::channel::<DispatcherRequest>(1);
        let mut client_rx = ReceiverStream::new(client_rx);
        drop(client_tx);
        assert!(conn.run(&mut client_rx).await.is_err());
    }

    #[tokio::test]
    async fn writer_task_clean_exit_is_reported_as_ok() {
        let (conn, _server) = connection_with_writer(async { Ok(()) }).await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherRequest>(1);
        let mut client_rx = ReceiverStream::new(client_rx);
        drop(client_tx);
        assert!(conn.run(&mut client_rx).await.is_ok());
    }

    #[tokio::test]
    async fn default_threshold_does_not_panic() {
        let addr = spawn_fake_server(echo_sync).await;
        let conn = Connection::new_inner(addr, None, None, 500, &RwLock::default()).await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn auth_response_with_matching_sync_is_accepted() {
        let addr = spawn_fake_server(echo_sync).await;
        let conn =
            Connection::new_inner(addr, Some("user"), Some("pass"), 500, &RwLock::default()).await;
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
        let conn =
            Connection::new_inner(addr, Some("user"), Some("pass"), 500, &RwLock::default()).await;
        assert!(matches!(conn, Err(Error::Other(_))));
    }

    #[tokio::test]
    async fn handshake_records_negotiated_features() {
        let addr = spawn_fake_server(echo_sync).await;
        let features = RwLock::new(ConnectionFeatures::default());
        Connection::new_inner(addr, None, None, 500, &features)
            .await
            .unwrap();
        assert_eq!(
            *features.read(),
            ConnectionFeatures {
                protocol_version: 3,
                streams: true,
                transactions: true,
                error_extension: true,
                watchers: false,
            }
        );
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
        let conn = Connection::new_inner(addr, None, None, 500, &RwLock::default()).await;
        assert!(matches!(conn, Err(Error::Other(_))));
    }

    #[tokio::test]
    #[traced_test]
    async fn teardown_answers_registered_request_without_unknown_sync() {
        // The writer queue's receiver is already gone, so handing the request
        // to the writer fails and the connection tears down while the request
        // is registered in `in_flights`.
        let (conn, _server) = connection_with_writer(async { Ok(()) }).await;
        let (client_tx, client_rx) = mpsc::channel::<DispatcherRequest>(1);
        let mut client_rx = ReceiverStream::new(client_rx);
        let sender = DispatcherSender::new_for_test(client_tx, Arc::default());

        // `join!` keeps both futures inside the test's tracing span, which
        // `logs_contain` needs; a spawned task would log outside it.
        let (run_res, send_res) = tokio::join!(
            conn.run(&mut client_rx),
            sender.send(EncodedRequest::new(&Ping {}, None).unwrap()),
        );

        assert!(run_res.is_err());
        assert!(
            matches!(send_res, Err(Error::ConnectionClosed)),
            "{send_res:?}"
        );
        assert!(!logs_contain("Unknown sync"));
    }
}
