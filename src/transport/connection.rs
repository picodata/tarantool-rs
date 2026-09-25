use std::{collections::HashMap, fmt::Display, time::Duration};

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

use super::dispatcher::{DispatcherRequest, DispatcherResponse, DispatcherResponseSender};
use crate::{
    codec::{
        ClientCodec, Greeting,
        request::{Auth, EncodedRequest},
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
mod tests {
    use super::*;
    use tokio::{io::AsyncWriteExt, net::TcpListener};
    use tracing_test::traced_test;

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

    /// Fake server which writes the greeting and then keeps the socket open,
    /// draining whatever the client sends.
    async fn spawn_greeting_only_server() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let _ = sock.write_all(&fake_greeting()).await;
                    let mut buf = [0u8; 1024];
                    while sock.read(&mut buf).await.unwrap_or(0) != 0 {}
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
        let addr = spawn_greeting_only_server().await;
        let conn = Connection::new_inner(addr, None, None, 50).await;
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
        let addr = spawn_greeting_only_server().await;
        let conn = Connection::new_inner(addr, None, None, 500).await;
        assert!(conn.is_ok());
    }

    /// Fake server which writes the greeting, waits for the AUTH request and
    /// answers it with an empty OK response carrying `sync`.
    async fn spawn_auth_server(sync: u8) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            sock.write_all(&fake_greeting()).await.unwrap();
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await.unwrap();
            // len 8, {RESPONSE_CODE: 0, SYNC: sync, SCHEMA_VERSION: 1}, {}
            let resp = [0x08, 0x83, 0x00, 0x00, 0x01, sync, 0x05, 0x01, 0x80];
            sock.write_all(&resp).await.unwrap();
            while sock.read(&mut buf).await.unwrap_or(0) != 0 {}
        });
        addr
    }

    #[tokio::test]
    async fn auth_response_with_matching_sync_is_accepted() {
        // AUTH is the first request on a connection, so it gets sync 0
        let addr = spawn_auth_server(0).await;
        let conn = Connection::new_inner(addr, Some("user"), Some("pass"), 500).await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn auth_response_with_other_sync_is_rejected() {
        let addr = spawn_auth_server(42).await;
        let conn = Connection::new_inner(addr, Some("user"), Some("pass"), 500).await;
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
        let sender = DispatcherSender::new_for_test(client_tx);

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
