use async_trait::async_trait;
use tarantool_rs::Connection;
pub use tarantool_test_container::{TarantoolImage, TarantoolTestContainer};

#[async_trait]
pub trait TarantoolTestContainerExt {
    fn new_with_test_data() -> Self;
    fn new_restartable() -> Self;
    async fn create_conn(&self) -> Result<Connection, tarantool_rs::errors::Error>;
}

#[async_trait]
impl TarantoolTestContainerExt for TarantoolTestContainer {
    fn new_with_test_data() -> Self {
        let image = TarantoolImage::default()
            .volume(
                format!("{}/tests", env!("CARGO_MANIFEST_DIR")),
                "/opt/tarantool".into(),
            )
            .cmd_args(["tarantool".into(), "/opt/tarantool/test_data.lua".into()]);
        Self::from_image(image)
    }

    fn new_restartable() -> Self {
        let image = TarantoolImage::default()
            .volume(
                format!("{}/tests", env!("CARGO_MANIFEST_DIR")),
                "/opt/tarantool".into(),
            )
            .cmd_args(["tarantool".into(), "/opt/tarantool/reconnect.lua".into()]);
        Self::from_image_with_host_port(image, free_port())
    }

    async fn create_conn(&self) -> Result<Connection, tarantool_rs::errors::Error> {
        Connection::builder()
            .build(format!("127.0.0.1:{}", self.connect_port()))
            .await
    }
}

/// A host port that was free a moment ago.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local address of the ephemeral listener")
        .port()
}
