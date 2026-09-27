use async_trait::async_trait;
use tarantool_rs::Connection;
use tarantool_test_container::testcontainers::{
    ContainerRequest, ImageExt,
    core::{IntoContainerPort, Mount},
};
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
        Self::from_image(image_running("test_data.lua"))
    }

    /// Port 3301 is published on a fixed host port, so the address stays
    /// valid across `restart` (Docker may pick a new ephemeral port when a
    /// container with a dynamic mapping restarts).
    fn new_restartable() -> Self {
        Self::from_image(image_running("reconnect.lua").with_mapped_port(free_port(), 3301.tcp()))
    }

    async fn create_conn(&self) -> Result<Connection, tarantool_rs::errors::Error> {
        Connection::builder()
            .build(format!("127.0.0.1:{}", self.connect_port()))
            .await
    }
}

/// The default image with the `tests` directory mounted at `/opt/tarantool`,
/// running `script` from it.
fn image_running(script: &str) -> ContainerRequest<TarantoolImage> {
    TarantoolImage::default()
        .with_mount(Mount::bind_mount(
            format!("{}/tests", env!("CARGO_MANIFEST_DIR")),
            "/opt/tarantool",
        ))
        .with_cmd(["tarantool".to_owned(), format!("/opt/tarantool/{script}")])
}

/// A host port that was free a moment ago.
fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("local address of the ephemeral listener")
        .port()
}
