use async_trait::async_trait;
use tarantool_rs::Connection;
use test_container::testcontainers::{
    ContainerRequest, ImageExt,
    core::{IntoContainerPort, Mount},
};
use test_container::{PicodataImage, TarantoolImage, TestContainer};

#[async_trait]
pub trait TestContainerExt {
    fn new_with_test_data() -> Self;
    fn new_restartable() -> Self;
    async fn create_conn(&self) -> Result<Connection, tarantool_rs::errors::Error>;
}

#[async_trait]
impl TestContainerExt for TestContainer<TarantoolImage> {
    fn new_with_test_data() -> Self {
        Self::from_image(tarantool_image_running("tarantool_test_data.lua"))
    }

    /// Port 3301 is published on a fixed host port, so the address stays
    /// valid across `restart` (Docker may pick a new ephemeral port when a
    /// container with a dynamic mapping restarts).
    fn new_restartable() -> Self {
        Self::from_image(
            tarantool_image_running("reconnect.lua").with_mapped_port(free_port(), 3301.tcp()),
        )
    }

    async fn create_conn(&self) -> Result<Connection, tarantool_rs::errors::Error> {
        Connection::builder()
            .build(format!("127.0.0.1:{}", self.connect_port()))
            .await
    }
}

/// The default image with the `tests` directory mounted at `/opt/tarantool`,
/// running `script` from it.
fn tarantool_image_running(script: &str) -> ContainerRequest<TarantoolImage> {
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

#[async_trait]
impl TestContainerExt for TestContainer<PicodataImage> {
    fn new_with_test_data() -> Self {
        Self::from_image(
            PicodataImage::default()
                .with_mount(Mount::bind_mount(
                    format!("{}/tests", env!("CARGO_MANIFEST_DIR")),
                    "/opt/picodata",
                ))
                .with_cmd([
                    "tarantool".to_owned(),
                    "--".to_owned(),
                    "/opt/picodata/picodata_test_data.lua".to_owned(),
                ]),
        )
    }

    fn new_restartable() -> Self {
        Self::new_with_test_data()
    }

    async fn create_conn(&self) -> Result<Connection, tarantool_rs::errors::Error> {
        Connection::builder()
            .build(format!("127.0.0.1:{}", self.connect_port()))
            .await
    }
}
