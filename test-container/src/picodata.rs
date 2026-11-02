pub use testcontainers;

use std::{borrow::Cow, collections::HashMap};

use testcontainers::{
    Container, ContainerRequest, Image,
    core::{ContainerPort, IntoContainerPort, WaitFor},
    runners::SyncRunner,
};

const IMAGE_NAME: &str = "docker-public.binary.picodata.io/picodata";
const DEFAULT_IMAGE_TAG: &str = "26.2.1";

fn image_tag() -> String {
    std::env::var("PICODATA_IMAGE_TAG").unwrap_or(DEFAULT_IMAGE_TAG.into())
}

/// Picodata image. Mounts and the command line come
/// from `ImageExt` (`with_mount`, `with_cmd`).
#[derive(Clone, Debug)]
pub struct PicodataImage {
    tag: String,
    env_vars: HashMap<String, String>,
}

impl Default for PicodataImage {
    fn default() -> Self {
        Self {
            tag: image_tag(),
            env_vars: HashMap::new(),
        }
    }
}

impl Image for PicodataImage {
    fn name(&self) -> &str {
        IMAGE_NAME
    }

    fn tag(&self) -> &str {
        &self.tag
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("entering the event loop")]
    }

    fn env_vars(
        &self,
    ) -> impl IntoIterator<Item = (impl Into<Cow<'_, str>>, impl Into<Cow<'_, str>>)> {
        self.env_vars.iter().map(|(k, v)| (k.as_str(), v.as_str()))
    }

    fn cmd(&self) -> impl IntoIterator<Item = impl Into<Cow<'_, str>>> {
        ["picodata", "tarantool"]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(3301)]
    }
}

/// Run `f` on a helper OS thread and wait for it.
///
/// testcontainers' blocking API drives its own tokio runtime under the hood,
/// and tokio panics when a runtime is entered from a worker thread of another
/// runtime (as happens in #[tokio::test]). Every blocking testcontainers call
/// is therefore pushed onto a dedicated OS thread.
fn off_runtime<T: Send>(f: impl FnOnce() -> T + Send) -> std::thread::Result<T> {
    std::thread::scope(|scope| scope.spawn(f).join())
}

pub struct PicodataTestContainer {
    container: Option<Container<PicodataImage>>,
}

impl Default for PicodataTestContainer {
    fn default() -> Self {
        Self::from_image(PicodataImage::default())
    }
}

impl PicodataTestContainer {
    /// Start a container from `image`: a `PicodataImage`, or a request built
    /// from one with `ImageExt`, for example with a mount, a command or a
    /// fixed host port (`with_mapped_port`).
    pub fn from_image(image: impl Into<ContainerRequest<PicodataImage>> + Send + 'static) -> Self {
        let container = off_runtime(move || image.start())
            .expect("picodata container start thread panicked")
            .map_err(|e| println!("{e}"))
            .expect("failed to start picodata test container");
        Self {
            container: Some(container),
        }
    }

    pub fn connect_port(&self) -> u16 {
        let container = self.container();
        off_runtime(|| container.get_host_port_ipv4(3301.tcp()))
            .expect("picodata container port thread panicked")
            .expect("failed to get mapped port 3301 of picodata test container")
    }

    fn container(&self) -> &Container<PicodataImage> {
        self.container
            .as_ref()
            .expect("container is present until drop")
    }

    /// Stop the container. The server process exits, so every client
    /// connection to it is dropped, and connecting fails until
    /// [`Self::start`].
    pub fn stop(&self) {
        let container = self.container();
        off_runtime(|| container.stop())
            .expect("picodata container stop thread panicked")
            .expect("failed to stop picodata test container");
    }

    /// Start the stopped container again. Returns once Docker has started
    /// the container, not once Picodata listens again.
    pub fn start(&self) {
        let container = self.container();
        off_runtime(|| container.start())
            .expect("picodata container start thread panicked")
            .expect("failed to start picodata test container");
    }

    /// Stop the container and start it again. The server process restarts,
    /// so every client connection to it is dropped. Returns once Docker has
    /// started the container, not once Picodata listens again.
    pub fn restart(&self) {
        self.stop();
        self.start();
    }
}

impl Drop for PicodataTestContainer {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let _ = off_runtime(move || drop(container));
        }
    }
}
