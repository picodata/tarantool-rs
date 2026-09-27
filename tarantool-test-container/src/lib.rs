pub use testcontainers;

use std::{borrow::Cow, collections::HashMap};

use testcontainers::{
    Container, ContainerRequest, Image,
    core::{ContainerPort, IntoContainerPort, WaitFor},
    runners::SyncRunner,
};

const IMAGE_NAME: &str = "tarantool/tarantool";
const DEFAULT_IMAGE_TAG: &str = "latest";

fn image_tag() -> String {
    std::env::var("TARANTOOL_IMAGE_TAG").unwrap_or(DEFAULT_IMAGE_TAG.into())
}

/// Tarantool image with the MVCC engine on. Mounts and the command line come
/// from `ImageExt` (`with_mount`, `with_cmd`).
#[derive(Clone, Debug)]
pub struct TarantoolImage {
    tag: String,
    env_vars: HashMap<String, String>,
}

impl Default for TarantoolImage {
    fn default() -> Self {
        Self {
            tag: image_tag(),
            env_vars: HashMap::from([("TT_MEMTX_USE_MVCC_ENGINE".to_owned(), "true".to_owned())]),
        }
    }
}

impl Image for TarantoolImage {
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
        ["tarantool"]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(3301)]
    }
}

impl TarantoolImage {
    pub fn disable_mvcc(mut self) -> Self {
        drop(self.env_vars.remove("TT_MEMTX_USE_MVCC_ENGINE"));
        self
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

pub struct TarantoolTestContainer {
    container: Option<Container<TarantoolImage>>,
}

impl Default for TarantoolTestContainer {
    fn default() -> Self {
        Self::from_image(TarantoolImage::default())
    }
}

impl TarantoolTestContainer {
    /// Start a container from `image`: a `TarantoolImage`, or a request built
    /// from one with `ImageExt`, for example with a mount, a command or a
    /// fixed host port (`with_mapped_port`).
    pub fn from_image(image: impl Into<ContainerRequest<TarantoolImage>> + Send + 'static) -> Self {
        let container = off_runtime(move || image.start())
            .expect("tarantool container start thread panicked")
            .expect("failed to start tarantool test container");
        Self {
            container: Some(container),
        }
    }

    pub fn connect_port(&self) -> u16 {
        let container = self.container();
        off_runtime(|| container.get_host_port_ipv4(3301.tcp()))
            .expect("tarantool container port thread panicked")
            .expect("failed to get mapped port 3301 of tarantool test container")
    }

    fn container(&self) -> &Container<TarantoolImage> {
        self.container
            .as_ref()
            .expect("container is present until drop")
    }

    /// Stop the container and start it again. The server process restarts,
    /// so every client connection to it is dropped. Returns once Docker has
    /// started the container, not once Tarantool listens again.
    pub fn restart(&self) {
        let container = self.container();
        off_runtime(|| {
            container.stop()?;
            container.start()
        })
        .expect("tarantool container restart thread panicked")
        .expect("failed to restart tarantool test container");
    }
}

impl Drop for TarantoolTestContainer {
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let _ = off_runtime(move || drop(container));
        }
    }
}
