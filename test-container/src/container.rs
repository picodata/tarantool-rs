use testcontainers::{Container, ContainerRequest, core::IntoContainerPort, runners::SyncRunner};

pub struct TestContainer<T>
where
    T: testcontainers::Image,
{
    container: Option<Container<T>>,
}

impl<T> Default for TestContainer<T>
where
    T: testcontainers::Image + Default + 'static,
{
    fn default() -> Self {
        TestContainer::<T>::from_image(T::default())
    }
}

impl<T> TestContainer<T>
where
    T: testcontainers::Image,
{
    /// Start a container from `image`: a `PicodataImage`, or a request built
    /// from one with `ImageExt`, for example with a mount, a command or a
    /// fixed host port (`with_mapped_port`).
    pub fn from_image(image: impl Into<ContainerRequest<T>> + Send + 'static) -> Self {
        let container = off_runtime(move || image.start())
            .expect("container start thread panicked")
            .map_err(|e| println!("failed to start test container: {e}"))
            .unwrap();
        Self {
            container: Some(container),
        }
    }

    pub fn connect_port(&self) -> u16 {
        let container = self.container();
        off_runtime(|| container.get_host_port_ipv4(3301.tcp()))
            .expect("container port thread panicked")
            .expect("failed to get mapped port 3301 of test container")
    }

    fn container(&self) -> &Container<T> {
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
            .expect("container stop thread panicked")
            .expect("failed to stop test container");
    }

    /// Start the stopped container again. Returns once Docker has started
    /// the container, not once Picodata listens again.
    pub fn start(&self) {
        let container = self.container();
        off_runtime(|| container.start())
            .expect("container start thread panicked")
            .expect("failed to start test container");
    }

    /// Stop the container and start it again. The server process restarts,
    /// so every client connection to it is dropped. Returns once Docker has
    /// started the container, not once Picodata listens again.
    pub fn restart(&self) {
        self.stop();
        self.start();
    }
}

impl<T> Drop for TestContainer<T>
where
    T: testcontainers::Image,
{
    fn drop(&mut self) {
        if let Some(container) = self.container.take() {
            let _ = off_runtime(move || drop(container));
        }
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
