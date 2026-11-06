pub use testcontainers;

use std::{borrow::Cow, collections::HashMap};

use testcontainers::{
    Image,
    core::{ContainerPort, WaitFor},
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
        ["tarantool"]
    }

    fn expose_ports(&self) -> &[ContainerPort] {
        &[ContainerPort::Tcp(3301)]
    }
}
