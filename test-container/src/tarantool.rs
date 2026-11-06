use std::{borrow::Cow, collections::HashMap};

use testcontainers::{
    Image,
    core::{ContainerPort, WaitFor},
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
