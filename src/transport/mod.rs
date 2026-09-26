pub(crate) use self::dispatcher::{Dispatcher, DispatcherSender, RequestSender};

#[cfg(test)]
pub(crate) use self::{
    connection::tests::{echo_sync, spawn_fake_server},
    dispatcher::ClientRequest,
};

mod connection;
mod dispatcher;
