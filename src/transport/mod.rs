pub(crate) use self::dispatcher::{Dispatcher, DispatcherSender};

#[cfg(test)]
pub(crate) use self::dispatcher::ClientRequest;

mod connection;
mod dispatcher;
