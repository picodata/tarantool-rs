pub(crate) use self::dispatcher::{Dispatcher, DispatcherSender, RequestSender};

#[cfg(test)]
pub(crate) use self::dispatcher::ClientRequest;

mod connection;
mod dispatcher;
