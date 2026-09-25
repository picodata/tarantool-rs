pub(crate) use self::dispatcher::{Dispatcher, DispatcherSender};

#[cfg(test)]
pub(crate) use self::dispatcher::DispatcherRequest;

mod connection;
mod dispatcher;
