pub(crate) use self::dispatcher::{Dispatcher, DispatcherSender};

#[cfg(test)]
pub(crate) use self::dispatcher::{ClientRequest, DispatcherMessage};

mod connection;
mod dispatcher;
