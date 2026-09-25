use anyhow::Context;
use bytes::{BufMut, Bytes, BytesMut};

use crate::errors::EncodingError;

pub(crate) use self::{
    auth::Auth,
    begin::Begin,
    call::Call,
    commit::Commit,
    delete::Delete,
    eval::Eval,
    execute::Execute,
    id::{ConnectionFeatures, Id},
    insert::Insert,
    ping::Ping,
    prepare::Prepare,
    replace::Replace,
    rollback::Rollback,
    select::Select,
    update::Update,
    upsert::Upsert,
};

use std::io::Write;

use super::consts::{RequestType, keys};

mod auth;
mod begin;
mod call;
mod commit;
mod delete;
mod eval;
mod execute;
mod id;
mod insert;
mod ping;
mod prepare;
mod replace;
mod rollback;
mod select;
mod update;
mod upsert;

pub const PROTOCOL_VERSION: u8 = 3;

const DEFAULT_ENCODE_BUFFER_SIZE: usize = 128;
const INDEX_BASE_VALUE: u32 = 0;

/// Longest request frame Tarantool accepts, `IPROTO_PACKET_SIZE_MAX` (2 GiB).
/// The server closes the connection on a longer one.
const MAX_REQUEST_FRAME_LEN: usize = 1 << 31;

/// Longest header a request can get: a map of four keys whose values are the
/// request type (2 bytes), the sync (up to 9), the schema version (5) and the
/// stream id (5). The sync and the stream id are set after
/// [`EncodedRequest::new`], so it counts the widest ones.
const MAX_HEADER_LEN: usize = 1 + (1 + 2) + (1 + 9) + (1 + 5) + (1 + 5);

/// Fail when a frame whose size prefix would carry `frame_len` is longer than
/// Tarantool accepts.
fn check_request_frame_len(frame_len: usize) -> Result<(), EncodingError> {
    if frame_len > MAX_REQUEST_FRAME_LEN {
        return Err(EncodingError::MessagePack(anyhow::anyhow!(
            "request frame of {frame_len} bytes exceeds Tarantool's limit of \
             {MAX_REQUEST_FRAME_LEN} bytes"
        )));
    }
    Ok(())
}

// TODO: docs
pub trait Request {
    /// Return type of this request.
    fn request_type() -> RequestType
    where
        Self: Sized;

    /// Encode body into `MessagePack` and write it to provided [`Write`].
    ///
    /// Currently all implementation in this crate uses [`rmp::encode`],
    /// which throw errors only if used `Write` throw an error. And since internally
    /// crate use [`bytes::BufMut::writer`], this methods shouldn't throw error in
    /// normal case, so we don't care about actual type. If necessary, it is possible
    /// to downcast error to specific type.
    fn encode(&self, buf: &mut dyn Write) -> Result<(), EncodingError>;
}

/// Request, encoded into MessagePack, and its meta data.
#[doc(hidden)]
pub struct EncodedRequest {
    pub(crate) request_type: RequestType,
    /// By default `sync` is set to 0 and replaced with the client-assigned
    /// value by [`crate::Connection`] before the request is sent.
    pub(crate) sync: u32,
    pub(crate) schema_version: Option<u32>,
    pub(crate) stream_id: Option<u32>,
    pub(crate) encoded_body: Bytes,
}

impl EncodedRequest {
    pub fn new<Body: Request>(body: &Body, stream_id: Option<u32>) -> Result<Self, EncodingError> {
        let mut buf = BytesMut::with_capacity(DEFAULT_ENCODE_BUFFER_SIZE).writer();
        body.encode(&mut buf)?;
        let encoded_body = buf.into_inner().freeze();
        // Only this request fails. Sent, it would make the server close the
        // connection and fail every request in flight.
        check_request_frame_len(MAX_HEADER_LEN.saturating_add(encoded_body.len()))?;
        Ok(Self {
            request_type: Body::request_type(),
            sync: 0,
            schema_version: None,
            stream_id,
            encoded_body,
        })
    }

    pub fn encode(&self, mut buf: impl Write) -> Result<(), EncodingError> {
        let map_len =
            2 + u32::from(self.schema_version.is_some()) + u32::from(self.stream_id.is_some());
        rmp::encode::write_map_len(&mut buf, map_len)?;
        rmp::encode::write_pfix(&mut buf, keys::REQUEST_TYPE)?;
        rmp::encode::write_u8(&mut buf, self.request_type as u8)?;
        rmp::encode::write_pfix(&mut buf, keys::SYNC)?;
        rmp::encode::write_u32(&mut buf, self.sync)?;
        if let Some(x) = self.schema_version {
            rmp::encode::write_pfix(&mut buf, keys::SCHEMA_VERSION)?;
            rmp::encode::write_u32(&mut buf, x)?;
        }
        if let Some(x) = self.stream_id {
            rmp::encode::write_pfix(&mut buf, keys::STREAM_ID)?;
            rmp::encode::write_u32(&mut buf, x)?;
        }
        buf.write_all(&self.encoded_body)
            .context("Failed to write encoded body to buffer")
            .map_err(EncodingError::MessagePack)
    }

    pub(crate) fn sync_mut(&mut self) -> &mut u32 {
        &mut self.sync
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_is_sent_as_upsert() {
        let req = EncodedRequest::new(&Upsert::new(512, ((),), ((),)), None).unwrap();
        assert_eq!(req.request_type as u8, RequestType::Upsert as u8);
    }

    #[test]
    fn request_frame_limit_is_tarantools_2_gib() {
        assert!(check_request_frame_len(1 << 31).is_ok());
        assert!(matches!(
            check_request_frame_len((1 << 31) + 1),
            Err(EncodingError::MessagePack(_))
        ));
    }

    #[test]
    fn other_dmo_request_types_are_correct() {
        assert_eq!(
            EncodedRequest::new(&Insert::new(512, ((),)), None)
                .unwrap()
                .request_type as u8,
            RequestType::Insert as u8
        );
        assert_eq!(
            EncodedRequest::new(&Replace::new(512, ((),)), None)
                .unwrap()
                .request_type as u8,
            RequestType::Replace as u8
        );
    }
}
