use rmp::decode::DecodeStringError;
use tracing::{debug, error};

use super::consts::response_codes::{ERROR_RANGE_END, ERROR_RANGE_START, OK};
use crate::{
    codec::consts::keys,
    errors::{DecodingError, ErrorResponse},
};

// TODO: add out-of-band (I.e. IPROTO_CHUNK)
// TODO: actually implement extra error data
// TODO: create bodies for specific responses (for optimization reasons)
#[derive(Clone, Debug)]
pub(crate) enum ResponseBody {
    Ok(rmpv::Value), // TODO: replace
    Error(ErrorResponse),
}

#[derive(Clone, Debug)]
pub(crate) struct Response {
    pub sync: u64,
    pub schema_version: u32,
    pub body: ResponseBody,
}

impl Response {
    // TODO: split function
    // Use [`anyhow::Error`] because any error would mean either entirely broken
    // implementation of protocol or underlying I/O error, which currently would be
    // implementation bug as well.
    pub(super) fn decode(mut buf: &[u8]) -> Result<Self, DecodingError> {
        let map_len = rmp::decode::read_map_len(&mut buf)?;
        let mut response_code: Option<u32> = None;
        let mut sync: Option<u64> = None;
        let mut schema_version: Option<u32> = None;
        for _ in 0..map_len {
            let key: u8 = rmp::decode::read_pfix(&mut buf)?;
            match key {
                keys::RESPONSE_CODE => {
                    response_code = Some(rmp::decode::read_int(&mut buf)?);
                }
                keys::SYNC => {
                    sync = Some(rmp::decode::read_int(&mut buf)?);
                }
                keys::SCHEMA_VERSION => {
                    schema_version = Some(rmp::decode::read_int(&mut buf)?);
                }
                rest => {
                    // TODO: configurable level for this warn?
                    debug!("Unexpected key encountered in response header: {}", rest);
                    let _ = rmpv::decode::read_value(&mut buf)?;
                }
            }
        }
        let Some(response_code) = response_code else {
            return Err(DecodingError::missing_key("RESPONSE_CODE"));
        };
        let Some(sync) = sync else {
            return Err(DecodingError::missing_key("SYNC"));
        };
        let Some(schema_version) = schema_version else {
            return Err(DecodingError::missing_key("SCHEMA_VERSION"));
        };
        let body = match response_code {
            OK => {
                let v = rmpv::decode::read_value(&mut buf)?;
                debug!("{}", v);
                ResponseBody::Ok(v)
            }
            code @ ERROR_RANGE_START..=ERROR_RANGE_END => {
                let code = code - 0x8000;
                let mut description = None;
                let mut extra = None;
                let map_len = rmp::decode::read_map_len(&mut buf)?;
                for _ in 0..map_len {
                    let key: u8 = rmp::decode::read_pfix(&mut buf)?;
                    match key {
                        keys::ERROR_24 => {
                            // Checks the bound and UTF-8 without a copy.
                            let (text, rest) = rmp::decode::read_str_from_slice(buf)
                                .map_err(|err| error_24_error(buf, &err).in_key("ERROR_24"))?;
                            description = Some(text.to_owned());
                            buf = rest;
                        }
                        keys::ERROR => {
                            extra = Some(rmpv::decode::read_value(&mut buf)?);
                        }
                        rest => {
                            error!("Unexpected key encountered in error description: {}", rest);
                            let _ = rmpv::decode::read_value(&mut buf)?;
                        }
                    }
                }
                let Some(description) = description else {
                    return Err(DecodingError::missing_key("ERROR_24"));
                };
                ResponseBody::Error(ErrorResponse {
                    code,
                    description,
                    extra,
                })
            }
            rest => return Err(DecodingError::unknown_response_code(rest)),
        };
        Ok(Self {
            sync,
            schema_version,
            body,
        })
    }
}

/// Error for an `ERROR_24` string that `read_str_from_slice` rejected. The
/// library's own `Display` is a fixed text, so the bound keeps its message.
fn error_24_error<E: rmp::decode::RmpReadErr>(
    buf: &[u8],
    err: &DecodeStringError<'_, E>,
) -> DecodingError {
    match err {
        DecodeStringError::BufferSizeTooSmall(len) => {
            // Count the bytes after the string's header.
            let mut data = buf;
            let _ = rmp::decode::read_str_len(&mut data);
            DecodingError::message_pack(anyhow::anyhow!(
                "string length {len} exceeds remaining {} bytes",
                data.len()
            ))
        }
        DecodeStringError::InvalidUtf8(_, _) => {
            DecodingError::message_pack(anyhow::anyhow!("String is not valid UTF-8 string"))
        }
        other => DecodingError::message_pack(anyhow::anyhow!("{other:?}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::DecodingErrorDetails;

    fn error_response(description: &[u8]) -> Vec<u8> {
        // {RESPONSE_CODE: 0x8001, SYNC: 1, SCHEMA_VERSION: 1}
        let mut frame = vec![0x83, 0x00, 0xcd, 0x80, 0x01, 0x01, 0x01, 0x05, 0x01];
        // {ERROR_24: <description>}
        frame.extend_from_slice(&[0x81, 0x31]);
        frame.extend_from_slice(description);
        frame
    }

    #[test]
    fn error_description_length_is_bounded_by_frame() {
        // str32 announcing u32::MAX bytes, followed by only 3 bytes
        let frame = error_response(&[0xdb, 0xff, 0xff, 0xff, 0xff, b'a', b'b', b'c']);
        let err = Response::decode(&frame[..]).unwrap_err();
        let DecodingErrorDetails::MessagePack(inner) = err.kind() else {
            panic!("unexpected error kind: {err:?}");
        };
        assert!(
            inner.to_string().contains("exceeds remaining"),
            "unexpected error: {inner}"
        );
    }

    #[test]
    fn error_description_is_decoded() {
        let frame = error_response(&[0xa3, b'a', b'b', b'c']);
        let resp = Response::decode(&frame[..]).unwrap();
        let ResponseBody::Error(err) = resp.body else {
            panic!("expected error body");
        };
        assert_eq!(err.code, 1);
        assert_eq!(err.description, "abc");
    }
}
