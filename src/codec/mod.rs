use anyhow::Context;
use base64::{Engine, engine::general_purpose::STANDARD_PAD_INDIFFERENT};
use bytes::{Buf, BufMut, BytesMut};
use rmp::Marker;
use tokio_util::codec::{Decoder, Encoder};
use tracing::trace;

use self::{request::EncodedRequest, response::Response};
use crate::{
    Error,
    errors::{CodecDecodeError, CodecEncodeError, DecodingError},
};

pub mod consts;
pub mod request;
pub mod response;
pub mod utils;

#[derive(Default)]
enum LengthDecoder {
    #[default]
    NoMarker,
    Marker(Marker),
    Value(usize),
}

impl LengthDecoder {
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<usize>, DecodingError> {
        if src.is_empty() {
            return Ok(None);
        }
        let marker = match self {
            LengthDecoder::NoMarker => {
                // Safety: `src.get_u8` might panic if there is no enough data,
                // but in this case we checked previously that `src` is not empty.
                let marker = Marker::from_u8(src.get_u8());
                *self = Self::Marker(marker);
                trace!("decoded length marker: {:?}", marker);
                marker
            }
            LengthDecoder::Marker(x) => *x,
            LengthDecoder::Value(x) => return Ok(Some(*x)),
        };
        // Safety: `src.get_uXX` might panic if there is no enough data,
        // but in this case we check before reading, so it shouldn't panic.
        let length = match marker {
            Marker::FixPos(x) => x as usize,
            Marker::U8 => {
                if src.is_empty() {
                    return Ok(None);
                }
                src.get_u8() as usize
            }
            Marker::U16 => {
                if src.len() >= 2 {
                    src.get_u16() as usize
                } else {
                    return Ok(None);
                }
            }
            Marker::U32 => {
                if src.len() >= 4 {
                    src.get_u32() as usize
                } else {
                    return Ok(None);
                }
            }
            // Payload length of a sane message fits in usize on all supported targets.
            #[allow(clippy::cast_possible_truncation)]
            Marker::U64 => {
                //
                if src.len() >= 8 {
                    src.get_u64() as usize
                } else {
                    return Ok(None);
                }
            }
            rest => {
                return Err(DecodingError::type_mismatch(
                    "unsigned integer",
                    format!("{rest:?}"),
                ));
            }
        };
        trace!("decoded frame length: {}", length);
        *self = LengthDecoder::Value(length);
        Ok(Some(length))
    }

    fn reset(&mut self) {
        *self = LengthDecoder::NoMarker;
    }
}

#[derive(Default)]
pub(crate) struct ClientCodec {
    length_decoder: LengthDecoder,
}

impl Decoder for ClientCodec {
    type Item = Response;

    type Error = CodecDecodeError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let Some(next_frame_length) = self
            .length_decoder
            .decode(src)
            .map_err(CodecDecodeError::Decode)?
        else {
            return Ok(None);
        };
        if src.len() >= next_frame_length {
            self.length_decoder.reset();
            let frame_bytes = src.split_to(next_frame_length);
            Response::decode(&frame_bytes)
                .map(Some)
                .map_err(CodecDecodeError::Decode)
        } else {
            src.reserve(next_frame_length - src.len());
            Ok(None)
        }
    }
}

impl Encoder<EncodedRequest> for ClientCodec {
    type Error = CodecEncodeError;

    // To omit creating intermediate BytesMut, encode message with 0 as length,
    // and after encoding calculate size of the encoded messages and overwrite
    // length field (0) with new data.
    fn encode(&mut self, item: EncodedRequest, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let begin_idx = dst.len();

        // TODO: calculate necessary integer type instead of using u64 always
        // Write message with fictional length (0)
        let mut writer = dst.writer();
        let res = rmp::encode::write_u64(&mut writer, 0)
            .map_err(|err| CodecEncodeError::Encode(err.into()))
            .and_then(|()| item.encode(&mut writer).map_err(CodecEncodeError::Encode));
        let dst = writer.into_inner();
        if let Err(err) = res {
            // Do not leave partially written frame in the buffer
            dst.truncate(begin_idx);
            return Err(err);
        }

        // Calculate length and override length field with actual value
        let data_len = dst.len() - begin_idx - 9;
        let mut len_writer = dst[begin_idx..].writer();
        rmp::encode::write_u64(&mut len_writer, data_len as u64)
            .map_err(|err| CodecEncodeError::Encode(err.into()))?;

        Ok(())
    }
}

/// Greeting message from server.
///
/// [Docs](https://www.tarantool.io/en/doc/latest/dev_guide/internals/box_protocol/#greeting-message).
#[derive(Debug)]
pub struct Greeting {
    pub server: String,
    pub salt: Vec<u8>,
}

impl Greeting {
    /// Size of the full message from server in bytes.
    pub const SIZE: usize = 128;

    // TODO: err
    /// Decode greeting from provided buffer without checking boundaries.
    pub fn decode(buffer: [u8; Self::SIZE]) -> Result<Self, Error> {
        let line1 = &buffer[0..62];
        let line2 = &buffer[64..126];
        // The salt is padded with spaces; its base64 may or may not be padded.
        let salt = STANDARD_PAD_INDIFFERENT
            .decode(line2.trim_ascii_end())
            .context("Failed to decode salt from base64")
            .map_err(Error::Other)?;
        Ok(Self {
            server: String::from_utf8_lossy(line1).into_owned(),
            salt,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn greeting_with_salt(salt_b64: &[u8]) -> [u8; Greeting::SIZE] {
        let mut buf = [b' '; Greeting::SIZE];
        buf[64..64 + salt_b64.len()].copy_from_slice(salt_b64);
        buf
    }

    #[test]
    fn encoded_frame_length_matches_body() {
        let mut dst = BytesMut::from(&b"prefix"[..]);
        let req = EncodedRequest::new(&request::Ping {}, None).unwrap();
        ClientCodec::default().encode(req, &mut dst).unwrap();
        assert_eq!(&dst[..6], b"prefix");
        assert_eq!(dst[6], 0xcf);
        let len = u64::from_be_bytes(dst[7..15].try_into().unwrap());
        assert_eq!(len, (dst.len() - 15) as u64);
    }

    #[test]
    fn greeting_salt_is_decoded_in_full() {
        // base64 of 32 zero bytes: 43 'A' followed by '='
        let mut padded = [b'A'; 44];
        padded[43] = b'=';
        let greeting = Greeting::decode(greeting_with_salt(&padded)).unwrap();
        assert_eq!(greeting.salt, vec![0u8; 32]);

        // base64 without padding keeps its last character
        let greeting = Greeting::decode(greeting_with_salt(b"AAAA")).unwrap();
        assert_eq!(greeting.salt, vec![0u8; 3]);

        let greeting = Greeting::decode(greeting_with_salt(b"AA==")).unwrap();
        assert_eq!(greeting.salt, vec![0u8; 1]);
    }

    #[test]
    fn length_decoder_accepts_exactly_complete_length() {
        for (bytes, expected) in [
            (&[0xccu8, 0x05][..], 5),
            (&[0xcd, 0x00, 0x05][..], 5),
            (&[0xce, 0x00, 0x00, 0x00, 0x05][..], 5),
            (
                &[0xcf, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x05][..],
                5,
            ),
        ] {
            let mut dec = LengthDecoder::default();
            let mut src = BytesMut::from(bytes);
            assert_eq!(dec.decode(&mut src).unwrap(), Some(expected), "{bytes:x?}");
            assert!(src.is_empty());
        }
    }

    #[test]
    fn length_decoder_waits_for_incomplete_length() {
        let mut dec = LengthDecoder::default();
        let mut src = BytesMut::from(&[0xceu8, 0x00, 0x00, 0x00][..]);
        assert_eq!(dec.decode(&mut src).unwrap(), None);
        src.extend_from_slice(&[0x05]);
        assert_eq!(dec.decode(&mut src).unwrap(), Some(5));
    }
}
