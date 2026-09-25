use std::io::Write;

use rmpv::Value;

use crate::{
    codec::consts::{RequestType, keys},
    errors::{DecodingError, EncodingError},
};

use super::{PROTOCOL_VERSION, Request};

#[derive(Clone, Debug)]
// Mirrors the independent IPROTO_ID feature flags.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct Id {
    pub streams: bool,
    pub transactions: bool,
    pub error_extension: bool,
    pub watchers: bool,
    pub protocol_version: u8,
}

impl Default for Id {
    fn default() -> Self {
        Self {
            streams: true,
            transactions: true,
            error_extension: true,
            watchers: false,
            protocol_version: PROTOCOL_VERSION,
        }
    }
}

impl Id {
    const STREAMS: u8 = 0;
    const TRANSACTIONS: u8 = 1;
    const ERROR_EXTENSION: u8 = 2;
    const WATCHERS: u8 = 3;
}

impl Request for Id {
    fn request_type() -> RequestType
    where
        Self: Sized,
    {
        RequestType::Id
    }

    // NOTE: `&mut buf: mut` is required since I don't get why compiler complain
    fn encode(&self, mut buf: &mut dyn Write) -> Result<(), EncodingError> {
        rmp::encode::write_map_len(&mut buf, 2)?;
        rmp::encode::write_pfix(&mut buf, keys::VERSION)?;
        rmp::encode::write_u8(&mut buf, self.protocol_version)?;
        rmp::encode::write_pfix(&mut buf, keys::FEATURES)?;
        let arr_len = u32::from(self.streams)
            + u32::from(self.transactions)
            + u32::from(self.error_extension)
            + u32::from(self.watchers);
        rmp::encode::write_array_len(&mut buf, arr_len)?;
        if self.streams {
            rmp::encode::write_u8(&mut buf, Self::STREAMS)?;
        }
        if self.transactions {
            rmp::encode::write_u8(&mut buf, Self::TRANSACTIONS)?;
        }
        if self.error_extension {
            rmp::encode::write_u8(&mut buf, Self::ERROR_EXTENSION)?;
        }
        if self.watchers {
            rmp::encode::write_u8(&mut buf, Self::WATCHERS)?;
        }
        Ok(())
    }
}

/// Protocol version and features the server reported in its `IPROTO_ID`
/// response.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
// Mirrors the independent IPROTO_ID feature flags.
#[allow(clippy::struct_excessive_bools)]
pub(crate) struct ConnectionFeatures {
    pub protocol_version: u64,
    pub streams: bool,
    pub transactions: bool,
    pub error_extension: bool,
    pub watchers: bool,
}

impl ConnectionFeatures {
    /// Decode the body of an `IPROTO_ID` response,
    /// `{VERSION: uint, FEATURES: [uint, ...]}`. Feature ids this crate does
    /// not know are ignored.
    pub(crate) fn decode(body: &Value) -> Result<Self, DecodingError> {
        let Value::Map(entries) = body else {
            return Err(DecodingError::type_mismatch("map", body.to_string()));
        };
        let mut features = Self::default();
        for (key, value) in entries {
            match key.as_u64() {
                Some(k) if k == u64::from(keys::VERSION) => {
                    features.protocol_version = value.as_u64().ok_or_else(|| {
                        DecodingError::type_mismatch("unsigned integer", value.to_string())
                            .in_key("VERSION")
                    })?;
                }
                Some(k) if k == u64::from(keys::FEATURES) => {
                    let Value::Array(ids) = value else {
                        return Err(DecodingError::type_mismatch("array", value.to_string())
                            .in_key("FEATURES"));
                    };
                    for id in ids {
                        match id.as_u64() {
                            Some(x) if x == u64::from(Id::STREAMS) => features.streams = true,
                            Some(x) if x == u64::from(Id::TRANSACTIONS) => {
                                features.transactions = true;
                            }
                            Some(x) if x == u64::from(Id::ERROR_EXTENSION) => {
                                features.error_extension = true;
                            }
                            Some(x) if x == u64::from(Id::WATCHERS) => features.watchers = true,
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        Ok(features)
    }
}

#[cfg(test)]
mod tests {
    use rmpv::Value;

    use super::*;

    #[test]
    fn id_response_is_decoded_into_features() {
        let body = Value::Map(vec![
            (Value::from(keys::VERSION), Value::from(3u8)),
            (
                Value::from(keys::FEATURES),
                // 99 is a feature id this crate does not know; it is ignored.
                Value::Array(vec![
                    Value::from(0u8),
                    Value::from(1u8),
                    Value::from(2u8),
                    Value::from(99u8),
                ]),
            ),
        ]);
        assert_eq!(
            ConnectionFeatures::decode(&body).unwrap(),
            ConnectionFeatures {
                protocol_version: 3,
                streams: true,
                transactions: true,
                error_extension: true,
                watchers: false,
            }
        );
    }
}
