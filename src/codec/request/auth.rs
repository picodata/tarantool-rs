// Docs: https://www.tarantool.io/en/doc/latest/dev_guide/internals/iproto/authentication/

use anyhow::anyhow;
use std::io::Write;

use super::Request;
use crate::{
    codec::{
        consts::{RequestType, keys},
        utils::write_kv_str,
    },
    errors::EncodingError,
};

#[derive(Clone, Copy, Debug, Default)]
pub enum AuthMethod {
    #[default]
    ChapSha1,
    Md5,
    Ldap,
}

impl AuthMethod {
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            AuthMethod::ChapSha1 => "chap-sha1",
            AuthMethod::Md5 => "md5",
            AuthMethod::Ldap => "ldap",
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Auth<'a> {
    pub user_name: &'a str,
    pub method: AuthMethod,
    pub scramble: Vec<u8>,
}

impl<'a> Auth<'a> {
    pub(crate) fn try_new(
        user: &'a str,
        password: Option<&'a str>,
        salt: &'a [u8],
        method: AuthMethod,
    ) -> Result<Self, EncodingError> {
        Ok(Self {
            user_name: user,
            method,
            scramble: prepare_scramble(method, user, password, salt)?,
        })
    }
}

impl Request for Auth<'_> {
    fn request_type() -> RequestType
    where
        Self: Sized,
    {
        RequestType::Auth
    }

    // NOTE: `&mut buf: mut` is required since I don't get why compiler complain
    fn encode(&self, mut buf: &mut dyn Write) -> Result<(), EncodingError> {
        rmp::encode::write_map_len(&mut buf, 2)?;
        write_kv_str(&mut buf, keys::USER_NAME, self.user_name)?;
        rmp::encode::write_pfix(&mut buf, keys::TUPLE)?;
        rmp::encode::write_array_len(&mut buf, 2)?;
        rmp::encode::write_str(&mut buf, self.method.as_str())?;
        rmp::encode::write_bin(&mut buf, &self.scramble)?;
        Ok(())
    }
}

#[inline]
fn prepare_scramble(
    method: AuthMethod,
    user: &str,
    password: Option<&str>,
    salt: &[u8],
) -> Result<Vec<u8>, EncodingError> {
    let password = password.unwrap_or("");
    match method {
        AuthMethod::ChapSha1 => {
            let salt = salt
                .first_chunk()
                .ok_or(EncodingError::MessagePack(anyhow!(
                    "bad salt length (expect >= 20)"
                )))?;
            Ok(chap_sha1_prepare(password, salt))
        }
        AuthMethod::Md5 => {
            let salt = salt
                .first_chunk()
                .ok_or(EncodingError::MessagePack(anyhow!(
                    "bad salt length (expect >= 4)"
                )))?;
            Ok(md5_prepare(user, password, *salt))
        }
        AuthMethod::Ldap => Ok(ldap_prepare(password)),
    }
}

#[inline]
fn chap_sha1_prepare(password: impl AsRef<[u8]>, salt: &[u8; 20]) -> Vec<u8> {
    // prepare 'chap-sha1' scramble:
    // salt = base64_decode(encoded_salt);
    // step_1 = sha1(password);
    // step_2 = sha1(step_1);
    // step_3 = sha1(first_20_bytes_of_salt, step_2);
    // scramble = xor(step_1, step_3);

    use sha1::{Digest as Sha1Digest, Sha1};

    let mut hasher = Sha1::new();
    hasher.update(password);
    let mut step_1_and_scramble = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(step_1_and_scramble);
    let step_2 = hasher.finalize();

    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(step_2);
    let step_3 = hasher.finalize();

    step_1_and_scramble
        .iter_mut()
        .zip(step_3.iter())
        .for_each(|(a, b)| *a ^= *b);

    let scramble_bytes = step_1_and_scramble.to_vec();
    debug_assert_eq!(scramble_bytes.len(), 20);
    scramble_bytes
}

#[inline]
fn md5_prepare(user: &str, password: impl AsRef<[u8]>, salt: [u8; 4]) -> Vec<u8> {
    // recv_from_db(salt)
    // recv_from_user(name, password)
    //   shadow_pass = md5(name + password), do not add "md5" prefix
    //   client_pass = md5(shadow_pass + salt)
    //   result = encode_as_msgpack_str(client_pass)
    // send_to_db(result)

    use md5::{Digest as Md5Digest, Md5};

    let mut md5 = Md5::new();

    md5.update(password);
    md5.update(user);
    let shadow_pass = format!("{:x}", md5.finalize_reset());

    md5.update(shadow_pass);
    md5.update(salt);
    let client_pass = format!("md5{:x}", md5.finalize());

    client_pass.into_bytes()
}

#[inline]
pub fn ldap_prepare(password: impl AsRef<[u8]>) -> Vec<u8> {
    password.as_ref().to_vec()
}
