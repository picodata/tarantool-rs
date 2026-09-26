use std::result::Result as StdResult;

use rmpv::Value;

use crate::{
    Executor, ExecutorExt, Result, SqlResponse, Tuple,
    codec::{consts::keys, request::Execute},
    errors::DecodingError,
    utils::{find_and_take_single_key_in_map, value_to_map},
};

/// SQL statement prepared on the server by [`ExecutorExt::prepare_sql`].
///
/// The statement is bound to the server session it was prepared on. After a
/// reconnect, `execute` on a statement from a plain `Connection` fails with
/// `Error::Response`, code 211 (`ER_WRONG_QUERY_ID`): prepare it again. A
/// statement from a `Stream` or `Transaction` fails with
/// [`Error::ConnectionReset`][crate::Error::ConnectionReset] instead.
#[derive(Debug)]
pub struct PreparedSqlStatement<E> {
    stmt_id: u64,
    executor: E,
}

impl<E> PreparedSqlStatement<E> {
    fn new(stmt_id: u64, executor: E) -> Self {
        Self { stmt_id, executor }
    }

    /// # Errors
    ///
    /// Returns an error if `response` cannot be decoded into a prepared
    /// statement.
    pub fn from_prepare_response(response: Value, executor: E) -> StdResult<Self, DecodingError> {
        let map = value_to_map(response).map_err(|err| err.in_other("OK prepare response body"))?;
        let value = find_and_take_single_key_in_map(keys::SQL_STMT_ID, map).ok_or_else(|| {
            DecodingError::missing_key("SQL_STMT_ID").in_other("OK prepare response body")
        })?;
        let stmt_id: u64 = rmpv::ext::deserialize_from(value)
            .map_err(|err| DecodingError::from(err).in_key("SQL_STMT_ID"))?;
        Ok(Self::new(stmt_id, executor))
    }

    pub(crate) fn stmt_id(&self) -> u64 {
        self.stmt_id
    }
}

impl<E: Clone> Clone for PreparedSqlStatement<E> {
    fn clone(&self) -> Self {
        Self {
            stmt_id: self.stmt_id,
            executor: self.executor.clone(),
        }
    }
}

impl<E: Executor> PreparedSqlStatement<E> {
    /// Execute prepared SQL query with parameters.
    ///
    /// After a reconnect it fails: the statement is bound to the session it
    /// was prepared on (see [`PreparedSqlStatement`]).
    /// # Errors
    ///
    /// Returns an error if the request failed to reach Tarantool or
    /// Tarantool responded with an error.
    pub async fn execute<T>(&self, binds: T) -> Result<SqlResponse>
    where
        T: Tuple + Send,
    {
        Ok(SqlResponse(
            self.executor
                .send_request(Execute::new_statement_id(self.stmt_id, binds))
                .await?,
        ))
    }
}

impl<E: Clone> PreparedSqlStatement<&E> {
    #[must_use]
    pub fn with_cloned_executor(&self) -> PreparedSqlStatement<E> {
        PreparedSqlStatement {
            stmt_id: self.stmt_id,
            executor: self.executor.clone(),
        }
    }
}
