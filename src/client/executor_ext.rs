use async_trait::async_trait;
use futures::{FutureExt, future::BoxFuture};
use rmpv::Value;
use serde::de::DeserializeOwned;

use crate::{
    CallResponse, DmoResponse, Error, Executor, IteratorType, PreparedSqlStatement, Result,
    SqlResponse,
    codec::{
        consts::error_codes::{ER_SQL_EXECUTE, ER_WRONG_QUERY_ID},
        request::{
            Call, Delete, EncodedRequest, Eval, Execute, Insert, Ping, Prepare, Replace, Request,
            Select, Update, Upsert,
        },
    },
    schema::{SchemaEntityKey, Space},
    tuple::Tuple,
    utils::extract_and_deserialize_iproto_data,
};

/// Helper trait around [`Executor`] trait, which allows to send specific requests
/// with any type, implementing `Execitor` trait.
#[async_trait]
pub trait ExecutorExt: Executor {
    /// Send request, receiving raw response body.
    ///
    /// It is not recommended to use this method directly, since some requests
    /// should be only sent in specific situations and might break connection.
    fn send_request<R>(&self, body: R) -> BoxFuture<'_, Result<Value>>
    where
        R: Request;

    /// Ping tarantool instance.
    async fn ping(&self) -> Result<()> {
        self.send_request(Ping {}).await.map(drop)
    }

    // TODO: add examples

    /// Evaluate Lua expression.
    ///
    /// Check [docs][crate#deserializing-lua-responses-in-call-and-eval] on how to deserialize response.
    async fn eval<A, I>(&self, expr: I, args: A) -> Result<CallResponse>
    where
        A: Tuple + Send,
        I: AsRef<str> + Send + Sync,
    {
        Ok(CallResponse(
            self.send_request(Eval::new(expr.as_ref(), args)).await?,
        ))
    }

    /// Remotely call function in Tarantool.
    ///
    /// Check [docs][crate#deserializing-lua-responses-in-call-and-eval] on how to deserialize response.
    async fn call<A, I>(&self, function_name: I, args: A) -> Result<CallResponse>
    where
        A: Tuple + Send,
        I: AsRef<str> + Send + Sync,
    {
        Ok(CallResponse(
            self.send_request(Call::new(function_name.as_ref(), args))
                .await?,
        ))
    }

    /// Select tuples from space.
    async fn select<T, A>(
        &self,
        space_id: u32,
        index_id: u32,
        limit: Option<u32>,
        offset: Option<u32>,
        iterator: Option<IteratorType>,
        keys: A,
    ) -> Result<Vec<T>>
    where
        T: DeserializeOwned,
        A: Tuple + Send,
    {
        let body = self
            .send_request(Select::new(
                space_id, index_id, limit, offset, iterator, keys,
            ))
            .await?;
        extract_and_deserialize_iproto_data(body).map_err(Into::into)
    }

    /// Insert tuple.
    async fn insert<T>(&self, space_id: u32, tuple: T) -> Result<DmoResponse>
    where
        T: Tuple + Send,
    {
        Ok(DmoResponse(
            self.send_request(Insert::new(space_id, tuple)).await?,
        ))
    }

    // TODO: docs and doctests for DmoOperation
    /// Update tuple.
    async fn update<K, O>(
        &self,
        space_id: u32,
        index_id: u32,
        keys: K,
        ops: O,
    ) -> Result<DmoResponse>
    where
        K: Tuple + Send,
        O: Tuple + Send,
    {
        Ok(DmoResponse(
            self.send_request(Update::new(space_id, index_id, keys, ops))
                .await?,
        ))
    }

    /// Update or insert tuple.
    ///
    /// UPSERT returns nothing: the server answers with no tuple (an empty DATA
    /// array).
    async fn upsert<T, O>(&self, space_id: u32, tuple: T, ops: O) -> Result<()>
    where
        T: Tuple + Send,
        O: Tuple + Send,
    {
        self.send_request(Upsert::new(space_id, ops, tuple))
            .await
            .map(drop)
    }

    /// Insert a tuple into a space. If a tuple with the same primary key already exists,
    /// replaces the existing tuple with a new one.
    async fn replace<T>(&self, space_id: u32, tuple: T) -> Result<DmoResponse>
    where
        T: Tuple + Send,
    {
        Ok(DmoResponse(
            self.send_request(Replace::new(space_id, tuple)).await?,
        ))
    }

    /// Delete a tuple identified by the primary key.
    async fn delete<T>(&self, space_id: u32, index_id: u32, keys: T) -> Result<DmoResponse>
    where
        T: Tuple + Send,
    {
        Ok(DmoResponse(
            self.send_request(Delete::new(space_id, index_id, keys))
                .await?,
        ))
    }

    // TODO: options
    /// Perform SQL query.
    ///
    /// Statements are prepared and cached per connection, unless the cache is
    /// disabled, and a cached statement is sent by its id. While another
    /// statement is being prepared, a call sends its text instead. After DDL,
    /// one call may fail with `ER_SQL_EXECUTE` (159); after a reconnect, one
    /// call may fail with `ER_WRONG_QUERY_ID` (211). The failing call evicts
    /// the cached id and the next call prepares the statement again; the call
    /// itself is not retried.
    async fn execute_sql<T, I>(&self, query: I, binds: T) -> Result<SqlResponse>
    where
        T: Tuple + Send,
        I: AsRef<str> + Send + Sync,
    {
        let query = query.as_ref();
        let Some(stmt_id) = self.get_cached_sql_statement_id(query).await else {
            return Ok(SqlResponse(
                self.send_request(Execute::new_query(query, binds)).await?,
            ));
        };
        let res = self
            .send_request(Execute::new_statement_id(stmt_id, binds))
            .await;
        // By code alone: 159 also covers "schema version has changed", and a
        // reworded message must not bring back an id that fails for good.
        if let Err(Error::Response(err)) = &res
            && (err.code == ER_SQL_EXECUTE || err.code == ER_WRONG_QUERY_ID)
        {
            self.evict_cached_sql_statement(query, stmt_id);
        }
        Ok(SqlResponse(res?))
    }

    // TODO: add caching in case of user incorrectly uses prepared statements
    /// Prepare SQL statement.
    ///
    /// The statement is bound to the server session it was prepared on; see
    /// [`PreparedSqlStatement`] for what happens after a reconnect.
    async fn prepare_sql<I>(&self, query: I) -> Result<PreparedSqlStatement<&Self>>
    where
        I: AsRef<str> + Send + Sync,
    {
        let response = self.send_request(Prepare::new(query.as_ref())).await?;
        Ok(PreparedSqlStatement::from_prepare_response(response, self)?)
    }

    /// Find and load space by key.
    ///
    /// Can be called with space's index (if passed unsigned integer) or name (if passed `&str`).
    ///
    /// Returned [`Space`] object contains reference to current executor.
    async fn space<K>(&self, key: K) -> Result<Option<Space<&Self>>>
    where
        Self: Sized + Send,
        K: Into<SchemaEntityKey> + Send,
    {
        Space::load(self, key.into()).await
    }

    /// Find and load space by key, moving current executor into [`Space`].
    ///
    /// Can be called with space's index (if passed unsigned integer) or name (if passed `&str`).
    ///
    /// Returned [`Space`] object contains current executor.
    async fn into_space<K>(self, key: K) -> Result<Option<Space<Self>>>
    where
        Self: Sized + Send,
        K: Into<SchemaEntityKey> + Send,
    {
        Space::load(self, key.into()).await
    }
}

#[async_trait]
impl<E: Executor + ?Sized> ExecutorExt for E {
    fn send_request<R>(&self, body: R) -> BoxFuture<'_, Result<Value>>
    where
        R: Request,
    {
        let req = EncodedRequest::new(&body, None);
        async move { (*self).send_encoded_request(req?).await }.boxed()
    }
}

#[cfg(test)]
mod ui {
    #![allow(unused)]

    use crate::{Connection, Transaction};

    use super::*;

    fn executor_ext_on_connection_ref() {
        async fn f(conn: &Connection) -> Space<&Connection> {
            conn.space("space").await.unwrap().unwrap()
        }
    }

    fn executor_ext_on_connection() {
        async fn f(conn: Connection) -> Space<Connection> {
            conn.into_space("space").await.unwrap().unwrap()
        }
    }

    fn executor_ext_on_connection_cloned() {
        async fn f(conn: &Connection) -> Space<Connection> {
            conn.clone().into_space("space").await.unwrap().unwrap()
        }
    }

    fn executor_ext_on_transaction_ref() {
        async fn f(tx: &Transaction) -> Space<&Transaction> {
            tx.space("space").await.unwrap().unwrap()
        }
    }

    fn executor_ext_on_transaction() {
        async fn f(tx: Transaction) {
            let space_tx: Space<Transaction> = tx.into_space("space").await.unwrap().unwrap();
            space_tx.delete((1,)).await.unwrap();
            space_tx.commit().await.unwrap();
        }
    }
}
