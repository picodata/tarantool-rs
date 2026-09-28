# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
 - `Error::ConnectionReset`, returned when a request is made through a stream or transaction whose connection was lost since it was created. Its message reads "Connection was lost; stream or transaction state on the server is gone".
 - `TransactionError`, returned by `commit` and `rollback` on failure; after a client-side failure it carries the transaction back unfinished, and the outcome of a COMMIT is then unknown.

### Changed
 - Breaking: `Transaction::commit` and `Transaction::rollback`, and the `commit` and `rollback` wrappers on `Space<Transaction>` and `OwnedIndex<Transaction>`, return `Result<(), TransactionError>`. Success and server errors consume the transaction. A client-side failure, such as a timeout or a closed connection, returns the transaction inside `TransactionError`, unfinished; dropping that error, or converting it into `Error`, sends a ROLLBACK unless the connection was lost. After such a failure the outcome of a COMMIT is unknown: it may have been applied, may have failed on the server, or may never have been sent. A retried COMMIT and that ROLLBACK succeed in every one of these cases, because Tarantool treats COMMIT and ROLLBACK without an active transaction as successful no-ops, so neither proves what happened.
 - Breaking: `DmoOperation::delete` takes a count and encodes the 3-element operation every Tarantool version requires.
 - Breaking: `upsert` returns `Result<()>`; UPSERT never returns a tuple.
 - Breaking: `Error::DuplicatedSync` carries a `u64`.
 - Streams and transactions no longer silently continue on the new connection after a reconnect. A request made through one fails with `Error::ConnectionReset` as soon as the loss of its connection is observed, even mid-reconnect, instead of running against the wrong server session. A stream or transaction created during the outage works after the reconnect.
 - IPROTO_ID is sent on every connect and reconnect, so every server session enables streams, transactions and the error extension. AUTH and ID share one round trip.
 - A request whose caller stops waiting (timeout, dropped future, lost `select!` branch) leaves the in-flight map.
 - The dispatcher closes the connection and exits as soon as the last client handle is dropped, whether connected or reconnecting.
 - The SQL statement cache is invalidated after a reconnect; a cached prepared statement no longer fails permanently with Tarantool's `ER_WRONG_QUERY_ID`. A cached statement id that the server rejects with `ER_SQL_EXECUTE` (for example "statement has expired" after DDL) or `ER_WRONG_QUERY_ID` is evicted; the error is returned and the next call prepares the statement again.
 - `reconnect_interval(None)` is documented as a hot reconnect loop with a real CPU cost.
 - The exponential reconnect backoff no longer uses the `backoff` crate. Ordinary parameters behave as before; degenerate ones are normalized instead of panicking or retrying back to back.
 - Requests queued together are written to the socket in one batch.
 - A request whose frame would exceed Tarantool's 2 GiB request limit fails on the client with `EncodingError::MessagePack`, instead of being sent and making the server close the connection.
 - The minimum versions are tokio 1.22, anyhow 1.0.47, async-trait 0.1.43, rmp 0.8.11, serde 1.0.136 and tracing 0.1.7.

### Removed
 - Dead resend machinery, which never resent a request.

### Fixed
 - `upsert` sends the `UPSERT` opcode instead of `REPLACE`.
 - `DmoOperation::insert` is encoded with the `!` operator instead of `|`.
 - A grab-bag of protocol and handshake hardening: frame length decoding, error description bounds, auth/ID response sync validation, and writer task join failures reported as connection errors.
 - The client reserves at most 64 MiB up front for a response frame and grows the buffer as bytes arrive; any length Tarantool can send is accepted.
 - Request syncs are 64-bit, as on the wire.
 - `connect_timeout` bounds the whole connection setup (TCP connect, greeting, AUTH, ID) of the first connection and every reconnect attempt. When it is unset, the request timeout does, as before. Before, `connect_timeout` was ignored. Users who set both now get `connect_timeout` for setup.
 - `internal_simultaneous_requests_threshold` outside its range is clamped instead of panicking in `build`.

## [picodata-0.12] - 2026-09-02

### Fixed
 - Failed channel means that connection closed

## [picodata-0.11] - 2026-08-31

### Changed
 - updated dependencies and cleaning up the project


## [0.0.10] - 2023-10-04
### Added
 - `internal_simultaneous_requests_threshold` parameter to builder, which allow to customize maximum number of simultaneously created requests, which connection can effectively handle.

### Changed
 - Rewritten internal logic of connection to Tarantool, which improved performance separated reading and writing to socket into separate tasks.

### Fixed
 - Increased size of internal channel between dispatcher and connection, which should significantly increase performance (previously it was degrading rapidly with a lot of concurrent requests).


## [0.0.9] - 2023-09-23 (broken, yanked)


## [0.0.8] - 2023-09-05
### Added
 - Data-manipulation operations (insert, update, upsert, replace, delete) now return `DmoResponse` with row, returned by operation ([#7](https://github.com/Flowneee/tarantool-rs/issues/7));
 - `TupleElement` trait, which allow to write type into `Tuple` without having `serde::Serialize` implemented for it;
 - `DmoOperation` for constructing operations in `update` and `upsert` calls.

### Changed
 - `TupleResponse` renamed to `CallResponse`.


## [0.0.7] - 2023-08-24
### Added
 - Support for preparing and executing SQL queries.


## [0.0.6] - 2023-08-20
### Added
 - `TupleResponse` type for decoding `eval` and `call` responses.

### Fixed
 - `delete` request sends correct request type.


## [0.0.5] - 2023-08-05
### Added
 - `into_space` method to `ExecutorExt` trait, wich return `Space` with underlying `Executor`;
 - `.commit()` and `.rollback()` methods to `Space<Transaction>` and `OwnedIndex<Transaction>`;
 - `timeout` parameter to `ConnectionBuilder`, allowing to set timeout for all requests in this `Connection`;
 - `Tuple` trait for passing arguments to requests.

### Changed
 - `get_space` moved to `ExecutorExt` trait and renamed to `space`, also now returning reference to underlying `Execitor`.


## [0.0.4] - 2023-08-01
### Added
 - `Index` API, which simplify making `select` and CRUD requsts on specific index.

### Changed
 - `ConnectionLike` renamed to `ExecutorExt`;
 - Few smaller renames;

### Removed
 - `Error::MetadataLoad` variant;
 - `IndexMetadata` from `SpaceMetadata`;
 - Public methods for loading metadata.


## [0.0.3] - 2023-07-30
### Fixed
 - `.update()` request sends correct request type.

### Added
 - `Executor` trait, which sends encoded request;
 - `.stream()`, `.transaction()` and `.transaction_builder()` methods moved to `Executor` trait;
 - `Request` struct renamed to `EncodedRequest`;
 - `RequestBody` trait renamed to `Request`;
 - `Space` API, which simplify making `select` and CRUD requsts on specific space.

### Changed
 - `ConnectionLike` now `Send` and `Sync`.


## [0.0.2] - 2023-05-18
### Added
 - `indices` method to `SpaceMetadata` for accessing space's indices;
 - `get_by_name` and `get_by_id` methods to `UniqueIdNameMap`;
 - reconnection in background, if current conection died;
 - optional timeout on connection.

### Changed
 - `ConnectionBuilder` most methods now accept new values as `impl Into<Option<...>>`;
 - `TransactionBuilder` methods now return `&mut Self`.


## [0.0.1] - 2023-05-15
### Added
 - Initial implementation.
