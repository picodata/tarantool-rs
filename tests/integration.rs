use std::time::Duration;

use assert_matches::assert_matches;
use rmpv::Value;
use serde::{Deserialize, Serialize};
use tarantool_rs::{Connection, DmoOperation, Executor, ExecutorExt, Stream, errors::Error};
use tracing_test::traced_test;

use crate::common::{TarantoolTestContainer, TarantoolTestContainerExt};

mod common;

#[derive(Debug, Deserialize, PartialEq, Serialize)]
struct CrewMember {
    id: u32,
    name: String,
    rank: String,
    occupation: String,
}

#[tokio::test]
#[traced_test]
async fn image_test() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = container.create_conn().await?;
    conn.ping().await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn auth_ok() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = Connection::builder()
        .auth("Sisko", Some("A-4-7-1"))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;
    conn.ping().await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn auth_err() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    assert_matches!(
        Connection::builder()
            .auth("Quark", Some("Q-0-0-0"))
            .build(format!("127.0.0.1:{}", container.connect_port()))
            .await
            .map(drop),
        Err(Error::Auth(_))
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn eval() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = container.create_conn().await?;
    let res: u32 = conn.eval("return ...", (42,)).await?.decode_result()?;
    assert_eq!(res, 42);

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn call() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = container.create_conn().await?;
    let res: String = conn.call("station_name", (false,)).await?.decode_first()?;
    assert_eq!(res, "Deep Space 9");

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn retrieve_schema() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");
    assert_eq!(
        space.metadata().id(),
        512,
        "First user space expected to have id 512"
    );
    assert_eq!(space.metadata().name(), "ds9_crew");

    let index_count = space.indices().count();
    assert!(index_count > 1, "There should be multiple indices");

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn select_all() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn: Connection = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");

    let members: Vec<CrewMember> = space
        .select(None, None, Some(tarantool_rs::IteratorType::All), ())
        .await?;
    assert_eq!(members.len(), 7);
    assert_eq!(
        members[1],
        CrewMember {
            id: 2,
            name: "Kira Nerys".into(),
            rank: "Major".into(),
            occupation: "First officer".into()
        }
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn select_limits() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn: Connection = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");

    let members: Vec<CrewMember> = space
        .select(Some(2), Some(2), Some(tarantool_rs::IteratorType::All), ())
        .await?;
    assert_eq!(members.len(), 2);
    assert_eq!(
        members[1],
        CrewMember {
            id: 4,
            name: "Julian Bashir".into(),
            rank: "Lieutenant".into(),
            occupation: "Chief medical officer".into()
        }
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn select_by_key() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn: Connection = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");
    let rank_idx = space.index("idx_rank").expect("Rank index present");

    let members: Vec<CrewMember> = rank_idx
        .select(None, None, None, ("Lieutenant Commander",))
        .await?;
    assert_eq!(members.len(), 2);
    assert_eq!(
        members[0],
        CrewMember {
            id: 3,
            name: "Jadzia Dax".into(),
            rank: "Lieutenant Commander".into(),
            occupation: "Science officer".into()
        }
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn timeout() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = Connection::builder()
        .timeout(Duration::from_millis(100))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;

    assert_matches!(
        conn.eval("require('fiber').sleep(1)", ()).await,
        Err(tarantool_rs::Error::Timeout)
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn dmo() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();

    let conn = Connection::builder()
        .timeout(Duration::from_millis(100))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;

    let tx = conn.transaction().await?;
    let space = tx.space("ds9_crew").await?.expect("Space 'ds9_crew' found");
    let name_idx = space.index("idx_name").unwrap();

    // update
    let new_value: CrewMember = name_idx
        .update(
            ("Benjamin Sisko",),
            (Value::Array(vec!["=".into(), 2.into(), "Captain".into()]),),
        )
        .await?
        .decode()?;
    assert_eq!(
        new_value,
        CrewMember {
            id: 1,
            name: "Benjamin Sisko".into(),
            rank: "Captain".into(),
            occupation: "Commanding officer".into()
        }
    );

    // delete
    let _: CrewMember = name_idx.delete(("Jadzia Dax",)).await?.decode()?;

    // insert
    let _: CrewMember = space
        .insert((None::<()>, "Ezri Dax", "Ensign", "Counselor"))
        .await?
        .decode()?;

    tx.commit().await?;

    Ok(())
}

/// Ping until the client is connected again, failing after `limit`.
async fn wait_until_reconnected(conn: &Connection, limit: Duration) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        match conn.ping().await {
            Ok(()) => return,
            Err(err) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "client did not reconnect within {limit:?}: {err:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[tokio::test]
#[traced_test]
async fn restart_mid_transaction_fails_the_stale_transaction() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_restartable();
    let conn = Connection::builder()
        .timeout(Duration::from_secs(5))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;

    let tx = conn.transaction().await?;
    let space = tx
        .space("reconnect")
        .await?
        .expect("Space 'reconnect' found");
    let _ = space.insert((1u32,)).await?;

    container.restart();
    // A fresh request succeeding proves the reconnect, including the ID
    // re-handshake, completed.
    wait_until_reconnected(&conn, Duration::from_secs(60)).await;

    assert_matches!(space.insert((2u32,)).await, Err(Error::ConnectionReset));
    drop(space);
    drop(tx);

    // The torn transaction left nothing behind.
    let fresh = conn
        .space("reconnect")
        .await?
        .expect("Space 'reconnect' found");
    let rows: Vec<(u32,)> = fresh
        .select(None, None, Some(tarantool_rs::IteratorType::All), ())
        .await?;
    assert!(rows.is_empty(), "uncommitted insert survived: {rows:?}");

    Ok(())
}

/// Ping `probe` until it fails with `ConnectionReset`, which proves that the
/// client observed the loss of the connection the probe was created on.
async fn wait_until_reset(probe: &Stream, limit: Duration) {
    let deadline = tokio::time::Instant::now() + limit;
    loop {
        match probe.ping().await {
            Err(Error::ConnectionReset) => return,
            other => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "the loss was not observed within {limit:?}: {other:?}"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

#[tokio::test]
#[traced_test]
async fn transaction_created_during_outage_commits_after_reconnect() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_restartable();
    // Long enough for BEGIN to wait out the restart in the queue.
    let conn = Connection::builder()
        .timeout(Duration::from_secs(30))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;
    let probe = conn.stream();
    probe.ping().await?;

    container.restart();
    wait_until_reset(&probe, Duration::from_secs(60)).await;

    // Begun after the loss was observed: it belongs to the next connection.
    let tx = conn.transaction().await?;
    let space = tx
        .space("reconnect")
        .await?
        .expect("Space 'reconnect' found");
    let _ = space.insert((10u32,)).await?;
    drop(space);
    tx.commit().await?;

    let rows: Vec<(u32,)> = conn
        .space("reconnect")
        .await?
        .expect("Space 'reconnect' found")
        .select(None, None, Some(tarantool_rs::IteratorType::All), ())
        .await?;
    assert_eq!(rows, vec![(10,)]);

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn execute_sql_after_ddl_fails_once_then_recovers() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();
    let conn = container.create_conn().await?;

    // Caches the statement.
    let _ = conn.execute_sql("SELECT ?", (1,)).await?;
    // DDL changes the schema version, which expires every prepared statement.
    let _ = conn
        .eval("box.schema.space.create('ddl_bump'):drop()", ())
        .await?;

    let err = conn
        .execute_sql("SELECT ?", (1,))
        .await
        .expect_err("the expired statement ran");
    assert_matches!(err, Error::Response(ref response) if response.code == 159);
    // The failing call evicted the id: the next one prepares again.
    let _ = conn.execute_sql("SELECT ?", (1,)).await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn prepared_statement_after_restart_returns_wrong_query_id() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_restartable();
    let conn = Connection::builder()
        .timeout(Duration::from_secs(5))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;
    let statement = conn.prepare_sql("SELECT ?").await?;
    let _ = statement.execute((1,)).await?;

    container.restart();
    wait_until_reconnected(&conn, Duration::from_secs(60)).await;

    // The new session does not know the id.
    let err = statement
        .execute((1,))
        .await
        .expect_err("the statement of the lost session ran");
    assert_matches!(err, Error::Response(ref response) if response.code == 211);
    // Prepared again, it runs.
    let statement = conn.prepare_sql("SELECT ?").await?;
    let _ = statement.execute((1,)).await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn cached_sql_during_outage_uses_the_new_session() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_restartable();
    // Long enough for the PREPARE to wait out the restart in the queue.
    let conn = Connection::builder()
        .timeout(Duration::from_secs(30))
        .build(format!("127.0.0.1:{}", container.connect_port()))
        .await?;
    // Cached on the first session.
    let _ = conn.execute_sql("SELECT ?", (1,)).await?;
    let probe = conn.stream();
    probe.ping().await?;

    container.restart();
    wait_until_reset(&probe, Duration::from_secs(60)).await;

    // The loss was observed, so the lookup misses and prepares on the new
    // session instead of sending the old id.
    let _ = conn.execute_sql("SELECT ?", (1,)).await?;

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn upsert_on_existing_tuple_applies_operations() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();
    let conn = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");

    // A REPLACE would store this tuple verbatim. An UPSERT on an existing key
    // must ignore the tuple and apply the operations instead. It returns
    // nothing, which the `()` binding pins.
    let () = space
        .upsert(
            (1u32, "Replaced", "Replaced", "Replaced"),
            (DmoOperation::assign(2u32, "Captain"),),
        )
        .await?;

    let members: Vec<CrewMember> = space.select(None, None, None, (1u32,)).await?;
    assert_eq!(
        members,
        vec![CrewMember {
            id: 1,
            name: "Benjamin Sisko".into(),
            rank: "Captain".into(),
            occupation: "Commanding officer".into()
        }]
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn dmo_insert_operation_inserts_a_field() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();
    let conn = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");

    // `!` shifts the old fields right. A bitwise OR (`|`) would instead be
    // rejected, because its argument is a string.
    let updated: (u32, String, String, String, String) = space
        .update((1u32,), (DmoOperation::insert(1u32, "Emissary"),))
        .await?
        .decode()?;
    assert_eq!(
        updated,
        (
            1,
            "Emissary".into(),
            "Benjamin Sisko".into(),
            "Commander".into(),
            "Commanding officer".into()
        )
    );

    Ok(())
}

#[tokio::test]
#[traced_test]
async fn dmo_delete_operation_removes_fields() -> Result<(), anyhow::Error> {
    let container = TarantoolTestContainer::new_with_test_data();
    let conn = container.create_conn().await?;
    let space = conn
        .space("ds9_crew")
        .await?
        .expect("Space 'ds9_crew' found");

    // Two updates: one request may not touch the same field twice. Deleting
    // the inserted field keeps the tuple within the space format.
    let _ = space
        .update((1u32,), (DmoOperation::insert(1u32, "Emissary"),))
        .await?;
    let restored: CrewMember = space
        .update((1u32,), (DmoOperation::delete(1u32, 1),))
        .await?
        .decode()?;
    assert_eq!(
        restored,
        CrewMember {
            id: 1,
            name: "Benjamin Sisko".into(),
            rank: "Commander".into(),
            occupation: "Commanding officer".into()
        }
    );

    Ok(())
}

// No `#[traced_test]`: the codec logs every response body at debug level, and
// capturing a 70 MiB body would only slow the test down.
#[tokio::test]
async fn response_above_64_mib_is_received() -> Result<(), anyhow::Error> {
    const LEN: usize = 70 * 1024 * 1024;
    let container = TarantoolTestContainer::new_with_test_data();
    let conn = container.create_conn().await?;
    // Both live on the server session: a reconnect would make them stale.
    let stream = conn.stream();
    let tx = conn.transaction().await?;

    let (big, ping) = tokio::join!(
        conn.eval("return string.rep('x', ...)", (LEN,)),
        conn.ping(),
    );
    let big: String = big?.decode_first()?;
    assert_eq!(big.len(), LEN);
    ping?;

    stream.ping().await?;
    tx.ping().await?;
    tx.commit().await?;

    Ok(())
}
