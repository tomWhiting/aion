//! Runtime-gated embedded-replica sync integration coverage.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use aion_store::{
    Event, EventEnvelope, Payload, ReadableEventStore, StoreError, WorkflowId, WritableEventStore,
    WriteToken,
};
use aion_store_libsql::{LibSqlConfig, LibSqlMode, LibSqlStore};
use chrono::{DateTime, Utc};
use uuid::Uuid;

static DATABASE_COUNTER: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn embedded_replica_sync_round_trips_through_primary() -> Result<(), StoreError> {
    let Ok(primary_url) = std::env::var("AION_LIBSQL_TEST_URL") else {
        tracing::info!(
            missing_env_var = "AION_LIBSQL_TEST_URL",
            "skipping embedded-replica sync test"
        );
        return Ok(());
    };
    let Ok(auth_token) = std::env::var("AION_LIBSQL_TEST_TOKEN") else {
        tracing::info!(
            missing_env_var = "AION_LIBSQL_TEST_TOKEN",
            "skipping embedded-replica sync test"
        );
        return Ok(());
    };

    let writer = LibSqlStore::connect(replica_config(
        "writer",
        primary_url.clone(),
        auth_token.clone(),
    ))
    .await?;
    writer.sync().await?;

    let workflow_id = workflow_id();
    let events = vec![
        workflow_started(1, &workflow_id, "replica-sync")?,
        signal_received(2, &workflow_id, "remote-round-trip")?,
    ];

    writer
        .append(WriteToken::recorder(), &workflow_id, &events, 0)
        .await?;
    assert_eq!(writer.read_history(&workflow_id).await?, events);
    writer.sync().await?;

    let reader = LibSqlStore::connect(replica_config("reader", primary_url, auth_token)).await?;
    reader.sync().await?;

    assert_eq!(reader.read_history(&workflow_id).await?, events);

    Ok(())
}

fn replica_config(name: &str, primary_url: String, auth_token: String) -> LibSqlConfig {
    LibSqlConfig {
        mode: LibSqlMode::EmbeddedReplica {
            path: unique_temp_path(name),
            primary_url,
            auth_token,
        },
        journal_mode: None,
        synchronous: None,
        sync_interval_seconds: None,
    }
}

fn workflow_id() -> WorkflowId {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let counter = u128::from(DATABASE_COUNTER.fetch_add(1, Ordering::Relaxed));

    WorkflowId::new(Uuid::from_u128(nanos ^ counter))
}

fn workflow_started(
    seq: u64,
    workflow_id: &WorkflowId,
    workflow_type: &str,
) -> Result<Event, StoreError> {
    Ok(Event::WorkflowStarted {
        envelope: envelope(seq, workflow_id)?,
        workflow_type: workflow_type.to_owned(),
        input: payload("input")?,
        run_id: aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        parent_run_id: None,
    })
}

fn signal_received(seq: u64, workflow_id: &WorkflowId, name: &str) -> Result<Event, StoreError> {
    Ok(Event::SignalReceived {
        envelope: envelope(seq, workflow_id)?,
        name: name.to_owned(),
        payload: payload("signal")?,
    })
}

fn envelope(seq: u64, workflow_id: &WorkflowId) -> Result<EventEnvelope, StoreError> {
    Ok(EventEnvelope {
        seq,
        recorded_at: recorded_at(seq_as_offset(seq)?)?,
        workflow_id: workflow_id.clone(),
    })
}

fn payload(label: &str) -> Result<Payload, StoreError> {
    Payload::from_json(&serde_json::json!({ "label": label }))
        .map_err(|error| StoreError::Serialization(error.to_string()))
}

fn recorded_at(offset_seconds: i64) -> Result<DateTime<Utc>, StoreError> {
    DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0)
        .ok_or_else(|| StoreError::Backend(String::from("test timestamp should be representable")))
}

fn seq_as_offset(seq: u64) -> Result<i64, StoreError> {
    i64::try_from(seq).map_err(|error| {
        StoreError::Backend(format!("event sequence out of timestamp range: {error}"))
    })
}

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let counter = DATABASE_COUNTER.fetch_add(1, Ordering::Relaxed);

    std::env::temp_dir().join(format!(
        "aion-store-libsql-replica-{name}-{}-{nanos}-{counter}.db",
        std::process::id()
    ))
}
