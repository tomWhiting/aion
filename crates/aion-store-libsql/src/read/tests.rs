use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use aion_store::{
    Event, ReadableEventStore, StoreError, WorkflowFilter, WorkflowId, WorkflowStatus,
    WritableEventStore, WriteToken,
};
use chrono::{DateTime, Utc};
use serde_json::json;

use crate::LibSqlStore;

#[tokio::test]
async fn read_history_returns_empty_for_unknown_workflow() -> Result<(), StoreError> {
    let store = open_test_store("unknown-history").await?;

    assert_eq!(store.read_history(&workflow_id(1)).await?, Vec::new());
    Ok(())
}

#[tokio::test]
async fn read_history_returns_events_in_appended_order() -> Result<(), StoreError> {
    let store = open_test_store("ordered-round-trip").await?;
    let workflow_id = workflow_id(1);
    let events = vec![
        workflow_started(1, &workflow_id, "checkout"),
        signal_received(2, &workflow_id, "wake"),
        workflow_completed(3, &workflow_id),
    ];

    store
        .append(WriteToken::recorder(), &workflow_id, &events, 0)
        .await?;

    assert_eq!(store.read_history(&workflow_id).await?, events);
    Ok(())
}

#[tokio::test]
async fn malformed_event_blob_maps_to_serialization_error() -> Result<(), StoreError> {
    let store = open_test_store("malformed-blob").await?;
    let workflow_id = workflow_id(1);

    insert_raw_event(&store, &workflow_id, b"not json".to_vec()).await?;

    assert!(matches!(
        store.read_history(&workflow_id).await,
        Err(StoreError::Serialization(_))
    ));
    Ok(())
}

#[tokio::test]
async fn validate_event_compatibility_rejects_stale_workflow_started_blob() -> Result<(), StoreError>
{
    let store = open_test_store("stale-workflow-started").await?;
    let workflow_id = workflow_id(1);
    let stale_event = json!({
        "type": "WorkflowStarted",
        "data": {
            "envelope": {
                "seq": 1,
                "recorded_at": recorded_at(1),
                "workflow_id": workflow_id,
            },
            "workflow_type": "checkout",
            "input": {
                "content_type": "Json",
                "bytes": [123, 125]
            }
        }
    });

    insert_raw_event(
        &store,
        &workflow_id,
        serde_json::to_vec(&stale_event).map_err(|error| {
            StoreError::Serialization(format!("stale fixture could not be encoded: {error}"))
        })?,
    )
    .await?;

    assert!(matches!(
        store.validate_event_compatibility().await,
        Err(StoreError::Serialization(_))
    ));
    Ok(())
}

#[tokio::test]
async fn status_projection_reports_running_and_completed() -> Result<(), StoreError> {
    let store = open_test_store("status-projection").await?;
    let running = workflow_id(1);
    let completed = workflow_id(2);

    store
        .append(
            WriteToken::recorder(),
            &running,
            &[workflow_started(1, &running, "checkout")],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &completed,
            &[
                workflow_started(1, &completed, "checkout"),
                workflow_completed(2, &completed),
            ],
            0,
        )
        .await?;

    let running_summary = one_summary(
        store
            .query(&WorkflowFilter {
                status: Some(WorkflowStatus::Running),
                ..WorkflowFilter::default()
            })
            .await?,
    )?;
    let completed_summary = one_summary(
        store
            .query(&WorkflowFilter {
                status: Some(WorkflowStatus::Completed),
                ..WorkflowFilter::default()
            })
            .await?,
    )?;

    assert_eq!(running_summary.workflow_id, running);
    assert_eq!(running_summary.status, WorkflowStatus::Running);
    assert_eq!(completed_summary.workflow_id, completed);
    assert_eq!(completed_summary.status, WorkflowStatus::Completed);
    Ok(())
}

#[tokio::test]
async fn list_active_returns_only_running_workflows() -> Result<(), StoreError> {
    let store = open_test_store("active-list").await?;
    let running = workflow_id(1);
    let completed = workflow_id(2);
    let failed = workflow_id(3);

    store
        .append(
            WriteToken::recorder(),
            &running,
            &[workflow_started(1, &running, "checkout")],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &completed,
            &[
                workflow_started(1, &completed, "checkout"),
                workflow_completed(2, &completed),
            ],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &failed,
            &[
                workflow_started(1, &failed, "billing"),
                workflow_failed(2, &failed),
            ],
            0,
        )
        .await?;

    assert_eq!(store.list_active().await?, vec![running]);
    Ok(())
}

#[tokio::test]
async fn query_default_returns_all_workflows() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-default").await?;

    let summaries = store.query(&WorkflowFilter::default()).await?;

    assert_eq!(
        summaries
            .iter()
            .map(|summary| summary.workflow_id.clone())
            .collect::<Vec<_>>(),
        vec![
            ids.parent,
            ids.running_checkout,
            ids.completed_checkout,
            ids.failed_billing
        ]
    );
    Ok(())
}

#[tokio::test]
async fn query_filters_by_workflow_type() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-type").await?;

    let summaries = store
        .query(&WorkflowFilter {
            workflow_type: Some(String::from("billing")),
            ..WorkflowFilter::default()
        })
        .await?;

    assert_eq!(one_summary(summaries)?.workflow_id, ids.failed_billing);
    Ok(())
}

#[tokio::test]
async fn query_filters_by_status() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-status").await?;

    let summaries = store
        .query(&WorkflowFilter {
            status: Some(WorkflowStatus::Completed),
            ..WorkflowFilter::default()
        })
        .await?;

    assert_eq!(one_summary(summaries)?.workflow_id, ids.completed_checkout);
    Ok(())
}

#[tokio::test]
async fn query_filters_by_start_time_bounds() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-time").await?;

    let summaries = store
        .query(&WorkflowFilter {
            started_after: Some(recorded_at(20)),
            started_before: Some(recorded_at(20)),
            ..WorkflowFilter::default()
        })
        .await?;

    assert_eq!(one_summary(summaries)?.workflow_id, ids.completed_checkout);
    Ok(())
}

#[tokio::test]
async fn query_filters_by_parent() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-parent").await?;

    let summaries = store
        .query(&WorkflowFilter {
            parent: Some(ids.parent.clone()),
            ..WorkflowFilter::default()
        })
        .await?;

    let summary = one_summary(summaries)?;
    assert_eq!(summary.workflow_id, ids.running_checkout);
    assert_eq!(summary.parent, Some(ids.parent));
    Ok(())
}

#[tokio::test]
async fn query_combines_filter_dimensions() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-combined").await?;

    let summaries = store
        .query(&WorkflowFilter {
            workflow_type: Some(String::from("checkout")),
            status: Some(WorkflowStatus::Completed),
            started_after: Some(recorded_at(20)),
            started_before: Some(recorded_at(20)),
            parent: None,
        })
        .await?;

    assert_eq!(one_summary(summaries)?.workflow_id, ids.completed_checkout);
    Ok(())
}

#[tokio::test]
async fn query_combines_parent_with_child_filters() -> Result<(), StoreError> {
    let (store, ids) = seeded_store("query-parent-combined").await?;

    let summaries = store
        .query(&WorkflowFilter {
            workflow_type: Some(String::from("checkout")),
            status: Some(WorkflowStatus::Running),
            started_after: Some(recorded_at(10)),
            started_before: Some(recorded_at(10)),
            parent: Some(ids.parent.clone()),
        })
        .await?;

    let summary = one_summary(summaries)?;
    assert_eq!(summary.workflow_id, ids.running_checkout);
    assert_eq!(summary.parent, Some(ids.parent));
    Ok(())
}

#[tokio::test]
async fn query_binds_filter_values_with_sql_metacharacters() -> Result<(), StoreError> {
    let store = open_test_store("query-sql-metacharacters").await?;
    let tricky = workflow_id(1);
    let normal = workflow_id(2);
    let workflow_type = "checkout' OR 1=1 --";

    store
        .append(
            WriteToken::recorder(),
            &tricky,
            &[workflow_started(1, &tricky, workflow_type)],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &normal,
            &[workflow_started(1, &normal, "checkout")],
            0,
        )
        .await?;

    let summaries = store
        .query(&WorkflowFilter {
            workflow_type: Some(String::from(workflow_type)),
            ..WorkflowFilter::default()
        })
        .await?;

    assert_eq!(one_summary(summaries)?.workflow_id, tricky);
    Ok(())
}

async fn seeded_store(name: &str) -> Result<(LibSqlStore, SeedIds), StoreError> {
    let store = open_test_store(name).await?;
    let ids = SeedIds {
        parent: workflow_id(10),
        running_checkout: workflow_id(11),
        completed_checkout: workflow_id(12),
        failed_billing: workflow_id(13),
    };

    store
        .append(
            WriteToken::recorder(),
            &ids.parent,
            &[
                workflow_started(1, &ids.parent, "parent"),
                child_workflow_started(2, &ids.parent, &ids.running_checkout, "checkout"),
            ],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &ids.running_checkout,
            &[workflow_started_at(
                1,
                &ids.running_checkout,
                "checkout",
                10,
            )],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &ids.completed_checkout,
            &[
                workflow_started_at(1, &ids.completed_checkout, "checkout", 20),
                workflow_completed_at(2, &ids.completed_checkout, 21),
            ],
            0,
        )
        .await?;
    store
        .append(
            WriteToken::recorder(),
            &ids.failed_billing,
            &[
                workflow_started_at(1, &ids.failed_billing, "billing", 30),
                workflow_failed_at(2, &ids.failed_billing, 31),
            ],
            0,
        )
        .await?;

    Ok((store, ids))
}

async fn open_test_store(name: &str) -> Result<LibSqlStore, StoreError> {
    LibSqlStore::open(unique_temp_path(name)).await
}

async fn insert_raw_event(
    store: &LibSqlStore,
    workflow_id: &WorkflowId,
    event: Vec<u8>,
) -> Result<(), StoreError> {
    store
        .connection()
        .execute(
            "INSERT INTO events (workflow_id, seq, event, recorded_at, event_kind, is_queryable_event) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            libsql::params![workflow_id.to_string(), 1_u64, event, recorded_at(1).to_rfc3339(), "WorkflowStarted", 1_i64],
        )
        .await
        .map(|_| ())
        .map_err(|error| crate::error::libsql_error(&error))
}

fn one_summary(
    summaries: Vec<aion_store::WorkflowSummary>,
) -> Result<aion_store::WorkflowSummary, StoreError> {
    let mut iter = summaries.into_iter();
    let Some(summary) = iter.next() else {
        return Err(StoreError::Backend(String::from(
            "expected one summary, got none",
        )));
    };
    if iter.next().is_some() {
        return Err(StoreError::Backend(String::from(
            "expected one summary, got multiple",
        )));
    }
    Ok(summary)
}

fn workflow_id(value: u128) -> WorkflowId {
    WorkflowId::new(uuid::Uuid::from_u128(value))
}

fn workflow_started(seq: u64, workflow_id: &WorkflowId, workflow_type: &str) -> Event {
    workflow_started_at(
        seq,
        workflow_id,
        workflow_type,
        i64::try_from(seq).unwrap_or_default(),
    )
}

fn workflow_started_at(
    seq: u64,
    workflow_id: &WorkflowId,
    workflow_type: &str,
    offset_seconds: i64,
) -> Event {
    Event::WorkflowStarted {
        envelope: envelope(seq, workflow_id, offset_seconds),
        workflow_type: workflow_type.to_owned(),
        input: payload("input"),
        run_id: aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        parent_run_id: None,
    }
}

fn workflow_completed(seq: u64, workflow_id: &WorkflowId) -> Event {
    workflow_completed_at(seq, workflow_id, i64::try_from(seq).unwrap_or_default())
}

fn workflow_completed_at(seq: u64, workflow_id: &WorkflowId, offset_seconds: i64) -> Event {
    Event::WorkflowCompleted {
        envelope: envelope(seq, workflow_id, offset_seconds),
        result: payload("result"),
    }
}

fn workflow_failed(seq: u64, workflow_id: &WorkflowId) -> Event {
    workflow_failed_at(seq, workflow_id, i64::try_from(seq).unwrap_or_default())
}

fn workflow_failed_at(seq: u64, workflow_id: &WorkflowId, offset_seconds: i64) -> Event {
    Event::WorkflowFailed {
        envelope: envelope(seq, workflow_id, offset_seconds),
        error: aion_store::WorkflowError {
            message: String::from("failed"),
            details: None,
        },
    }
}

fn signal_received(seq: u64, workflow_id: &WorkflowId, name: &str) -> Event {
    Event::SignalReceived {
        envelope: envelope(seq, workflow_id, i64::try_from(seq).unwrap_or_default()),
        name: name.to_owned(),
        payload: payload("signal"),
    }
}

fn child_workflow_started(
    seq: u64,
    parent: &WorkflowId,
    child: &WorkflowId,
    workflow_type: &str,
) -> Event {
    Event::ChildWorkflowStarted {
        envelope: envelope(seq, parent, i64::try_from(seq).unwrap_or_default()),
        child_workflow_id: child.clone(),
        workflow_type: workflow_type.to_owned(),
        input: payload("child-input"),
    }
}

fn envelope(seq: u64, workflow_id: &WorkflowId, offset_seconds: i64) -> aion_store::EventEnvelope {
    aion_store::EventEnvelope {
        seq,
        recorded_at: recorded_at(offset_seconds),
        workflow_id: workflow_id.clone(),
    }
}

fn payload(label: &str) -> aion_store::Payload {
    aion_store::Payload::from_json(&json!({ "label": label })).unwrap_or_else(|error| {
        aion_store::Payload::new(
            aion_store::ContentType::Json,
            format!("{{\"payload_error\":\"{error}\"}}").into_bytes(),
        )
    })
}

fn recorded_at(offset_seconds: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0).unwrap_or_default()
}

fn unique_temp_path(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    std::env::temp_dir().join(format!(
        "aion-store-libsql-read-{name}-{}-{nanos}.db",
        std::process::id()
    ))
}

struct SeedIds {
    parent: WorkflowId,
    running_checkout: WorkflowId,
    completed_checkout: WorkflowId,
    failed_billing: WorkflowId,
}
