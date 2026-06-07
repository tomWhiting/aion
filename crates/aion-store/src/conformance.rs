//! Reusable behavioural conformance suite for [`EventStore`] implementations.

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use aion_core::{
    Event, EventEnvelope, Payload, RunId, TimerId, WorkflowError, WorkflowFilter, WorkflowId,
    WorkflowStatus, WorkflowSummary,
};
use chrono::{DateTime, Utc};

use crate::{EventStore, RunSummary, StoreError, TimerEntry};

/// Runs the shared behavioural suite for an [`EventStore`] implementation.
///
/// The supplied factory is called once per scenario and must return a fresh, isolated store. Durable
/// backends should create a new temporary database or namespace each time the factory is invoked.
///
/// # Errors
///
/// Returns the first store error or contract assertion failure encountered by the suite.
pub async fn run_event_store_suite<MakeStore, MakeFuture>(
    make_store: MakeStore,
) -> Result<(), StoreError>
where
    MakeStore: Fn() -> MakeFuture,
    MakeFuture: Future<Output = Arc<dyn EventStore>>,
{
    append_and_read_history_round_trip(make_store().await).await?;
    multi_batch_append_advances_sequence(make_store().await).await?;
    stale_expected_sequence_writes_nothing(make_store().await).await?;
    concurrent_appends_on_same_expected_sequence_has_one_winner(make_store().await).await?;
    large_history_round_trip(make_store().await).await?;
    list_active_reflects_projected_status(make_store().await).await?;
    query_applies_all_filters(make_store().await).await?;
    continued_as_new_query_returns_correct_status_and_ended_at(make_store().await).await?;
    read_run_chain_orders_continuations(make_store().await).await?;
    read_run_chain_single_and_multi_continuations(make_store().await).await?;
    expired_timers_include_due_boundary_and_exclude_future(make_store().await).await?;
    rescheduling_same_timer_replaces_prior_fire_at(make_store().await).await?;
    Ok(())
}

async fn append_and_read_history_round_trip(store: Arc<dyn EventStore>) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    expect_empty(
        store.read_history(&workflow_id).await?,
        "unknown workflow history should be empty",
    )?;

    let events = vec![
        workflow_started(1, &workflow_id, "checkout")?,
        activity_scheduled(2, &workflow_id, "reserve-inventory")?,
        timer_started(3, &workflow_id, TimerId::anonymous(3), recorded_at(30)?)?,
        signal_received(4, &workflow_id, "payment-authorized")?,
    ];

    store.append(&workflow_id, &events, 0).await?;

    expect_eq(
        store.read_history(&workflow_id).await?,
        events,
        "read_history should round-trip appended events in ascending sequence order",
    )
}

async fn multi_batch_append_advances_sequence(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    let first = workflow_started(1, &workflow_id, "checkout")?;
    let second_batch = vec![
        activity_scheduled(2, &workflow_id, "charge-card")?,
        workflow_completed(3, &workflow_id)?,
    ];

    store
        .append(&workflow_id, std::slice::from_ref(&first), 0)
        .await?;
    store.append(&workflow_id, &second_batch, 1).await?;

    let mut expected = vec![first];
    expected.extend(second_batch);
    expect_eq(
        store.read_history(&workflow_id).await?,
        expected,
        "append should accept the current head as the next expected sequence",
    )
}

async fn stale_expected_sequence_writes_nothing(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    let first = workflow_started(1, &workflow_id, "checkout")?;
    let rejected = vec![workflow_completed(2, &workflow_id)?];

    store
        .append(&workflow_id, std::slice::from_ref(&first), 0)
        .await?;
    let conflict = store.append(&workflow_id, &rejected, 0).await;

    expect_eq(
        conflict,
        Err(StoreError::SequenceConflict {
            expected: 0,
            found: 1,
        }),
        "stale expected_seq should report the observed workflow head",
    )?;
    expect_eq(
        store.read_history(&workflow_id).await?,
        vec![first],
        "SequenceConflict should leave history unchanged with no partial write",
    )
}

async fn concurrent_appends_on_same_expected_sequence_has_one_winner(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    let first_batch = vec![workflow_started(1, &workflow_id, "checkout")?];
    let second_batch = vec![workflow_started(1, &workflow_id, "billing")?];

    let first_append = store.append(&workflow_id, &first_batch, 0);
    let second_append = store.append(&workflow_id, &second_batch, 0);
    let (first_result, second_result) = futures::future::join(first_append, second_append).await;

    let success_count = [&first_result, &second_result]
        .into_iter()
        .filter(|result| result.is_ok())
        .count();
    expect_eq(
        success_count,
        1,
        "concurrent appends with the same expected_seq should have exactly one winner",
    )?;

    let conflict_count = [&first_result, &second_result]
        .into_iter()
        .filter(|result| {
            matches!(
                result,
                Err(StoreError::SequenceConflict {
                    expected: 0,
                    found: 1,
                })
            )
        })
        .count();
    expect_eq(
        conflict_count,
        1,
        "losing concurrent append should return SequenceConflict for the winning head",
    )?;

    let expected_history = if first_result.is_ok() {
        first_batch
    } else {
        second_batch
    };
    expect_eq(
        store.read_history(&workflow_id).await?,
        expected_history,
        "history should contain only the winning concurrent append batch",
    )
}

async fn large_history_round_trip(store: Arc<dyn EventStore>) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    let batch_size = 25_u64;
    let batch_count = 5_u64;
    let mut expected = Vec::new();

    for batch_index in 0..batch_count {
        let batch_start = batch_index * batch_size + 1;
        let mut batch = Vec::new();
        for seq in batch_start..batch_start + batch_size {
            let event = if seq == 1 {
                workflow_started(seq, &workflow_id, "large-history")?
            } else {
                activity_scheduled(seq, &workflow_id, &format!("activity-{seq}"))?
            };
            batch.push(event);
        }

        store
            .append(&workflow_id, &batch, batch_start.saturating_sub(1))
            .await?;
        expected.extend(batch);
    }

    let history = store.read_history(&workflow_id).await?;
    expect_eq(
        history.len(),
        expected.len(),
        "large history read should return the full appended event count",
    )?;
    expect_eq(
        event_sequences(&history),
        (1..=batch_size * batch_count).collect::<Vec<_>>(),
        "large history read should return events in ascending sequence order",
    )?;
    expect_eq(
        history,
        expected,
        "large history read should round-trip all appended batches",
    )
}

async fn list_active_reflects_projected_status(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    expect_empty(
        store.list_active().await?,
        "empty store should have no active workflows",
    )?;

    let running = workflow_id();
    let completing = workflow_id();
    let continuing = workflow_id();

    store
        .append(&running, &[workflow_started(1, &running, "checkout")?], 0)
        .await?;
    store
        .append(
            &completing,
            &[workflow_started(1, &completing, "billing")?],
            0,
        )
        .await?;
    store
        .append(
            &continuing,
            &[workflow_started(1, &continuing, "fulfillment")?],
            0,
        )
        .await?;

    expect_contains_exactly(
        store.list_active().await?,
        &[running.clone(), completing.clone(), continuing.clone()],
        "started workflows should be listed as active before terminal events",
    )?;

    store
        .append(&completing, &[workflow_completed(2, &completing)?], 1)
        .await?;
    store
        .append(
            &continuing,
            &[workflow_continued_as_new(2, &continuing)?],
            1,
        )
        .await?;

    expect_contains_exactly(
        store.list_active().await?,
        &[running],
        "terminal workflow lifecycle events, including continued-as-new, should remove workflows from active listing",
    )
}

async fn query_applies_all_filters(store: Arc<dyn EventStore>) -> Result<(), StoreError> {
    let running_checkout = workflow_id();
    let completed_checkout = workflow_id();
    let failed_billing = workflow_id();
    let parent = workflow_id();

    store
        .append(
            &running_checkout,
            &[workflow_started_at(1, &running_checkout, "checkout", 1)?],
            0,
        )
        .await?;
    store
        .append(
            &completed_checkout,
            &[
                workflow_started_at(1, &completed_checkout, "checkout", 10)?,
                workflow_completed_at(2, &completed_checkout, 11)?,
            ],
            0,
        )
        .await?;
    store
        .append(
            &failed_billing,
            &[
                workflow_started_at(1, &failed_billing, "billing", 20)?,
                workflow_failed_at(2, &failed_billing, 21)?,
            ],
            0,
        )
        .await?;

    let summaries = store.query(&WorkflowFilter::default()).await?;
    let expected_workflows = [
        running_checkout.clone(),
        completed_checkout.clone(),
        failed_billing,
    ];
    expect_summary_workflows(
        summaries,
        &expected_workflows,
        "default query filter should match every workflow with a start event",
    )?;

    let completed_checkout_filter = WorkflowFilter {
        workflow_type: Some(String::from("checkout")),
        status: Some(WorkflowStatus::Completed),
        started_after: Some(recorded_at(10)?),
        started_before: Some(recorded_at(10)?),
        parent: None,
    };
    let summaries = store.query(&completed_checkout_filter).await?;
    expect_eq(
        summaries.len(),
        1,
        "combined type/status/time-range query should require all filters to match inclusively",
    )?;
    let summary = summaries
        .first()
        .ok_or_else(|| contract_error("query should return the completed checkout summary"))?;
    expect_eq(
        summary.workflow_id.clone(),
        completed_checkout,
        "query should return the workflow matching all requested filters",
    )?;
    expect_eq(
        summary.workflow_type.clone(),
        String::from("checkout"),
        "query summary should preserve workflow type",
    )?;
    expect_eq(
        summary.status,
        WorkflowStatus::Completed,
        "query summary should project status from history",
    )?;
    expect_eq(
        summary.started_at,
        recorded_at(10)?,
        "query summary should use WorkflowStarted recorded_at as started_at",
    )?;
    expect_eq(
        summary.ended_at,
        Some(recorded_at(11)?),
        "query summary should use terminal event recorded_at as ended_at",
    )?;

    let parent_filter = WorkflowFilter {
        parent: Some(parent),
        ..WorkflowFilter::default()
    };
    expect_empty(
        store.query(&parent_filter).await?,
        "parent filters should return no workflows when no summary carries that parent",
    )
}

async fn continued_as_new_query_returns_correct_status_and_ended_at(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    store
        .append(
            &workflow_id,
            &[
                workflow_started_at(1, &workflow_id, "checkout", 10)?,
                workflow_continued_as_new_at(2, &workflow_id, 12)?,
            ],
            0,
        )
        .await?;

    let summaries = store.query(&WorkflowFilter::default()).await?;
    expect_eq(
        summaries.len(),
        1,
        "default query filter should return the continued-as-new workflow",
    )?;
    let summary = summaries
        .first()
        .ok_or_else(|| contract_error("query should return the continued-as-new summary"))?;
    expect_eq(
        summary.workflow_id.clone(),
        workflow_id.clone(),
        "default query filter should include the continued-as-new workflow id",
    )?;
    expect_eq(
        summary.status,
        WorkflowStatus::ContinuedAsNew,
        "query summary should project ContinuedAsNew status from WorkflowContinuedAsNew",
    )?;
    expect_eq(
        summary.ended_at,
        Some(recorded_at(12)?),
        "query summary ended_at should use the WorkflowContinuedAsNew recorded_at timestamp",
    )?;

    let continued_filter = WorkflowFilter {
        status: Some(WorkflowStatus::ContinuedAsNew),
        ..WorkflowFilter::default()
    };
    let summaries = store.query(&continued_filter).await?;
    expect_eq(
        summaries.len(),
        1,
        "ContinuedAsNew status query should match the continued-as-new workflow",
    )?;
    let summary = summaries.first().ok_or_else(|| {
        contract_error("status-filtered query should return the continued-as-new summary")
    })?;
    expect_eq(
        summary.workflow_id.clone(),
        workflow_id,
        "ContinuedAsNew status query should return the matching workflow id",
    )?;
    expect_eq(
        summary.status,
        WorkflowStatus::ContinuedAsNew,
        "ContinuedAsNew status query should preserve the projected status",
    )?;
    expect_eq(
        summary.ended_at,
        Some(recorded_at(12)?),
        "ContinuedAsNew status query should preserve the continued event timestamp as ended_at",
    )
}

async fn read_run_chain_orders_continuations(store: Arc<dyn EventStore>) -> Result<(), StoreError> {
    let unknown = workflow_id();
    expect_empty(
        store.read_run_chain(&unknown).await?,
        "unknown workflow run chain should be empty",
    )?;

    let one_continuation = workflow_id();
    let first_continuation_run = run_id(101);
    let second_continuation_run = run_id(102);
    store
        .append(
            &one_continuation,
            &[
                workflow_started_with_run(
                    1,
                    &one_continuation,
                    "checkout",
                    &first_continuation_run,
                    None,
                )?,
                workflow_continued_as_new_with_parent(
                    2,
                    &one_continuation,
                    &first_continuation_run,
                )?,
                workflow_started_with_run(
                    3,
                    &one_continuation,
                    "checkout-v2",
                    &second_continuation_run,
                    Some(first_continuation_run.clone()),
                )?,
            ],
            0,
        )
        .await?;
    expect_eq(
        store.read_run_chain(&one_continuation).await?,
        vec![
            RunSummary {
                run_id: first_continuation_run.clone(),
                parent_run_id: None,
                status: WorkflowStatus::ContinuedAsNew,
                started_at: recorded_at(1)?,
                closed_at: Some(recorded_at(2)?),
            },
            RunSummary {
                run_id: second_continuation_run,
                parent_run_id: Some(first_continuation_run),
                status: WorkflowStatus::Running,
                started_at: recorded_at(3)?,
                closed_at: None,
            },
        ],
        "read_run_chain should handle a workflow with exactly one continuation",
    )
}

async fn read_run_chain_single_and_multi_continuations(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let single = workflow_id();
    let single_run = run_id(1);
    store
        .append(
            &single,
            &[workflow_started_with_run(
                1,
                &single,
                "checkout",
                &single_run,
                None,
            )?],
            0,
        )
        .await?;
    expect_eq(
        store.read_run_chain(&single).await?,
        vec![RunSummary {
            run_id: single_run,
            parent_run_id: None,
            status: WorkflowStatus::Running,
            started_at: recorded_at(1)?,
            closed_at: None,
        }],
        "single-run workflow should return one running RunSummary",
    )?;

    let workflow = workflow_id();
    let first = run_id(11);
    let second = run_id(12);
    let third = run_id(13);
    store
        .append(
            &workflow,
            &[
                workflow_started_with_run(1, &workflow, "checkout", &first, None)?,
                workflow_continued_as_new_with_parent(2, &workflow, &first)?,
                workflow_started_with_run(
                    3,
                    &workflow,
                    "checkout-v2",
                    &second,
                    Some(first.clone()),
                )?,
                workflow_continued_as_new_with_parent(4, &workflow, &second)?,
                workflow_started_with_run(
                    5,
                    &workflow,
                    "checkout-v3",
                    &third,
                    Some(second.clone()),
                )?,
                workflow_completed(6, &workflow)?,
            ],
            0,
        )
        .await?;

    expect_eq(
        store.read_run_chain(&workflow).await?,
        vec![
            RunSummary {
                run_id: first.clone(),
                parent_run_id: None,
                status: WorkflowStatus::ContinuedAsNew,
                started_at: recorded_at(1)?,
                closed_at: Some(recorded_at(2)?),
            },
            RunSummary {
                run_id: second.clone(),
                parent_run_id: Some(first),
                status: WorkflowStatus::ContinuedAsNew,
                started_at: recorded_at(3)?,
                closed_at: Some(recorded_at(4)?),
            },
            RunSummary {
                run_id: third,
                parent_run_id: Some(second),
                status: WorkflowStatus::Completed,
                started_at: recorded_at(5)?,
                closed_at: Some(recorded_at(6)?),
            },
        ],
        "read_run_chain should follow parent_run_id links oldest to newest",
    )
}

async fn expired_timers_include_due_boundary_and_exclude_future(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    let past_timer = TimerId::anonymous(1);
    let boundary_timer = TimerId::anonymous(2);
    let future_timer = TimerId::anonymous(3);
    let as_of = recorded_at(20)?;

    store
        .schedule_timer(&workflow_id, &future_timer, recorded_at(30)?)
        .await?;
    store
        .schedule_timer(&workflow_id, &boundary_timer, as_of)
        .await?;
    store
        .schedule_timer(&workflow_id, &past_timer, recorded_at(10)?)
        .await?;

    expect_timer_entries(
        store.expired_timers(as_of).await?,
        &[
            TimerEntry {
                workflow_id: workflow_id.clone(),
                timer_id: past_timer,
                fire_at: recorded_at(10)?,
            },
            TimerEntry {
                workflow_id,
                timer_id: boundary_timer,
                fire_at: as_of,
            },
        ],
        "expired_timers should include fire_at <= as_of and exclude future timers",
    )
}

async fn rescheduling_same_timer_replaces_prior_fire_at(
    store: Arc<dyn EventStore>,
) -> Result<(), StoreError> {
    let workflow_id = workflow_id();
    let timer_id = TimerId::anonymous(1);
    let first_fire_at = recorded_at(10)?;
    let replacement_fire_at = recorded_at(30)?;

    store
        .schedule_timer(&workflow_id, &timer_id, first_fire_at)
        .await?;
    store
        .schedule_timer(&workflow_id, &timer_id, replacement_fire_at)
        .await?;

    expect_empty(
        store.expired_timers(first_fire_at).await?,
        "rescheduling the same timer should replace its earlier fire_at",
    )?;
    expect_timer_entries(
        store.expired_timers(replacement_fire_at).await?,
        &[TimerEntry {
            workflow_id,
            timer_id,
            fire_at: replacement_fire_at,
        }],
        "rescheduled timer should expire at the replacement fire_at",
    )
}

fn recorded_at(offset_seconds: i64) -> Result<DateTime<Utc>, StoreError> {
    DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0)
        .ok_or_else(|| contract_error("test timestamp should be representable"))
}

fn workflow_id() -> WorkflowId {
    static NEXT_WORKFLOW_ID: AtomicU64 = AtomicU64::new(1);

    WorkflowId::new(uuid::Uuid::from_u128(u128::from(
        NEXT_WORKFLOW_ID.fetch_add(1, Ordering::Relaxed),
    )))
}

fn run_id(value: u128) -> RunId {
    RunId::new(uuid::Uuid::from_u128(value))
}

fn envelope(seq: u64, workflow_id: &WorkflowId) -> Result<EventEnvelope, StoreError> {
    let offset = i64::try_from(seq).map_err(|error| {
        StoreError::Backend(format!("event sequence out of timestamp range: {error}"))
    })?;
    envelope_at(seq, workflow_id, offset)
}

fn envelope_at(
    seq: u64,
    workflow_id: &WorkflowId,
    offset_seconds: i64,
) -> Result<EventEnvelope, StoreError> {
    Ok(EventEnvelope {
        seq,
        recorded_at: recorded_at(offset_seconds)?,
        workflow_id: workflow_id.clone(),
    })
}

fn event_sequences(events: &[Event]) -> Vec<u64> {
    events.iter().map(Event::seq).collect()
}

fn payload(label: &str) -> Result<Payload, StoreError> {
    Payload::from_json(&serde_json::json!({ "label": label }))
        .map_err(|error| StoreError::Serialization(error.to_string()))
}

fn workflow_started(
    seq: u64,
    workflow_id: &WorkflowId,
    workflow_type: &str,
) -> Result<Event, StoreError> {
    workflow_started_with_run(
        seq,
        workflow_id,
        workflow_type,
        &run_id(u128::from(seq)),
        None,
    )
}

fn workflow_started_with_run(
    seq: u64,
    workflow_id: &WorkflowId,
    workflow_type: &str,
    run_id: &RunId,
    parent_run_id: Option<RunId>,
) -> Result<Event, StoreError> {
    Ok(Event::WorkflowStarted {
        envelope: envelope(seq, workflow_id)?,
        workflow_type: workflow_type.to_owned(),
        input: payload("input")?,
        run_id: run_id.clone(),
        parent_run_id,
    })
}

fn workflow_started_at(
    seq: u64,
    workflow_id: &WorkflowId,
    workflow_type: &str,
    offset_seconds: i64,
) -> Result<Event, StoreError> {
    Ok(Event::WorkflowStarted {
        envelope: envelope_at(seq, workflow_id, offset_seconds)?,
        workflow_type: workflow_type.to_owned(),
        input: payload("input")?,
        run_id: run_id(u128::from(seq)),
        parent_run_id: None,
    })
}

fn workflow_completed(seq: u64, workflow_id: &WorkflowId) -> Result<Event, StoreError> {
    workflow_completed_at(
        seq,
        workflow_id,
        i64::try_from(seq).map_err(|error| {
            StoreError::Backend(format!("event sequence out of timestamp range: {error}"))
        })?,
    )
}

fn workflow_completed_at(
    seq: u64,
    workflow_id: &WorkflowId,
    offset_seconds: i64,
) -> Result<Event, StoreError> {
    Ok(Event::WorkflowCompleted {
        envelope: envelope_at(seq, workflow_id, offset_seconds)?,
        result: payload("result")?,
    })
}

fn workflow_failed_at(
    seq: u64,
    workflow_id: &WorkflowId,
    offset_seconds: i64,
) -> Result<Event, StoreError> {
    Ok(Event::WorkflowFailed {
        envelope: envelope_at(seq, workflow_id, offset_seconds)?,
        error: WorkflowError {
            message: String::from("failed"),
            details: None,
        },
    })
}

fn workflow_continued_as_new(seq: u64, workflow_id: &WorkflowId) -> Result<Event, StoreError> {
    workflow_continued_as_new_with_parent(seq, workflow_id, &run_id(42))
}

fn workflow_continued_as_new_with_parent(
    seq: u64,
    workflow_id: &WorkflowId,
    parent_run_id: &RunId,
) -> Result<Event, StoreError> {
    workflow_continued_as_new_with_parent_at(seq, workflow_id, parent_run_id, i64::try_from(seq).map_err(|error| {
        StoreError::Backend(format!("event sequence out of timestamp range: {error}"))
    })?)
}

fn workflow_continued_as_new_at(
    seq: u64,
    workflow_id: &WorkflowId,
    offset_seconds: i64,
) -> Result<Event, StoreError> {
    workflow_continued_as_new_with_parent_at(seq, workflow_id, &run_id(42), offset_seconds)
}

fn workflow_continued_as_new_with_parent_at(
    seq: u64,
    workflow_id: &WorkflowId,
    parent_run_id: &RunId,
    offset_seconds: i64,
) -> Result<Event, StoreError> {
    Ok(Event::WorkflowContinuedAsNew {
        envelope: envelope_at(seq, workflow_id, offset_seconds)?,
        input: payload("continued-input")?,
        workflow_type: Some(String::from("continued-workflow")),
        parent_run_id: parent_run_id.clone(),
    })
}

fn activity_scheduled(
    seq: u64,
    workflow_id: &WorkflowId,
    activity_type: &str,
) -> Result<Event, StoreError> {
    Ok(Event::ActivityScheduled {
        envelope: envelope(seq, workflow_id)?,
        activity_id: aion_core::ActivityId::from_sequence_position(seq),
        activity_type: activity_type.to_owned(),
        input: payload("activity-input")?,
    })
}

fn timer_started(
    seq: u64,
    workflow_id: &WorkflowId,
    timer_id: TimerId,
    fire_at: DateTime<Utc>,
) -> Result<Event, StoreError> {
    Ok(Event::TimerStarted {
        envelope: envelope(seq, workflow_id)?,
        timer_id,
        fire_at,
    })
}

fn signal_received(seq: u64, workflow_id: &WorkflowId, name: &str) -> Result<Event, StoreError> {
    Ok(Event::SignalReceived {
        envelope: envelope(seq, workflow_id)?,
        name: name.to_owned(),
        payload: payload("signal")?,
    })
}

fn expect_empty<T>(items: Vec<T>, message: &str) -> Result<(), StoreError> {
    let len = items.len();
    drop(items);

    if len == 0 {
        Ok(())
    } else {
        Err(contract_error(&format!("{message} (got {len} items)")))
    }
}

fn expect_eq<T>(actual: T, expected: T, message: &str) -> Result<(), StoreError>
where
    T: PartialEq + std::fmt::Debug,
{
    if actual == expected {
        drop((actual, expected));
        Ok(())
    } else {
        Err(contract_error(&format!(
            "{message}\n  expected: {expected:?}\n  actual: {actual:?}"
        )))
    }
}

fn expect_contains_exactly(
    mut actual: Vec<WorkflowId>,
    expected: &[WorkflowId],
    message: &str,
) -> Result<(), StoreError> {
    actual.sort_by_key(ToString::to_string);
    let mut expected = expected.to_vec();
    expected.sort_by_key(ToString::to_string);
    expect_eq(actual, expected, message)
}

fn expect_summary_workflows(
    actual: Vec<WorkflowSummary>,
    expected: &[WorkflowId],
    message: &str,
) -> Result<(), StoreError> {
    let actual = actual
        .into_iter()
        .map(|summary| summary.workflow_id)
        .collect::<Vec<_>>();
    expect_contains_exactly(actual, expected, message)
}

fn expect_timer_entries(
    mut actual: Vec<TimerEntry>,
    expected: &[TimerEntry],
    message: &str,
) -> Result<(), StoreError> {
    actual.sort_by(timer_entry_order);
    let mut expected = expected.to_vec();
    expected.sort_by(timer_entry_order);
    expect_eq(actual, expected, message)
}

fn timer_entry_order(left: &TimerEntry, right: &TimerEntry) -> std::cmp::Ordering {
    left.fire_at
        .cmp(&right.fire_at)
        .then_with(|| {
            left.workflow_id
                .to_string()
                .cmp(&right.workflow_id.to_string())
        })
        .then_with(|| left.timer_id.to_string().cmp(&right.timer_id.to_string()))
}

fn contract_error(message: &str) -> StoreError {
    StoreError::Backend(format!("event store conformance failure: {message}"))
}
