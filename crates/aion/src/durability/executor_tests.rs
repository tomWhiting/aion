use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use aion_core::{
    ActivityError, ActivityErrorKind, ActivityId, Event, EventEnvelope, Payload, TimerId,
    WorkflowId,
};
use aion_store::{EventStore, InMemoryStore, ReadableEventStore};
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;

use super::{
    HandoffOutcome, LiveActivityOutcome, LiveChildOutcome, LiveExecutor, resolve_or_execute_live,
};
use crate::durability::{
    Command, CorrelationKey, DurabilityError, HistoryCursor, Recorder, Resolution, Resolver,
};

fn workflow_id() -> WorkflowId {
    WorkflowId::new(Uuid::nil())
}

fn child_workflow_id() -> WorkflowId {
    WorkflowId::new(Uuid::from_u128(1))
}

fn timestamp(offset_seconds: i64) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    Utc.timestamp_opt(offset_seconds, 0)
        .single()
        .ok_or_else(|| "invalid timestamp".into())
}

fn envelope(seq: u64) -> Result<EventEnvelope, Box<dyn std::error::Error>> {
    Ok(EventEnvelope {
        seq,
        recorded_at: timestamp(0)?,
        workflow_id: workflow_id(),
    })
}

fn payload(label: &str) -> Result<Payload, Box<dyn std::error::Error>> {
    Ok(Payload::from_json(&json!({ "label": label }))?)
}

fn activity_scheduled(seq: u64, ordinal: u64) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::ActivityScheduled {
        envelope: envelope(seq)?,
        activity_id: ActivityId::from_sequence_position(ordinal),
        activity_type: "activity".to_owned(),
        input: payload("activity-input")?,
    })
}

fn activity_completed(
    seq: u64,
    ordinal: u64,
    result: Payload,
) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::ActivityCompleted {
        envelope: envelope(seq)?,
        activity_id: ActivityId::from_sequence_position(ordinal),
        result,
    })
}

fn timer_started(seq: u64, timer_id: TimerId) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::TimerStarted {
        envelope: envelope(seq)?,
        timer_id,
        fire_at: timestamp(10)?,
    })
}

fn timer_fired(seq: u64, timer_id: TimerId) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::TimerFired {
        envelope: envelope(seq)?,
        timer_id,
    })
}

fn signal_received(
    seq: u64,
    name: &str,
    payload: Payload,
) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::SignalReceived {
        envelope: envelope(seq)?,
        name: name.to_owned(),
        payload,
    })
}

fn child_started(seq: u64) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::ChildWorkflowStarted {
        envelope: envelope(seq)?,
        child_workflow_id: child_workflow_id(),
        workflow_type: "child".to_owned(),
        input: payload("child-input")?,
    })
}

fn child_completed(seq: u64, result: Payload) -> Result<Event, Box<dyn std::error::Error>> {
    Ok(Event::ChildWorkflowCompleted {
        envelope: envelope(seq)?,
        child_workflow_id: child_workflow_id(),
        result,
    })
}

fn run_activity_command(ordinal: u64) -> Result<Command, Box<dyn std::error::Error>> {
    Ok(Command::RunActivity {
        key: CorrelationKey::Activity(ordinal),
        activity_type: "activity".to_owned(),
        input: payload("activity-input")?,
    })
}

fn resolver_for(events: Vec<Event>) -> Result<Resolver, DurabilityError> {
    Ok(Resolver::new(workflow_id(), HistoryCursor::new(events)?))
}

struct FailingExecutor;

#[async_trait::async_trait]
impl LiveExecutor for FailingExecutor {
    async fn run_activity(
        &self,
        activity_type: String,
        input: Payload,
    ) -> Result<LiveActivityOutcome, DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: format!(
                "live activity must not run during recorded replay: {activity_type} {input:?}"
            ),
        })
    }

    async fn start_timer(
        &self,
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: format!("live timer must not run during recorded replay: {timer_id} {fire_at}"),
        })
    }

    async fn await_signal(&self, name: String, index: usize) -> Result<Payload, DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: format!("live signal must not run during recorded replay: {name} {index}"),
        })
    }

    async fn spawn_child(
        &self,
        workflow_type: String,
        input: Payload,
    ) -> Result<LiveChildOutcome, DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: format!(
                "live child must not run during recorded replay: {workflow_type} {input:?}"
            ),
        })
    }
}

struct RecordingExecutor {
    activity_calls: AtomicUsize,
    activity_result: Payload,
    requests: Mutex<Vec<String>>,
}

impl RecordingExecutor {
    fn new(activity_result: Payload) -> Self {
        Self {
            activity_calls: AtomicUsize::new(0),
            activity_result,
            requests: Mutex::new(Vec::new()),
        }
    }

    fn activity_calls(&self) -> usize {
        self.activity_calls.load(Ordering::SeqCst)
    }

    fn record_request(&self, request: String) -> Result<(), DurabilityError> {
        let mut requests = self
            .requests
            .lock()
            .map_err(|error| DurabilityError::HistoryShape {
                reason: format!("recording executor request lock poisoned: {error}"),
            })?;
        requests.push(request);
        Ok(())
    }
}

#[async_trait::async_trait]
impl LiveExecutor for RecordingExecutor {
    async fn run_activity(
        &self,
        activity_type: String,
        input: Payload,
    ) -> Result<LiveActivityOutcome, DurabilityError> {
        self.record_request(format!("activity:{activity_type}:{input:?}"))?;
        self.activity_calls.fetch_add(1, Ordering::SeqCst);
        Ok(LiveActivityOutcome::Completed(self.activity_result.clone()))
    }

    async fn start_timer(
        &self,
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), DurabilityError> {
        self.record_request(format!("timer:{timer_id}:{fire_at}"))?;
        Ok(())
    }

    async fn await_signal(&self, name: String, index: usize) -> Result<Payload, DurabilityError> {
        self.record_request(format!("signal:{name}:{index}"))?;
        payload("signal-live").map_err(|error| DurabilityError::HistoryShape {
            reason: error.to_string(),
        })
    }

    async fn spawn_child(
        &self,
        workflow_type: String,
        input: Payload,
    ) -> Result<LiveChildOutcome, DurabilityError> {
        self.record_request(format!("child:{workflow_type}:{input:?}"))?;
        Ok(LiveChildOutcome::Completed {
            child_workflow_id: child_workflow_id(),
            result: payload("child-live").map_err(|error| DurabilityError::HistoryShape {
                reason: error.to_string(),
            })?,
        })
    }
}

struct RetryableFailureExecutor;

#[async_trait::async_trait]
impl LiveExecutor for RetryableFailureExecutor {
    async fn run_activity(
        &self,
        _activity_type: String,
        _input: Payload,
    ) -> Result<LiveActivityOutcome, DurabilityError> {
        Ok(LiveActivityOutcome::Failed(ActivityError {
            kind: ActivityErrorKind::Retryable,
            message: "retryable live failure must not be recorded as terminal".to_owned(),
            details: None,
        }))
    }

    async fn start_timer(
        &self,
        _timer_id: TimerId,
        _fire_at: DateTime<Utc>,
    ) -> Result<(), DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: "retryable failure executor does not support timers".to_owned(),
        })
    }

    async fn await_signal(&self, _name: String, _index: usize) -> Result<Payload, DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: "retryable failure executor does not support signals".to_owned(),
        })
    }

    async fn spawn_child(
        &self,
        _workflow_type: String,
        _input: Payload,
    ) -> Result<LiveChildOutcome, DurabilityError> {
        Err(DurabilityError::HistoryShape {
            reason: "retryable failure executor does not support children".to_owned(),
        })
    }
}

#[test]
fn live_executor_is_object_safe() {
    let _: Option<Arc<dyn LiveExecutor>> = None;
}

#[tokio::test]
async fn recorded_history_returns_resolutions_without_live_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let activity_result = payload("activity-result")?;
    let signal_payload = payload("signal-payload")?;
    let child_result = payload("child-result")?;
    let timer_id = TimerId::anonymous(9);
    let mut resolver = resolver_for(vec![
        activity_scheduled(1, 0)?,
        activity_completed(2, 0, activity_result.clone())?,
        timer_started(3, timer_id.clone())?,
        timer_fired(4, timer_id.clone())?,
        signal_received(5, "ready", signal_payload.clone())?,
        child_started(6)?,
        child_completed(7, child_result.clone())?,
    ])?;
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let mut recorder = Recorder::new(workflow_id(), store);
    let executor = FailingExecutor;

    assert_eq!(
        resolve_or_execute_live(
            &mut resolver,
            &mut recorder,
            &executor,
            run_activity_command(0)?,
            timestamp(20)?,
        )
        .await?,
        HandoffOutcome::Resolved(Resolution::ActivityCompleted(activity_result))
    );
    assert_eq!(
        resolve_or_execute_live(
            &mut resolver,
            &mut recorder,
            &executor,
            Command::StartTimer {
                key: CorrelationKey::Timer(timer_id),
                fire_at: timestamp(10)?,
            },
            timestamp(20)?,
        )
        .await?,
        HandoffOutcome::Resolved(Resolution::TimerFired)
    );
    assert_eq!(
        resolve_or_execute_live(
            &mut resolver,
            &mut recorder,
            &executor,
            Command::AwaitSignal {
                key: CorrelationKey::Signal {
                    name: "ready".to_owned(),
                    index: 0,
                },
            },
            timestamp(20)?,
        )
        .await?,
        HandoffOutcome::Resolved(Resolution::SignalDelivered(signal_payload))
    );
    assert_eq!(
        resolve_or_execute_live(
            &mut resolver,
            &mut recorder,
            &executor,
            Command::SpawnChild {
                key: CorrelationKey::Child(6),
                workflow_type: "child".to_owned(),
                input: payload("child-input")?,
            },
            timestamp(20)?,
        )
        .await?,
        HandoffOutcome::Resolved(Resolution::ChildStarted(child_workflow_id()))
    );
    assert_eq!(
        resolve_or_execute_live(
            &mut resolver,
            &mut recorder,
            &executor,
            Command::AwaitChild {
                child_workflow_id: child_workflow_id(),
            },
            timestamp(20)?,
        )
        .await?,
        HandoffOutcome::Resolved(Resolution::ChildCompleted(child_result))
    );
    Ok(())
}

#[tokio::test]
async fn resume_live_activity_rejects_retryable_failure_without_recording_terminal_outcome()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::default());
    let workflow_id = workflow_id();
    let store_for_recorder: Arc<dyn EventStore> = store.clone();
    let mut resolver = resolver_for(Vec::new())?;
    let mut recorder = Recorder::new(workflow_id.clone(), store_for_recorder);
    let executor = RetryableFailureExecutor;

    let error = resolve_or_execute_live(
        &mut resolver,
        &mut recorder,
        &executor,
        run_activity_command(0)?,
        timestamp(50)?,
    )
    .await
    .err()
    .ok_or_else(|| "retryable failure was unexpectedly accepted".to_owned())?;

    assert!(matches!(error, DurabilityError::HistoryShape { .. }));
    let history = store.read_history(&workflow_id).await?;
    assert_eq!(history.len(), 2);
    assert!(matches!(history[0], Event::ActivityScheduled { .. }));
    assert!(matches!(history[1], Event::ActivityStarted { .. }));
    assert!(
        history
            .iter()
            .all(|event| !matches!(event, Event::ActivityFailed { .. }))
    );
    Ok(())
}

#[tokio::test]
async fn resume_live_activity_records_result_and_replay_uses_cache()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::default());
    let store_for_recorder: Arc<dyn EventStore> = store.clone();
    let workflow_id = workflow_id();
    let live_result = payload("live-activity-result")?;
    let executor = RecordingExecutor::new(live_result.clone());
    let mut resolver = resolver_for(Vec::new())?;
    let mut recorder = Recorder::new(workflow_id.clone(), store_for_recorder);

    let outcome = resolve_or_execute_live(
        &mut resolver,
        &mut recorder,
        &executor,
        run_activity_command(0)?,
        timestamp(30)?,
    )
    .await?;

    assert_eq!(executor.activity_calls(), 1);
    assert_eq!(
        outcome,
        HandoffOutcome::Resolved(Resolution::ActivityCompleted(live_result.clone()))
    );
    let history = store.read_history(&workflow_id).await?;
    assert_eq!(history.len(), 3);
    assert!(matches!(history[0], Event::ActivityScheduled { .. }));
    assert!(matches!(history[1], Event::ActivityStarted { .. }));
    match &history[2] {
        Event::ActivityCompleted {
            envelope,
            activity_id,
            result,
        } => {
            assert_eq!(envelope.seq, 3);
            assert_eq!(activity_id.sequence_position(), 0);
            assert_eq!(result, &live_result);
        }
        other => {
            return Err(format!("expected ActivityCompleted, got {other:?}").into());
        }
    }

    let mut replay_resolver = Resolver::new(workflow_id.clone(), HistoryCursor::new(history)?);
    let store_for_replay_recorder: Arc<dyn EventStore> = store.clone();
    let mut replay_recorder = Recorder::resume_at(workflow_id, store_for_replay_recorder, 3);
    let replay_outcome = resolve_or_execute_live(
        &mut replay_resolver,
        &mut replay_recorder,
        &executor,
        run_activity_command(0)?,
        timestamp(40)?,
    )
    .await?;

    assert_eq!(executor.activity_calls(), 1);
    assert_eq!(
        replay_outcome,
        HandoffOutcome::Resolved(Resolution::ActivityCompleted(live_result))
    );
    Ok(())
}
