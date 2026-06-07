//! Behavioural replay tests over the in-memory event store.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use aion::durability::{
    Command, CorrelationKey, DurabilityError, LiveActivityOutcome, LiveChildOutcome, LiveExecutor,
    Recorder, RecoveryDriver, RecoveryOutcome, RecoveryPlan, Replay, ReplayOutcome, ReplayStep,
    ReplayTerminal, Resolution, recover,
};
use aion_core::{
    ActivityError, ActivityErrorKind, ActivityId, Event, Payload, RunId, TimerId, WorkflowId,
};
use aion_store::{EventStore, InMemoryStore};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde_json::json;
use uuid::Uuid;

fn workflow_id() -> WorkflowId {
    WorkflowId::new(Uuid::from_u128(0x1111))
}

fn child_workflow_id() -> WorkflowId {
    WorkflowId::new(Uuid::from_u128(0x2222))
}

fn run_id() -> RunId {
    RunId::new(Uuid::from_u128(0x3333))
}

fn different_run_id() -> RunId {
    RunId::new(Uuid::from_u128(0x4444))
}

fn timestamp(seconds: i64) -> Result<DateTime<Utc>, Box<dyn std::error::Error>> {
    Utc.timestamp_opt(seconds, 0)
        .single()
        .ok_or_else(|| format!("invalid timestamp {seconds}").into())
}

fn payload(label: &str) -> Result<Payload, Box<dyn std::error::Error>> {
    Ok(Payload::from_json(&json!({ "label": label }))?)
}

fn activity_command(ordinal: u64) -> Result<Command, Box<dyn std::error::Error>> {
    Ok(Command::RunActivity {
        key: CorrelationKey::Activity(ordinal),
        activity_type: "activity".to_owned(),
        input: payload("activity-input")?,
    })
}

fn timer_command(timer_id: TimerId) -> Result<Command, Box<dyn std::error::Error>> {
    Ok(Command::StartTimer {
        key: CorrelationKey::Timer(timer_id),
        fire_at: timestamp(100)?,
    })
}

fn signal_command(name: &str, index: usize) -> Command {
    Command::AwaitSignal {
        key: CorrelationKey::Signal {
            name: name.to_owned(),
            index,
        },
    }
}

fn child_command(child_start_seq: u64) -> Result<Command, Box<dyn std::error::Error>> {
    Ok(Command::SpawnChild {
        key: CorrelationKey::Child(child_start_seq),
        workflow_type: "child".to_owned(),
        input: payload("child-input")?,
    })
}

async fn record_full_history(
    store: Arc<dyn EventStore>,
) -> Result<Vec<aion_core::Event>, Box<dyn std::error::Error>> {
    let mut recorder = Recorder::new(workflow_id(), Arc::clone(&store));
    let activity_id = ActivityId::from_sequence_position(0);
    let timer_id = TimerId::anonymous(4);

    recorder
        .record_workflow_started(
            timestamp(10)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(20)?,
            activity_id.clone(),
            "activity".to_owned(),
            payload("activity-input")?,
        )
        .await?;
    recorder
        .record_activity_completed(timestamp(30)?, activity_id, payload("activity-result")?)
        .await?;
    recorder
        .record_timer_started(timestamp(40)?, timer_id.clone(), timestamp(100)?)
        .await?;
    recorder
        .record_timer_fired(timestamp(50)?, timer_id)
        .await?;
    recorder
        .record_signal_received(
            timestamp(60)?,
            "ready".to_owned(),
            payload("signal-payload")?,
        )
        .await?;
    recorder
        .record_child_workflow_started(
            timestamp(70)?,
            child_workflow_id(),
            "child".to_owned(),
            payload("child-input")?,
        )
        .await?;
    recorder
        .record_child_workflow_completed(
            timestamp(80)?,
            child_workflow_id(),
            payload("child-result")?,
        )
        .await?;
    recorder
        .record_workflow_completed(timestamp(90)?, payload("workflow-result")?)
        .await?;

    Ok(store.read_history(&workflow_id()).await?)
}

async fn record_partial_history_for(
    store: Arc<dyn EventStore>,
    workflow_id: WorkflowId,
    timestamp_base: i64,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut recorder = Recorder::new(workflow_id, store);
    let activity_id = ActivityId::from_sequence_position(0);

    recorder
        .record_workflow_started(
            timestamp(timestamp_base)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(timestamp_base + 10)?,
            activity_id.clone(),
            "activity".to_owned(),
            payload("activity-input")?,
        )
        .await?;
    recorder
        .record_activity_completed(
            timestamp(timestamp_base + 20)?,
            activity_id,
            payload("activity-result")?,
        )
        .await?;
    Ok(())
}

async fn record_terminal_history_for(
    store: Arc<dyn EventStore>,
    workflow_id: WorkflowId,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut recorder = Recorder::new(workflow_id, store);
    recorder
        .record_workflow_started(
            timestamp(200)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_workflow_completed(timestamp(210)?, payload("workflow-result")?)
        .await?;
    Ok(())
}

async fn record_divergent_history_for(
    store: Arc<dyn EventStore>,
    workflow_id: WorkflowId,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut recorder = Recorder::new(workflow_id, store);
    recorder
        .record_workflow_started(
            timestamp(300)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_timer_started(timestamp(310)?, TimerId::anonymous(99), timestamp(400)?)
        .await?;
    Ok(())
}

#[derive(Default)]
struct StaticRecoveryDriver {
    plans: HashMap<WorkflowId, RecoveryPlan>,
}

impl StaticRecoveryDriver {
    fn insert(
        &mut self,
        workflow_id: WorkflowId,
        commands: Vec<Command>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.plans.insert(
            workflow_id,
            RecoveryPlan {
                run_id: run_id(),
                commands,
                failure_recorded_at: timestamp(900)?,
            },
        );
        Ok(())
    }
}

impl RecoveryDriver for StaticRecoveryDriver {
    fn recovery_plan(
        &self,
        workflow_id: &WorkflowId,
        history: &[Event],
    ) -> Result<RecoveryPlan, DurabilityError> {
        if history.is_empty() {
            return Err(DurabilityError::HistoryShape {
                reason: format!("test recovery driver saw empty history for {workflow_id}"),
            });
        }
        self.plans
            .get(workflow_id)
            .cloned()
            .ok_or_else(|| DurabilityError::HistoryShape {
                reason: format!("missing test recovery plan for {workflow_id}"),
            })
    }
}

async fn record_round_trip_history(
    store: Arc<dyn EventStore>,
) -> Result<Vec<aion_core::Event>, Box<dyn std::error::Error>> {
    let mut recorder = Recorder::new(workflow_id(), Arc::clone(&store));
    let first_activity_id = ActivityId::from_sequence_position(0);
    let second_activity_id = ActivityId::from_sequence_position(1);
    let timer_id = TimerId::anonymous(4);

    recorder
        .record_workflow_started(
            timestamp(10)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(20)?,
            first_activity_id.clone(),
            "activity".to_owned(),
            payload("first-activity-input")?,
        )
        .await?;
    recorder
        .record_activity_completed(
            timestamp(30)?,
            first_activity_id,
            payload("first-activity-result")?,
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(40)?,
            second_activity_id.clone(),
            "activity".to_owned(),
            payload("second-activity-input")?,
        )
        .await?;
    recorder
        .record_activity_completed(
            timestamp(50)?,
            second_activity_id,
            payload("second-activity-result")?,
        )
        .await?;
    recorder
        .record_timer_started(timestamp(60)?, timer_id.clone(), timestamp(100)?)
        .await?;
    recorder
        .record_timer_fired(timestamp(70)?, timer_id)
        .await?;
    recorder
        .record_signal_received(
            timestamp(80)?,
            "ready".to_owned(),
            payload("signal-payload")?,
        )
        .await?;
    recorder
        .record_workflow_completed(timestamp(90)?, payload("workflow-result")?)
        .await?;

    Ok(store.read_history(&workflow_id()).await?)
}

fn assert_round_trip_history_shape(
    history: &[Event],
) -> Result<Vec<DateTime<Utc>>, Box<dyn std::error::Error>> {
    assert_eq!(history.len(), 9);
    let recorded_timestamps = history
        .iter()
        .map(|event| *event.recorded_at())
        .collect::<Vec<_>>();
    assert_eq!(
        recorded_timestamps,
        vec![
            timestamp(10)?,
            timestamp(20)?,
            timestamp(30)?,
            timestamp(40)?,
            timestamp(50)?,
            timestamp(60)?,
            timestamp(70)?,
            timestamp(80)?,
            timestamp(90)?,
        ]
    );

    assert!(matches!(history[0], Event::WorkflowStarted { .. }));
    assert!(matches!(history[1], Event::ActivityScheduled { .. }));
    assert!(matches!(history[2], Event::ActivityCompleted { .. }));
    assert!(matches!(history[3], Event::ActivityScheduled { .. }));
    assert!(matches!(history[4], Event::ActivityCompleted { .. }));
    assert!(matches!(history[5], Event::TimerStarted { .. }));
    assert!(matches!(history[6], Event::TimerFired { .. }));
    assert!(matches!(history[7], Event::SignalReceived { .. }));
    assert!(matches!(history[8], Event::WorkflowCompleted { .. }));

    Ok(recorded_timestamps)
}

async fn record_partial_history(
    store: Arc<dyn EventStore>,
) -> Result<Vec<aion_core::Event>, Box<dyn std::error::Error>> {
    let mut recorder = Recorder::new(workflow_id(), Arc::clone(&store));
    let activity_id = ActivityId::from_sequence_position(0);

    recorder
        .record_workflow_started(
            timestamp(10)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(20)?,
            activity_id.clone(),
            "activity".to_owned(),
            payload("activity-input")?,
        )
        .await?;
    recorder
        .record_activity_completed(timestamp(30)?, activity_id, payload("activity-result")?)
        .await?;

    Ok(store.read_history(&workflow_id()).await?)
}

#[derive(Default)]
struct CountingExecutor {
    calls: AtomicUsize,
}

impl CountingExecutor {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }

    fn count_call(&self) {
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl LiveExecutor for CountingExecutor {
    async fn run_activity(
        &self,
        activity_type: String,
        input: Payload,
    ) -> Result<LiveActivityOutcome, DurabilityError> {
        self.count_call();
        drop((activity_type, input));
        Err(DurabilityError::HistoryShape {
            reason: "replay must not invoke live activity execution".to_owned(),
        })
    }

    async fn start_timer(
        &self,
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), DurabilityError> {
        self.count_call();
        drop((timer_id, fire_at));
        Err(DurabilityError::HistoryShape {
            reason: "replay must not invoke live timer execution".to_owned(),
        })
    }

    async fn await_signal(&self, name: String, index: usize) -> Result<Payload, DurabilityError> {
        self.count_call();
        drop((name, index));
        Err(DurabilityError::HistoryShape {
            reason: "replay must not invoke live signal execution".to_owned(),
        })
    }

    async fn spawn_child(
        &self,
        workflow_type: String,
        input: Payload,
    ) -> Result<LiveChildOutcome, DurabilityError> {
        self.count_call();
        drop((workflow_type, input));
        Err(DurabilityError::HistoryShape {
            reason: "replay must not invoke live child execution".to_owned(),
        })
    }
}

#[tokio::test]
async fn fully_recorded_history_replays_to_terminal_with_zero_live_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let history = record_full_history(store).await?;
    let workflow_id = workflow_id();
    let run_id = run_id();
    let mut replay = Replay::new(&workflow_id, &run_id, history)?;
    let executor = CountingExecutor::default();

    let commands = vec![
        activity_command(0)?,
        timer_command(TimerId::anonymous(4))?,
        signal_command("ready", 0),
        child_command(7)?,
    ];
    let outcome = replay.drive(commands)?;

    assert_eq!(
        outcome,
        ReplayOutcome::Terminal {
            terminal: ReplayTerminal::Completed(payload("workflow-result")?),
            recorded: vec![
                Resolution::ActivityCompleted(payload("activity-result")?),
                Resolution::TimerFired,
                Resolution::SignalDelivered(payload("signal-payload")?),
                Resolution::ChildCompleted(payload("child-result")?),
            ],
        }
    );
    assert_eq!(executor.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn recover_realistic_multi_event_workflow_to_resume_live()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let workflow_id = workflow_id();
    let mut recorder = Recorder::new(workflow_id.clone(), Arc::clone(&store));
    let activity_id = ActivityId::from_sequence_position(0);
    let timer_id = TimerId::anonymous(4);
    let resume_command = activity_command(1)?;

    recorder
        .record_workflow_started(
            timestamp(10)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(20)?,
            activity_id.clone(),
            "activity".to_owned(),
            payload("activity-input")?,
        )
        .await?;
    recorder
        .record_activity_completed(timestamp(30)?, activity_id, payload("activity-result")?)
        .await?;
    recorder
        .record_signal_received(
            timestamp(40)?,
            "ready".to_owned(),
            payload("signal-payload")?,
        )
        .await?;
    recorder
        .record_timer_started(timestamp(50)?, timer_id.clone(), timestamp(100)?)
        .await?;
    recorder
        .record_timer_fired(timestamp(60)?, timer_id.clone())
        .await?;
    recorder
        .record_child_workflow_started(
            timestamp(70)?,
            child_workflow_id(),
            "child".to_owned(),
            payload("child-input")?,
        )
        .await?;
    recorder
        .record_child_workflow_completed(
            timestamp(80)?,
            child_workflow_id(),
            payload("child-result")?,
        )
        .await?;

    let history = store.read_history(&workflow_id).await?;
    assert_eq!(history.len(), 8);
    assert!(matches!(history[0], Event::WorkflowStarted { .. }));
    assert!(matches!(history[1], Event::ActivityScheduled { .. }));
    assert!(matches!(history[2], Event::ActivityCompleted { .. }));
    assert!(matches!(history[3], Event::SignalReceived { .. }));
    assert!(matches!(history[4], Event::TimerStarted { .. }));
    assert!(matches!(history[5], Event::TimerFired { .. }));
    assert!(matches!(history[6], Event::ChildWorkflowStarted { .. }));
    assert!(matches!(history[7], Event::ChildWorkflowCompleted { .. }));
    assert!(!history.iter().any(|event| {
        matches!(
            event,
            Event::WorkflowCompleted { .. }
                | Event::WorkflowFailed { .. }
                | Event::WorkflowCancelled { .. }
                | Event::WorkflowTimedOut { .. }
                | Event::WorkflowContinuedAsNew { .. }
        )
    }));

    let active = store.list_active().await?;
    assert_eq!(active, vec![workflow_id.clone()]);

    let child_start_seq = history[6].seq();
    let commands = vec![
        activity_command(0)?,
        signal_command("ready", 0),
        timer_command(timer_id.clone())?,
        child_command(child_start_seq)?,
        resume_command.clone(),
    ];
    let expected_recorded = vec![
        Resolution::ActivityCompleted(payload("activity-result")?),
        Resolution::SignalDelivered(payload("signal-payload")?),
        Resolution::TimerFired,
        Resolution::ChildCompleted(payload("child-result")?),
    ];

    let mut direct_replay = Replay::new(&workflow_id, &run_id(), history.clone())?;
    assert_eq!(
        direct_replay.drive(commands.clone())?,
        ReplayOutcome::ResumeLive {
            command_index: 4,
            command: resume_command.clone(),
            recorded: expected_recorded.clone(),
        }
    );

    let mut driver = StaticRecoveryDriver::default();
    driver.insert(workflow_id.clone(), commands)?;
    let executor = CountingExecutor::default();

    let report = recover(Arc::clone(&store), &executor, &driver).await?;

    assert_eq!(report.len(), 1);
    assert_eq!(report[0].workflow_id, workflow_id);
    match &report[0].outcome {
        RecoveryOutcome::Resumed {
            resume_point,
            recorded,
        } => {
            assert_eq!(resume_point.command_index, 4);
            assert_eq!(resume_point.command, resume_command);
            assert_eq!(resume_point.head, 8);
            assert_eq!(recorded, &expected_recorded);
        }
        other => return Err(format!("expected realistic workflow to resume, got {other:?}").into()),
    }
    assert_eq!(executor.calls(), 0);

    let mut resumed_recorder = Recorder::resume_at(workflow_id.clone(), Arc::clone(&store), 8);
    resumed_recorder
        .record_activity_scheduled(
            timestamp(90)?,
            ActivityId::from_sequence_position(1),
            "activity".to_owned(),
            payload("activity-input")?,
        )
        .await?;
    let recovered_history = store.read_history(&workflow_id).await?;
    let appended = recovered_history
        .last()
        .ok_or("recovered history should contain appended event")?;
    assert_eq!(appended.seq(), 9);
    assert!(matches!(appended, Event::ActivityScheduled { .. }));
    Ok(())
}

#[tokio::test]
async fn recover_replays_only_active_workflows_to_resume_points()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let active_timer = WorkflowId::new(Uuid::from_u128(0x4401));
    let active_child = WorkflowId::new(Uuid::from_u128(0x4402));
    let terminal = WorkflowId::new(Uuid::from_u128(0x4403));
    record_partial_history_for(Arc::clone(&store), active_timer.clone(), 10).await?;
    record_partial_history_for(Arc::clone(&store), active_child.clone(), 40).await?;
    record_terminal_history_for(Arc::clone(&store), terminal.clone()).await?;

    let timer_resume = timer_command(TimerId::anonymous(10))?;
    let child_resume = child_command(0)?;
    let mut driver = StaticRecoveryDriver::default();
    driver.insert(
        active_timer.clone(),
        vec![activity_command(0)?, timer_resume.clone()],
    )?;
    driver.insert(
        active_child.clone(),
        vec![activity_command(0)?, child_resume.clone()],
    )?;
    let executor = CountingExecutor::default();

    let report = recover(Arc::clone(&store), &executor, &driver).await?;

    assert_eq!(report.len(), 2);
    assert!(report.iter().all(|entry| entry.workflow_id != terminal));
    let timer_outcome = report
        .iter()
        .find(|entry| entry.workflow_id == active_timer)
        .map(|entry| &entry.outcome)
        .ok_or("missing timer workflow recovery report")?;
    match timer_outcome {
        RecoveryOutcome::Resumed {
            resume_point,
            recorded,
        } => {
            assert_eq!(resume_point.command_index, 1);
            assert_eq!(resume_point.command, timer_resume);
            assert_eq!(resume_point.head, 3);
            assert_eq!(
                recorded,
                &vec![Resolution::ActivityCompleted(payload("activity-result")?)]
            );
        }
        other => return Err(format!("expected resumed timer workflow, got {other:?}").into()),
    }
    let child_outcome = report
        .iter()
        .find(|entry| entry.workflow_id == active_child)
        .map(|entry| &entry.outcome)
        .ok_or("missing child workflow recovery report")?;
    match child_outcome {
        RecoveryOutcome::Resumed { resume_point, .. } => {
            assert_eq!(resume_point.command_index, 1);
            assert_eq!(resume_point.command, child_resume);
            assert_eq!(resume_point.head, 3);
        }
        other => return Err(format!("expected resumed child workflow, got {other:?}").into()),
    }
    assert_eq!(executor.calls(), 0);

    let mut recorder = Recorder::resume_at(active_timer.clone(), Arc::clone(&store), 3);
    recorder
        .record_timer_started(timestamp(500)?, TimerId::anonymous(10), timestamp(600)?)
        .await?;
    let timer_history = store.read_history(&active_timer).await?;
    let appended = timer_history
        .last()
        .ok_or("timer history should contain appended event")?;
    assert_eq!(appended.seq(), 4);
    Ok(())
}

#[tokio::test]
async fn recover_isolates_divergent_workflow_failure_and_continues()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let clean_timer = WorkflowId::new(Uuid::from_u128(0x5501));
    let clean_child = WorkflowId::new(Uuid::from_u128(0x5502));
    let divergent = WorkflowId::new(Uuid::from_u128(0x5503));
    record_partial_history_for(Arc::clone(&store), clean_timer.clone(), 100).await?;
    record_partial_history_for(Arc::clone(&store), clean_child.clone(), 130).await?;
    record_divergent_history_for(Arc::clone(&store), divergent.clone()).await?;

    let mut driver = StaticRecoveryDriver::default();
    driver.insert(
        clean_timer.clone(),
        vec![activity_command(0)?, timer_command(TimerId::anonymous(20))?],
    )?;
    driver.insert(
        clean_child.clone(),
        vec![activity_command(0)?, child_command(0)?],
    )?;
    driver.insert(divergent.clone(), vec![activity_command(0)?])?;
    let executor = CountingExecutor::default();

    let report = recover(Arc::clone(&store), &executor, &driver).await?;

    let resumed = report
        .iter()
        .filter(|entry| matches!(entry.outcome, RecoveryOutcome::Resumed { .. }))
        .count();
    assert_eq!(resumed, 2);
    let failed = report
        .iter()
        .find(|entry| entry.workflow_id == divergent)
        .map(|entry| &entry.outcome)
        .ok_or("missing divergent workflow recovery report")?;
    match failed {
        RecoveryOutcome::Failed {
            error: DurabilityError::NonDeterminism(violation),
            failure_recorded,
        } => {
            assert_eq!(violation.workflow_id, divergent);
            assert!(*failure_recorded);
        }
        other => return Err(format!("expected non-determinism failure, got {other:?}").into()),
    }
    assert_eq!(executor.calls(), 0);
    let divergent_history = store.read_history(&divergent).await?;
    let failure_events = divergent_history
        .iter()
        .filter(|event| matches!(event, Event::WorkflowFailed { .. }))
        .collect::<Vec<_>>();
    assert_eq!(failure_events.len(), 1);
    assert_eq!(failure_events[0].seq(), 3);
    Ok(())
}

#[tokio::test]
async fn record_then_replay_round_trip_reaches_terminal_without_resume_live()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let history = record_round_trip_history(store).await?;
    assert_round_trip_history_shape(&history)?;
    let workflow_id = workflow_id();
    let run_id = run_id();
    let mut replay = Replay::new(&workflow_id, &run_id, history)?;

    let outcome = replay.drive(vec![
        activity_command(0)?,
        activity_command(1)?,
        timer_command(TimerId::anonymous(4))?,
        signal_command("ready", 0),
    ])?;

    assert_eq!(
        outcome,
        ReplayOutcome::Terminal {
            terminal: ReplayTerminal::Completed(payload("workflow-result")?),
            recorded: vec![
                Resolution::ActivityCompleted(payload("first-activity-result")?),
                Resolution::ActivityCompleted(payload("second-activity-result")?),
                Resolution::TimerFired,
                Resolution::SignalDelivered(payload("signal-payload")?),
            ],
        }
    );
    Ok(())
}

#[tokio::test]
async fn replay_determinism_round_trip_uses_recorded_now_and_seeded_random()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let history = record_round_trip_history(store).await?;
    let workflow_id = workflow_id();
    let recorded_timestamps = assert_round_trip_history_shape(&history)?;
    let run_id = run_id();
    let mut replay = Replay::new(&workflow_id, &run_id, history.clone())?;

    assert_eq!(replay.now(), timestamp(10)?);
    assert_eq!(
        replay.step(&activity_command(0)?)?,
        ReplayStep::Recorded(Resolution::ActivityCompleted(payload(
            "first-activity-result"
        )?))
    );
    assert_eq!(replay.now(), timestamp(30)?);
    assert_eq!(
        replay.step(&activity_command(1)?)?,
        ReplayStep::Recorded(Resolution::ActivityCompleted(payload(
            "second-activity-result"
        )?))
    );
    assert_eq!(replay.now(), timestamp(50)?);
    assert_eq!(
        replay.step(&timer_command(TimerId::anonymous(4))?)?,
        ReplayStep::Recorded(Resolution::TimerFired)
    );
    assert_eq!(replay.now(), timestamp(70)?);
    assert_eq!(
        replay.step(&signal_command("ready", 0))?,
        ReplayStep::Recorded(Resolution::SignalDelivered(payload("signal-payload")?))
    );
    assert_eq!(replay.now(), timestamp(80)?);

    let mut first_replay = Replay::new(&workflow_id, &run_id, history.clone())?;
    assert_eq!(first_replay.now(), recorded_timestamps[0]);
    let mut second_replay = Replay::new(&workflow_id, &run_id, history.clone())?;
    assert_eq!(second_replay.now(), recorded_timestamps[0]);
    let mut same_run_bytes = [0_u8; 64];
    let mut repeated_same_run_bytes = [0_u8; 64];
    first_replay
        .determinism_mut()
        .fill_random_bytes(&mut same_run_bytes);
    second_replay
        .determinism_mut()
        .fill_random_bytes(&mut repeated_same_run_bytes);
    assert_eq!(same_run_bytes, repeated_same_run_bytes);

    let different_run_id = different_run_id();
    let mut different_run_replay = Replay::new(&workflow_id, &different_run_id, history)?;
    let mut different_run_bytes = [0_u8; 64];
    different_run_replay
        .determinism_mut()
        .fill_random_bytes(&mut different_run_bytes);
    assert_ne!(same_run_bytes, different_run_bytes);
    Ok(())
}

#[tokio::test]
async fn partial_history_reports_resume_point_and_last_recorded_timestamp()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let history = record_partial_history(store).await?;
    let workflow_id = workflow_id();
    let run_id = run_id();
    let mut replay = Replay::new(&workflow_id, &run_id, history)?;
    let resume_command = timer_command(TimerId::anonymous(4))?;

    let outcome = replay.drive(vec![activity_command(0)?, resume_command.clone()])?;

    assert_eq!(
        outcome,
        ReplayOutcome::ResumeLive {
            command_index: 1,
            command: resume_command,
            recorded: vec![Resolution::ActivityCompleted(payload("activity-result")?)],
        }
    );
    assert_eq!(replay.now(), timestamp(30)?);
    Ok(())
}

#[tokio::test]
async fn activity_completed_is_served_from_history_cache_without_live_call()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let history = record_partial_history(store).await?;
    let workflow_id = workflow_id();
    let run_id = run_id();
    let mut replay = Replay::new(&workflow_id, &run_id, history)?;
    let executor = CountingExecutor::default();

    let step = replay.step(&activity_command(0)?)?;

    assert_eq!(
        step,
        ReplayStep::Recorded(Resolution::ActivityCompleted(payload("activity-result")?))
    );
    assert_eq!(executor.calls(), 0);
    Ok(())
}

#[tokio::test]
async fn terminal_activity_failure_is_served_from_history_cache_without_live_call()
-> Result<(), Box<dyn std::error::Error>> {
    let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
    let mut recorder = Recorder::new(workflow_id(), Arc::clone(&store));
    let activity_id = ActivityId::from_sequence_position(0);
    let terminal_error = ActivityError {
        kind: ActivityErrorKind::Terminal,
        message: "terminal activity failure".to_owned(),
        details: None,
    };
    let executor = CountingExecutor::default();

    recorder
        .record_workflow_started(
            timestamp(10)?,
            "workflow".to_owned(),
            payload("input")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_activity_scheduled(
            timestamp(20)?,
            activity_id.clone(),
            "activity".to_owned(),
            payload("activity-input")?,
        )
        .await?;
    recorder
        .record_activity_failed(timestamp(30)?, activity_id, terminal_error.clone(), 1)
        .await?;

    let history = store.read_history(&workflow_id()).await?;
    let workflow_id = workflow_id();
    let run_id = run_id();
    let mut replay = Replay::new(&workflow_id, &run_id, history)?;

    assert_eq!(
        replay.step(&activity_command(0)?)?,
        ReplayStep::Recorded(Resolution::ActivityFailedTerminal(terminal_error))
    );
    assert_eq!(executor.calls(), 0);
    Ok(())
}
