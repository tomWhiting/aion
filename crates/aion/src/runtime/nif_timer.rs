//! Timer NIF implementations for the `aion_flow_ffi` namespace.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use aion_core::{Event, TimerId, WorkflowId};
use aion_store::{EventStore, ReadableEventStore};
use beamr::native::ProcessContext;
use beamr::term::Term;
use chrono::{DateTime, Utc};
use tokio::runtime::Handle;

use crate::durability::{Command, CorrelationKey, DurabilityError, Resolution, ResolveOutcome};
use crate::engine_seam::{
    ChildWorkflowSpawnRequest, ChildWorkflowSpawnResult, EngineHandle, EngineSeamError,
    TimerWheelEntry, WorkflowMailboxMessage, WorkflowProcessHandle, WorkflowResidency,
};
use crate::registry::Registry;
use crate::runtime::engine_nifs::{decode_string_arg, error_result_term, ok_result_term};
use crate::runtime::nif_context::{NifContext, NifContextError};
use crate::time::{self, TimerService};

static TIMER_BRIDGE: OnceLock<Mutex<Option<Arc<TimerNifBridge>>>> = OnceLock::new();

struct TimerNifBridge {
    registry: Arc<Registry>,
    store: Arc<dyn EventStore>,
    tokio_handle: Handle,
}

impl TimerNifBridge {
    fn service(self: &Arc<Self>) -> TimerService {
        let engine: Arc<dyn EngineHandle> = self.clone();
        let store: Arc<dyn ReadableEventStore> =
            Arc::clone(&self.store) as Arc<dyn ReadableEventStore>;
        TimerService::with_recorded_at(engine, store, deterministic_epoch)
    }
}

enum TimerOutcome {
    Fired(TimerId),
    Cancelled(TimerId),
}

impl EngineHandle for TimerNifBridge {
    fn resolve_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<WorkflowResidency, EngineSeamError> {
        let handle = self
            .registry
            .list()
            .map_err(|error| EngineSeamError::Delivery {
                reason: error.to_string(),
            })?
            .into_iter()
            .find(|handle| handle.workflow_id() == workflow_id);
        Ok(match handle {
            Some(handle) if handle.residency() == crate::HandleResidency::Resident => {
                WorkflowResidency::Resident(WorkflowProcessHandle::new(handle.pid()))
            }
            Some(_) => WorkflowResidency::NonResident,
            None => WorkflowResidency::Unknown,
        })
    }

    fn deliver_workflow_message(
        &self,
        process: WorkflowProcessHandle,
        message: WorkflowMailboxMessage,
    ) -> Result<(), EngineSeamError> {
        let _ = (process, message);
        Ok(())
    }

    fn spawn_child_workflow(
        &self,
        request: ChildWorkflowSpawnRequest,
    ) -> Result<ChildWorkflowSpawnResult, EngineSeamError> {
        let _ = request;
        Err(EngineSeamError::ChildSpawn {
            reason: "timer NIF bridge does not spawn child workflows".to_owned(),
        })
    }

    fn terminate_linked_child_workflow(
        &self,
        parent_workflow_id: &WorkflowId,
        child_process: WorkflowProcessHandle,
        correlation: u64,
    ) -> Result<(), EngineSeamError> {
        let _ = (parent_workflow_id, child_process, correlation);
        Err(EngineSeamError::ChildTermination {
            reason: "timer NIF bridge does not terminate child workflows".to_owned(),
        })
    }

    fn terminate_linked_activity(
        &self,
        parent_workflow_id: &WorkflowId,
        activity_process: crate::Pid,
        correlation: u64,
    ) -> Result<(), EngineSeamError> {
        let _ = (parent_workflow_id, activity_process, correlation);
        Err(EngineSeamError::ChildTermination {
            reason: "timer NIF bridge does not terminate activities".to_owned(),
        })
    }

    fn arm_timer(&self, entry: TimerWheelEntry) -> Result<(), EngineSeamError> {
        let _ = entry;
        Ok(())
    }

    fn disarm_timer(
        &self,
        process: WorkflowProcessHandle,
        timer_id: &TimerId,
    ) -> Result<(), EngineSeamError> {
        let _ = (process, timer_id);
        Ok(())
    }

    fn record_workflow_event(
        &self,
        workflow_id: &WorkflowId,
        event: Event,
    ) -> Result<(), EngineSeamError> {
        let recorded_at = *event.recorded_at();
        let outcome = match event {
            Event::TimerFired { timer_id, .. } => TimerOutcome::Fired(timer_id),
            Event::TimerCancelled { timer_id, .. } => TimerOutcome::Cancelled(timer_id),
            other => {
                return Err(EngineSeamError::Recorder {
                    reason: format!("timer NIF bridge cannot record {}", event_kind(&other)),
                });
            }
        };
        let handle = self
            .registry
            .list()
            .map_err(|error| EngineSeamError::Recorder {
                reason: error.to_string(),
            })?
            .into_iter()
            .find(|handle| handle.workflow_id() == workflow_id)
            .ok_or_else(|| EngineSeamError::UnknownWorkflow {
                workflow_id: workflow_id.clone(),
            })?;
        let recorder = handle.recorder();
        self.tokio_handle
            .block_on(async {
                let mut recorder = recorder.lock().await;
                match outcome {
                    TimerOutcome::Fired(timer_id) => {
                        recorder.record_timer_fired(recorded_at, timer_id).await
                    }
                    TimerOutcome::Cancelled(timer_id) => {
                        recorder.record_timer_cancelled(recorded_at, timer_id).await
                    }
                }
            })
            .map_err(|error| EngineSeamError::Recorder {
                reason: error.to_string(),
            })
    }
}

/// Install the process-wide timer bridge used by raw NIF function pointers.
pub(crate) fn install_timer_nif_bridge(
    registry: Arc<Registry>,
    store: Arc<dyn EventStore>,
    tokio_handle: Handle,
) {
    let bridge = Arc::new(TimerNifBridge {
        registry,
        store,
        tokio_handle,
    });
    let bridge_slot = TIMER_BRIDGE.get_or_init(|| Mutex::new(None));
    match bridge_slot.lock() {
        Ok(mut installed) => *installed = Some(bridge),
        Err(poisoned) => *poisoned.into_inner() = Some(bridge),
    }
}

/// NIF backing `aion_flow_ffi:sleep/1`.
pub(super) fn sleep_impl(args: &[Term], ctx: &mut ProcessContext) -> Result<Term, Term> {
    timer_call("sleep", 1, args, ctx, |mut context, args| {
        let duration = decode_duration_arg("sleep duration", args[0])?;
        let recorded_now = recorded_now(&context)?;
        let timer_id = TimerId::anonymous(current_head(&context)?);
        let fire_at = add_duration(recorded_now, duration)?;
        match context.resolve_command(timer_command(timer_id.clone(), fire_at))? {
            ResolveOutcome::Recorded(Resolution::TimerFired) => Ok(ok_result("fired")),
            ResolveOutcome::Recorded(Resolution::TimerCancelled) => Ok(error_result("cancelled")),
            ResolveOutcome::Recorded(_) => Ok(error_result("sleep history mismatch")),
            ResolveOutcome::ResumeLive => {
                record_started(&context, recorded_now, timer_id.clone(), fire_at)?;
                schedule_sleep_timer(&context, duration, recorded_now, &timer_id, fire_at)?;
                wait_duration(duration)?;
                record_fired(&context, fire_at, timer_id)?;
                Ok(ok_result("fired"))
            }
        }
    })
}

/// NIF backing `aion_flow_ffi:start_timer/2`.
pub(super) fn start_timer_impl(args: &[Term], ctx: &mut ProcessContext) -> Result<Term, Term> {
    timer_call("start_timer", 2, args, ctx, |mut context, args| {
        let timer_id = decode_timer_id_arg("start_timer timer_id", args[0])?;
        let duration = decode_duration_arg("start_timer duration", args[1])?;
        let recorded_now = recorded_now(&context)?;
        let fire_at = add_duration(recorded_now, duration)?;
        match context.resolve_command(timer_command(timer_id.clone(), fire_at))? {
            ResolveOutcome::Recorded(Resolution::TimerStarted | Resolution::TimerFired) => {
                Ok(ok_result(timer_ref(&timer_id)))
            }
            ResolveOutcome::Recorded(Resolution::TimerCancelled) => Ok(error_result("cancelled")),
            ResolveOutcome::Recorded(_) => Ok(error_result("start_timer history mismatch")),
            ResolveOutcome::ResumeLive => {
                record_started(&context, recorded_now, timer_id.clone(), fire_at)?;
                schedule_timer(&context, timer_id.clone(), fire_at, TimerKind::Named)?;
                Ok(ok_result(timer_ref(&timer_id)))
            }
        }
    })
}

/// NIF backing `aion_flow_ffi:cancel_timer/1`.
pub(super) fn cancel_timer_impl(args: &[Term], ctx: &mut ProcessContext) -> Result<Term, Term> {
    timer_call("cancel_timer", 1, args, ctx, |mut context, args| {
        let timer_id = decode_timer_id_arg("cancel_timer timer_id", args[0])?;
        let recorded_now = recorded_now(&context)?;
        match context.resolve_command(timer_command(timer_id.clone(), recorded_now))? {
            ResolveOutcome::Recorded(Resolution::TimerCancelled | Resolution::TimerFired) => {
                Ok(ok_result("cancelled"))
            }
            ResolveOutcome::Recorded(_) => Ok(error_result("cancel_timer history mismatch")),
            ResolveOutcome::ResumeLive => {
                cancel_live_timer(&context, timer_id)?;
                Ok(ok_result("cancelled"))
            }
        }
    })
}

/// NIF backing `aion_flow_ffi:with_timeout/2`.
pub(super) fn with_timeout_impl(args: &[Term], ctx: &mut ProcessContext) -> Result<Term, Term> {
    timer_call("with_timeout", 2, args, ctx, |mut context, args| {
        let duration = decode_duration_arg("with_timeout duration", args[0])?;
        let operation = decode_string_arg(args[1]).map_err(NifTimerError::Argument)?;
        let recorded_now = recorded_now(&context)?;
        let timer_id = TimerId::anonymous(current_head(&context)?);
        let fire_at = add_duration(recorded_now, duration)?;
        match context.resolve_command(timer_command(timer_id.clone(), fire_at))? {
            ResolveOutcome::Recorded(Resolution::TimerFired) => Ok(error_result("timeout")),
            ResolveOutcome::Recorded(Resolution::TimerCancelled | Resolution::TimerStarted) => {
                Ok(ok_result(&operation))
            }
            ResolveOutcome::Recorded(_) => Ok(error_result("with_timeout history mismatch")),
            ResolveOutcome::ResumeLive => {
                record_started(&context, recorded_now, timer_id.clone(), fire_at)?;
                schedule_sleep_timer(&context, duration, recorded_now, &timer_id, fire_at)?;
                if operation == "timeout" || duration.is_zero() {
                    record_fired(&context, fire_at, timer_id)?;
                    Ok(error_result("timeout"))
                } else {
                    record_cancelled(&context, recorded_now, timer_id)?;
                    Ok(ok_result(&operation))
                }
            }
        }
    })
}

fn timer_call<F>(
    name: &str,
    arity: usize,
    args: &[Term],
    process_context: &mut ProcessContext,
    f: F,
) -> Result<Term, Term>
where
    F: FnOnce(NifContext, &[Term]) -> Result<NifResult, NifTimerError>,
{
    if args.len() > 255 {
        return Err(Term::NIL);
    }
    if args.len() != arity {
        return Ok(error_result_term(&format!(
            "{name}: expected {arity} arguments, got {}",
            args.len()
        ))
        .unwrap_or(Term::NIL));
    }
    match build_context(process_context).and_then(|context| f(context, args)) {
        Ok(NifResult::Ok(value)) => Ok(ok_result_term(&value).unwrap_or(Term::NIL)),
        Ok(NifResult::Error(message)) => Ok(error_result_term(&message).unwrap_or(Term::NIL)),
        Err(error) => Ok(error_result_term(&error.to_string()).unwrap_or(Term::NIL)),
    }
}

fn build_context(process_context: &ProcessContext) -> Result<NifContext, NifTimerError> {
    let pid = process_context
        .pid()
        .ok_or_else(|| NifTimerError::Context("missing calling pid".to_owned()))?;
    let bridge = timer_bridge()?;
    NifContext::new_with_history_store(
        pid,
        bridge.registry.as_ref(),
        bridge.tokio_handle.clone(),
        Some(Arc::clone(&bridge.store)),
    )
    .map_err(Into::into)
}

fn decode_duration_arg(label: &str, term: Term) -> Result<Duration, NifTimerError> {
    let text = decode_string_arg(term).map_err(NifTimerError::Argument)?;
    let millis = text
        .parse::<u64>()
        .map_err(|error| NifTimerError::Argument(format!("{label}: {error}")))?;
    Ok(Duration::from_millis(millis))
}

fn decode_timer_id_arg(label: &str, term: Term) -> Result<TimerId, NifTimerError> {
    let raw = decode_string_arg(term).map_err(NifTimerError::Argument)?;
    let name = raw.strip_prefix("timer:named:").unwrap_or(raw.as_str());
    TimerId::named(name).map_err(|error| NifTimerError::Argument(format!("{label}: {error}")))
}

fn timer_ref(timer_id: &TimerId) -> &str {
    timer_id.name().unwrap_or("anonymous")
}

fn timer_command(timer_id: TimerId, fire_at: DateTime<Utc>) -> Command {
    Command::StartTimer {
        key: CorrelationKey::Timer(timer_id),
        fire_at,
    }
}

fn recorded_now(context: &NifContext) -> Result<DateTime<Utc>, NifTimerError> {
    context
        .block_on_recorder(|recorder| {
            Box::pin(async move {
                let history = recorder.read_history().await?;
                Ok(history
                    .last()
                    .map(Event::recorded_at)
                    .copied()
                    .unwrap_or(DateTime::<Utc>::UNIX_EPOCH))
            })
        })
        .map_err(Into::into)
}

fn current_head(context: &NifContext) -> Result<u64, NifTimerError> {
    context
        .block_on_recorder(|recorder| Box::pin(async move { Ok(recorder.current_head()) }))
        .map_err(Into::into)
}

fn add_duration(
    recorded_now: DateTime<Utc>,
    duration: Duration,
) -> Result<DateTime<Utc>, NifTimerError> {
    let chrono_duration = chrono::Duration::from_std(duration)
        .map_err(|_| NifTimerError::Argument("duration is out of range".to_owned()))?;
    recorded_now
        .checked_add_signed(chrono_duration)
        .ok_or_else(|| NifTimerError::Argument("timer fire_at overflowed".to_owned()))
}

fn record_started(
    context: &NifContext,
    recorded_at: DateTime<Utc>,
    timer_id: TimerId,
    fire_at: DateTime<Utc>,
) -> Result<(), NifTimerError> {
    context
        .block_on_recorder(|recorder| {
            Box::pin(async move {
                recorder
                    .record_timer_started(recorded_at, timer_id, fire_at)
                    .await
            })
        })
        .map_err(Into::into)
}

fn record_fired(
    context: &NifContext,
    recorded_at: DateTime<Utc>,
    timer_id: TimerId,
) -> Result<(), NifTimerError> {
    context
        .block_on_recorder(|recorder| {
            Box::pin(async move { recorder.record_timer_fired(recorded_at, timer_id).await })
        })
        .map_err(Into::into)
}

fn record_cancelled(
    context: &NifContext,
    recorded_at: DateTime<Utc>,
    timer_id: TimerId,
) -> Result<(), NifTimerError> {
    context
        .block_on_recorder(|recorder| {
            Box::pin(async move { recorder.record_timer_cancelled(recorded_at, timer_id).await })
        })
        .map_err(Into::into)
}

#[derive(Clone, Copy)]
enum TimerKind {
    Anonymous {
        duration: Duration,
        recorded_now: DateTime<Utc>,
    },
    Named,
}

fn schedule_sleep_timer(
    context: &NifContext,
    duration: Duration,
    recorded_now: DateTime<Utc>,
    timer_id: &TimerId,
    fire_at: DateTime<Utc>,
) -> Result<(), NifTimerError> {
    let kind = TimerKind::Anonymous {
        duration,
        recorded_now,
    };
    let scheduled = schedule_timer(context, timer_id.clone(), fire_at, kind)?;
    if scheduled == *timer_id {
        Ok(())
    } else {
        Err(NifTimerError::Context(
            "AT sleep service returned a mismatched timer id".to_owned(),
        ))
    }
}

fn schedule_timer(
    context: &NifContext,
    timer_id: TimerId,
    fire_at: DateTime<Utc>,
    kind: TimerKind,
) -> Result<TimerId, NifTimerError> {
    let workflow_id = context.workflow_id().clone();
    let bridge = timer_bridge()?;
    let service = bridge.service();
    let scheduled = bridge.tokio_handle.block_on(async {
        match kind {
            TimerKind::Anonymous {
                duration,
                recorded_now,
            } => time::sleep(
                &service,
                workflow_id,
                duration,
                recorded_now,
                timer_id.sequence_position().ok_or_else(|| {
                    NifTimerError::Context("sleep timer id is not anonymous".to_owned())
                })?,
            )
            .await
            .map(|sleep| sleep.timer_id)
            .map_err(NifTimerError::from),
            TimerKind::Named => time::start_timer(&service, workflow_id, timer_id.clone(), fire_at)
                .await
                .map(|()| timer_id)
                .map_err(NifTimerError::from),
        }
    })?;
    Ok(scheduled)
}

fn cancel_live_timer(context: &NifContext, timer_id: TimerId) -> Result<(), NifTimerError> {
    let workflow_id = context.workflow_id().clone();
    let bridge = timer_bridge()?;
    let service = bridge.service();
    bridge
        .tokio_handle
        .block_on(async { time::cancel_timer(&service, workflow_id, timer_id).await })?;
    Ok(())
}

fn wait_duration(duration: Duration) -> Result<(), NifTimerError> {
    let bridge = timer_bridge()?;
    bridge.tokio_handle.block_on(tokio::time::sleep(duration));
    Ok(())
}

fn timer_bridge() -> Result<Arc<TimerNifBridge>, NifTimerError> {
    let bridge_slot = TIMER_BRIDGE
        .get()
        .ok_or_else(|| NifTimerError::Context("timer bridge is not configured".to_owned()))?;
    bridge_slot
        .lock()
        .map_err(|_| NifTimerError::Context("timer bridge lock is poisoned".to_owned()))?
        .clone()
        .ok_or_else(|| NifTimerError::Context("timer bridge is not configured".to_owned()))
}

fn deterministic_epoch() -> DateTime<Utc> {
    DateTime::<Utc>::UNIX_EPOCH
}
fn event_kind(event: &Event) -> &'static str {
    match event {
        Event::TimerFired { .. } => "TimerFired",
        Event::TimerCancelled { .. } => "TimerCancelled",
        _ => "non-timer",
    }
}

fn ok_result(value: impl Into<String>) -> NifResult {
    NifResult::Ok(value.into())
}

fn error_result(value: impl Into<String>) -> NifResult {
    NifResult::Error(value.into())
}

enum NifResult {
    Ok(String),
    Error(String),
}

#[derive(thiserror::Error, Debug)]
enum NifTimerError {
    #[error("argument:{0}")]
    Argument(String),
    #[error("context:{0}")]
    Context(String),
    #[error("durability:{0}")]
    Durability(#[from] DurabilityError),
    #[error("timer:{0}")]
    Timer(#[from] crate::time::TimerServiceError),
    #[error("sleep:{0}")]
    Sleep(#[from] crate::time::SleepTimerError),
}

impl From<NifContextError> for NifTimerError {
    fn from(error: NifContextError) -> Self {
        match error {
            NifContextError::Durability(error) => Self::Durability(error),
            other => Self::Context(other.to_string()),
        }
    }
}
