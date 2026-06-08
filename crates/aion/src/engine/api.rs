//! `Engine` start, cancel, result, list, and shutdown support.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

use aion_core::{
    Event, EventEnvelope, Payload, RunId, ScheduleConfig, ScheduleId, WorkflowError,
    WorkflowFilter, WorkflowId, WorkflowSummary,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::Mutex as AsyncMutex;

use crate::durability::Recorder;
use crate::schedule::{
    NoopScheduleCanceller, ScheduleEvaluator, ScheduleEvaluatorError, ScheduleEventSink,
    ScheduleEventSource, ScheduleExecution, ScheduleState, ScheduleTimer, ScheduleWorkflowStarter,
    StoreScheduleTimer, TimerEvaluationOutcome,
};
use aion_store::EventStore;
use aion_store::visibility::VisibilityStore;

use crate::lifecycle::continue_as_new::{self, ContinueAsNewContext, ContinueAsNewRequest};
use crate::lifecycle::start::{self, StartWorkflowContext};
use crate::lifecycle::terminate::{self, TerminateWorkflowContext};
use crate::lifecycle::transition;
use crate::registry::{TerminalOutcome, WorkflowHandle};
use crate::{
    EngineError, LoadedWorkflows, Registry, RuntimeHandle, SupervisionTree,
    engine_seam::{
        EngineHandle, EngineSeamError, WorkflowMailboxMessage, WorkflowProcessHandle,
        WorkflowResidency,
    },
    signal::SignalResumeHandoff,
};

use super::delegated::DelegatedSeams;

/// Live embedded workflow engine assembled by [`crate::EngineBuilder`].
pub struct Engine {
    store: Arc<dyn EventStore>,
    visibility_store: Arc<dyn VisibilityStore>,
    schedule_recorder: Arc<AsyncMutex<Recorder>>,
    schedule_evaluator: Arc<AsyncMutex<ScheduleEvaluator>>,
    schedule_coordinator_workflow_id: WorkflowId,
    runtime: Arc<RuntimeHandle>,
    loaded_workflows: LoadedWorkflows,
    registry: Arc<Registry>,
    supervision: Arc<SupervisionTree>,
    delegated: DelegatedSeams,
    signal_handoff: Arc<SignalResumeHandoff>,
    shutdown_gate: ShutdownGate,
}

/// Components required to construct an [`Engine`].
pub(crate) struct EngineComponents {
    pub(crate) store: Arc<dyn EventStore>,
    pub(crate) visibility_store: Arc<dyn VisibilityStore>,
    pub(crate) runtime: Arc<RuntimeHandle>,
    pub(crate) loaded_workflows: LoadedWorkflows,
    pub(crate) registry: Registry,
    pub(crate) supervision: SupervisionTree,
    pub(crate) delegated: DelegatedSeams,
    pub(crate) signal_handoff: Arc<SignalResumeHandoff>,
}

impl Engine {
    /// Construct an engine from already-assembled components.
    #[must_use]
    pub(crate) fn new(components: EngineComponents) -> Self {
        let EngineComponents {
            store,
            visibility_store,
            runtime,
            loaded_workflows,
            registry,
            supervision,
            delegated,
            signal_handoff,
        } = components;
        let schedule_coordinator_workflow_id = schedule_coordinator_workflow_id();
        let schedule_recorder = Arc::new(AsyncMutex::new(Recorder::new(
            schedule_coordinator_workflow_id.clone(),
            Arc::clone(&store),
        )));
        let runtime_arc = runtime;
        let registry_arc = Arc::new(registry);
        let supervision_arc = Arc::new(supervision);
        let schedule_evaluator = Arc::new(AsyncMutex::new(default_schedule_evaluator(
            schedule_coordinator_workflow_id.clone(),
            Arc::clone(&schedule_recorder),
            ScheduleRuntimeDeps {
                store: Arc::clone(&store),
                visibility_store: Arc::clone(&visibility_store),
                runtime: Arc::clone(&runtime_arc),
                loaded_workflows: loaded_workflows.clone(),
                registry: Arc::clone(&registry_arc),
                supervision: Arc::clone(&supervision_arc),
            },
        )));
        Self {
            store,
            visibility_store,
            schedule_recorder,
            schedule_evaluator,
            schedule_coordinator_workflow_id,
            runtime: runtime_arc,
            loaded_workflows,
            registry: registry_arc,
            supervision: supervision_arc,
            delegated,
            signal_handoff,
            shutdown_gate: ShutdownGate::default(),
        }
    }

    /// Advance the schedule coordinator's recorder head to match persisted
    /// events so that a rebuilt engine resumes appending at the correct
    /// sequence rather than conflicting at head 0.
    ///
    /// # Errors
    ///
    /// Returns store read errors.
    pub(crate) async fn catchup_schedule_coordinator(&self) -> Result<(), EngineError> {
        let history = self
            .store
            .read_history(&self.schedule_coordinator_workflow_id)
            .await?;
        let head = u64::try_from(history.len()).unwrap_or(u64::MAX);
        if head > 0 {
            let mut recorder = self.schedule_recorder.lock().await;
            *recorder = Recorder::resume_at(
                self.schedule_coordinator_workflow_id.clone(),
                Arc::clone(&self.store),
                head,
            );
        }
        Ok(())
    }

    /// Event store used by lifecycle and delegated AD/AT operations.
    #[must_use]
    pub fn store(&self) -> Arc<dyn EventStore> {
        Arc::clone(&self.store)
    }

    /// Visibility store used for workflow summary projections.
    #[must_use]
    pub fn visibility_store(&self) -> Arc<dyn VisibilityStore> {
        Arc::clone(&self.visibility_store)
    }

    /// Runtime boundary assembled for this engine.
    #[must_use]
    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    /// Loaded workflow package registry.
    #[must_use]
    pub const fn loaded_workflows(&self) -> &LoadedWorkflows {
        &self.loaded_workflows
    }

    /// Active execution registry.
    #[must_use]
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Supervision tree snapshot/model.
    #[must_use]
    pub fn supervision(&self) -> &SupervisionTree {
        &self.supervision
    }

    /// Delegated signal/query/subscribe seams installed for AT/AD integration.
    #[must_use]
    pub const fn delegated(&self) -> &DelegatedSeams {
        &self.delegated
    }

    /// Shared in-memory handoff for already-recorded non-resident signals.
    #[must_use]
    pub fn signal_handoff(&self) -> Arc<SignalResumeHandoff> {
        Arc::clone(&self.signal_handoff)
    }

    /// Workflow history used for durable schedule events and timer ownership.
    #[must_use]
    pub const fn schedule_coordinator_workflow_id(&self) -> &WorkflowId {
        &self.schedule_coordinator_workflow_id
    }

    /// Create a durable schedule and arm its first timer.
    ///
    /// # Errors
    ///
    /// Returns shutdown, durability, schedule projection, or timer arming errors.
    pub async fn create_schedule(&self, config: ScheduleConfig) -> Result<ScheduleId, EngineError> {
        let operation = self.shutdown_gate.begin_start()?;
        let recorded_at = Utc::now();
        let schedule_id = ScheduleId::new_v4();
        let result = self
            .create_schedule_inner(schedule_id, config, recorded_at)
            .await;
        drop(operation);
        result
    }

    /// Update an existing schedule's configuration and re-arm it.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::ScheduleNotFound`] for absent/deleted schedules, or typed durability,
    /// projection, and timer errors.
    pub async fn update_schedule(
        &self,
        schedule_id: &ScheduleId,
        config: ScheduleConfig,
    ) -> Result<(), EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = self.update_schedule_inner(schedule_id, config).await;
        drop(operation);
        result
    }

    /// Pause an existing schedule.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::ScheduleNotFound`] for absent/deleted schedules, or typed durability
    /// and projection errors.
    pub async fn pause_schedule(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = self.pause_schedule_inner(schedule_id).await;
        drop(operation);
        result
    }

    /// Resume an existing schedule and re-arm it.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::ScheduleNotFound`] for absent/deleted schedules, or typed durability,
    /// projection, and timer errors.
    pub async fn resume_schedule(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = self.resume_schedule_inner(schedule_id).await;
        drop(operation);
        result
    }

    /// Delete an existing schedule so it is no longer listed or armed.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::ScheduleNotFound`] for absent/deleted schedules, or typed durability
    /// and projection errors.
    pub async fn delete_schedule(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = self.delete_schedule_inner(schedule_id).await;
        drop(operation);
        result
    }

    /// List all non-deleted schedules from projected state.
    ///
    /// # Errors
    ///
    /// Currently returns only infallible projected state, wrapped for API consistency.
    pub async fn list_schedules(&self) -> Result<Vec<ScheduleState>, EngineError> {
        let evaluator = self.schedule_evaluator.lock().await;
        let mut schedules = evaluator
            .states()
            .filter(|state| !state.is_deleted)
            .cloned()
            .collect::<Vec<_>>();
        schedules.sort_by(|left, right| {
            left.next_trigger_at
                .cmp(&right.next_trigger_at)
                .then_with(|| {
                    left.schedule_id
                        .to_string()
                        .cmp(&right.schedule_id.to_string())
                })
        });
        Ok(schedules)
    }

    /// Describe one non-deleted schedule.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::ScheduleNotFound`] for absent or deleted schedules.
    pub async fn describe_schedule(
        &self,
        schedule_id: &ScheduleId,
    ) -> Result<ScheduleState, EngineError> {
        self.schedule_evaluator
            .lock()
            .await
            .state(schedule_id)
            .filter(|state| !state.is_deleted)
            .cloned()
            .ok_or_else(|| EngineError::ScheduleNotFound {
                schedule_id: schedule_id.clone(),
            })
    }

    async fn create_schedule_inner(
        &self,
        schedule_id: ScheduleId,
        config: ScheduleConfig,
        recorded_at: DateTime<Utc>,
    ) -> Result<ScheduleId, EngineError> {
        self.schedule_recorder
            .lock()
            .await
            .record_schedule_created(recorded_at, schedule_id.clone(), config.clone())
            .await?;

        let state = ScheduleState::created(schedule_id.clone(), config, recorded_at)?;
        let mut evaluator = self.schedule_evaluator.lock().await;
        evaluator.upsert_state(state);
        evaluator.arm_active_schedule(&schedule_id).await?;
        Ok(schedule_id)
    }

    async fn update_schedule_inner(
        &self,
        schedule_id: &ScheduleId,
        config: ScheduleConfig,
    ) -> Result<(), EngineError> {
        self.ensure_schedule_exists(schedule_id).await?;
        let recorded_at = Utc::now();
        self.schedule_recorder
            .lock()
            .await
            .record_schedule_updated(recorded_at, schedule_id.clone(), config.clone())
            .await?;
        let event = Event::ScheduleUpdated {
            envelope: schedule_event_envelope(recorded_at),
            schedule_id: schedule_id.clone(),
            config,
        };
        self.apply_schedule_event(schedule_id, &event, true).await
    }

    async fn pause_schedule_inner(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        self.ensure_schedule_exists(schedule_id).await?;
        let recorded_at = Utc::now();
        self.schedule_recorder
            .lock()
            .await
            .record_schedule_paused(recorded_at, schedule_id.clone())
            .await?;
        let event = Event::SchedulePaused {
            envelope: schedule_event_envelope(recorded_at),
            schedule_id: schedule_id.clone(),
        };
        self.apply_schedule_event(schedule_id, &event, false).await
    }

    async fn resume_schedule_inner(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        self.ensure_schedule_exists(schedule_id).await?;
        let recorded_at = Utc::now();
        self.schedule_recorder
            .lock()
            .await
            .record_schedule_resumed(recorded_at, schedule_id.clone())
            .await?;
        let event = Event::ScheduleResumed {
            envelope: schedule_event_envelope(recorded_at),
            schedule_id: schedule_id.clone(),
        };
        self.apply_schedule_event(schedule_id, &event, true).await
    }

    async fn delete_schedule_inner(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        self.ensure_schedule_exists(schedule_id).await?;
        let recorded_at = Utc::now();
        self.schedule_recorder
            .lock()
            .await
            .record_schedule_deleted(recorded_at, schedule_id.clone())
            .await?;
        let event = Event::ScheduleDeleted {
            envelope: schedule_event_envelope(recorded_at),
            schedule_id: schedule_id.clone(),
        };
        self.apply_schedule_event(schedule_id, &event, false).await
    }

    async fn ensure_schedule_exists(&self, schedule_id: &ScheduleId) -> Result<(), EngineError> {
        self.schedule_evaluator
            .lock()
            .await
            .state(schedule_id)
            .filter(|state| !state.is_deleted)
            .map(|_| ())
            .ok_or_else(|| EngineError::ScheduleNotFound {
                schedule_id: schedule_id.clone(),
            })
    }

    async fn apply_schedule_event(
        &self,
        schedule_id: &ScheduleId,
        event: &Event,
        should_arm: bool,
    ) -> Result<(), EngineError> {
        let mut evaluator = self.schedule_evaluator.lock().await;
        let mut state = evaluator
            .state(schedule_id)
            .filter(|state| !state.is_deleted)
            .cloned()
            .ok_or_else(|| EngineError::ScheduleNotFound {
                schedule_id: schedule_id.clone(),
            })?;
        state.apply(event)?;
        evaluator.upsert_state(state);
        if should_arm {
            evaluator.arm_active_schedule(schedule_id).await?;
        }
        Ok(())
    }

    /// Handles a fired durable schedule timer through the schedule evaluator.
    ///
    /// # Errors
    ///
    /// Returns schedule evaluator or shutdown errors.
    pub async fn handle_schedule_timer_fired(
        &self,
        schedule_id: &ScheduleId,
        fire_at: DateTime<Utc>,
    ) -> Result<TimerEvaluationOutcome, EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = self
            .schedule_evaluator
            .lock()
            .await
            .handle_timer_fired(schedule_id, fire_at)
            .await
            .map_err(EngineError::from);
        drop(operation);
        result
    }

    /// Rebuilds schedule state from durable coordinator history and re-arms active schedules.
    ///
    /// # Errors
    ///
    /// Returns schedule projection, catch-up, timer, or workflow-start errors.
    pub async fn recover_schedules_on_startup(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), EngineError> {
        let source = StoreScheduleEventSource {
            store: self.store(),
            coordinator_workflow_id: self.schedule_coordinator_workflow_id.clone(),
        };
        self.schedule_evaluator
            .lock()
            .await
            .recover_on_startup(&source, now)
            .await
            .map_err(EngineError::from)
    }

    /// Start a loaded workflow type as a new BEAM process.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::ShuttingDown`] after shutdown begins. Otherwise
    /// delegates to the start lifecycle transition and returns its typed errors.
    pub async fn start_workflow(
        &self,
        workflow_type: &str,
        input: Payload,
    ) -> Result<WorkflowHandle, EngineError> {
        let operation = self.shutdown_gate.begin_start()?;
        let result = start::start_workflow(
            StartWorkflowContext {
                store: self.store(),
                visibility_store: self.visibility_store(),
                loaded_workflows: &self.loaded_workflows,
                runtime: &self.runtime,
                supervision: &self.supervision,
                registry: &self.registry,
                signal_handoff: Some(self.signal_handoff()),
            },
            workflow_type,
            input,
        )
        .await;
        drop(operation);
        result
    }

    /// Resume a suspended workflow run and flush deferred signals through its mailbox.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::WorkflowNotFound`] when the `(workflow, run)` pair
    /// is absent, or registry errors from the residency transition. Deferred
    /// delivery failures are logged and dropped because signals are already durable.
    pub fn resume_workflow(
        &self,
        id: &WorkflowId,
        run: &RunId,
    ) -> Result<WorkflowHandle, EngineError> {
        let handle = transition::resume(self.registry(), id, run)?;
        if let Err(error) = self.signal_handoff.deliver_deferred(self, id) {
            tracing::warn!(
                workflow_id = %id,
                run_id = %run,
                error = %error,
                "failed to flush deferred signals after workflow resume"
            );
        }
        Ok(handle)
    }

    /// Cancel a live workflow run by killing its runtime process.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::WorkflowNotFound`] when the `(workflow, run)` pair
    /// is not live. Other typed errors come from the cancel transition.
    pub async fn cancel(
        &self,
        id: &WorkflowId,
        run: &RunId,
        reason: impl Into<String>,
    ) -> Result<(), EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = terminate::cancel(
            TerminateWorkflowContext {
                runtime: &self.runtime,
                store: self.store(),
                visibility_store: self.visibility_store(),
                registry: &self.registry,
            },
            id,
            run,
            reason,
        )
        .await;
        drop(operation);
        result
    }

    /// Continue a live workflow run as a new run under the same workflow id.
    ///
    /// # Errors
    ///
    /// Returns [`EngineError::WorkflowNotFound`] when the `(workflow, run)` pair
    /// is not live. Other typed errors come from the continue-as-new transition.
    pub async fn continue_as_new(
        &self,
        id: &WorkflowId,
        run: &RunId,
        input: Payload,
        workflow_type: Option<String>,
    ) -> Result<WorkflowHandle, EngineError> {
        let operation = self.shutdown_gate.begin_operation()?;
        let result = continue_as_new::continue_as_new(
            ContinueAsNewContext {
                store: self.store(),
                visibility_store: Arc::clone(&self.visibility_store),
                loaded_workflows: &self.loaded_workflows,
                runtime: &self.runtime,
                supervision: &self.supervision,
                registry: &self.registry,
            },
            id,
            run,
            ContinueAsNewRequest {
                input,
                workflow_type,
            },
        )
        .await;
        drop(operation);
        result
    }

    /// Await a workflow run's terminal result.
    ///
    /// Already-terminal histories return immediately. Live workflows await their
    /// completion notifier. Unknown workflow/run pairs return not found.
    ///
    /// # Errors
    ///
    /// Returns store, registry, or runtime channel errors as typed [`EngineError`]
    /// variants, or [`EngineError::WorkflowNotFound`] when no live handle or
    /// terminal history exists for the requested pair.
    pub async fn result(
        &self,
        id: &WorkflowId,
        run: &RunId,
    ) -> Result<Result<Payload, WorkflowError>, EngineError> {
        if let Some(outcome) = terminal_outcome_from_history(&self.store.read_history(id).await?) {
            return Ok(outcome_to_result(outcome));
        }

        let handle = self
            .registry
            .get(id, run)?
            .ok_or_else(|| workflow_not_found(id, run))?;
        let runtime_outcome = self.runtime.workflow_outcome(handle.pid())?;
        match runtime_outcome {
            Ok(payload) => {
                terminate::complete(
                    TerminateWorkflowContext {
                        runtime: &self.runtime,
                        store: self.store(),
                        visibility_store: self.visibility_store(),
                        registry: &self.registry,
                    },
                    id,
                    run,
                    payload,
                )
                .await?;
            }
            Err(error) => {
                terminate::fail(
                    TerminateWorkflowContext {
                        runtime: &self.runtime,
                        store: self.store(),
                        visibility_store: self.visibility_store(),
                        registry: &self.registry,
                    },
                    id,
                    run,
                    error,
                )
                .await?;
            }
        }
        if let Some(outcome) = terminal_outcome_from_history(&self.store.read_history(id).await?) {
            return Ok(outcome_to_result(outcome));
        }

        let handle = self
            .registry
            .get(id, run)?
            .ok_or_else(|| workflow_not_found(id, run))?;
        let mut receiver = handle.completion().subscribe();
        loop {
            if let Some(outcome) = receiver.borrow().clone() {
                return Ok(outcome_to_result(outcome));
            }
            if receiver.changed().await.is_err() {
                if let Some(outcome) =
                    terminal_outcome_from_history(&self.store.read_history(id).await?)
                {
                    return Ok(outcome_to_result(outcome));
                }
                return Err(EngineError::Runtime {
                    reason: format!(
                        "completion channel closed before workflow `{id}/{run}` finished"
                    ),
                });
            }
        }
    }

    /// List live and terminal workflow summaries matching `filter`.
    ///
    /// Store projections are authoritative; live registry entries are projected
    /// from durable history before being merged and deduplicated.
    ///
    /// # Errors
    ///
    /// Returns typed store or registry errors when visibility data cannot be read.
    pub async fn list_workflows(
        &self,
        filter: WorkflowFilter,
    ) -> Result<Vec<WorkflowSummary>, EngineError> {
        let mut summaries = self
            .store
            .query(&filter)
            .await?
            .into_iter()
            .map(|summary| (summary.workflow_id.clone(), summary))
            .collect::<HashMap<_, _>>();

        for handle in self.registry.list()? {
            let history = self.store.read_history(handle.workflow_id()).await?;
            self.registry
                .reconcile(handle.workflow_id(), handle.run_id(), &history)?;
            if let Some(summary) = WorkflowSummary::from_history(&history) {
                if filter.matches(&summary) {
                    summaries.insert(summary.workflow_id.clone(), summary);
                }
            }
        }

        let mut summaries = summaries.into_values().collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            left.started_at.cmp(&right.started_at).then_with(|| {
                left.workflow_id
                    .to_string()
                    .cmp(&right.workflow_id.to_string())
            })
        });
        Ok(summaries)
    }

    /// Gracefully stop accepting new starts and shut down the embedded runtime.
    ///
    /// # Errors
    ///
    /// Returns registry poison or runtime shutdown failures as typed errors.
    pub fn shutdown(&self) -> Result<(), EngineError> {
        self.shutdown_gate.close_and_wait()?;
        self.runtime.shutdown()
    }
}

#[derive(Clone, Default)]
struct ShutdownGate {
    inner: Arc<ShutdownGateInner>,
}

#[derive(Default)]
struct ShutdownGateInner {
    state: Mutex<ShutdownState>,
    idle: Condvar,
}

#[derive(Default)]
struct ShutdownState {
    shutting_down: bool,
    active_operations: usize,
}

impl ShutdownGate {
    fn begin_start(&self) -> Result<LifecycleOperation, EngineError> {
        let mut state = self.state()?;
        if state.shutting_down {
            return Err(EngineError::ShuttingDown);
        }
        state.active_operations += 1;
        Ok(LifecycleOperation {
            inner: Arc::clone(&self.inner),
        })
    }

    fn begin_operation(&self) -> Result<LifecycleOperation, EngineError> {
        let mut state = self.state()?;
        state.active_operations += 1;
        Ok(LifecycleOperation {
            inner: Arc::clone(&self.inner),
        })
    }

    fn close_and_wait(&self) -> Result<(), EngineError> {
        let mut state = self.state()?;
        state.shutting_down = true;
        while state.active_operations > 0 {
            state = self
                .inner
                .idle
                .wait(state)
                .map_err(|_| EngineError::RegistryPoisoned)?;
        }
        Ok(())
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, ShutdownState>, EngineError> {
        self.inner
            .state
            .lock()
            .map_err(|_| EngineError::RegistryPoisoned)
    }
}

struct LifecycleOperation {
    inner: Arc<ShutdownGateInner>,
}

impl Drop for LifecycleOperation {
    fn drop(&mut self) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.active_operations = state.active_operations.saturating_sub(1);
            if state.active_operations == 0 {
                self.inner.idle.notify_all();
            }
        }
    }
}

impl EngineHandle for Engine {
    fn resolve_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<WorkflowResidency, EngineSeamError> {
        let handle = self
            .registry()
            .list()
            .map_err(|error| EngineSeamError::Delivery {
                reason: error.to_string(),
            })?
            .into_iter()
            .find(|handle| handle.workflow_id() == workflow_id);
        match handle {
            Some(handle) if handle.residency() == crate::HandleResidency::Resident => Ok(
                WorkflowResidency::Resident(WorkflowProcessHandle::new(handle.pid())),
            ),
            Some(_) => Ok(WorkflowResidency::NonResident),
            None => Ok(WorkflowResidency::Unknown),
        }
    }

    fn deliver_workflow_message(
        &self,
        process: WorkflowProcessHandle,
        message: WorkflowMailboxMessage,
    ) -> Result<(), EngineSeamError> {
        match message {
            WorkflowMailboxMessage::SignalReceived { name, payload } => self
                .runtime()
                .deliver_signal_received(process.pid(), name, payload)
                .map_err(|error| EngineSeamError::Delivery {
                    reason: error.to_string(),
                }),
            other => Err(EngineSeamError::Delivery {
                reason: format!("unsupported workflow mailbox message: {other:?}"),
            }),
        }
    }

    fn spawn_child_workflow(
        &self,
        request: crate::engine_seam::ChildWorkflowSpawnRequest,
    ) -> Result<crate::engine_seam::ChildWorkflowSpawnResult, EngineSeamError> {
        let _ = request;
        Err(EngineSeamError::ChildSpawn {
            reason: "engine handle child spawning is not wired here".to_owned(),
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
            reason: "engine handle child termination is not wired here".to_owned(),
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
            reason: "engine handle activity termination is not wired here".to_owned(),
        })
    }

    fn arm_timer(&self, entry: crate::engine_seam::TimerWheelEntry) -> Result<(), EngineSeamError> {
        let _ = entry;
        Err(EngineSeamError::TimerWheel {
            reason: "engine handle timer arming is not wired here".to_owned(),
        })
    }

    fn disarm_timer(
        &self,
        process: WorkflowProcessHandle,
        timer_id: &aion_core::TimerId,
    ) -> Result<(), EngineSeamError> {
        let _ = (process, timer_id);
        Err(EngineSeamError::TimerWheel {
            reason: "engine handle timer disarming is not wired here".to_owned(),
        })
    }

    fn record_workflow_event(
        &self,
        workflow_id: &WorkflowId,
        event: Event,
    ) -> Result<(), EngineSeamError> {
        let _ = (workflow_id, event);
        Err(EngineSeamError::Recorder {
            reason: "engine handle event recording is not wired here".to_owned(),
        })
    }
}

pub(crate) fn terminal_outcome_from_history(events: &[Event]) -> Option<TerminalOutcome> {
    for event in events.iter().rev() {
        match event {
            Event::WorkflowStarted { .. } => return None,
            Event::WorkflowCompleted { result, .. } => {
                return Some(TerminalOutcome::Completed(result.clone()));
            }
            Event::WorkflowFailed { error, .. } => {
                return Some(TerminalOutcome::Failed(error.clone()));
            }
            Event::WorkflowCancelled { reason, .. } => {
                return Some(TerminalOutcome::Cancelled(reason.clone()));
            }
            Event::WorkflowTimedOut { timeout, .. } => {
                return Some(TerminalOutcome::TimedOut(timeout.clone()));
            }
            Event::WorkflowContinuedAsNew {
                input,
                workflow_type,
                parent_run_id,
                ..
            } => {
                return Some(TerminalOutcome::ContinuedAsNew {
                    input: input.clone(),
                    workflow_type: workflow_type.clone(),
                    parent_run_id: parent_run_id.clone(),
                });
            }
            Event::SearchAttributesUpdated { .. }
            | Event::ActivityScheduled { .. }
            | Event::ActivityStarted { .. }
            | Event::ActivityCompleted { .. }
            | Event::ActivityFailed { .. }
            | Event::ActivityCancelled { .. }
            | Event::TimerStarted { .. }
            | Event::TimerFired { .. }
            | Event::TimerCancelled { .. }
            | Event::SignalReceived { .. }
            | Event::ChildWorkflowStarted { .. }
            | Event::ChildWorkflowCompleted { .. }
            | Event::ChildWorkflowFailed { .. }
            | Event::ChildWorkflowCancelled { .. }
            | Event::ScheduleCreated { .. }
            | Event::ScheduleUpdated { .. }
            | Event::SchedulePaused { .. }
            | Event::ScheduleResumed { .. }
            | Event::ScheduleDeleted { .. }
            | Event::ScheduleTriggered { .. } => {}
        }
    }
    None
}

fn outcome_to_result(outcome: TerminalOutcome) -> Result<Payload, WorkflowError> {
    match outcome {
        TerminalOutcome::Completed(payload) => Ok(payload),
        TerminalOutcome::Failed(error) => Err(error),
        TerminalOutcome::Cancelled(reason) => Err(WorkflowError {
            message: format!("workflow cancelled: {reason}"),
            details: None,
        }),
        TerminalOutcome::TimedOut(timeout) => Err(WorkflowError {
            message: format!("workflow timed out: {timeout}"),
            details: None,
        }),
        TerminalOutcome::ContinuedAsNew { parent_run_id, .. } => Err(WorkflowError {
            message: format!("workflow continued as new from run {parent_run_id}"),
            details: None,
        }),
    }
}

pub(crate) fn workflow_not_found(id: &WorkflowId, run: &RunId) -> EngineError {
    EngineError::WorkflowNotFound {
        workflow_type: format!("{id}/{run}"),
    }
}

pub(crate) fn schedule_coordinator_workflow_id() -> WorkflowId {
    WorkflowId::new(uuid::Uuid::from_u128(
        0x0000_0000_a10a_0000_0000_0000_0000_0004,
    ))
}

pub(crate) fn schedule_coordinator_run_id() -> RunId {
    RunId::new(uuid::Uuid::from_u128(
        0x0000_0000_a10a_0000_0000_0000_0000_0005,
    ))
}

pub(crate) const fn schedule_coordinator_workflow_type() -> &'static str {
    "aion.schedule_coordinator"
}

fn schedule_event_envelope(recorded_at: DateTime<Utc>) -> EventEnvelope {
    EventEnvelope {
        seq: 0,
        recorded_at,
        workflow_id: schedule_coordinator_workflow_id(),
    }
}

fn default_schedule_evaluator(
    coordinator_workflow_id: WorkflowId,
    recorder: Arc<AsyncMutex<Recorder>>,
    deps: ScheduleRuntimeDeps,
) -> ScheduleEvaluator {
    let timer: Arc<dyn ScheduleTimer> = Arc::new(StoreScheduleTimer::new(
        Arc::clone(&deps.store),
        coordinator_workflow_id,
    ));
    let starter: Arc<dyn ScheduleWorkflowStarter> = Arc::new(EngineScheduleStarter { deps });
    let canceller: Arc<dyn crate::schedule::ScheduleWorkflowCanceller> =
        Arc::new(NoopScheduleCanceller);
    let events: Arc<dyn ScheduleEventSink> = recorder;
    ScheduleEvaluator::new(timer, starter, canceller, events)
}

struct ScheduleRuntimeDeps {
    store: Arc<dyn EventStore>,
    visibility_store: Arc<dyn VisibilityStore>,
    runtime: Arc<RuntimeHandle>,
    loaded_workflows: LoadedWorkflows,
    registry: Arc<Registry>,
    supervision: Arc<SupervisionTree>,
}

struct EngineScheduleStarter {
    deps: ScheduleRuntimeDeps,
}

#[async_trait]
impl ScheduleWorkflowStarter for EngineScheduleStarter {
    async fn start_scheduled_workflow(
        &self,
        workflow_type: &str,
        input: Payload,
    ) -> Result<ScheduleExecution, ScheduleEvaluatorError> {
        let handle = start::start_workflow(
            StartWorkflowContext {
                store: Arc::clone(&self.deps.store),
                visibility_store: Arc::clone(&self.deps.visibility_store),
                loaded_workflows: &self.deps.loaded_workflows,
                runtime: &self.deps.runtime,
                supervision: &self.deps.supervision,
                registry: &self.deps.registry,
                signal_handoff: None,
            },
            workflow_type,
            input,
        )
        .await
        .map_err(|error| ScheduleEvaluatorError::side_effect(error.to_string()))?;
        Ok(ScheduleExecution::new(
            handle.workflow_id().clone(),
            handle.run_id().clone(),
        ))
    }
}

struct StoreScheduleEventSource {
    store: Arc<dyn EventStore>,
    coordinator_workflow_id: WorkflowId,
}

#[async_trait]
impl ScheduleEventSource for StoreScheduleEventSource {
    async fn schedule_events(&self) -> Result<Vec<Event>, ScheduleEvaluatorError> {
        self.store
            .read_history(&self.coordinator_workflow_id)
            .await
            .map_err(|error| ScheduleEvaluatorError::side_effect(error.to_string()))
    }
}

#[async_trait]
impl ScheduleEventSink for AsyncMutex<Recorder> {
    async fn record_schedule_triggered(
        &self,
        schedule_id: &ScheduleId,
        execution: &ScheduleExecution,
        recorded_at: DateTime<Utc>,
    ) -> Result<(), ScheduleEvaluatorError> {
        self.lock()
            .await
            .record_schedule_triggered(
                recorded_at,
                schedule_id.clone(),
                execution.workflow_id.clone(),
                execution.run_id.clone(),
            )
            .await
            .map_err(|error| ScheduleEvaluatorError::side_effect(error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aion_core::{Event, Payload, WorkflowFilter, WorkflowStatus};
    use aion_package::ContentHash;
    use aion_store::visibility::VisibilityStore;
    use aion_store::{EventStore, InMemoryStore};
    use serde_json::json;

    use super::{DelegatedSeams, Engine, EngineComponents};
    use crate::durability::Recorder;
    use crate::lifecycle::terminate::{self, TerminateWorkflowContext};
    use crate::registry::{CompletionNotifier, HandleResidency, WorkflowHandleParts};
    use crate::{
        EngineError, LoadedWorkflows, Registry, RuntimeConfig, RuntimeHandle, SupervisionTree,
        WorkflowHandle,
    };

    fn payload(label: &str) -> Result<Payload, aion_core::PayloadError> {
        Payload::from_json(&json!({ "label": label }))
    }

    fn workflow_error(message: &str) -> aion_core::WorkflowError {
        aion_core::WorkflowError {
            message: message.to_owned(),
            details: None,
        }
    }

    fn loaded_workflows(workflow_type: &str, deployed_module: &str) -> LoadedWorkflows {
        let mut loaded = LoadedWorkflows::new();
        loaded.note_loaded_workflow_for_test(
            workflow_type,
            deployed_module,
            "run",
            ContentHash::from_bytes([5; 32]),
        );
        loaded
    }

    fn engine_with_loaded_workflow(
        store: Arc<dyn EventStore>,
        workflow_type: &str,
        deployed_module: &str,
    ) -> Result<Engine, EngineError> {
        let runtime = RuntimeHandle::new(RuntimeConfig::new(Some(1)))?;
        runtime.register_waiting_test_module(deployed_module, "run");
        let visibility_store: Arc<dyn VisibilityStore> = Arc::new(InMemoryStore::default());
        Ok(Engine::new(EngineComponents {
            store,
            visibility_store,
            runtime: Arc::new(runtime),
            loaded_workflows: loaded_workflows(workflow_type, deployed_module),
            registry: Registry::default(),
            supervision: SupervisionTree::new(),
            delegated: DelegatedSeams::default(),
            signal_handoff: Arc::new(crate::signal::SignalResumeHandoff::new()),
        }))
    }

    fn termination_context(engine: &Engine) -> TerminateWorkflowContext<'_> {
        TerminateWorkflowContext {
            runtime: engine.runtime(),
            store: engine.store(),
            visibility_store: engine.visibility_store(),
            registry: engine.registry(),
        }
    }

    async fn insert_active_handle(
        engine: &Engine,
        store: Arc<dyn EventStore>,
        workflow_type: &str,
    ) -> Result<WorkflowHandle, Box<dyn std::error::Error>> {
        let workflow_id = aion_core::WorkflowId::new_v4();
        let run_id = aion_core::RunId::new_v4();
        let mut recorder = Recorder::new(workflow_id.clone(), store);
        recorder
            .record_workflow_started(
                chrono::Utc::now(),
                workflow_type.to_owned(),
                payload("input")?,
                run_id.clone(),
            )
            .await?;
        let pid = engine.runtime().spawn_test_process_with_trap_exit(true)?;
        let handle = WorkflowHandle::new(WorkflowHandleParts {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
            pid,
            workflow_type: workflow_type.to_owned(),
            loaded_version: ContentHash::from_bytes([9; 32]),
            cached_status: WorkflowStatus::Running,
            residency: HandleResidency::Resident,
            recorder,
            completion: CompletionNotifier::new(),
        });
        engine
            .registry()
            .insert((workflow_id, run_id), handle.clone())?;
        Ok(handle)
    }

    #[tokio::test]
    async fn start_then_cancel_records_started_then_cancelled()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let handle = engine.start_workflow("checkout", payload("input")?).await?;

        engine
            .cancel(
                handle.workflow_id(),
                handle.run_id(),
                "caller requested cancellation",
            )
            .await?;

        let history = store.read_history(handle.workflow_id()).await?;
        match history.as_slice() {
            [
                Event::WorkflowStarted { .. },
                Event::WorkflowCancelled { reason, .. },
            ] => {
                assert_eq!(reason, "caller requested cancellation");
            }
            other => return Err(format!("expected started then cancelled, found {other:?}").into()),
        }
        engine.shutdown()?;
        Ok(())
    }

    #[tokio::test]
    async fn result_returns_completed_payload() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let handle = engine.start_workflow("checkout", payload("input")?).await?;
        let result_payload = payload("result")?;

        terminate::complete(
            termination_context(&engine),
            handle.workflow_id(),
            handle.run_id(),
            result_payload.clone(),
        )
        .await?;

        assert_eq!(
            engine.result(handle.workflow_id(), handle.run_id()).await?,
            Ok(result_payload)
        );
        engine.shutdown()?;
        Ok(())
    }

    #[tokio::test]
    async fn result_returns_failed_workflow_error() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let handle = engine.start_workflow("checkout", payload("input")?).await?;
        let error = workflow_error("workflow failed");

        terminate::fail(
            termination_context(&engine),
            handle.workflow_id(),
            handle.run_id(),
            error.clone(),
        )
        .await?;

        assert_eq!(
            engine.result(handle.workflow_id(), handle.run_id()).await?,
            Err(error)
        );
        engine.shutdown()?;
        Ok(())
    }

    #[tokio::test]
    async fn result_unknown_workflow_returns_not_found() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine = engine_with_loaded_workflow(store, "checkout", "checkout_deployed")?;
        let workflow_id = aion_core::WorkflowId::new_v4();
        let run_id = aion_core::RunId::new_v4();

        let result = engine.result(&workflow_id, &run_id).await;

        assert!(matches!(result, Err(EngineError::WorkflowNotFound { .. })));
        engine.shutdown()?;
        Ok(())
    }

    #[tokio::test]
    async fn continue_as_new_unknown_workflow_returns_not_found()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine = engine_with_loaded_workflow(store, "checkout", "checkout_deployed")?;
        let workflow_id = aion_core::WorkflowId::new_v4();
        let run_id = aion_core::RunId::new_v4();

        let result = engine
            .continue_as_new(&workflow_id, &run_id, payload("next")?, None)
            .await;

        assert!(matches!(result, Err(EngineError::WorkflowNotFound { .. })));
        engine.shutdown()?;
        Ok(())
    }

    #[tokio::test]
    async fn list_workflows_merges_live_and_terminal_without_duplicates()
    -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let running = insert_active_handle(&engine, Arc::clone(&store), "checkout").await?;
        let completed = engine.start_workflow("checkout", payload("input")?).await?;
        terminate::complete(
            termination_context(&engine),
            completed.workflow_id(),
            completed.run_id(),
            payload("result")?,
        )
        .await?;

        let summaries = engine.list_workflows(WorkflowFilter::default()).await?;
        assert_eq!(summaries.len(), 2);
        assert!(summaries.iter().any(|summary| {
            &summary.workflow_id == running.workflow_id()
                && summary.status == WorkflowStatus::Running
        }));
        assert!(summaries.iter().any(|summary| {
            &summary.workflow_id == completed.workflow_id()
                && summary.status == WorkflowStatus::Completed
        }));

        let completed_only = engine
            .list_workflows(WorkflowFilter {
                status: Some(WorkflowStatus::Completed),
                ..WorkflowFilter::default()
            })
            .await?;
        assert_eq!(completed_only.len(), 1);
        assert_eq!(&completed_only[0].workflow_id, completed.workflow_id());
        engine.shutdown()?;
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_rejects_subsequent_starts() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let handle = engine.start_workflow("checkout", payload("input")?).await?;
        terminate::complete(
            termination_context(&engine),
            handle.workflow_id(),
            handle.run_id(),
            payload("result")?,
        )
        .await?;

        engine.shutdown()?;
        let result = engine
            .start_workflow("checkout", payload("after-shutdown")?)
            .await;

        assert!(matches!(result, Err(EngineError::ShuttingDown)));
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let handle = engine.start_workflow("checkout", payload("input")?).await?;
        terminate::complete(
            termination_context(&engine),
            handle.workflow_id(),
            handle.run_id(),
            payload("result")?,
        )
        .await?;

        engine.shutdown()?;
        let second = engine.shutdown();

        assert!(
            second.is_ok(),
            "double shutdown should succeed; got {second:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn shutdown_rejects_schedule_creation() -> Result<(), Box<dyn std::error::Error>> {
        let store: Arc<dyn EventStore> = Arc::new(InMemoryStore::default());
        let engine =
            engine_with_loaded_workflow(Arc::clone(&store), "checkout", "checkout_deployed")?;
        let handle = engine.start_workflow("checkout", payload("input")?).await?;
        terminate::complete(
            termination_context(&engine),
            handle.workflow_id(),
            handle.run_id(),
            payload("result")?,
        )
        .await?;
        engine.shutdown()?;

        let config = aion_core::ScheduleConfig {
            trigger: aion_core::TriggerSpec::Interval {
                period: std::time::Duration::from_secs(60),
            },
            overlap_policy: aion_core::OverlapPolicy::Skip,
            catch_up_policy: aion_core::CatchUpPolicy::Skip,
            workflow_type: String::from("checkout"),
            input: payload("scheduled")?,
        };
        let result = engine.create_schedule(config).await;

        assert!(
            matches!(result, Err(EngineError::ShuttingDown)),
            "create_schedule after shutdown should return ShuttingDown; got {result:?}"
        );
        Ok(())
    }
}
