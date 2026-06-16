//! NIF bridge dispatcher that routes `run_activity` calls to connected workers.
//!
//! `WorkerActivityDispatcher` implements `aion::ActivityDispatcher` so the
//! engine's activity NIFs can synchronously dispatch to a remote worker and
//! block until the result comes back.
//!
//! # Threading contract
//!
//! The engine invokes [`aion::ActivityDispatcher::dispatch`] from two kinds of
//! threads: beamr scheduler threads (concurrency combinators) and spawned
//! tokio tasks (the two-phase `dispatch_activity` completion task). The task
//! send uses `try_send()` (non-blocking channel push) and the response wait
//! blocks on `std::sync::mpsc::Receiver::recv`.
//!
//! Blocking is harmless on a beamr thread, but on a tokio runtime worker it
//! must be wrapped in `tokio::task::block_in_place`: the `try_send` wakes the
//! per-worker gRPC stream forwarder task, and tokio schedules a task woken
//! from task context into the *current* worker's LIFO slot, which no other
//! runtime worker can steal. Without the `block_in_place` core handoff the
//! forwarder sits trapped in that slot while this thread blocks, so the queued
//! `ActivityTask` is never flushed to the worker even though the worker is
//! healthy. `block_in_place` moves the worker's scheduler core (LIFO slot
//! included) to another thread before the wait begins, so dispatch-to-delivery
//! stays in the millisecond range and the runtime keeps full parallelism.
//!
//! # Wait termination
//!
//! The engine imposes no activity timeout of its own: agent-style activities
//! legitimately run for over an hour, so the completion wait is unbounded.
//! The blocking `recv` terminates on exactly one of:
//!
//! - **Completion** — the worker reports a result and the stream handler
//!   delivers it through [`ActivityCompletionSink::complete_activity`].
//! - **Worker loss** — the worker's gRPC stream ends (process death,
//!   disconnect, expired token); the stream teardown sweeps the worker's
//!   in-flight tasks through the same sink as retryable lost-worker
//!   failures ([`HeartbeatTracker::fail_disconnected_worker`]).
//! - **Drain timeout at shutdown** — the shutdown coordinator fails all
//!   remaining in-flight tasks through the sink
//!   (`HeartbeatTracker::fail_all_in_flight_workers`).
//! - **Channel teardown** — every sender for the pending entry is dropped
//!   (a cleanup path removed the entry without completing it); surfaced as
//!   a channel-closed dispatch error, never a hang.
//!
//! An activity's duration is bounded only by the workflow's own
//! `timeout_seconds` and by worker liveness — never by an engine constant.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use aion::{ActivityDispatch, ActivityDispatcher};
use aion_core::{ActivityId, WorkflowId};
use aion_proto::{ProtoActivityId, ProtoActivityTask, ProtoPayload, ProtoWorkflowId};

use super::heartbeat::{HeartbeatTracker, InFlightActivity};
use super::pending::{
    ActivityDispatchContext, PendingActivities, SyncReceiver, log_activity_completion,
};
use super::registry::{ConnectedWorkerRegistry, WorkerHandle, WorkerId, WorkerMessage};
use crate::shutdown::DrainState;
use tracing::info_span;

/// Dispatcher that routes `run_activity` NIF calls to connected workers.
///
/// Synchronous interface — uses `try_send` for the task channel and
/// `std::sync::mpsc::Receiver::recv` for the response. Callers on a
/// multi-thread tokio runtime are detected and moved into
/// `tokio::task::block_in_place` so the blocking wait never starves the
/// runtime tasks that flush the worker stream (see the module docs).
pub struct WorkerActivityDispatcher {
    registry: ConnectedWorkerRegistry,
    namespace: String,
    pending: PendingActivities,
    heartbeat_tracker: HeartbeatTracker,
    drain_state: DrainState,
    tokio_handle: Option<tokio::runtime::Handle>,
}

impl std::fmt::Debug for WorkerActivityDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerActivityDispatcher")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl WorkerActivityDispatcher {
    /// Build a dispatcher for the given namespace, worker registry, and
    /// liveness tracker.
    ///
    /// The tracker must be the same instance the worker stream handler and
    /// shutdown coordinator share: the unbounded completion wait relies on
    /// stream teardown sweeping this tracker's in-flight entries to fail
    /// dispatches whose worker was lost.
    #[must_use]
    pub fn new(
        registry: ConnectedWorkerRegistry,
        namespace: impl Into<String>,
        heartbeat_tracker: HeartbeatTracker,
    ) -> Self {
        Self {
            registry,
            namespace: namespace.into(),
            pending: PendingActivities::default(),
            heartbeat_tracker,
            drain_state: DrainState::default(),
            tokio_handle: None,
        }
    }

    /// Share a caller-supplied pending-activities tracker.
    #[must_use]
    pub fn with_pending(mut self, pending: PendingActivities) -> Self {
        self.pending = pending;
        self
    }

    /// Share the server drain gate.
    #[must_use]
    pub fn with_drain_state(mut self, drain_state: DrainState) -> Self {
        self.drain_state = drain_state;
        self
    }

    /// Share the server runtime handle for sync history writes from dirty NIF threads.
    #[must_use]
    pub fn with_tokio_handle(mut self, tokio_handle: tokio::runtime::Handle) -> Self {
        self.tokio_handle = Some(tokio_handle);
        self
    }
}

impl WorkerActivityDispatcher {
    fn ensure_accepting(
        &self,
        namespace: &str,
        activity_type: &str,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
        worker_id: Option<WorkerId>,
    ) -> Result<(), String> {
        self.drain_state
            .ensure_accepting(namespace, activity_type)
            .map_err(|error| {
                let reason = error.to_string();
                log_worker_error(
                    "WorkerDispatch",
                    namespace,
                    activity_type,
                    workflow_id,
                    activity_id,
                    worker_id,
                    &reason,
                );
                reason
            })
    }

    /// Obtain the rotation-ordered candidate workers for the namespace and
    /// activity type, waiting if none is currently registered. Blocks until at
    /// least one matching worker registers or the server begins draining.
    ///
    /// The returned list is never empty; ordering is the registry's
    /// per-`(namespace, activity_type)` round-robin so successive dispatches
    /// start at different workers.
    fn candidates_or_wait(
        &self,
        namespace: &str,
        activity_type: &str,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) -> Result<Vec<WorkerHandle>, String> {
        loop {
            match self.registry.workers_for(namespace, activity_type) {
                Ok(workers) if !workers.is_empty() => return Ok(workers),
                Ok(_) => {
                    self.ensure_accepting(
                        namespace,
                        activity_type,
                        workflow_id,
                        activity_id,
                        None,
                    )?;
                    tracing::info!(
                        namespace,
                        activity_type,
                        workflow_id = %workflow_id,
                        activity_id = %activity_id,
                        "no connected worker; waiting for a matching worker to register"
                    );
                    self.wait_for_worker();
                }
                Err(error) => {
                    let reason = format!("registry error: {error}");
                    log_worker_error(
                        "WorkerRegistry",
                        namespace,
                        activity_type,
                        workflow_id,
                        activity_id,
                        None,
                        &reason,
                    );
                    return Err(reason);
                }
            }
        }
    }

    /// Block the calling thread until a new worker registers, using whichever
    /// runtime handle is available. Mirrors the dispatch blocking contract: on
    /// a beamr/plain thread with no runtime it falls back to a short sleep so
    /// the caller re-polls the registry.
    fn wait_for_worker(&self) {
        match &self.tokio_handle {
            Some(handle) => handle.block_on(self.registry.wait_for_worker()),
            None => match tokio::runtime::Handle::try_current() {
                Ok(handle) => handle.block_on(self.registry.wait_for_worker()),
                Err(_) => std::thread::sleep(Duration::from_millis(500)),
            },
        }
    }

    fn track_worker_task(
        &self,
        worker_id: WorkerId,
        activity_type: &str,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) -> Result<(), String> {
        self.heartbeat_tracker
            .track_task(
                worker_id,
                InFlightActivity {
                    workflow_id: workflow_id.clone(),
                    activity_id: activity_id.clone(),
                },
                Instant::now(),
            )
            .map_err(|error| {
                let reason = error.to_string();
                log_worker_error(
                    "WorkerHeartbeatTracker",
                    &self.namespace,
                    activity_type,
                    workflow_id,
                    activity_id,
                    Some(worker_id),
                    &reason,
                );
                reason
            })
    }

    fn cleanup_activity(
        &self,
        worker_id: WorkerId,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) {
        self.pending.remove(workflow_id, activity_id);
        let _ = self
            .heartbeat_tracker
            .complete_task(worker_id, workflow_id, activity_id);
        self.drain_state.notify_activity_drained();
    }

    /// Place the activity with a connected worker, round-robining across the
    /// rotation-ordered candidates and falling back past any whose channel is
    /// full or closed. Returns the worker that accepted the task.
    ///
    /// The pending completion entry is registered by the caller before this
    /// runs and is shared across attempts — it is keyed by the execution
    /// `(workflow_id, activity_id)`, not by worker — so a failed attempt rolls
    /// back only that worker's heartbeat tracking and the shared entry survives
    /// for the next candidate. The drain gate is re-checked before every
    /// attempt. On any failure to place the task — no candidate accepted it, the
    /// drain gate closed (including while waiting for a worker), or a registry
    /// or tracker error — the shared pending entry is abandoned exactly once so
    /// it can never outlive a dispatch that never reached a worker.
    fn place_activity(
        &self,
        namespace: &str,
        activity_type: &str,
        task: &ProtoActivityTask,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) -> Result<WorkerId, String> {
        self.place_on_candidate(namespace, activity_type, task, workflow_id, activity_id)
            .inspect_err(|_| self.abandon_pending(workflow_id, activity_id))
    }

    /// Inner placement: obtain the candidate list (waiting if needed) and try
    /// each until one accepts. Every error path returns through here so the
    /// single `abandon_pending` in [`place_activity`] cleans up the shared
    /// pending entry — including the `candidates_or_wait` drain/registry errors
    /// that the pending entry is already live for.
    fn place_on_candidate(
        &self,
        namespace: &str,
        activity_type: &str,
        task: &ProtoActivityTask,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) -> Result<WorkerId, String> {
        let candidates =
            self.candidates_or_wait(namespace, activity_type, workflow_id, activity_id)?;
        match self.try_candidates(&candidates, activity_type, task, workflow_id, activity_id)? {
            Some(worker_id) => Ok(worker_id),
            None => Err(self.all_workers_closed(activity_type, workflow_id, activity_id)),
        }
    }

    /// Try each rotation-ordered candidate until one accepts the task.
    ///
    /// Returns `Ok(Some(worker_id))` for the worker that accepted, `Ok(None)`
    /// when every candidate's channel was closed, or `Err` if the drain gate
    /// closed or heartbeat tracking failed mid-iteration. A candidate is
    /// tracked immediately before its send so the heartbeat tracker reflects
    /// only the in-flight attempt; a closed channel rolls that tracking back
    /// and deregisters the worker before advancing.
    fn try_candidates(
        &self,
        candidates: &[WorkerHandle],
        activity_type: &str,
        task: &ProtoActivityTask,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) -> Result<Option<WorkerId>, String> {
        for worker in candidates {
            let worker_id = worker.id();
            self.ensure_accepting(
                &self.namespace,
                activity_type,
                workflow_id,
                activity_id,
                Some(worker_id),
            )?;
            self.track_worker_task(worker_id, activity_type, workflow_id, activity_id)?;
            match worker
                .sender()
                .try_send(WorkerMessage::ActivityTask(task.clone()))
            {
                Ok(()) => return Ok(Some(worker_id)),
                Err(error) => {
                    let reason = format!("worker task channel full or closed: {error}");
                    self.fallback_past_closed_worker(
                        worker_id,
                        activity_type,
                        workflow_id,
                        activity_id,
                        &reason,
                    );
                }
            }
        }
        Ok(None)
    }

    /// Roll back a failed delivery attempt: untrack this worker's heartbeat
    /// entry (waking any drain waiter, since in-flight accounting may now be
    /// zero) and deregister the worker whose channel is gone so the rotation
    /// never offers it again. The shared pending entry is deliberately left in
    /// place for the next candidate.
    fn fallback_past_closed_worker(
        &self,
        worker_id: WorkerId,
        activity_type: &str,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
        reason: &str,
    ) {
        let _ = self
            .heartbeat_tracker
            .complete_task(worker_id, workflow_id, activity_id);
        self.drain_state.notify_activity_drained();
        log_worker_error(
            "WorkerChannelClosed",
            &self.namespace,
            activity_type,
            workflow_id,
            activity_id,
            Some(worker_id),
            reason,
        );
        if let Err(error) = self.registry.deregister(worker_id) {
            log_worker_error(
                "WorkerRegistry",
                &self.namespace,
                activity_type,
                workflow_id,
                activity_id,
                Some(worker_id),
                &format!("deregister after closed channel failed: {error}"),
            );
        }
    }

    /// Drop the shared pending completion entry for an execution that never
    /// reached a worker and wake any drain waiter. Used when placement gives up
    /// (drain closed, every candidate closed, or a registry/tracker error).
    fn abandon_pending(&self, workflow_id: &WorkflowId, activity_id: &ActivityId) {
        self.pending.remove(workflow_id, activity_id);
        self.drain_state.notify_activity_drained();
    }

    /// Build and log the error returned when every rotation candidate's channel
    /// was closed before the task could be delivered.
    ///
    /// Deliberately unprefixed: this is an engine-level "could not place the
    /// work anywhere" failure, which the SDK maps to an activity engine
    /// failure — the same classification the single-worker closed-channel path
    /// produced before this became a fallback loop. Reclassifying worker
    /// exhaustion as retryable is the workflow-resilience work's call, not this
    /// brief's.
    fn all_workers_closed(
        &self,
        activity_type: &str,
        workflow_id: &WorkflowId,
        activity_id: &ActivityId,
    ) -> String {
        let reason = "all matching worker streams closed before task could be delivered".to_owned();
        log_worker_error(
            "WorkerChannelClosed",
            &self.namespace,
            activity_type,
            workflow_id,
            activity_id,
            None,
            &reason,
        );
        reason
    }

    /// Block until the dispatch terminates (see the module docs for the
    /// exhaustive termination list). The wait is deliberately unbounded:
    /// the engine imposes no activity timeout of its own.
    fn await_activity_result(
        &self,
        context: &ActivityDispatchContext<'_>,
        rx: &SyncReceiver,
    ) -> Result<String, String> {
        // Close the dispatch/disconnect race before blocking. A worker whose
        // stream tore down *before* this dispatch tracked its task was swept
        // without this entry, so nothing would ever deliver through `rx`.
        // `fail_lost_worker` deregisters before it collects tasks, and this
        // dispatch tracked its task before sending, so: if the worker is
        // still registered here, any later sweep is guaranteed to include
        // this task and unblock the `recv` below.
        match self.registry.is_registered(context.worker_id) {
            Ok(true) => {}
            Ok(false) => {
                // A sweep that did include this task may have delivered
                // already; prefer its verdict (or a genuine result that
                // raced the disconnect) over fabricating one.
                if let Ok(result) = rx.try_recv() {
                    return self.deliver_result(context, result);
                }
                self.cleanup_activity(context.worker_id, context.workflow_id, context.activity_id);
                let reason = format!(
                    "retryable:{}",
                    super::dispatch::lost_worker_error(context.worker_id).message
                );
                log_worker_error(
                    "WorkerLost",
                    &self.namespace,
                    context.activity_type,
                    context.workflow_id,
                    context.activity_id,
                    Some(context.worker_id),
                    &reason,
                );
                return Err(reason);
            }
            Err(error) => {
                self.cleanup_activity(context.worker_id, context.workflow_id, context.activity_id);
                let reason = format!("worker registry inspection failed: {error}");
                log_worker_error(
                    "WorkerRegistry",
                    &self.namespace,
                    context.activity_type,
                    context.workflow_id,
                    context.activity_id,
                    Some(context.worker_id),
                    &reason,
                );
                return Err(reason);
            }
        }
        if let Ok(result) = rx.recv() {
            return self.deliver_result(context, result);
        }
        // Every sender was dropped without completing: a cleanup path
        // removed the pending entry. Surface it instead of hanging.
        self.cleanup_activity(context.worker_id, context.workflow_id, context.activity_id);
        let reason = "activity response channel dropped".to_owned();
        log_worker_error(
            "WorkerChannelClosed",
            &self.namespace,
            context.activity_type,
            context.workflow_id,
            context.activity_id,
            Some(context.worker_id),
            &reason,
        );
        Err(reason)
    }

    fn deliver_result(
        &self,
        context: &ActivityDispatchContext<'_>,
        result: Result<String, String>,
    ) -> Result<String, String> {
        self.pending
            .remove(context.workflow_id, context.activity_id);
        log_activity_completion(context, result.is_ok());
        result.inspect_err(|reason| {
            log_worker_error(
                "ActivityFailed",
                &self.namespace,
                context.activity_type,
                context.workflow_id,
                context.activity_id,
                Some(context.worker_id),
                reason,
            );
        })
    }
}

impl ActivityDispatcher for WorkerActivityDispatcher {
    fn dispatch(&self, request: ActivityDispatch) -> Result<String, String> {
        match tokio::runtime::Handle::try_current() {
            Ok(handle) => match handle.runtime_flavor() {
                tokio::runtime::RuntimeFlavor::MultiThread => {
                    // We are inside a tokio runtime (the engine spawns the
                    // sync dispatch onto its handle). Hand this worker's
                    // scheduler core to another thread before blocking so the
                    // stream forwarder woken by our `try_send` can actually
                    // run — otherwise it is trapped in this worker's
                    // non-stealable LIFO slot for as long as we block.
                    tokio::task::block_in_place(|| self.dispatch_blocking(request))
                }
                flavor => Err(format!(
                    "activity dispatch blocks the calling thread until the worker responds; \
                     a {flavor:?} tokio runtime cannot host that wait because the worker \
                     stream forwarder shares its only executor thread and the task could \
                     never be delivered — run the engine on a multi-thread tokio runtime"
                )),
            },
            // No tokio context: a beamr scheduler thread or other plain OS
            // thread. Blocking here is the designed contract and cannot starve
            // the server runtime.
            Err(_) => self.dispatch_blocking(request),
        }
    }
}

impl WorkerActivityDispatcher {
    /// Dispatch the activity and block the calling thread until the worker
    /// responds, the worker is declared lost, or the server drains (see the
    /// module docs for the exhaustive termination list).
    ///
    /// The request carries the *real* workflow and activity ids the engine
    /// recorded in history, so the worker logs, the pending-completion key,
    /// and the heartbeat tracker all correlate directly against the event
    /// store. `config` is forwarded by the engine seam but not yet consumed
    /// here (the retry executor that reads it is unbuilt).
    ///
    /// Must never run while the calling thread still owns a tokio scheduler
    /// core: the response can only arrive after the runtime's stream
    /// forwarder flushes the queued [`WorkerMessage::ActivityTask`] to the
    /// worker, so the thread blocking here must not be the one responsible
    /// for polling that forwarder. [`ActivityDispatcher::dispatch`] enforces
    /// this with `tokio::task::block_in_place`.
    fn dispatch_blocking(&self, request: ActivityDispatch) -> Result<String, String> {
        let ActivityDispatch {
            namespace,
            workflow_id,
            activity_id,
            name,
            input,
            config: _,
            attempt,
            labels,
        } = request;
        let started_at = Instant::now();
        self.ensure_accepting(&namespace, &name, &workflow_id, &activity_id, None)?;

        // Register the shared completion entry before placement so a fast
        // worker result can never arrive before the receiver exists. Placement
        // round-robins across the rotation-ordered candidates and falls back
        // past closed channels; on success the chosen worker is tracked in the
        // heartbeat tracker, on exhaustion this entry is abandoned for us.
        let task = activity_task(&name, &input, &workflow_id, &activity_id, attempt, labels);
        let rx = self
            .pending
            .insert(workflow_id.clone(), activity_id.clone());
        let worker_id =
            self.place_activity(&namespace, &name, &task, &workflow_id, &activity_id)?;

        let span = info_span!(
            "activity_dispatch",
            operation = "activity_dispatch",
            namespace = %namespace,
            workflow_id = %workflow_id,
            activity_id = %activity_id,
            activity_type = %name,
            worker_id = ?worker_id,
        );
        let _span_guard = span.enter();
        let context = ActivityDispatchContext {
            namespace: &namespace,
            activity_type: &name,
            worker_id,
            workflow_id: &workflow_id,
            activity_id: &activity_id,
            started_at,
        };
        self.await_activity_result(&context, &rx)
    }
}

fn activity_task(
    activity_type: &str,
    input: &str,
    workflow_id: &WorkflowId,
    activity_id: &ActivityId,
    attempt: u32,
    labels: BTreeMap<String, String>,
) -> ProtoActivityTask {
    ProtoActivityTask {
        workflow_id: Some(ProtoWorkflowId::from(workflow_id.clone())),
        activity_id: Some(ProtoActivityId::from(activity_id.clone())),
        activity_type: activity_type.to_owned(),
        input: Some(ProtoPayload {
            content_type: String::from("application/json"),
            bytes: input.as_bytes().to_vec(),
        }),
        attempt,
        labels: labels.into_iter().collect(),
    }
}

fn log_worker_error(
    error_type: &'static str,
    namespace: &str,
    activity_type: &str,
    workflow_id: &WorkflowId,
    activity_id: &ActivityId,
    worker_id: Option<super::registry::WorkerId>,
    reason: &str,
) {
    tracing::error!(
        operation = "activity_dispatch",
        namespace,
        workflow_id = %workflow_id,
        activity_id = %activity_id,
        activity_type,
        worker_id = ?worker_id,
        error_type,
        reason,
        "worker interaction failed"
    );
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use aion_core::{ContentType, Payload};

    use super::super::dispatch::{
        ActivityCompletion, ActivityCompletionOutcome, ActivityCompletionSink,
    };
    use super::super::registry::WorkerRegistration;
    use super::*;
    use crate::error::ServerError;

    /// Liveness tracker for dispatcher unit tests; the window only matters
    /// to expiry checks, which nothing in these tests drives.
    fn test_tracker() -> HeartbeatTracker {
        HeartbeatTracker::new(Duration::from_secs(5))
    }

    /// A `greet` dispatch request carrying real (test-synthesized) ids, the
    /// engine-seam shape `WorkerActivityDispatcher::dispatch` now consumes.
    fn greet_request() -> ActivityDispatch {
        ActivityDispatch {
            namespace: "default".to_owned(),
            workflow_id: WorkflowId::new_v4(),
            activity_id: ActivityId::from_sequence_position(0),
            name: "greet".to_owned(),
            input: "{}".to_owned(),
            config: "{}".to_owned(),
            attempt: 1,
            labels: std::collections::BTreeMap::new(),
        }
    }

    #[test]
    fn dispatcher_fails_immediately_when_draining_without_workers() {
        let registry = ConnectedWorkerRegistry::default();
        let drain = DrainState::default();
        let dispatcher = WorkerActivityDispatcher::new(registry, "default", test_tracker())
            .with_drain_state(drain.clone());

        let _ = drain.begin();

        let result = dispatcher.dispatch(greet_request());

        assert!(result.is_err());
        let err = result.err().unwrap_or_default();
        assert!(
            err.contains("drain"),
            "expected drain rejection, got: {err}"
        );
    }

    /// Regression test for the production stall where every remote activity
    /// timed out: the engine invoked the sync `dispatch` from inside a
    /// spawned tokio task (`futures::future::lazy` polled on a runtime
    /// worker), and the woken stream-consumer task landed in that blocked
    /// worker's non-stealable LIFO slot, so the queued `ActivityTask` was
    /// only delivered when the then-extant 30s dispatch timeout fired (the
    /// dispatch wait is unbounded today; the stall would now be a hang).
    ///
    /// Mirrors the real wiring minus tonic: the real registry channel that
    /// the gRPC stream forwarder drains, a worker task awaiting that channel
    /// on the same runtime, completion through the production
    /// `ActivityCompletionSink`, and the sync dispatch invoked from a
    /// runtime worker task — the worst case the `block_in_place` guard in
    /// `dispatch` defends against (the engine itself now routes through
    /// `dispatch_async`, off the async workers).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dispatch_inside_runtime_task_delivers_promptly_and_round_trips()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(32);
        let activity_types = [String::from("greet")];
        let registration = registry.register("default", activity_types.iter(), worker_tx)?;

        let sink = pending.clone();
        let echo_worker = tokio::spawn(async move {
            let Some(WorkerMessage::ActivityTask(task)) = worker_rx.recv().await else {
                return Err("expected an activity task on the worker channel".to_owned());
            };
            let workflow_id = task
                .workflow_id
                .ok_or("task missing workflow id")
                .and_then(|id| WorkflowId::try_from(id).map_err(|_| "bad workflow id"))?;
            let activity_id = task
                .activity_id
                .map(ActivityId::from)
                .ok_or("task missing activity id")?;
            sink.complete_activity(ActivityCompletion {
                workflow_id,
                activity_id,
                outcome: ActivityCompletionOutcome::Succeeded(Payload::new(
                    ContentType::Json,
                    br#"{"greeting":"hello"}"#.to_vec(),
                )),
            })
            .map_err(|error| error.to_string())
        });

        let dispatcher = Arc::new(
            WorkerActivityDispatcher::new(registry, "default", test_tracker())
                .with_pending(pending),
        );
        let started = Instant::now();
        // Invoke the sync dispatch inside the first poll of a spawned task:
        // the worst-case calling context for the `block_in_place` guard.
        let dispatch_task = tokio::spawn(futures::future::lazy(move |_| {
            dispatcher.dispatch(greet_request())
        }));
        let result = dispatch_task.await.map_err(|error| error.to_string())?;
        let elapsed = started.elapsed();

        assert_eq!(result, Ok(r#"{"greeting":"hello"}"#.to_owned()));
        assert!(
            elapsed < Duration::from_secs(5),
            "dispatch round trip took {elapsed:?}; task delivery must not \
             depend on the blocked dispatch thread"
        );
        echo_worker.await.map_err(|error| error.to_string())??;
        registration.deregister()?;
        Ok(())
    }

    /// A current-thread runtime cannot host the blocking wait (the stream
    /// forwarder would share its only executor thread), so dispatch must
    /// fail fast with a precise error instead of blocking forever.
    #[tokio::test]
    async fn dispatch_on_current_thread_runtime_fails_fast()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let (worker_tx, _worker_rx) = tokio::sync::mpsc::channel(32);
        let activity_types = [String::from("greet")];
        let registration = registry.register("default", activity_types.iter(), worker_tx)?;
        let dispatcher = WorkerActivityDispatcher::new(registry, "default", test_tracker());

        let started = Instant::now();
        let result = dispatcher.dispatch(greet_request());
        let elapsed = started.elapsed();

        let err = result.err().ok_or("expected dispatch to fail")?;
        assert!(
            err.contains("multi-thread tokio runtime"),
            "unexpected error: {err}"
        );
        assert!(
            elapsed < Duration::from_secs(5),
            "fail-fast path took {elapsed:?}"
        );
        registration.deregister()?;
        Ok(())
    }

    /// Register a live `greet` worker whose stream echoes every task back as a
    /// success through the shared completion sink, and count the tasks it
    /// receives. The returned registration keeps the worker connected until the
    /// caller drops or deregisters it (which closes the stream and ends the
    /// echo task).
    fn spawn_echo_worker(
        registry: &ConnectedWorkerRegistry,
        pending: &PendingActivities,
    ) -> Result<
        (
            Arc<AtomicUsize>,
            WorkerRegistration,
            tokio::task::JoinHandle<()>,
        ),
        ServerError,
    > {
        let (worker_tx, mut worker_rx) = tokio::sync::mpsc::channel(8);
        let activity_types = [String::from("greet")];
        let registration = registry.register("default", activity_types.iter(), worker_tx)?;
        let counter = Arc::new(AtomicUsize::new(0));
        let task_counter = Arc::clone(&counter);
        let sink = pending.clone();
        let echo = tokio::spawn(async move {
            while let Some(WorkerMessage::ActivityTask(task)) = worker_rx.recv().await {
                task_counter.fetch_add(1, Ordering::SeqCst);
                let workflow_id = task
                    .workflow_id
                    .and_then(|id| WorkflowId::try_from(id).ok());
                let activity_id = task.activity_id.map(ActivityId::from);
                if let (Some(workflow_id), Some(activity_id)) = (workflow_id, activity_id) {
                    let _ = sink.complete_activity(ActivityCompletion {
                        workflow_id,
                        activity_id,
                        outcome: ActivityCompletionOutcome::Succeeded(Payload::new(
                            ContentType::Json,
                            b"{}".to_vec(),
                        )),
                    });
                }
            }
        });
        Ok((counter, registration, echo))
    }

    /// Round-robin reaches the live dispatch path: three connected workers and
    /// three same-type dispatches put exactly one task on each worker, because
    /// the registry advances its rotation on every placement. This is the
    /// production seam (`WorkerActivityDispatcher`), not the registry helper in
    /// isolation.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_dispatch_round_robins_one_task_per_worker()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();
        let mut counters = Vec::new();
        let mut registrations = Vec::new();
        let mut echoes = Vec::new();
        for _ in 0..3 {
            let (counter, registration, echo) = spawn_echo_worker(&registry, &pending)?;
            counters.push(counter);
            registrations.push(registration);
            echoes.push(echo);
        }

        let dispatcher = Arc::new(
            WorkerActivityDispatcher::new(registry, "default", test_tracker())
                .with_pending(pending),
        );
        for _ in 0..3 {
            let dispatcher = Arc::clone(&dispatcher);
            let result = tokio::spawn(futures::future::lazy(move |_| {
                dispatcher.dispatch(greet_request())
            }))
            .await?;
            assert!(result.is_ok(), "dispatch failed: {result:?}");
        }

        for (index, counter) in counters.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::SeqCst),
                1,
                "worker {index} should have received exactly one task"
            );
        }
        for registration in registrations {
            registration.deregister()?;
        }
        for echo in echoes {
            echo.await?;
        }
        Ok(())
    }

    /// Even distribution across the live path: six dispatches over three workers
    /// land two on each.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_dispatch_distributes_evenly() -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();
        let mut counters = Vec::new();
        let mut registrations = Vec::new();
        let mut echoes = Vec::new();
        for _ in 0..3 {
            let (counter, registration, echo) = spawn_echo_worker(&registry, &pending)?;
            counters.push(counter);
            registrations.push(registration);
            echoes.push(echo);
        }

        let dispatcher = Arc::new(
            WorkerActivityDispatcher::new(registry, "default", test_tracker())
                .with_pending(pending),
        );
        for _ in 0..6 {
            let dispatcher = Arc::clone(&dispatcher);
            let result = tokio::spawn(futures::future::lazy(move |_| {
                dispatcher.dispatch(greet_request())
            }))
            .await?;
            assert!(result.is_ok(), "dispatch failed: {result:?}");
        }

        for (index, counter) in counters.iter().enumerate() {
            assert_eq!(
                counter.load(Ordering::SeqCst),
                2,
                "worker {index} should have received exactly two tasks"
            );
        }
        for registration in registrations {
            registration.deregister()?;
        }
        for echo in echoes {
            echo.await?;
        }
        Ok(())
    }

    /// Closed-channel fallback on the live path: the rotation starts at a worker
    /// whose stream is closed, so the dispatcher deregisters it and delivers to
    /// the next candidate. The closed worker registered first, so it sorts to
    /// the rotation start of a fresh registry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_dispatch_falls_back_past_closed_channel() -> Result<(), Box<dyn std::error::Error>>
    {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();

        let (closed_tx, closed_rx) = tokio::sync::mpsc::channel(1);
        let activity_types = [String::from("greet")];
        let closed_registration = registry.register("default", activity_types.iter(), closed_tx)?;
        drop(closed_rx);
        let (live_counter, live_registration, live_echo) = spawn_echo_worker(&registry, &pending)?;

        let dispatcher = Arc::new(
            WorkerActivityDispatcher::new(registry.clone(), "default", test_tracker())
                .with_pending(pending),
        );
        let dispatch = Arc::clone(&dispatcher);
        let result = tokio::spawn(futures::future::lazy(move |_| {
            dispatch.dispatch(greet_request())
        }))
        .await?;

        assert!(
            result.is_ok(),
            "dispatch should fall back to the live worker: {result:?}"
        );
        assert_eq!(
            live_counter.load(Ordering::SeqCst),
            1,
            "the live worker received the task"
        );
        assert_eq!(
            registry.workers_for("default", "greet")?.len(),
            1,
            "the closed worker was deregistered"
        );

        closed_registration.deregister()?;
        live_registration.deregister()?;
        live_echo.await?;
        Ok(())
    }

    /// All-candidates-closed on the live path: every matching worker's stream is
    /// closed, so the dispatch fails with the canonical all-closed message, both
    /// workers are deregistered, and no pending entry or heartbeat tracking is
    /// leaked.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_dispatch_fails_when_all_candidates_closed()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();
        let tracker = test_tracker();
        let activity_types = [String::from("greet")];

        let (tx_a, rx_a) = tokio::sync::mpsc::channel(1);
        let (tx_b, rx_b) = tokio::sync::mpsc::channel(1);
        let registration_a = registry.register("default", activity_types.iter(), tx_a)?;
        let registration_b = registry.register("default", activity_types.iter(), tx_b)?;
        drop(rx_a);
        drop(rx_b);

        let dispatcher =
            WorkerActivityDispatcher::new(registry.clone(), "default", tracker.clone())
                .with_pending(pending.clone());
        let result = dispatcher.dispatch(greet_request());

        let err = result.err().ok_or("expected an all-closed failure")?;
        assert!(
            err.contains("all matching worker streams closed before task could be delivered"),
            "unexpected error: {err}"
        );
        assert!(
            registry.workers_for("default", "greet")?.is_empty(),
            "both closed workers were deregistered"
        );
        assert_eq!(
            tracker.in_flight_count()?,
            0,
            "no heartbeat tracking was leaked"
        );
        assert_eq!(pending.len(), 0, "no pending completion was leaked");

        registration_a.deregister()?;
        registration_b.deregister()?;
        Ok(())
    }

    /// The drain gate is re-checked before each delivery attempt: with the
    /// server draining and a worker registered, the dispatch fails with the
    /// drain error (not a channel error) and abandons the pending entry.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_dispatch_rechecks_drain_before_each_attempt()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();
        let drain = DrainState::default();
        let activity_types = [String::from("greet")];
        let (worker_tx, _worker_rx) = tokio::sync::mpsc::channel(1);
        let registration = registry.register("default", activity_types.iter(), worker_tx)?;

        let _ = drain.begin();
        let dispatcher = WorkerActivityDispatcher::new(registry, "default", test_tracker())
            .with_pending(pending.clone())
            .with_drain_state(drain);

        let result = dispatcher.dispatch(greet_request());

        let err = result.err().ok_or("expected a drain failure")?;
        assert!(err.contains("drain"), "expected drain error, got: {err}");
        assert_eq!(pending.len(), 0, "the pending completion was abandoned");
        registration.deregister()?;
        Ok(())
    }

    /// Regression: a dispatch that has already registered its pending entry and
    /// is parked waiting for a worker must not leak that entry when the server
    /// begins draining. The pending entry is inserted before placement (to win
    /// the fast-worker race), so the drain error raised while waiting has to
    /// abandon it. Drains with no matching worker, then wakes the waiter via an
    /// unrelated registration so it re-checks the gate with an empty candidate
    /// list — the `candidates_or_wait` drain-error path, distinct from the
    /// per-attempt check above.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_dispatch_parked_for_worker_abandons_pending_on_drain()
    -> Result<(), Box<dyn std::error::Error>> {
        let registry = ConnectedWorkerRegistry::default();
        let pending = PendingActivities::default();
        let drain = DrainState::default();

        let dispatcher = Arc::new(
            WorkerActivityDispatcher::new(registry.clone(), "default", test_tracker())
                .with_pending(pending.clone())
                .with_drain_state(drain.clone()),
        );
        let dispatch = Arc::clone(&dispatcher);
        let handle = tokio::spawn(futures::future::lazy(move |_| {
            dispatch.dispatch(greet_request())
        }));

        // Let the dispatch reach the wait-for-worker park with its pending entry
        // registered but no greet worker to place on.
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !handle.is_finished(),
            "dispatch should be waiting for a worker"
        );
        assert_eq!(pending.len(), 1, "pending entry registered while waiting");

        // Begin draining, then wake the parked waiter with an unrelated
        // registration so it re-evaluates the gate with greet still empty.
        let _ = drain.begin();
        let other_types = [String::from("other")];
        let (other_tx, _other_rx) = tokio::sync::mpsc::channel(1);
        let other = registry.register("default", other_types.iter(), other_tx)?;

        let result = handle.await?;
        let err = result.err().ok_or("expected a drain failure")?;
        assert!(err.contains("drain"), "expected drain error, got: {err}");
        assert_eq!(
            pending.len(),
            0,
            "the pending entry must be abandoned, not leaked, on the drain-while-waiting path"
        );

        other.deregister()?;
        Ok(())
    }
}
