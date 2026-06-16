//! Activity-result tracking: the completion side of the worker bridge.
//!
//! [`PendingActivities`] holds one in-flight entry per dispatched activity and
//! delivers the worker-reported result back to the blocked dispatch thread. The
//! dispatcher in [`super::bridge`] owns the send side (selecting a worker and
//! pushing the task); this module owns the receive side (matching the result to
//! its execution and waking the waiter), plus the completion logging that
//! records when a dispatch resolves.

use std::sync::Arc;
use std::time::{Duration, Instant};

use aion_core::{ActivityId, ContentType, Payload, WorkflowId};
use dashmap::DashMap;

use super::dispatch::{ActivityCompletion, ActivityCompletionOutcome, ActivityCompletionSink};
use super::registry::WorkerId;
use crate::error::ServerError;

type SyncSender = std::sync::mpsc::SyncSender<Result<String, String>>;

/// Receiver handed to the dispatch thread; it blocks on this until the worker
/// result, a lost-worker sweep, or a channel teardown resolves the dispatch.
pub(super) type SyncReceiver = std::sync::mpsc::Receiver<Result<String, String>>;

/// Execution-scoped key for an in-flight activity dispatch.
///
/// The engine seam ([`super::bridge`]) carries the *real* workflow id and the
/// *real* per-workflow activity ordinal recorded in history, so this pair
/// uniquely and stably identifies one execution. Keying by bare [`ActivityId`]
/// would be unsafe across server restarts — a stale result re-reported from a
/// worker's previous session could complete a *different* post-restart dispatch
/// reusing the same ordinal — but pairing it with the real workflow id closes
/// that race: two different workflow executions never share a workflow id, so a
/// stale `(workflow_id, activity_id)` from a previous server life can only ever
/// match the exact execution it belongs to.
///
/// The wire (`ActivityResult`) carries both ids, plus an attempt discriminator
/// (`ActivityTask.attempt`). The pending key stays attempt-free for now: a
/// retry re-dispatches under the same `(workflow_id, activity_id)` and the
/// outstanding entry is the one awaiting completion. Redelivery bookkeeping can
/// widen this key with the wire attempt later — no protocol change needed.
type PendingActivityKey = (WorkflowId, ActivityId);

/// Tracks in-flight activity dispatches waiting for worker results.
///
/// When the server's worker stream handler receives an `ActivityResult`, it
/// calls [`complete_activity`](ActivityCompletionSink::complete_activity) to
/// deliver the result to the blocked NIF thread. Entries are keyed by
/// [`PendingActivityKey`] so a stale result from a previous server life can
/// never be matched to a different execution (#59).
#[derive(Clone, Debug, Default)]
pub struct PendingActivities {
    pending: Arc<DashMap<PendingActivityKey, SyncSender>>,
}

impl PendingActivities {
    /// Register an execution and return the receiver the dispatch thread blocks
    /// on. Called before the task is pushed so a fast worker result can never
    /// arrive before the receiver exists.
    pub(super) fn insert(&self, workflow_id: WorkflowId, activity_id: ActivityId) -> SyncReceiver {
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.pending.insert((workflow_id, activity_id), tx);
        rx
    }

    /// Drop the entry for an execution that will never complete through this
    /// tracker (a dispatch that gave up before, or instead of, a worker result).
    pub(super) fn remove(&self, workflow_id: &WorkflowId, activity_id: &ActivityId) {
        self.pending
            .remove(&(workflow_id.clone(), activity_id.clone()));
    }

    fn complete(&self, key: &PendingActivityKey, result: Result<String, String>) -> bool {
        if let Some((_, sender)) = self.pending.remove(key) {
            sender.send(result).is_ok()
        } else {
            false
        }
    }

    /// Number of entries currently outstanding. Test-only: dispatch never needs
    /// the count, but tests assert no entry is leaked on the failure paths.
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.pending.len()
    }
}

impl ActivityCompletionSink for PendingActivities {
    fn complete_activity(&self, completion: ActivityCompletion) -> Result<(), ServerError> {
        let result = match completion.outcome {
            ActivityCompletionOutcome::Succeeded(payload) => {
                payload_to_string(&payload).map_err(|reason| {
                    tracing::error!(
                        operation = "activity_complete",
                        workflow_id = %completion.workflow_id,
                        activity_id = %completion.activity_id,
                        error_type = "ActivityResultDecode",
                        %reason,
                        "activity completion failed"
                    );
                    ServerError::worker_dispatch("", "", format!("payload decode: {reason}"))
                })?
            }
            ActivityCompletionOutcome::Failed(error) => {
                let prefix = if error.is_retryable() {
                    "retryable"
                } else {
                    "terminal"
                };
                tracing::error!(
                    operation = "activity_complete",
                    workflow_id = %completion.workflow_id,
                    activity_id = %completion.activity_id,
                    error_type = "ActivityFailed",
                    error_kind = prefix,
                    reason = %error.message,
                    "activity completion failed"
                );
                Err(format!("{prefix}:{}", error.message))
            }
        };
        self.complete(&(completion.workflow_id, completion.activity_id), result);
        Ok(())
    }
}

fn payload_to_string(payload: &Payload) -> Result<Result<String, String>, String> {
    match payload.content_type() {
        ContentType::Json => String::from_utf8(payload.bytes().to_vec())
            .map(Ok)
            .map_err(|_| "activity result payload is not valid UTF-8".to_owned()),
    }
}

/// Identity and timing carried through a single dispatch so the completion log
/// correlates against the event store. Borrowed for the lifetime of the
/// dispatch call.
pub(super) struct ActivityDispatchContext<'a> {
    pub(super) namespace: &'a str,
    pub(super) activity_type: &'a str,
    pub(super) worker_id: WorkerId,
    pub(super) workflow_id: &'a WorkflowId,
    pub(super) activity_id: &'a ActivityId,
    pub(super) started_at: Instant,
}

/// Emit the structured completion line once a dispatch resolves.
pub(super) fn log_activity_completion(context: &ActivityDispatchContext<'_>, succeeded: bool) {
    let duration_ms = duration_ms(context.started_at.elapsed());
    tracing::info!(
        operation = "activity_complete",
        namespace = context.namespace,
        workflow_id = %context.workflow_id,
        activity_id = %context.activity_id,
        activity_type = context.activity_type,
        worker_id = ?context.worker_id,
        duration_ms,
        outcome = if succeeded { "succeeded" } else { "failed" },
        "activity completed"
    );
}

fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use aion_core::{ActivityError, ActivityErrorKind, ContentType, Payload};

    use super::*;

    fn activity_id(pos: u64) -> ActivityId {
        ActivityId::from_sequence_position(pos)
    }

    #[test]
    fn pending_insert_and_complete_delivers_result() {
        let pending = PendingActivities::default();
        let workflow_id = WorkflowId::new_v4();
        let id = activity_id(1);
        let rx = pending.insert(workflow_id.clone(), id.clone());

        assert!(pending.complete(&(workflow_id, id), Ok("done".to_owned())));
        assert_eq!(
            rx.recv_timeout(Duration::from_millis(50)),
            Ok(Ok("done".to_owned()))
        );
    }

    #[test]
    fn pending_complete_unknown_returns_false() {
        let pending = PendingActivities::default();
        assert!(!pending.complete(
            &(WorkflowId::new_v4(), activity_id(99)),
            Ok("orphan".to_owned())
        ));
    }

    #[test]
    fn completion_sink_routes_success() -> Result<(), ServerError> {
        let pending = PendingActivities::default();
        let workflow_id = WorkflowId::new_v4();
        let id = activity_id(2);
        let rx = pending.insert(workflow_id.clone(), id.clone());
        let payload = Payload::new(ContentType::Json, br#"{"greeting":"hi"}"#.to_vec());

        pending.complete_activity(ActivityCompletion {
            workflow_id,
            activity_id: id,
            outcome: ActivityCompletionOutcome::Succeeded(payload),
        })?;

        let result = rx
            .recv_timeout(Duration::from_millis(50))
            .map_err(|e| ServerError::worker_dispatch("", "", format!("channel: {e}")))?;
        assert_eq!(result, Ok(r#"{"greeting":"hi"}"#.to_owned()));
        Ok(())
    }

    #[test]
    fn completion_sink_routes_retryable_error() -> Result<(), ServerError> {
        let pending = PendingActivities::default();
        let workflow_id = WorkflowId::new_v4();
        let id = activity_id(3);
        let rx = pending.insert(workflow_id.clone(), id.clone());

        pending.complete_activity(ActivityCompletion {
            workflow_id,
            activity_id: id,
            outcome: ActivityCompletionOutcome::Failed(ActivityError {
                kind: ActivityErrorKind::Retryable,
                message: "temporary".to_owned(),
                details: None,
            }),
        })?;

        let result = rx
            .recv_timeout(Duration::from_millis(50))
            .map_err(|e| ServerError::worker_dispatch("", "", format!("channel: {e}")))?;
        assert_eq!(result, Err("retryable:temporary".to_owned()));
        Ok(())
    }

    /// Regression test (#59, brief D12): pending tracking must be keyed by the
    /// full `(WorkflowId, ActivityId)` pair. A stale result re-reported from a
    /// worker's previous session carries the same bare `ActivityId` as a fresh
    /// post-restart dispatch; under bare-`ActivityId` keying the stale result
    /// completed the wrong execution. With pair keying it is dropped and the
    /// genuine result still completes.
    #[test]
    fn stale_result_for_other_workflow_does_not_complete_pending_dispatch()
    -> Result<(), ServerError> {
        let pending = PendingActivities::default();
        let post_restart_workflow = WorkflowId::new_v4();
        let pre_restart_workflow = WorkflowId::new_v4();
        // Counter resets to the same sequence position after restart.
        let id = activity_id(1);
        let rx = pending.insert(post_restart_workflow.clone(), id.clone());

        // Stale pre-restart result: same activity id, different workflow.
        pending.complete_activity(ActivityCompletion {
            workflow_id: pre_restart_workflow,
            activity_id: id.clone(),
            outcome: ActivityCompletionOutcome::Succeeded(Payload::new(
                ContentType::Json,
                br#""stale""#.to_vec(),
            )),
        })?;
        assert!(
            rx.try_recv().is_err(),
            "stale result for a different workflow must not complete this dispatch"
        );

        // The genuine result for the pending execution still completes.
        pending.complete_activity(ActivityCompletion {
            workflow_id: post_restart_workflow,
            activity_id: id,
            outcome: ActivityCompletionOutcome::Succeeded(Payload::new(
                ContentType::Json,
                br#""fresh""#.to_vec(),
            )),
        })?;
        let result = rx
            .recv_timeout(Duration::from_millis(50))
            .map_err(|e| ServerError::worker_dispatch("", "", format!("channel: {e}")))?;
        assert_eq!(result, Ok(r#""fresh""#.to_owned()));
        Ok(())
    }
}
