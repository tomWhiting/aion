//! Push dispatch for remote activity workers and result handoff to the engine contract.

use aion_core::{ActivityError, ActivityErrorKind, ActivityId, Payload, WorkflowId};
use aion_proto::{ProtoActivityResult, WireError, proto_activity_result};

use crate::error::ServerError;

/// Decoded activity outcome reported by a worker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ActivityCompletionOutcome {
    /// Activity completed successfully with an output payload.
    Succeeded(Payload),
    /// Activity failed, preserving retryability classification for the engine.
    Failed(ActivityError),
}

/// Correlated activity completion handed to the engine-owned activity contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ActivityCompletion {
    /// Owning workflow id.
    pub workflow_id: WorkflowId,
    /// Correlating activity id.
    pub activity_id: ActivityId,
    /// Worker-reported outcome.
    pub outcome: ActivityCompletionOutcome,
}

impl TryFrom<ProtoActivityResult> for ActivityCompletion {
    type Error = ServerError;

    fn try_from(value: ProtoActivityResult) -> Result<Self, Self::Error> {
        let workflow_id = value
            .workflow_id
            .ok_or_else(|| wire_error("activity result workflow id is missing"))
            .and_then(|id| WorkflowId::try_from(id).map_err(ServerError::from))?;
        let activity_id = value
            .activity_id
            .ok_or_else(|| wire_error("activity result activity id is missing"))
            .map(ActivityId::from)?;
        let outcome = match value.outcome {
            Some(proto_activity_result::Outcome::Result(payload)) => {
                ActivityCompletionOutcome::Succeeded(
                    Payload::try_from(payload).map_err(ServerError::from)?,
                )
            }
            Some(proto_activity_result::Outcome::Error(error)) => {
                ActivityCompletionOutcome::Failed(
                    ActivityError::try_from(error).map_err(ServerError::from)?,
                )
            }
            None => return Err(wire_error("activity result outcome is missing")),
        };

        Ok(Self {
            workflow_id,
            activity_id,
            outcome,
        })
    }
}

/// Engine-owned activity completion contract used by the worker endpoint.
pub trait ActivityCompletionSink {
    /// Feed one worker-reported result into the engine activity contract.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] when the engine rejects or cannot record the completion.
    fn complete_activity(&self, completion: ActivityCompletion) -> Result<(), ServerError>;
}

/// Decode and hand a worker result to the engine-owned activity completion sink.
///
/// # Errors
///
/// Returns [`ServerError`] for malformed wire results or sink failures.
pub fn handle_activity_result(
    sink: &impl ActivityCompletionSink,
    result: ProtoActivityResult,
) -> Result<(), ServerError> {
    sink.complete_activity(ActivityCompletion::try_from(result)?)
}

/// Build the retryable failure reported when a worker loses ownership of an in-flight task.
///
/// The retryable classification models worker loss as infrastructure failure: aion-server
/// only reports the failure to the engine activity contract; the engine remains responsible
/// for applying the activity retry policy.
#[must_use]
pub fn lost_worker_error(worker_id: crate::worker::registry::WorkerId) -> ActivityError {
    ActivityError {
        kind: ActivityErrorKind::Retryable,
        message: format!("worker {worker_id:?} lost before reporting activity result"),
        details: None,
    }
}

fn wire_error(message: &'static str) -> ServerError {
    ServerError::Wire {
        wire: WireError::backend(message),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use aion_core::ActivityErrorKind;
    use aion_proto::{
        ProtoActivityError, ProtoActivityErrorKind, ProtoActivityId, ProtoPayload, ProtoWorkflowId,
    };
    use serde_json::json;
    use uuid::Uuid;

    use super::*;

    fn workflow_id() -> WorkflowId {
        WorkflowId::new(Uuid::nil())
    }

    fn activity_id() -> ActivityId {
        ActivityId::from_sequence_position(42)
    }

    fn payload(value: &serde_json::Value) -> Result<Payload, Box<dyn std::error::Error>> {
        Ok(Payload::from_json(value)?)
    }

    #[derive(Default)]
    struct RecordingSink {
        completions: Mutex<Vec<ActivityCompletion>>,
    }

    impl ActivityCompletionSink for RecordingSink {
        fn complete_activity(&self, completion: ActivityCompletion) -> Result<(), ServerError> {
            self.completions
                .lock()
                .map_err(|_| ServerError::lock_poisoned("recording completion sink"))?
                .push(completion);
            Ok(())
        }
    }

    #[test]
    fn successful_activity_result_calls_completion_sink() -> Result<(), Box<dyn std::error::Error>>
    {
        let sink = RecordingSink::default();
        let output = payload(&json!({"ok": true}))?;
        let result = ProtoActivityResult {
            workflow_id: Some(ProtoWorkflowId::from(workflow_id())),
            activity_id: Some(ProtoActivityId::from(activity_id())),
            outcome: Some(proto_activity_result::Outcome::Result(ProtoPayload::from(
                output.clone(),
            ))),
        };

        handle_activity_result(&sink, result)?;
        let completions = sink
            .completions
            .lock()
            .map_err(|_| ServerError::lock_poisoned("recording completion sink"))?;

        assert_eq!(completions.len(), 1);
        assert_eq!(completions[0].workflow_id, workflow_id());
        assert_eq!(completions[0].activity_id, activity_id());
        assert_eq!(
            completions[0].outcome,
            ActivityCompletionOutcome::Succeeded(output)
        );
        Ok(())
    }

    #[test]
    fn failed_activity_result_preserves_error_classification()
    -> Result<(), Box<dyn std::error::Error>> {
        let sink = RecordingSink::default();
        let error = ProtoActivityError {
            kind: ProtoActivityErrorKind::Retryable as i32,
            message: String::from("temporary outage"),
            details: Some(ProtoPayload::from(payload(
                &json!({"retry_after_ms": 500}),
            )?)),
        };
        let result = ProtoActivityResult {
            workflow_id: Some(ProtoWorkflowId::from(workflow_id())),
            activity_id: Some(ProtoActivityId::from(activity_id())),
            outcome: Some(proto_activity_result::Outcome::Error(error)),
        };

        handle_activity_result(&sink, result)?;
        let completions = sink
            .completions
            .lock()
            .map_err(|_| ServerError::lock_poisoned("recording completion sink"))?;

        assert_eq!(completions.len(), 1);
        match &completions[0].outcome {
            ActivityCompletionOutcome::Failed(error) => {
                assert_eq!(error.kind, ActivityErrorKind::Retryable);
                assert!(error.is_retryable());
            }
            ActivityCompletionOutcome::Succeeded(_) => return Err("expected failed outcome".into()),
        }
        Ok(())
    }
}
