//! Visibility projection updates for workflow lifecycle state changes.

use std::collections::HashMap;
use std::sync::Arc;

use aion_core::{Event, RunId, SearchAttributeValue, WorkflowId, status_from_events};
use aion_store::EventStore;
use aion_store::visibility::{VisibilityRecord, VisibilityStore};
use chrono::{DateTime, Utc};

use crate::EngineError;

/// Rebuilds and upserts the full visibility projection for a workflow execution.
///
/// # Errors
///
/// Returns store errors when history cannot be read or visibility cannot be recorded, and a load
/// error if the workflow history has no `WorkflowStarted` event to project.
pub async fn upsert_workflow_visibility(
    event_store: Arc<dyn EventStore>,
    visibility_store: Arc<dyn VisibilityStore>,
    workflow_id: &WorkflowId,
    run_id: &RunId,
) -> Result<(), EngineError> {
    let history = event_store.read_history(workflow_id).await?;
    let record = visibility_record_from_history(&history, run_id)?;
    visibility_store.record_visibility(record).await?;
    Ok(())
}

fn visibility_record_from_history(
    history: &[Event],
    run_id: &RunId,
) -> Result<VisibilityRecord, EngineError> {
    let (workflow_id, workflow_type, start_time) = started_projection(history)?;
    Ok(VisibilityRecord {
        workflow_id,
        run_id: run_id.clone(),
        workflow_type,
        status: status_from_events(history),
        start_time,
        close_time: terminal_recorded_at(history),
        search_attributes: search_attributes_from_history(history),
    })
}

fn started_projection(
    history: &[Event],
) -> Result<(WorkflowId, String, DateTime<Utc>), EngineError> {
    history
        .iter()
        .find_map(|event| match event {
            Event::WorkflowStarted {
                envelope,
                workflow_type,
                ..
            } => Some((
                envelope.workflow_id.clone(),
                workflow_type.clone(),
                envelope.recorded_at,
            )),
            _ => None,
        })
        .ok_or_else(|| EngineError::Load {
            reason: String::from(
                "workflow history has no WorkflowStarted event for visibility projection",
            ),
        })
}

fn terminal_recorded_at(history: &[Event]) -> Option<DateTime<Utc>> {
    history.iter().rev().find_map(|event| match event {
        Event::WorkflowCompleted { envelope, .. }
        | Event::WorkflowFailed { envelope, .. }
        | Event::WorkflowCancelled { envelope, .. }
        | Event::WorkflowTimedOut { envelope, .. }
        | Event::WorkflowContinuedAsNew { envelope, .. } => Some(envelope.recorded_at),
        Event::WorkflowStarted { .. }
        | Event::SearchAttributesUpdated { .. }
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
        | Event::ScheduleTriggered { .. } => None,
    })
}

fn search_attributes_from_history(history: &[Event]) -> HashMap<String, SearchAttributeValue> {
    let mut search_attributes = HashMap::new();
    for event in history {
        if let Event::SearchAttributesUpdated { attributes, .. } = event {
            search_attributes.extend(attributes.clone());
        }
    }
    search_attributes
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use aion_core::{
        Event, EventEnvelope, Payload, RunId, SearchAttributeValue, WorkflowError, WorkflowId,
        WorkflowStatus,
    };
    use chrono::{TimeZone, Utc};

    use super::{
        search_attributes_from_history, started_projection, terminal_recorded_at,
        visibility_record_from_history,
    };
    use crate::EngineError;

    fn envelope(workflow_id: &WorkflowId, seq: u64) -> EventEnvelope {
        EventEnvelope {
            seq,
            recorded_at: Utc
                .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
                .single()
                .expect("test timestamp")
                + chrono::Duration::seconds(i64::from(seq as u32)),
            workflow_id: workflow_id.clone(),
        }
    }

    fn payload() -> Payload {
        Payload::from_json(&serde_json::json!({})).expect("empty payload")
    }

    fn workflow_started(workflow_id: &WorkflowId, run_id: &RunId) -> Event {
        Event::WorkflowStarted {
            envelope: envelope(workflow_id, 1),
            workflow_type: String::from("order_processing"),
            input: payload(),
            run_id: run_id.clone(),
            parent_run_id: None,
        }
    }

    #[test]
    fn started_projection_extracts_fields_from_workflow_started() {
        let wf_id = WorkflowId::new_v4();
        let run_id = RunId::new_v4();
        let history = vec![workflow_started(&wf_id, &run_id)];

        let (projected_id, projected_type, projected_time) =
            started_projection(&history).expect("should project");
        assert_eq!(projected_id, wf_id);
        assert_eq!(projected_type, "order_processing");
        assert_eq!(projected_time, envelope(&wf_id, 1).recorded_at);
    }

    #[test]
    fn started_projection_returns_load_error_for_empty_history() {
        let result = started_projection(&[]);
        assert!(
            matches!(result, Err(EngineError::Load { .. })),
            "expected Load error, got {result:?}"
        );
    }

    #[test]
    fn started_projection_returns_load_error_when_no_started_event() {
        let wf_id = WorkflowId::new_v4();
        let history = vec![Event::WorkflowCompleted {
            envelope: envelope(&wf_id, 1),
            result: payload(),
        }];
        let result = started_projection(&history);
        assert!(matches!(result, Err(EngineError::Load { .. })));
    }

    #[test]
    fn terminal_recorded_at_returns_completed_timestamp() {
        let wf_id = WorkflowId::new_v4();
        let env = envelope(&wf_id, 2);
        let expected = env.recorded_at;
        let history = vec![Event::WorkflowCompleted {
            envelope: env,
            result: payload(),
        }];
        assert_eq!(terminal_recorded_at(&history), Some(expected));
    }

    #[test]
    fn terminal_recorded_at_returns_failed_timestamp() {
        let wf_id = WorkflowId::new_v4();
        let env = envelope(&wf_id, 2);
        let expected = env.recorded_at;
        let history = vec![Event::WorkflowFailed {
            envelope: env,
            error: WorkflowError {
                message: String::from("boom"),
                details: None,
            },
        }];
        assert_eq!(terminal_recorded_at(&history), Some(expected));
    }

    #[test]
    fn terminal_recorded_at_returns_cancelled_timestamp() {
        let wf_id = WorkflowId::new_v4();
        let env = envelope(&wf_id, 2);
        let expected = env.recorded_at;
        let history = vec![Event::WorkflowCancelled {
            envelope: env,
            reason: String::from("user request"),
        }];
        assert_eq!(terminal_recorded_at(&history), Some(expected));
    }

    #[test]
    fn terminal_recorded_at_returns_timed_out_timestamp() {
        let wf_id = WorkflowId::new_v4();
        let env = envelope(&wf_id, 2);
        let expected = env.recorded_at;
        let history = vec![Event::WorkflowTimedOut {
            envelope: env,
            timeout: String::from("workflow_execution"),
        }];
        assert_eq!(terminal_recorded_at(&history), Some(expected));
    }

    #[test]
    fn terminal_recorded_at_returns_continued_as_new_timestamp() {
        let wf_id = WorkflowId::new_v4();
        let parent_run = RunId::new_v4();
        let env = envelope(&wf_id, 2);
        let expected = env.recorded_at;
        let history = vec![Event::WorkflowContinuedAsNew {
            envelope: env,
            input: payload(),
            workflow_type: None,
            parent_run_id: parent_run,
        }];
        assert_eq!(terminal_recorded_at(&history), Some(expected));
    }

    #[test]
    fn terminal_recorded_at_returns_none_for_non_terminal_history() {
        let wf_id = WorkflowId::new_v4();
        let run_id = RunId::new_v4();
        let history = vec![workflow_started(&wf_id, &run_id)];
        assert_eq!(terminal_recorded_at(&history), None);
    }

    #[test]
    fn terminal_recorded_at_finds_terminal_after_interleaved_non_terminal_events() {
        let wf_id = WorkflowId::new_v4();
        let run_id = RunId::new_v4();
        let terminal_env = envelope(&wf_id, 4);
        let expected = terminal_env.recorded_at;
        let history = vec![
            workflow_started(&wf_id, &run_id),
            Event::SignalReceived {
                envelope: envelope(&wf_id, 2),
                name: String::from("wake"),
                payload: payload(),
            },
            Event::TimerFired {
                envelope: envelope(&wf_id, 3),
                timer_id: aion_core::TimerId::named("t1").expect("test timer id"),
            },
            Event::WorkflowCompleted {
                envelope: terminal_env,
                result: payload(),
            },
        ];
        assert_eq!(terminal_recorded_at(&history), Some(expected));
    }

    #[test]
    fn search_attributes_collects_from_multiple_updates() {
        let wf_id = WorkflowId::new_v4();
        let mut first_attrs = HashMap::new();
        first_attrs.insert(
            String::from("customer"),
            SearchAttributeValue::String(String::from("acme")),
        );
        let mut second_attrs = HashMap::new();
        second_attrs.insert(String::from("priority"), SearchAttributeValue::Int(5));
        second_attrs.insert(
            String::from("customer"),
            SearchAttributeValue::String(String::from("globex")),
        );

        let history = vec![
            Event::SearchAttributesUpdated {
                envelope: envelope(&wf_id, 2),
                workflow_id: wf_id.clone(),
                attributes: first_attrs,
            },
            Event::SearchAttributesUpdated {
                envelope: envelope(&wf_id, 3),
                workflow_id: wf_id.clone(),
                attributes: second_attrs,
            },
        ];

        let result = search_attributes_from_history(&history);
        assert_eq!(result.len(), 2);
        assert_eq!(
            result.get("customer"),
            Some(&SearchAttributeValue::String(String::from("globex")))
        );
        assert_eq!(result.get("priority"), Some(&SearchAttributeValue::Int(5)));
    }

    #[test]
    fn search_attributes_returns_empty_when_no_updates() {
        let wf_id = WorkflowId::new_v4();
        let run_id = RunId::new_v4();
        let history = vec![workflow_started(&wf_id, &run_id)];
        assert!(search_attributes_from_history(&history).is_empty());
    }

    #[test]
    fn visibility_record_from_history_projects_running_workflow() {
        let wf_id = WorkflowId::new_v4();
        let run_id = RunId::new_v4();
        let history = vec![workflow_started(&wf_id, &run_id)];

        let record = visibility_record_from_history(&history, &run_id).expect("should project");
        assert_eq!(record.workflow_id, wf_id);
        assert_eq!(record.run_id, run_id);
        assert_eq!(record.workflow_type, "order_processing");
        assert_eq!(record.status, WorkflowStatus::Running);
        assert!(record.close_time.is_none());
        assert!(record.search_attributes.is_empty());
    }

    #[test]
    fn visibility_record_from_history_projects_completed_workflow_with_attributes() {
        let wf_id = WorkflowId::new_v4();
        let run_id = RunId::new_v4();
        let mut attrs = HashMap::new();
        attrs.insert(
            String::from("region"),
            SearchAttributeValue::String(String::from("eu-west-1")),
        );
        let terminal_env = envelope(&wf_id, 3);
        let expected_close = terminal_env.recorded_at;

        let history = vec![
            workflow_started(&wf_id, &run_id),
            Event::SearchAttributesUpdated {
                envelope: envelope(&wf_id, 2),
                workflow_id: wf_id.clone(),
                attributes: attrs,
            },
            Event::WorkflowCompleted {
                envelope: terminal_env,
                result: payload(),
            },
        ];

        let record = visibility_record_from_history(&history, &run_id).expect("should project");
        assert_eq!(record.status, WorkflowStatus::Completed);
        assert_eq!(record.close_time, Some(expected_close));
        assert_eq!(
            record.search_attributes.get("region"),
            Some(&SearchAttributeValue::String(String::from("eu-west-1")))
        );
    }
}
