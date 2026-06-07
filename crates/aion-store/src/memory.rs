//! `InMemoryStore` reference implementation and behavioural test suite.

use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard};

use aion_core::{
    Event, TimerId, WorkflowFilter, WorkflowId, WorkflowStatus, WorkflowSummary, status_from_events,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::visibility::{ListWorkflowsFilter, VisibilityRecord, VisibilityStore};
use crate::{EventStore, RunSummary, StoreError, TimerEntry};

/// Correct non-durable [`EventStore`] implementation for tests and backend equivalence.
#[derive(Debug, Default)]
pub struct InMemoryStore {
    state: Mutex<InMemoryState>,
}

#[async_trait]
impl VisibilityStore for InMemoryStore {
    async fn record_visibility(&self, record: VisibilityRecord) -> Result<(), StoreError> {
        let mut state = self.lock_state()?;
        state
            .visibility
            .insert((record.workflow_id.clone(), record.run_id.clone()), record);
        Ok(())
    }

    async fn list_workflows(
        &self,
        filter: ListWorkflowsFilter,
    ) -> Result<Vec<crate::visibility::WorkflowSummary>, StoreError> {
        let state = self.lock_state()?;
        let mut summaries = state
            .visibility
            .values()
            .cloned()
            .map(crate::visibility::WorkflowSummary::from)
            .filter(|summary| filter.matches(summary))
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            left.start_time.cmp(&right.start_time).then_with(|| {
                left.workflow_id
                    .to_string()
                    .cmp(&right.workflow_id.to_string())
            })
        });
        let offset = filter.offset.and_then(|value| usize::try_from(value).ok());
        if let Some(offset) = offset {
            summaries = summaries.into_iter().skip(offset).collect();
        }
        if let Some(limit) = filter.limit.and_then(|value| usize::try_from(value).ok()) {
            summaries.truncate(limit);
        }
        Ok(summaries)
    }

    async fn count_workflows(&self, filter: ListWorkflowsFilter) -> Result<u64, StoreError> {
        let state = self.lock_state()?;
        Ok(state
            .visibility
            .values()
            .cloned()
            .map(crate::visibility::WorkflowSummary::from)
            .filter(|summary| filter.matches(summary))
            .count()
            .try_into()
            .unwrap_or(u64::MAX))
    }
}

#[derive(Debug, Default)]
struct InMemoryState {
    histories: HashMap<WorkflowId, Vec<Event>>,
    timers: HashMap<(WorkflowId, TimerId), TimerEntry>,
    visibility: HashMap<(WorkflowId, aion_core::RunId), VisibilityRecord>,
}

impl InMemoryStore {
    fn lock_state(&self) -> Result<MutexGuard<'_, InMemoryState>, StoreError> {
        self.state
            .lock()
            .map_err(|error| StoreError::Backend(format!("in-memory store lock poisoned: {error}")))
    }
}

fn history_head(history: &[Event]) -> u64 {
    history.iter().map(Event::seq).max().unwrap_or_default()
}

fn history_in_sequence_order(history: &[Event]) -> Vec<Event> {
    let mut ordered = history.to_vec();
    ordered.sort_by_key(Event::seq);
    ordered
}

#[async_trait]
impl EventStore for InMemoryStore {
    async fn append(
        &self,
        workflow_id: &WorkflowId,
        events: &[Event],
        expected_seq: u64,
    ) -> Result<(), StoreError> {
        let mut state = self.lock_state()?;
        let current_head = state
            .histories
            .get(workflow_id)
            .map_or(0, |history| history_head(history));

        if current_head != expected_seq {
            return Err(StoreError::SequenceConflict {
                expected: expected_seq,
                found: current_head,
            });
        }

        if events.is_empty() {
            return Ok(());
        }

        for (event, expected_seq) in events.iter().zip((expected_seq + 1)..) {
            if event.seq() != expected_seq {
                return Err(StoreError::Backend(format!(
                    "event sequence must be contiguous: expected {expected_seq}, got {}",
                    event.seq()
                )));
            }
        }

        state
            .histories
            .entry(workflow_id.clone())
            .or_default()
            .extend(events.iter().cloned());
        Ok(())
    }

    async fn read_history(&self, workflow_id: &WorkflowId) -> Result<Vec<Event>, StoreError> {
        let state = self.lock_state()?;
        Ok(state
            .histories
            .get(workflow_id)
            .map_or_else(Vec::new, |history| history_in_sequence_order(history)))
    }

    async fn read_run_chain(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<RunSummary>, StoreError> {
        let state = self.lock_state()?;
        let Some(history) = state.histories.get(workflow_id) else {
            return Ok(Vec::new());
        };

        crate::run_chain::run_chain_from_history(history)
    }

    async fn list_active(&self) -> Result<Vec<WorkflowId>, StoreError> {
        let state = self.lock_state()?;
        let mut active = state
            .histories
            .iter()
            .filter(|(_, history)| {
                matches!(
                    status_from_events(&history_in_sequence_order(history)),
                    WorkflowStatus::Running
                )
            })
            .map(|(workflow_id, _)| workflow_id.clone())
            .collect::<Vec<_>>();
        active.sort_by_key(ToString::to_string);
        Ok(active)
    }

    async fn query(&self, filter: &WorkflowFilter) -> Result<Vec<WorkflowSummary>, StoreError> {
        let state = self.lock_state()?;
        let mut summaries = state
            .histories
            .values()
            .filter_map(|history| {
                WorkflowSummary::from_history(&history_in_sequence_order(history))
            })
            .filter(|summary| filter.matches(summary))
            .collect::<Vec<_>>();
        summaries.sort_by(|left, right| {
            left.started_at.cmp(&right.started_at).then_with(|| {
                left.workflow_id
                    .to_string()
                    .cmp(&right.workflow_id.to_string())
            })
        });
        Ok(summaries)
    }

    async fn schedule_timer(
        &self,
        workflow_id: &WorkflowId,
        timer_id: &TimerId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), StoreError> {
        let mut state = self.lock_state()?;
        state.timers.insert(
            (workflow_id.clone(), timer_id.clone()),
            TimerEntry {
                workflow_id: workflow_id.clone(),
                timer_id: timer_id.clone(),
                fire_at,
            },
        );
        Ok(())
    }

    async fn expired_timers(&self, as_of: DateTime<Utc>) -> Result<Vec<TimerEntry>, StoreError> {
        let state = self.lock_state()?;
        let mut timers = state
            .timers
            .values()
            .filter(|entry| entry.fire_at <= as_of)
            .cloned()
            .collect::<Vec<_>>();
        timers.sort_by(|left, right| {
            left.fire_at
                .cmp(&right.fire_at)
                .then_with(|| {
                    left.workflow_id
                        .to_string()
                        .cmp(&right.workflow_id.to_string())
                })
                .then_with(|| left.timer_id.to_string().cmp(&right.timer_id.to_string()))
        });
        Ok(timers)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aion_core::{
        Event, EventEnvelope, Payload, TimerId, WorkflowError, WorkflowFilter, WorkflowId,
        WorkflowStatus,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use tokio::task;
    use uuid::Uuid;

    use super::InMemoryStore;
    use crate::{EventStore, StoreError, TimerEntry};

    fn recorded_at(offset_seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0).unwrap_or_default()
    }

    fn workflow_id(value: u128) -> WorkflowId {
        WorkflowId::new(Uuid::from_u128(value))
    }

    fn envelope(seq: u64, workflow_id: &WorkflowId) -> EventEnvelope {
        EventEnvelope {
            seq,
            recorded_at: recorded_at(i64::try_from(seq).unwrap_or_default()),
            workflow_id: workflow_id.clone(),
        }
    }

    fn run_id(value: u128) -> aion_core::RunId {
        aion_core::RunId::new(Uuid::from_u128(value))
    }

    fn payload(label: &str) -> Payload {
        Payload::from_json(&json!({ "label": label })).unwrap_or_else(|error| {
            Payload::new(
                aion_core::ContentType::Json,
                format!("{{\"payload_error\":\"{error}\"}}").into_bytes(),
            )
        })
    }

    fn workflow_started(seq: u64, workflow_id: &WorkflowId, workflow_type: &str) -> Event {
        Event::WorkflowStarted {
            envelope: envelope(seq, workflow_id),
            workflow_type: workflow_type.to_owned(),
            input: payload("input"),
            run_id: aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            parent_run_id: None,
        }
    }

    fn workflow_completed(seq: u64, workflow_id: &WorkflowId) -> Event {
        Event::WorkflowCompleted {
            envelope: envelope(seq, workflow_id),
            result: payload("result"),
        }
    }

    fn workflow_failed(seq: u64, workflow_id: &WorkflowId) -> Event {
        Event::WorkflowFailed {
            envelope: envelope(seq, workflow_id),
            error: WorkflowError {
                message: String::from("failed"),
                details: None,
            },
        }
    }

    #[tokio::test]
    async fn read_history_returns_empty_for_unknown_workflow() -> Result<(), StoreError> {
        let store = InMemoryStore::default();

        assert_eq!(store.read_history(&workflow_id(1)).await?, Vec::new());
        Ok(())
    }

    #[tokio::test]
    async fn append_preserves_sequence_order() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let workflow_id = workflow_id(1);
        let first = workflow_started(1, &workflow_id, "checkout");
        let second = workflow_completed(2, &workflow_id);

        store
            .append(&workflow_id, std::slice::from_ref(&first), 0)
            .await?;
        store
            .append(&workflow_id, std::slice::from_ref(&second), 1)
            .await?;

        assert_eq!(store.read_history(&workflow_id).await?, vec![first, second]);
        Ok(())
    }

    #[tokio::test]
    async fn list_active_returns_only_running_workflows() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let running = workflow_id(1);
        let completed = workflow_id(2);

        store
            .append(&running, &[workflow_started(1, &running, "checkout")], 0)
            .await?;
        store
            .append(
                &completed,
                &[
                    workflow_started(1, &completed, "checkout"),
                    workflow_completed(2, &completed),
                ],
                0,
            )
            .await?;

        assert_eq!(store.list_active().await?, vec![running]);
        Ok(())
    }

    #[tokio::test]
    async fn read_run_chain_projects_run_id_from_started_event() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let workflow_id = workflow_id(1);

        store
            .append(
                &workflow_id,
                &[
                    workflow_started(1, &workflow_id, "checkout"),
                    workflow_completed(2, &workflow_id),
                ],
                0,
            )
            .await?;

        let chain = store.read_run_chain(&workflow_id).await?;

        assert_eq!(chain.len(), 1);
        // run_id comes from the WorkflowStarted event (hardcoded to from_u128(1) in the helper)
        assert_eq!(chain[0].run_id, run_id(1));
        assert_eq!(chain[0].status, WorkflowStatus::Completed);
        assert_eq!(chain[0].closed_at, Some(recorded_at(2)));
        Ok(())
    }

    #[tokio::test]
    async fn query_uses_core_filter_semantics() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let running_checkout = workflow_id(1);
        let completed_checkout = workflow_id(2);
        let failed_billing = workflow_id(3);

        store
            .append(
                &running_checkout,
                &[workflow_started(1, &running_checkout, "checkout")],
                0,
            )
            .await?;
        store
            .append(
                &completed_checkout,
                &[
                    workflow_started(1, &completed_checkout, "checkout"),
                    workflow_completed(2, &completed_checkout),
                ],
                0,
            )
            .await?;
        store
            .append(
                &failed_billing,
                &[
                    workflow_started(1, &failed_billing, "billing"),
                    workflow_failed(2, &failed_billing),
                ],
                0,
            )
            .await?;

        let filter = WorkflowFilter {
            workflow_type: Some(String::from("checkout")),
            status: Some(WorkflowStatus::Completed),
            started_after: Some(recorded_at(1)),
            started_before: Some(recorded_at(1)),
            parent: None,
        };
        let summaries = store.query(&filter).await?;

        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].workflow_id, completed_checkout);
        assert_eq!(summaries[0].status, WorkflowStatus::Completed);
        Ok(())
    }

    #[tokio::test]
    async fn stale_expected_sequence_writes_nothing() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let workflow_id = workflow_id(1);
        let first = workflow_started(1, &workflow_id, "checkout");

        store
            .append(&workflow_id, std::slice::from_ref(&first), 0)
            .await?;
        let conflict = store
            .append(&workflow_id, &[workflow_completed(2, &workflow_id)], 0)
            .await;

        assert_eq!(
            conflict,
            Err(StoreError::SequenceConflict {
                expected: 0,
                found: 1,
            })
        );
        assert_eq!(store.read_history(&workflow_id).await?, vec![first]);
        Ok(())
    }

    #[tokio::test]
    async fn append_rejects_non_contiguous_event_sequences() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let wf = workflow_id(1);

        let result = store
            .append(
                &wf,
                &[
                    workflow_started(1, &wf, "checkout"),
                    workflow_completed(5, &wf),
                ],
                0,
            )
            .await;

        assert!(result.is_err());
        assert!(matches!(result, Err(StoreError::Backend(_))));
        assert_eq!(store.read_history(&wf).await?, Vec::new());
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_appends_on_same_expected_sequence_conflict_once() -> Result<(), StoreError>
    {
        let store = Arc::new(InMemoryStore::default());
        let workflow_id = workflow_id(1);
        let first_store = Arc::clone(&store);
        let first_workflow = workflow_id.clone();
        let second_store = Arc::clone(&store);
        let second_workflow = workflow_id.clone();

        let first = task::spawn(async move {
            first_store
                .append(
                    &first_workflow,
                    &[workflow_started(1, &first_workflow, "checkout")],
                    0,
                )
                .await
        });
        let second = task::spawn(async move {
            second_store
                .append(
                    &second_workflow,
                    &[workflow_completed(1, &second_workflow)],
                    0,
                )
                .await
        });

        let results = [
            first
                .await
                .map_err(|error| StoreError::Backend(format!("append task failed: {error}")))?,
            second
                .await
                .map_err(|error| StoreError::Backend(format!("append task failed: {error}")))?,
        ];

        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(StoreError::SequenceConflict {
                        expected: 0,
                        found: 1
                    })
                ))
                .count(),
            1
        );
        assert_eq!(store.read_history(&workflow_id).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn rescheduling_same_timer_replaces_prior_fire_at() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let workflow_id = workflow_id(1);
        let timer_id = TimerId::anonymous(1);
        let first_fire_at = recorded_at(10);
        let replacement_fire_at = recorded_at(30);

        store
            .schedule_timer(&workflow_id, &timer_id, first_fire_at)
            .await?;
        store
            .schedule_timer(&workflow_id, &timer_id, replacement_fire_at)
            .await?;

        assert_eq!(store.expired_timers(first_fire_at).await?, Vec::new());
        assert_eq!(
            store.expired_timers(replacement_fire_at).await?,
            vec![TimerEntry {
                workflow_id,
                timer_id,
                fire_at: replacement_fire_at,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn expired_timers_include_boundary_and_exclude_future() -> Result<(), StoreError> {
        let store = InMemoryStore::default();
        let workflow_id = workflow_id(1);
        let past_timer = TimerId::anonymous(1);
        let boundary_timer = TimerId::anonymous(2);
        let future_timer = TimerId::anonymous(3);
        let as_of = recorded_at(20);

        store
            .schedule_timer(&workflow_id, &future_timer, recorded_at(30))
            .await?;
        store
            .schedule_timer(&workflow_id, &boundary_timer, as_of)
            .await?;
        store
            .schedule_timer(&workflow_id, &past_timer, recorded_at(10))
            .await?;

        assert_eq!(
            store.expired_timers(as_of).await?,
            vec![
                TimerEntry {
                    workflow_id: workflow_id.clone(),
                    timer_id: past_timer,
                    fire_at: recorded_at(10),
                },
                TimerEntry {
                    workflow_id,
                    timer_id: boundary_timer,
                    fire_at: as_of,
                },
            ]
        );
        Ok(())
    }
}
