//! Recorder: single-writer append path over `EventStore`.

use std::collections::HashMap;
use std::sync::Arc;

use aion_core::{
    ActivityError, ActivityId, Event, EventEnvelope, Payload, RunId, ScheduleConfig, ScheduleId,
    SearchAttributeSchema, SearchAttributeValue, TimerId, WorkflowError, WorkflowId,
};
use aion_store::visibility::{VisibilityRecord, VisibilityStore};
use aion_store::{EventStore, WriteToken};
use chrono::{DateTime, Utc};

use crate::durability::{DurabilityError, seq::SequenceHead};

/// Single append authority for one workflow history.
///
/// A recorder owns the workflow's tracked sequence head and computes every `expected_seq` from that
/// tracker. It never reads the store head after construction; a sequence conflict is surfaced as a
/// hard durability error because it indicates a second writer for the same workflow.
pub struct Recorder {
    workflow_id: WorkflowId,
    store: Arc<dyn EventStore>,
    sequence: SequenceHead,
    write_token: WriteToken,
    visibility: Option<RecorderVisibility>,
}

struct RecorderVisibility {
    run_id: RunId,
    store: Arc<dyn VisibilityStore>,
}

impl Recorder {
    /// Creates a recorder for a fresh workflow history starting at sequence head `0`.
    #[must_use]
    pub fn new(workflow_id: WorkflowId, store: Arc<dyn EventStore>) -> Self {
        Self::resume_at(workflow_id, store, 0)
    }

    /// Creates a recorder for a workflow whose existing history head was already derived.
    #[must_use]
    pub fn resume_at(workflow_id: WorkflowId, store: Arc<dyn EventStore>, head: u64) -> Self {
        Self {
            workflow_id,
            store,
            write_token: WriteToken::recorder(),
            sequence: SequenceHead::from_head(head),
            visibility: None,
        }
    }

    /// Enables visibility projection upserts after workflow-level state-changing events recorded
    /// directly through this recorder.
    #[must_use]
    pub fn with_visibility(mut self, run_id: RunId, store: Arc<dyn VisibilityStore>) -> Self {
        self.visibility = Some(RecorderVisibility { run_id, store });
        self
    }

    /// Returns the workflow this recorder appends to.
    #[must_use]
    pub const fn workflow_id(&self) -> &WorkflowId {
        &self.workflow_id
    }

    /// Returns the current tracked sequence head.
    #[must_use]
    pub const fn current_head(&self) -> u64 {
        self.sequence.current()
    }

    /// Reads this workflow's recorded history without appending new events.
    ///
    /// This narrow read seam lets replay contexts build a history cursor from the same store the
    /// recorder writes to while preserving the recorder as the only event append authority.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError::Store`] when the backing event store rejects the read.
    pub async fn read_history(&self) -> Result<Vec<Event>, DurabilityError> {
        self.store
            .read_history(&self.workflow_id)
            .await
            .map_err(Into::into)
    }

    /// Records workflow start.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_workflow_started(
        &mut self,
        recorded_at: DateTime<Utc>,
        workflow_type: String,
        input: Payload,
        run_id: RunId,
    ) -> Result<(), DurabilityError> {
        self.record_workflow_started_with_parent(recorded_at, workflow_type, input, run_id, None)
            .await
    }

    /// Records workflow start with an optional parent run for continue-as-new chains.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_workflow_started_with_parent(
        &mut self,
        recorded_at: DateTime<Utc>,
        workflow_type: String,
        input: Payload,
        run_id: RunId,
        parent_run_id: Option<RunId>,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::WorkflowStarted {
            envelope,
            workflow_type,
            input,
            run_id,
            parent_run_id,
        })
        .await
    }

    /// Records schedule creation in the schedule coordinator history.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_schedule_created(
        &mut self,
        recorded_at: DateTime<Utc>,
        schedule_id: ScheduleId,
        config: ScheduleConfig,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ScheduleCreated {
            envelope,
            schedule_id,
            config,
        })
        .await
    }

    /// Records schedule configuration update in the schedule coordinator history.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_schedule_updated(
        &mut self,
        recorded_at: DateTime<Utc>,
        schedule_id: ScheduleId,
        config: ScheduleConfig,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ScheduleUpdated {
            envelope,
            schedule_id,
            config,
        })
        .await
    }

    /// Records schedule pause in the schedule coordinator history.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_schedule_paused(
        &mut self,
        recorded_at: DateTime<Utc>,
        schedule_id: ScheduleId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::SchedulePaused {
            envelope,
            schedule_id,
        })
        .await
    }

    /// Records schedule resume in the schedule coordinator history.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_schedule_resumed(
        &mut self,
        recorded_at: DateTime<Utc>,
        schedule_id: ScheduleId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ScheduleResumed {
            envelope,
            schedule_id,
        })
        .await
    }

    /// Records schedule deletion in the schedule coordinator history.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_schedule_deleted(
        &mut self,
        recorded_at: DateTime<Utc>,
        schedule_id: ScheduleId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ScheduleDeleted {
            envelope,
            schedule_id,
        })
        .await
    }

    /// Records a schedule-triggered workflow execution in the schedule coordinator history.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_schedule_triggered(
        &mut self,
        recorded_at: DateTime<Utc>,
        schedule_id: ScheduleId,
        workflow_id: WorkflowId,
        run_id: RunId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ScheduleTriggered {
            envelope,
            schedule_id,
            workflow_id,
            run_id,
        })
        .await
    }

    /// Records workflow completion.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_workflow_completed(
        &mut self,
        recorded_at: DateTime<Utc>,
        result: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::WorkflowCompleted {
            envelope,
            result,
        })
        .await
    }

    /// Records terminal workflow failure.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_workflow_failed(
        &mut self,
        recorded_at: DateTime<Utc>,
        error: WorkflowError,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::WorkflowFailed {
            envelope,
            error,
        })
        .await
    }

    /// Records workflow cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_workflow_cancelled(
        &mut self,
        recorded_at: DateTime<Utc>,
        reason: String,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::WorkflowCancelled {
            envelope,
            reason,
        })
        .await
    }

    /// Records workflow continue-as-new as a terminal event for this run.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_workflow_continued_as_new(
        &mut self,
        recorded_at: DateTime<Utc>,
        input: Payload,
        workflow_type: Option<String>,
        parent_run_id: RunId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::WorkflowContinuedAsNew {
            envelope,
            input,
            workflow_type,
            parent_run_id,
        })
        .await?;
        self.upsert_visibility_projection().await
    }

    /// Records a validated search-attribute update for this workflow.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] when any attribute is unregistered or has a type that does not
    /// match `schema`, or when the event store rejects the append / sequence advance.
    pub async fn record_search_attributes_updated(
        &mut self,
        recorded_at: DateTime<Utc>,
        attributes: HashMap<String, SearchAttributeValue>,
        schema: &SearchAttributeSchema,
    ) -> Result<(), DurabilityError> {
        for (name, value) in &attributes {
            schema.validate(name, value)?;
        }
        let workflow_id = self.workflow_id.clone();
        self.append_with(recorded_at, |envelope| Event::SearchAttributesUpdated {
            envelope,
            workflow_id,
            attributes,
        })
        .await?;
        self.upsert_visibility_projection().await
    }

    /// Records activity scheduling.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_activity_scheduled(
        &mut self,
        recorded_at: DateTime<Utc>,
        activity_id: ActivityId,
        activity_type: String,
        input: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ActivityScheduled {
            envelope,
            activity_id,
            activity_type,
            input,
        })
        .await
    }

    /// Records activity start.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_activity_started(
        &mut self,
        recorded_at: DateTime<Utc>,
        activity_id: ActivityId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ActivityStarted {
            envelope,
            activity_id,
        })
        .await
    }

    /// Records successful activity completion.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_activity_completed(
        &mut self,
        recorded_at: DateTime<Utc>,
        activity_id: ActivityId,
        result: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ActivityCompleted {
            envelope,
            activity_id,
            result,
        })
        .await
    }

    /// Records failed activity attempt.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_activity_failed(
        &mut self,
        recorded_at: DateTime<Utc>,
        activity_id: ActivityId,
        error: ActivityError,
        attempt: u32,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ActivityFailed {
            envelope,
            activity_id,
            error,
            attempt,
        })
        .await
    }

    /// Records activity cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_activity_cancelled(
        &mut self,
        recorded_at: DateTime<Utc>,
        activity_id: ActivityId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ActivityCancelled {
            envelope,
            activity_id,
        })
        .await
    }

    /// Records timer scheduling.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_timer_started(
        &mut self,
        recorded_at: DateTime<Utc>,
        timer_id: TimerId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::TimerStarted {
            envelope,
            timer_id,
            fire_at,
        })
        .await
    }

    /// Records timer firing.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_timer_fired(
        &mut self,
        recorded_at: DateTime<Utc>,
        timer_id: TimerId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::TimerFired {
            envelope,
            timer_id,
        })
        .await
    }

    /// Records timer cancellation.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_timer_cancelled(
        &mut self,
        recorded_at: DateTime<Utc>,
        timer_id: TimerId,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::TimerCancelled {
            envelope,
            timer_id,
        })
        .await
    }

    /// Records signal delivery.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_signal_received(
        &mut self,
        recorded_at: DateTime<Utc>,
        name: String,
        payload: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::SignalReceived {
            envelope,
            name,
            payload,
        })
        .await
    }

    /// Records a signal sent by this workflow.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_signal_sent(
        &mut self,
        recorded_at: DateTime<Utc>,
        target_workflow_id: WorkflowId,
        name: String,
        payload: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::SignalSent {
            envelope,
            target_workflow_id,
            name,
            payload,
        })
        .await
    }

    /// Records child workflow start.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_child_workflow_started(
        &mut self,
        recorded_at: DateTime<Utc>,
        child_workflow_id: WorkflowId,
        workflow_type: String,
        input: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ChildWorkflowStarted {
            envelope,
            child_workflow_id,
            workflow_type,
            input,
        })
        .await
    }

    /// Records child workflow completion.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_child_workflow_completed(
        &mut self,
        recorded_at: DateTime<Utc>,
        child_workflow_id: WorkflowId,
        result: Payload,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ChildWorkflowCompleted {
            envelope,
            child_workflow_id,
            result,
        })
        .await
    }

    /// Records child workflow failure.
    ///
    /// # Errors
    ///
    /// Returns [`DurabilityError`] if the event store rejects the append or the sequence
    /// tracker cannot advance after a successful append.
    pub async fn record_child_workflow_failed(
        &mut self,
        recorded_at: DateTime<Utc>,
        child_workflow_id: WorkflowId,
        error: WorkflowError,
    ) -> Result<(), DurabilityError> {
        self.append_with(recorded_at, |envelope| Event::ChildWorkflowFailed {
            envelope,
            child_workflow_id,
            error,
        })
        .await
    }

    fn next_envelope(&self, recorded_at: DateTime<Utc>) -> Result<EventEnvelope, DurabilityError> {
        let seq = self
            .sequence
            .next_seq()
            .ok_or_else(|| DurabilityError::HistoryShape {
                reason: format!(
                    "sequence head overflow advancing {} by 1",
                    self.sequence.current()
                ),
            })?;
        Ok(EventEnvelope {
            seq,
            recorded_at,
            workflow_id: self.workflow_id.clone(),
        })
    }

    async fn append_with(
        &mut self,
        recorded_at: DateTime<Utc>,
        build_event: impl FnOnce(EventEnvelope) -> Event,
    ) -> Result<(), DurabilityError> {
        let envelope = self.next_envelope(recorded_at)?;
        self.append_one(build_event(envelope)).await
    }

    async fn append_one(&mut self, event: Event) -> Result<(), DurabilityError> {
        let expected_seq = self.sequence.current();
        self.store
            .append(
                self.write_token,
                &self.workflow_id,
                std::slice::from_ref(&event),
                expected_seq,
            )
            .await?;
        self.sequence.mark_append_success(1)
    }

    async fn upsert_visibility_projection(&self) -> Result<(), DurabilityError> {
        let Some(visibility) = &self.visibility else {
            return Ok(());
        };
        let history = self.store.read_history(&self.workflow_id).await?;
        let record = visibility_record_from_history(&history, &visibility.run_id)?;
        visibility.store.record_visibility(record).await?;
        Ok(())
    }
}

fn visibility_record_from_history(
    history: &[Event],
    run_id: &RunId,
) -> Result<VisibilityRecord, DurabilityError> {
    let (workflow_id, workflow_type, start_time) = history
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
        .ok_or_else(|| DurabilityError::HistoryShape {
            reason: String::from(
                "workflow history has no WorkflowStarted event for visibility projection",
            ),
        })?;

    Ok(VisibilityRecord {
        workflow_id,
        run_id: run_id.clone(),
        workflow_type,
        status: aion_core::status_from_events(history),
        start_time,
        close_time: terminal_recorded_at(history),
        search_attributes: search_attributes_from_history(history),
    })
}

fn terminal_recorded_at(history: &[Event]) -> Option<DateTime<Utc>> {
    history.iter().rev().find_map(|event| match event {
        Event::WorkflowCompleted { envelope, .. }
        | Event::WorkflowFailed { envelope, .. }
        | Event::WorkflowCancelled { envelope, .. }
        | Event::WorkflowTimedOut { envelope, .. }
        | Event::WorkflowContinuedAsNew { envelope, .. } => Some(envelope.recorded_at),
        _ => None,
    })
}

fn search_attributes_from_history(history: &[Event]) -> HashMap<String, SearchAttributeValue> {
    let mut attributes = HashMap::new();
    for event in history {
        if let Event::SearchAttributesUpdated {
            attributes: updated,
            ..
        } = event
        {
            attributes.extend(updated.clone());
        }
    }
    attributes
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use aion_core::{
        Event, Payload, SearchAttributeError, SearchAttributeSchema, SearchAttributeType,
        SearchAttributeValue, TimerId,
    };
    use aion_store::visibility::{ListWorkflowsFilter, VisibilityStore};
    use aion_store::{
        InMemoryStore, ReadableEventStore, StoreError, WritableEventStore, WriteToken,
    };
    use chrono::{DateTime, Utc};
    use serde_json::json;

    use super::Recorder;
    use crate::durability::DurabilityError;

    fn workflow_id(value: u128) -> aion_core::WorkflowId {
        aion_core::WorkflowId::new(uuid::Uuid::from_u128(value))
    }

    fn recorded_at(offset_seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0).unwrap_or_default()
    }

    fn payload(label: &str) -> Result<Payload, Box<dyn std::error::Error>> {
        Ok(Payload::from_json(&json!({ "label": label }))?)
    }

    fn workflow_started(
        seq: u64,
        workflow_id: &aion_core::WorkflowId,
    ) -> Result<Event, Box<dyn std::error::Error>> {
        Ok(Event::WorkflowStarted {
            envelope: aion_core::EventEnvelope {
                seq,
                recorded_at: recorded_at(i64::try_from(seq)?),
                workflow_id: workflow_id.clone(),
            },
            workflow_type: String::from("checkout"),
            input: payload("workflow-input")?,
            run_id: aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            parent_run_id: None,
        })
    }

    #[tokio::test]
    async fn recorder_advances_expected_sequence_between_appends()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(1);
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone());

        recorder
            .record_workflow_started(
                recorded_at(1),
                String::from("checkout"),
                payload("input")?,
                aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            )
            .await?;
        recorder
            .record_workflow_completed(recorded_at(2), payload("result")?)
            .await?;

        let history = store.read_history(&workflow_id).await?;
        assert_eq!(history[0].seq(), 1);
        assert_eq!(history[1].seq(), 2);
        assert_eq!(recorder.current_head(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn records_workflow_continued_as_new_terminal_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(2);
        let parent_run_id = aion_core::RunId::new(uuid::Uuid::from_u128(20));
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone());
        let continued_at = recorded_at(2);
        let continued_input = payload("continued-input")?;
        let workflow_type = Some(String::from("checkout-v2"));

        recorder
            .record_workflow_started(
                recorded_at(1),
                String::from("checkout"),
                payload("input")?,
                aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            )
            .await?;
        recorder
            .record_workflow_continued_as_new(
                continued_at,
                continued_input.clone(),
                workflow_type.clone(),
                parent_run_id.clone(),
            )
            .await?;

        let history = store.read_history(&workflow_id).await?;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].seq(), 1);
        assert_eq!(history[1].seq(), 2);
        match &history[1] {
            Event::WorkflowContinuedAsNew {
                envelope,
                input,
                workflow_type: recorded_workflow_type,
                parent_run_id: recorded_parent_run_id,
            } => {
                assert_eq!(envelope.recorded_at, continued_at);
                assert_eq!(input, &continued_input);
                assert_eq!(recorded_workflow_type, &workflow_type);
                assert_eq!(recorded_parent_run_id, &parent_run_id);
            }
            other => return Err(format!("expected WorkflowContinuedAsNew, got {other:?}").into()),
        }
        assert_eq!(recorder.current_head(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn records_validated_search_attributes_updated_event()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(7);
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone());
        let mut schema = SearchAttributeSchema::new();
        schema.register("customer_id", SearchAttributeType::String)?;
        schema.register("attempt", SearchAttributeType::Int)?;
        let attributes = HashMap::from([
            (
                String::from("customer_id"),
                SearchAttributeValue::String(String::from("customer-123")),
            ),
            (String::from("attempt"), SearchAttributeValue::Int(2)),
        ]);

        recorder
            .record_workflow_started(
                recorded_at(1),
                String::from("checkout"),
                payload("input")?,
                aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            )
            .await?;
        recorder
            .record_search_attributes_updated(recorded_at(2), attributes.clone(), &schema)
            .await?;

        let history = store.read_history(&workflow_id).await?;
        match history.as_slice() {
            [
                Event::WorkflowStarted { .. },
                Event::SearchAttributesUpdated {
                    envelope,
                    workflow_id: recorded_workflow_id,
                    attributes: stored_attributes,
                },
            ] => {
                assert_eq!(envelope.seq, 2);
                assert_eq!(recorded_workflow_id, &workflow_id);
                assert_eq!(stored_attributes, &attributes);
            }
            other => {
                return Err(
                    format!("expected started then search attributes, found {other:?}").into(),
                );
            }
        }
        assert_eq!(recorder.current_head(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn invalid_search_attributes_return_error_without_appending()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(8);
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone());
        let mut schema = SearchAttributeSchema::new();
        schema.register("attempt", SearchAttributeType::Int)?;

        recorder
            .record_workflow_started(
                recorded_at(1),
                String::from("checkout"),
                payload("input")?,
                aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            )
            .await?;
        let attributes = HashMap::from([(
            String::from("attempt"),
            SearchAttributeValue::String(String::from("two")),
        )]);
        let error = recorder
            .record_search_attributes_updated(recorded_at(2), attributes, &schema)
            .await;

        match error {
            Err(DurabilityError::SearchAttribute(SearchAttributeError::TypeMismatch {
                name,
                expected,
                actual,
            })) => {
                assert_eq!(name, "attempt");
                assert_eq!(expected, SearchAttributeType::Int);
                assert_eq!(actual, SearchAttributeType::String);
            }
            Err(other) => {
                return Err(format!("expected search attribute error, got {other:?}").into());
            }
            Ok(()) => return Err("expected search attribute validation error".into()),
        }
        assert_eq!(recorder.current_head(), 1);
        assert_eq!(store.read_history(&workflow_id).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn recorder_visibility_updates_after_search_attributes()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(9);
        let run_id = aion_core::RunId::new(uuid::Uuid::from_u128(90));
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone())
            .with_visibility(run_id.clone(), store.clone());
        let mut schema = SearchAttributeSchema::new();
        schema.register("customer_id", SearchAttributeType::String)?;
        let attributes = HashMap::from([(
            String::from("customer_id"),
            SearchAttributeValue::String(String::from("customer-123")),
        )]);

        recorder
            .record_workflow_started(
                recorded_at(1),
                String::from("checkout"),
                payload("input")?,
                aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            )
            .await?;
        recorder
            .record_search_attributes_updated(recorded_at(2), attributes.clone(), &schema)
            .await?;

        let summaries = store.list_workflows(ListWorkflowsFilter::default()).await?;
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].workflow_id, workflow_id);
        assert_eq!(summaries[0].run_id, run_id);
        assert_eq!(summaries[0].search_attributes, attributes);
        Ok(())
    }

    #[tokio::test]
    async fn records_activity_events_in_sequence_order() -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(6);
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone());
        let activity_id = aion_core::ActivityId::from_sequence_position(1);

        recorder
            .record_activity_scheduled(
                recorded_at(1),
                activity_id.clone(),
                String::from("charge-card"),
                payload("input")?,
            )
            .await?;
        recorder
            .record_activity_completed(recorded_at(2), activity_id.clone(), payload("result")?)
            .await?;

        let history = store.read_history(&workflow_id).await?;
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].seq(), 1);
        assert_eq!(history[1].seq(), 2);
        match &history[0] {
            Event::ActivityScheduled {
                activity_id: recorded_activity_id,
                ..
            } => assert_eq!(recorded_activity_id, &activity_id),
            other => return Err(format!("expected ActivityScheduled, got {other:?}").into()),
        }
        match &history[1] {
            Event::ActivityCompleted {
                activity_id: recorded_activity_id,
                ..
            } => assert_eq!(recorded_activity_id, &activity_id),
            other => return Err(format!("expected ActivityCompleted, got {other:?}").into()),
        }
        Ok(())
    }

    #[tokio::test]
    async fn resume_at_continues_from_existing_history_head()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(3);
        let store = Arc::new(InMemoryStore::default());
        let seeded = [
            workflow_started(1, &workflow_id)?,
            workflow_started(2, &workflow_id)?,
        ];
        store
            .append(WriteToken::recorder(), &workflow_id, &seeded, 0)
            .await?;

        let mut recorder = Recorder::resume_at(workflow_id.clone(), store.clone(), 2);
        recorder
            .record_signal_received(recorded_at(3), String::from("approve"), payload("signal")?)
            .await?;

        let history = store.read_history(&workflow_id).await?;
        assert_eq!(history.len(), 3);
        assert_eq!(history[2].seq(), 3);
        assert_eq!(recorder.current_head(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn sequence_conflict_surfaces_without_advancing_or_retrying()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(4);
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::new(workflow_id.clone(), store.clone());
        let rogue_event = workflow_started(1, &workflow_id)?;
        store
            .append(WriteToken::recorder(), &workflow_id, &[rogue_event], 0)
            .await?;

        let error = recorder
            .record_timer_fired(recorded_at(2), TimerId::anonymous(2))
            .await;

        match error {
            Err(DurabilityError::Store(StoreError::SequenceConflict { expected, found })) => {
                assert_eq!(expected, 0);
                assert_eq!(found, 1);
            }
            Err(other) => return Err(format!("expected sequence conflict, got {other:?}").into()),
            Ok(()) => return Err("expected sequence conflict".into()),
        }
        assert_eq!(recorder.current_head(), 0);
        let history = store.read_history(&workflow_id).await?;
        assert_eq!(history.len(), 1);
        assert_eq!(history[0].seq(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn sequence_overflow_returns_error_without_appending()
    -> Result<(), Box<dyn std::error::Error>> {
        let workflow_id = workflow_id(5);
        let store = Arc::new(InMemoryStore::default());
        let mut recorder = Recorder::resume_at(workflow_id.clone(), store.clone(), u64::MAX);

        let error = recorder
            .record_workflow_completed(recorded_at(1), payload("result")?)
            .await;

        match error {
            Err(DurabilityError::HistoryShape { reason }) => {
                assert!(reason.contains("sequence head overflow"));
            }
            Err(other) => return Err(format!("expected sequence overflow, got {other:?}").into()),
            Ok(()) => return Err("expected sequence overflow".into()),
        }
        assert_eq!(recorder.current_head(), u64::MAX);
        assert!(store.read_history(&workflow_id).await?.is_empty());
        Ok(())
    }
}
