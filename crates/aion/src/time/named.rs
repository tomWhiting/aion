//! Named/cancellable timers and anonymous sleeps.

use std::time::Duration;

use aion_core::{TimerId, WorkflowId};
use chrono::{DateTime, Utc};

use crate::time::{TimerService, TimerServiceError};

/// Result returned when an anonymous sleep timer is scheduled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SleepTimer {
    /// Engine-assigned anonymous timer id derived from the deterministic sequence position.
    pub timer_id: TimerId,
    /// Deterministic fire timestamp computed from the workflow's recorded timestamp.
    pub fire_at: DateTime<Utc>,
}

/// Errors returned by anonymous sleep scheduling.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum SleepTimerError {
    /// The supplied standard-library duration cannot be represented by chrono.
    #[error("sleep duration cannot be represented as a chrono duration")]
    DurationOutOfRange,

    /// Adding the duration to the recorded workflow timestamp overflowed.
    #[error("sleep fire_at timestamp overflowed recorded workflow time")]
    FireAtOutOfRange,

    /// Durable timer scheduling failed.
    #[error("sleep timer scheduling failed: {0}")]
    Timer(#[from] TimerServiceError),
}

/// Starts a named, cancellable timer with the author-assigned [`TimerId`].
///
/// The supplied `timer_id` is preserved verbatim in the durable timer row and `TimerStarted` event;
/// this wrapper does not derive or rewrite named ids.
///
/// # Errors
///
/// Returns [`TimerServiceError`] when durable scheduling, recording, residency resolution, or live
/// wheel arming fails.
pub async fn start_timer(
    service: &TimerService,
    workflow_id: WorkflowId,
    timer_id: TimerId,
    fire_at: DateTime<Utc>,
) -> Result<(), TimerServiceError> {
    service.schedule(workflow_id, timer_id, fire_at).await
}

/// Cancels a named timer if it has not already fired or been cancelled.
///
/// Active resident timers are disarmed through the engine seam and then recorded as
/// `TimerCancelled`. Already-fired or already-cancelled timers are idempotent no-ops.
///
/// # Errors
///
/// Returns [`TimerServiceError`] when an anonymous timer id is supplied, or when history inspection,
/// residency resolution, disarming, or cancellation recording fails.
pub async fn cancel_timer(
    service: &TimerService,
    workflow_id: WorkflowId,
    timer_id: TimerId,
) -> Result<(), TimerServiceError> {
    service.cancel(workflow_id, timer_id).await
}

/// Schedules an anonymous durable sleep timer using deterministic workflow inputs.
///
/// `recorded_now` must be the current timestamp supplied by AD's determinism context, not the wall
/// clock. The anonymous [`TimerId`] is deterministically derived from `sequence_position` via
/// [`TimerId::anonymous`], so replay can reconstruct the same id. Anonymous sleep timers do not have
/// a separate public cancel entrypoint; cancelling a sleep is modelled as cancelling the owning
/// workflow.
///
/// # Errors
///
/// Returns [`SleepTimerError`] when duration conversion overflows, `fire_at` overflows, or durable
/// timer scheduling fails.
pub async fn sleep(
    service: &TimerService,
    workflow_id: WorkflowId,
    duration: Duration,
    recorded_now: DateTime<Utc>,
    sequence_position: u64,
) -> Result<SleepTimer, SleepTimerError> {
    let chrono_duration =
        chrono::Duration::from_std(duration).map_err(|_| SleepTimerError::DurationOutOfRange)?;
    let fire_at = recorded_now
        .checked_add_signed(chrono_duration)
        .ok_or(SleepTimerError::FireAtOutOfRange)?;
    let timer_id = TimerId::anonymous(sequence_position);

    service
        .schedule(workflow_id, timer_id.clone(), fire_at)
        .await?;

    Ok(SleepTimer { timer_id, fire_at })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use aion_core::{Event, IdError, TimerId, WorkflowId};
    use aion_store::{EventStore, InMemoryStore, ReadableEventStore, StoreError};
    use chrono::{DateTime, Utc};

    use super::{SleepTimerError, cancel_timer, sleep, start_timer};
    use crate::engine_seam::test_support::{FakeEngineHandle, FakeEngineOperation};
    use crate::engine_seam::{TimerWheelEntry, WorkflowProcessHandle, WorkflowResidency};
    use crate::time::{TimerService, TimerServiceError};

    #[derive(thiserror::Error, Debug)]
    enum TestError {
        #[error(transparent)]
        Timer(#[from] TimerServiceError),
        #[error(transparent)]
        Sleep(#[from] SleepTimerError),
        #[error(transparent)]
        Store(#[from] StoreError),
        #[error(transparent)]
        Engine(#[from] crate::engine_seam::EngineSeamError),
        #[error(transparent)]
        Id(#[from] IdError),
    }

    fn instant(offset_seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0).unwrap_or_default()
    }

    fn recorded_at() -> DateTime<Utc> {
        instant(1)
    }

    fn workflow_id() -> WorkflowId {
        WorkflowId::new_v4()
    }

    fn service() -> (Arc<InMemoryStore>, Arc<FakeEngineHandle>, TimerService) {
        let concrete_store = Arc::new(InMemoryStore::default());
        let store: Arc<dyn EventStore> = concrete_store.clone();
        let engine = Arc::new(FakeEngineHandle::recording_to(store.clone()));
        let service = TimerService::with_recorded_at(engine.clone(), store, recorded_at);
        (concrete_store, engine, service)
    }

    async fn history(
        store: &InMemoryStore,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<Event>, StoreError> {
        store.read_history(workflow_id).await
    }

    fn count_cancelled(events: &[Event], timer_id: &TimerId) -> usize {
        events
            .iter()
            .filter(|event| {
                matches!(event, Event::TimerCancelled { timer_id: recorded, .. } if recorded == timer_id)
            })
            .count()
    }

    fn count_fired(events: &[Event], timer_id: &TimerId) -> usize {
        events
            .iter()
            .filter(|event| {
                matches!(event, Event::TimerFired { timer_id: recorded, .. } if recorded == timer_id)
            })
            .count()
    }

    #[tokio::test]
    async fn start_timer_preserves_named_id_in_history_and_timer_row() -> Result<(), TestError> {
        let (store, _engine, service) = service();
        let workflow_id = workflow_id();
        let timer_id = TimerId::named("deadline")?;
        let fire_at = instant(10);

        start_timer(&service, workflow_id.clone(), timer_id.clone(), fire_at).await?;

        let expired = store.expired_timers(fire_at).await?;
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].workflow_id, workflow_id);
        assert_eq!(expired[0].timer_id, timer_id);
        assert_eq!(expired[0].fire_at, fire_at);

        // TimerStarted is now recorded by AD's resume-live handoff, not by the timer service.
        let history = history(&store, &workflow_id).await?;
        assert!(history.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn cancel_timer_disarms_resident_wheel_and_records_cancelled() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, service) = service();
        let workflow_id = workflow_id();
        let timer_id = TimerId::named("deadline")?;
        let fire_at = instant(20);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;

        start_timer(&service, workflow_id.clone(), timer_id.clone(), fire_at).await?;
        cancel_timer(&service, workflow_id.clone(), timer_id.clone()).await?;

        assert!(engine.armed_timers()?.is_empty());
        let history = history(&store, &workflow_id).await?;
        assert_eq!(count_cancelled(&history, &timer_id), 1);
        // TimerStarted is recorded by AD's resume-live handoff, not by the timer service,
        // so only the TimerCancelled event appears in history.
        assert!(matches!(
            history.as_slice(),
            [
                Event::TimerCancelled {
                    envelope,
                    timer_id: recorded,
                }
            ] if envelope.seq == 1 && recorded == &timer_id
        ));
        assert!(engine.operations()?.iter().any(|operation| matches!(
            operation,
            FakeEngineOperation::TimerDisarmed { process: disarmed_process, timer_id: disarmed }
                if disarmed_process == &process && disarmed == &timer_id
        )));
        Ok(())
    }

    #[tokio::test]
    async fn cancel_timer_after_fire_is_noop() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, service) = service();
        let workflow_id = workflow_id();
        let timer_id = TimerId::named("deadline")?;
        let fire_at = instant(30);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;

        start_timer(&service, workflow_id.clone(), timer_id.clone(), fire_at).await?;
        service
            .fire_timer(workflow_id.clone(), timer_id.clone(), fire_at)
            .await?;
        let operation_count = engine.operations()?.len();

        cancel_timer(&service, workflow_id.clone(), timer_id.clone()).await?;

        let history = history(&store, &workflow_id).await?;
        assert_eq!(count_fired(&history, &timer_id), 1);
        assert_eq!(count_cancelled(&history, &timer_id), 0);
        assert_eq!(engine.operations()?.len(), operation_count);
        Ok(())
    }

    #[tokio::test]
    async fn cancel_timer_after_cancel_is_idempotent_noop() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, service) = service();
        let workflow_id = workflow_id();
        let timer_id = TimerId::named("deadline")?;
        let fire_at = instant(40);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;

        start_timer(&service, workflow_id.clone(), timer_id.clone(), fire_at).await?;
        cancel_timer(&service, workflow_id.clone(), timer_id.clone()).await?;
        let operation_count = engine.operations()?.len();

        cancel_timer(&service, workflow_id.clone(), timer_id.clone()).await?;

        let history = history(&store, &workflow_id).await?;
        assert_eq!(count_cancelled(&history, &timer_id), 1);
        assert_eq!(engine.operations()?.len(), operation_count);
        Ok(())
    }

    #[tokio::test]
    async fn cancel_timer_rejects_anonymous_sleep_timer() -> Result<(), TestError> {
        let (store, engine, service) = service();
        let workflow_id = workflow_id();
        let timer_id = TimerId::anonymous(42);

        let result = cancel_timer(&service, workflow_id.clone(), timer_id.clone()).await;

        assert!(matches!(
            result,
            Err(TimerServiceError::AnonymousTimerNotCancellable { timer_id: rejected })
                if rejected == timer_id
        ));
        assert!(history(&store, &workflow_id).await?.is_empty());
        assert!(engine.operations()?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn sleep_derives_anonymous_id_and_fire_at_from_recorded_inputs() -> Result<(), TestError>
    {
        let (store, _engine, service) = service();
        let workflow_id = workflow_id();
        let recorded_now = instant(50);
        let duration = Duration::from_secs(15);
        let sequence_position = 9;
        let expected_timer_id = TimerId::anonymous(sequence_position);
        let expected_fire_at = instant(65);

        let scheduled = sleep(
            &service,
            workflow_id.clone(),
            duration,
            recorded_now,
            sequence_position,
        )
        .await?;

        assert_eq!(scheduled.timer_id, expected_timer_id);
        assert_eq!(scheduled.fire_at, expected_fire_at);
        // TimerStarted is recorded by AD's resume-live handoff, not by the timer service.
        let history = history(&store, &workflow_id).await?;
        assert!(history.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn start_timer_arms_named_timer_without_rewriting_id() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (_store, engine, service) = service();
        let workflow_id = workflow_id();
        let timer_id = TimerId::named("deadline")?;
        let fire_at = instant(70);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;

        start_timer(&service, workflow_id, timer_id.clone(), fire_at).await?;

        assert_eq!(
            engine.armed_timers()?,
            vec![TimerWheelEntry {
                process,
                timer_id,
                fire_at,
            }]
        );
        Ok(())
    }
}
