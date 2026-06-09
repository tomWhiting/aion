//! Expired timer polling on startup and periodic recovery tick.

use std::sync::Arc;
use std::time::Duration;

use aion_store::{ReadableEventStore, StoreError};
use chrono::{DateTime, Utc};

use crate::time::{TimerService, TimerServiceError};

/// Recovery service for durable timers that elapsed outside the live wheel path.
pub struct TimerRecovery {
    store: Arc<dyn ReadableEventStore>,
    timer_service: Arc<TimerService>,
    recovery_interval: Duration,
    now: fn() -> DateTime<Utc>,
}

/// Errors returned by [`TimerRecovery`].
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum TimerRecoveryError {
    /// Durable timer polling failed.
    #[error("timer recovery store operation failed: {0}")]
    Store(#[from] StoreError),

    /// Recovered timer firing failed.
    #[error("timer recovery fire operation failed: {0}")]
    Timer(#[from] TimerServiceError),
}

impl TimerRecovery {
    /// Creates a timer recovery service with an engine-configured recovery cadence.
    #[must_use]
    pub fn new(
        store: Arc<dyn ReadableEventStore>,
        timer_service: Arc<TimerService>,
        recovery_interval: Duration,
    ) -> Self {
        Self::with_clock(store, timer_service, recovery_interval, Utc::now)
    }

    /// Creates a timer recovery service with an injected clock for deterministic ticking.
    #[must_use]
    pub fn with_clock(
        store: Arc<dyn ReadableEventStore>,
        timer_service: Arc<TimerService>,
        recovery_interval: Duration,
        now: fn() -> DateTime<Utc>,
    ) -> Self {
        Self {
            store,
            timer_service,
            recovery_interval,
            now,
        }
    }

    /// Runs the engine-startup recovery sweep for timers due as of `now`.
    ///
    /// Each due timer is delegated to [`TimerService::fire_timer`], which owns terminal filtering,
    /// the in-flight fire guard, recording `TimerFired`, and mailbox delivery.
    ///
    /// # Errors
    ///
    /// Returns [`TimerRecoveryError`] when polling expired timers or firing a due timer fails.
    pub async fn recover_on_startup(
        &self,
        now: DateTime<Utc>,
    ) -> Result<usize, TimerRecoveryError> {
        self.recover_due(now).await
    }

    /// Runs one recovery tick using the injected clock.
    ///
    /// AE owns driving this method at [`Self::recovery_interval`]; this service intentionally does
    /// not spawn or own the production runtime task.
    ///
    /// # Errors
    ///
    /// Returns [`TimerRecoveryError`] when polling expired timers or firing a due timer fails.
    pub async fn tick(&self) -> Result<usize, TimerRecoveryError> {
        self.recover_due((self.now)()).await
    }

    /// Returns the engine-configured recovery cadence.
    #[must_use]
    pub const fn recovery_interval(&self) -> Duration {
        self.recovery_interval
    }

    async fn recover_due(&self, now: DateTime<Utc>) -> Result<usize, TimerRecoveryError> {
        let due_timers = self.store.expired_timers(now).await?;
        let count = due_timers.len();
        for entry in due_timers {
            self.timer_service
                .fire_timer(entry.workflow_id, entry.timer_id, entry.fire_at)
                .await?;
        }
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use aion_core::{Event, EventEnvelope, TimerId, WorkflowId};
    use aion_store::{EventStore, InMemoryStore, ReadableEventStore, StoreError};
    use chrono::{DateTime, Utc};

    use super::{TimerRecovery, TimerRecoveryError};
    use crate::engine_seam::test_support::{DeliveredWorkflowMessage, FakeEngineHandle};
    use crate::engine_seam::{
        EngineHandle, EngineSeamError, WorkflowProcessHandle, WorkflowResidency,
    };
    use crate::time::TimerService;

    const RECOVERY_INTERVAL: Duration = Duration::from_millis(10);

    #[derive(Debug, thiserror::Error)]
    enum TestError {
        #[error(transparent)]
        Recovery(#[from] TimerRecoveryError),

        #[error(transparent)]
        Store(#[from] StoreError),

        #[error(transparent)]
        Engine(#[from] EngineSeamError),
    }

    fn instant(offset_seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000 + offset_seconds, 0).unwrap_or_default()
    }

    fn recorded_at() -> DateTime<Utc> {
        instant(1)
    }

    fn tick_now() -> DateTime<Utc> {
        instant(30)
    }

    fn workflow_id() -> WorkflowId {
        WorkflowId::new_v4()
    }

    fn timer_id(sequence: u64) -> TimerId {
        TimerId::anonymous(sequence)
    }

    fn recovery() -> (Arc<InMemoryStore>, Arc<FakeEngineHandle>, TimerRecovery) {
        let concrete_store = Arc::new(InMemoryStore::default());
        let store: Arc<dyn EventStore> = concrete_store.clone();
        let readable_store: Arc<dyn ReadableEventStore> = concrete_store.clone();
        let engine = Arc::new(FakeEngineHandle::recording_to(store));
        let timer_service = Arc::new(TimerService::with_recorded_at(
            engine.clone(),
            readable_store.clone(),
            recorded_at,
        ));
        let recovery =
            TimerRecovery::with_clock(readable_store, timer_service, RECOVERY_INTERVAL, tick_now);
        (concrete_store, engine, recovery)
    }

    async fn history(
        store: &InMemoryStore,
        workflow_id: &WorkflowId,
    ) -> Result<Vec<Event>, StoreError> {
        store.read_history(workflow_id).await
    }

    fn count_timer_fired(events: &[Event], timer_id: &TimerId) -> usize {
        events
            .iter()
            .filter(|event| {
                matches!(event, Event::TimerFired { timer_id: recorded, .. } if recorded == timer_id)
            })
            .count()
    }

    #[tokio::test]
    async fn startup_sweep_fires_past_timer_and_delivers() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, recovery) = recovery();
        let workflow_id = workflow_id();
        let timer_id = timer_id(1);
        let fire_at = instant(10);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;
        store
            .schedule_timer(&workflow_id, &timer_id, fire_at)
            .await?;

        let recovered = recovery.recover_on_startup(instant(20)).await?;

        assert_eq!(recovered, 1);
        assert_eq!(
            count_timer_fired(&history(&store, &workflow_id).await?, &timer_id),
            1
        );
        assert_eq!(
            engine.delivered_messages()?,
            vec![(
                process,
                DeliveredWorkflowMessage::TimerFired {
                    timer_id: timer_id.clone(),
                    fire_at
                }
            )]
        );
        Ok(())
    }

    #[tokio::test]
    async fn startup_sweep_does_not_fire_future_timer() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, recovery) = recovery();
        let workflow_id = workflow_id();
        let timer_id = timer_id(2);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;
        store
            .schedule_timer(&workflow_id, &timer_id, instant(30))
            .await?;

        let recovered = recovery.recover_on_startup(instant(20)).await?;

        assert_eq!(recovered, 0);
        assert_eq!(
            count_timer_fired(&history(&store, &workflow_id).await?, &timer_id),
            0
        );
        assert!(engine.delivered_messages()?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn tick_uses_injected_clock_and_fires_newly_due_timer_once() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, recovery) = recovery();
        let workflow_id = workflow_id();
        let timer_id = timer_id(3);
        let fire_at = instant(25);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;
        store
            .schedule_timer(&workflow_id, &timer_id, fire_at)
            .await?;

        assert_eq!(recovery.recovery_interval(), RECOVERY_INTERVAL);
        assert_eq!(recovery.tick().await?, 1);
        assert_eq!(recovery.tick().await?, 1);

        assert_eq!(
            count_timer_fired(&history(&store, &workflow_id).await?, &timer_id),
            1
        );
        assert_eq!(engine.delivered_messages()?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn running_startup_sweep_twice_fires_due_timer_once_total() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, recovery) = recovery();
        let workflow_id = workflow_id();
        let timer_id = timer_id(4);
        let fire_at = instant(10);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;
        store
            .schedule_timer(&workflow_id, &timer_id, fire_at)
            .await?;

        recovery.recover_on_startup(instant(20)).await?;
        recovery.recover_on_startup(instant(20)).await?;

        assert_eq!(
            count_timer_fired(&history(&store, &workflow_id).await?, &timer_id),
            1
        );
        assert_eq!(engine.delivered_messages()?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_timer_is_never_fired_by_recovery() -> Result<(), TestError> {
        let process = WorkflowProcessHandle::new(42);
        let (store, engine, recovery) = recovery();
        let workflow_id = workflow_id();
        let timer_id = timer_id(5);
        let fire_at = instant(10);
        engine.set_residency(workflow_id.clone(), WorkflowResidency::Resident(process))?;
        store
            .schedule_timer(&workflow_id, &timer_id, fire_at)
            .await?;
        engine.record_workflow_event(
            &workflow_id,
            Event::TimerCancelled {
                envelope: EventEnvelope {
                    seq: 1,
                    recorded_at: instant(9),
                    workflow_id: workflow_id.clone(),
                },
                timer_id: timer_id.clone(),
            },
        )?;

        recovery.recover_on_startup(instant(20)).await?;

        assert_eq!(
            count_timer_fired(&history(&store, &workflow_id).await?, &timer_id),
            0
        );
        assert!(engine.delivered_messages()?.is_empty());
        Ok(())
    }
}
