//! Connected-worker registry keyed by namespace and activity type.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex, MutexGuard};

use aion_proto::{ProtoActivityTask, ProtoRegisterWorker};
use tokio::sync::{Notify, mpsc};

use crate::error::ServerError;
use crate::namespace::{CallerIdentity, NamespaceGuard, NamespaceOperation};
use crate::observability::Metrics;

/// Server-side handle used to push activity tasks to a connected worker stream.
pub type WorkerTaskSender = mpsc::Sender<WorkerMessage>;

/// Message queued from server-side dispatch/shutdown into a worker stream writer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerMessage {
    /// Activity invocation pushed to a worker.
    ActivityTask(ProtoActivityTask),
    /// Graceful-shutdown notification; no new work will be dispatched.
    DrainRequest,
}

type ActivityKey = (String, String);
type WorkerMap = HashMap<WorkerId, WorkerHandle>;
type RegistryMap = HashMap<ActivityKey, WorkerMap>;

/// Stable identifier assigned to a connected worker stream.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct WorkerId(u64);

impl WorkerId {
    /// Raw numeric value, as carried by the wire `RegisterAck.worker_id` so
    /// workers can correlate their logs with the server's.
    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }
}

/// Cloneable handle for a registered worker stream.
#[derive(Clone, Debug)]
pub struct WorkerHandle {
    id: WorkerId,
    namespace: String,
    activity_types: BTreeSet<String>,
    sender: WorkerTaskSender,
}

impl WorkerHandle {
    /// Worker identifier assigned by this server process.
    #[must_use]
    pub const fn id(&self) -> WorkerId {
        self.id
    }

    /// Namespace authorized for this worker stream.
    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Activity types advertised by this worker.
    #[must_use]
    pub fn activity_types(&self) -> &BTreeSet<String> {
        &self.activity_types
    }

    /// Sender used by dispatch to push work to the stream task.
    #[must_use]
    pub fn sender(&self) -> &WorkerTaskSender {
        &self.sender
    }
}

#[derive(Debug)]
struct RegistryState {
    next_worker_id: u64,
    workers: BTreeMap<WorkerId, WorkerHandle>,
    by_activity: RegistryMap,
    rotation: HashMap<ActivityKey, usize>,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            next_worker_id: 1,
            workers: BTreeMap::new(),
            by_activity: HashMap::new(),
            rotation: HashMap::new(),
        }
    }
}

/// Cloneable registry of currently connected worker streams.
#[derive(Clone, Debug)]
pub struct ConnectedWorkerRegistry {
    inner: Arc<Mutex<RegistryState>>,
    metrics: Option<Metrics>,
    worker_arrived: Arc<Notify>,
}

impl Default for ConnectedWorkerRegistry {
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState::default())),
            metrics: None,
            worker_arrived: Arc::new(Notify::new()),
        }
    }
}

impl ConnectedWorkerRegistry {
    /// Build a registry that records connected-worker gauge updates.
    #[must_use]
    pub fn with_metrics(metrics: Metrics) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RegistryState::default())),
            metrics: Some(metrics),
            worker_arrived: Arc::new(Notify::new()),
        }
    }

    /// Authorize a worker registration and insert it into the connected-worker registry.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError`] if namespace authorization fails or the registry lock is poisoned.
    pub async fn accept_registration(
        &self,
        guard: &NamespaceGuard,
        caller: &CallerIdentity,
        registration: &ProtoRegisterWorker,
        sender: WorkerTaskSender,
    ) -> Result<WorkerRegistration, ServerError> {
        let scoped = guard
            .scope(caller, &NamespaceOperation::register_worker(registration))
            .await?;
        self.register(
            scoped.namespace(),
            registration.activity_types.iter(),
            sender,
        )
    }

    /// Insert an already-authorized worker stream.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn register<'a>(
        &self,
        namespace: impl Into<String>,
        activity_types: impl IntoIterator<Item = &'a String>,
        sender: WorkerTaskSender,
    ) -> Result<WorkerRegistration, ServerError> {
        let namespace = namespace.into();
        let activity_types = activity_types.into_iter().cloned().collect::<BTreeSet<_>>();
        let mut state = self.state()?;
        let worker_id = WorkerId(state.next_worker_id);
        state.next_worker_id = state.next_worker_id.saturating_add(1);

        let handle = WorkerHandle {
            id: worker_id,
            namespace: namespace.clone(),
            activity_types: activity_types.clone(),
            sender,
        };

        for activity_type in &activity_types {
            state
                .by_activity
                .entry((namespace.clone(), activity_type.clone()))
                .or_default()
                .insert(worker_id, handle.clone());
        }
        state.workers.insert(worker_id, handle);
        drop(state);

        if let Some(metrics) = &self.metrics {
            metrics.worker_connected(&namespace);
        }

        self.worker_arrived.notify_waiters();

        Ok(WorkerRegistration {
            registry: self.clone(),
            parts: Some(WorkerRegistrationParts {
                worker_id,
                namespace,
                activity_types,
            }),
        })
    }

    /// Wait until at least one new worker registers.
    ///
    /// Returns immediately if a registration occurred since the last call.
    /// Callers should re-check the registry after waking — the newly arrived
    /// worker may not serve the namespace or activity type the caller needs.
    pub async fn wait_for_worker(&self) {
        self.worker_arrived.notified().await;
    }

    /// Return a snapshot of workers registered for the namespace and activity
    /// type, ordered by worker id and then rotated so each call starts from the
    /// next worker in the pool.
    ///
    /// The id sort matters: `by_activity` holds workers in a `HashMap`, whose
    /// iteration order is unspecified. Sorting first makes the rotation below
    /// the sole, deterministic source of ordering — true round-robin across
    /// calls with the same membership, not a wobble layered on hash order.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn workers_for(
        &self,
        namespace: &str,
        activity_type: &str,
    ) -> Result<Vec<WorkerHandle>, ServerError> {
        let mut state = self.state()?;
        let key = (namespace.to_owned(), activity_type.to_owned());
        let mut workers: Vec<WorkerHandle> = state
            .by_activity
            .get(&key)
            .map(|workers| workers.values().cloned().collect())
            .unwrap_or_default();
        if workers.is_empty() {
            return Ok(workers);
        }
        workers.sort_by_key(WorkerHandle::id);
        let idx = state.rotation.entry(key).or_insert(0);
        let start = *idx % workers.len();
        *idx = idx.wrapping_add(1);
        let mut rotated = Vec::with_capacity(workers.len());
        rotated.extend_from_slice(&workers[start..]);
        rotated.extend_from_slice(&workers[..start]);
        Ok(rotated)
    }

    /// Return a snapshot of every connected worker stream.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn all_workers(&self) -> Result<Vec<WorkerHandle>, ServerError> {
        let state = self.state()?;
        Ok(state.workers.values().cloned().collect())
    }

    /// Broadcast a graceful drain request to every connected worker stream.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn broadcast_drain(&self) -> Result<usize, ServerError> {
        let workers = self.all_workers()?;
        let mut delivered = 0usize;
        for worker in workers {
            if worker
                .sender()
                .try_send(WorkerMessage::DrainRequest)
                .is_ok()
            {
                delivered = delivered.saturating_add(1);
            } else {
                self.deregister(worker.id())?;
            }
        }
        Ok(delivered)
    }

    /// Return whether a worker stream is currently registered.
    ///
    /// The activity dispatch path uses this after queuing a task to detect a
    /// worker whose stream tore down concurrently: a sweep that ran before
    /// the dispatch tracked its task can never complete it, so the dispatch
    /// must fail the activity itself instead of waiting forever.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn is_registered(&self, worker_id: WorkerId) -> Result<bool, ServerError> {
        Ok(self.state()?.workers.contains_key(&worker_id))
    }

    /// Remove a worker by id from every namespace/activity index it advertised.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn deregister(&self, worker_id: WorkerId) -> Result<(), ServerError> {
        let mut state = self.state()?;
        let removed_namespace = Self::remove_worker(&mut state, worker_id);
        drop(state);

        if let (Some(namespace), Some(metrics)) = (removed_namespace, &self.metrics) {
            metrics.worker_disconnected(&namespace);
        }

        Ok(())
    }

    fn remove_worker(state: &mut RegistryState, worker_id: WorkerId) -> Option<String> {
        let handle = state.workers.remove(&worker_id)?;
        let namespace = handle.namespace.clone();

        for activity_type in handle.activity_types {
            let key = (handle.namespace.clone(), activity_type);
            if let Some(workers) = state.by_activity.get_mut(&key) {
                workers.remove(&worker_id);
                if workers.is_empty() {
                    state.by_activity.remove(&key);
                    // Drop the rotation cursor too: with no workers for the key
                    // the index is dead, and leaving it would let the rotation
                    // map grow without bound across every activity type the
                    // server ever serves. A later re-registration restarts the
                    // rotation from 0, which is acceptable (it already resets on
                    // restart).
                    state.rotation.remove(&key);
                }
            }
        }

        Some(namespace)
    }

    fn state(&self) -> Result<MutexGuard<'_, RegistryState>, ServerError> {
        self.inner
            .lock()
            .map_err(|_| ServerError::lock_poisoned("connected worker registry"))
    }

    /// Number of `(namespace, activity_type)` rotation cursors currently held.
    /// Test-only: dispatch never reads it, but tests assert the rotation map is
    /// pruned when a key's workers all deregister.
    #[cfg(test)]
    fn rotation_key_count(&self) -> Result<usize, ServerError> {
        Ok(self.state()?.rotation.len())
    }
}

#[derive(Clone, Debug)]
struct WorkerRegistrationParts {
    worker_id: WorkerId,
    namespace: String,
    activity_types: BTreeSet<String>,
}

/// Registration token owned by the worker stream task.
///
/// Dropping the token performs best-effort cleanup for disconnect paths. Call
/// [`WorkerRegistration::deregister`] when the caller needs a typed poison error.
#[derive(Debug)]
pub struct WorkerRegistration {
    registry: ConnectedWorkerRegistry,
    parts: Option<WorkerRegistrationParts>,
}

impl WorkerRegistration {
    /// Worker id assigned to this registration.
    #[must_use]
    pub fn worker_id(&self) -> Option<WorkerId> {
        self.parts.as_ref().map(|parts| parts.worker_id)
    }

    /// Authorized namespace for this registration.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.parts.as_ref().map(|parts| parts.namespace.as_str())
    }

    /// Activity types advertised by this registration.
    #[must_use]
    pub fn activity_types(&self) -> Option<&BTreeSet<String>> {
        self.parts.as_ref().map(|parts| &parts.activity_types)
    }

    /// Explicitly remove this worker from the registry.
    ///
    /// # Errors
    ///
    /// Returns [`ServerError::LockPoisoned`] if the registry lock is poisoned.
    pub fn deregister(mut self) -> Result<(), ServerError> {
        let Some(parts) = self.parts.take() else {
            return Ok(());
        };
        self.registry.deregister(parts.worker_id)
    }
}

impl Drop for WorkerRegistration {
    fn drop(&mut self) {
        let Some(parts) = self.parts.take() else {
            return;
        };
        if let Ok(mut state) = self.registry.inner.lock() {
            let removed_namespace =
                ConnectedWorkerRegistry::remove_worker(&mut state, parts.worker_id);
            if let (Some(namespace), Some(metrics)) = (removed_namespace, &self.registry.metrics) {
                metrics.worker_disconnected(&namespace);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::config::NamespaceMode;
    use crate::namespace::{NamespaceResolver, StaticScheduleNamespaces, StaticWorkflowNamespaces};

    use super::*;

    fn guard() -> NamespaceGuard {
        NamespaceGuard::new(NamespaceResolver::authorization_only(
            NamespaceMode::SharedEngine,
            StaticWorkflowNamespaces::default(),
            StaticScheduleNamespaces::default(),
        ))
    }

    fn caller(namespace: &str) -> CallerIdentity {
        CallerIdentity::new("worker", [namespace.to_owned()])
    }

    fn registration(namespace: &str, activity_types: &[&str]) -> ProtoRegisterWorker {
        ProtoRegisterWorker {
            namespace: namespace.to_owned(),
            activity_types: activity_types
                .iter()
                .map(|value| (*value).to_owned())
                .collect(),
        }
    }

    #[tokio::test]
    async fn register_and_deregister_are_namespace_isolated() -> Result<(), ServerError> {
        let registry = ConnectedWorkerRegistry::default();
        let (tenant_a_tx, _tenant_a_rx) = mpsc::channel(1);
        let (tenant_b_tx, _tenant_b_rx) = mpsc::channel(1);

        let tenant_a = registry
            .accept_registration(
                &guard(),
                &caller("tenant-a"),
                &registration("tenant-a", &["charge", "charge"]),
                tenant_a_tx,
            )
            .await?;
        let tenant_b = registry
            .accept_registration(
                &guard(),
                &caller("tenant-b"),
                &registration("tenant-b", &["charge"]),
                tenant_b_tx,
            )
            .await?;

        assert_eq!(registry.workers_for("tenant-a", "charge")?.len(), 1);
        assert_eq!(registry.workers_for("tenant-b", "charge")?.len(), 1);
        assert!(registry.workers_for("tenant-a", "missing")?.is_empty());

        let tenant_a_id = tenant_a.worker_id();
        tenant_a.deregister()?;

        assert!(registry.workers_for("tenant-a", "charge")?.is_empty());
        assert_eq!(registry.workers_for("tenant-b", "charge")?.len(), 1);
        assert_ne!(tenant_a_id, tenant_b.worker_id());

        tenant_b.deregister()?;
        assert!(registry.workers_for("tenant-b", "charge")?.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn workers_for_rotates_start_across_calls_with_stable_membership()
    -> Result<(), ServerError> {
        let registry = ConnectedWorkerRegistry::default();
        // Hold the receivers and registrations for the whole test so the
        // worker senders stay open and the workers stay registered.
        let mut receivers = Vec::new();
        let mut registrations = Vec::new();
        let mut ids = Vec::new();
        for _ in 0..3 {
            let (tx, rx) = mpsc::channel(1);
            receivers.push(rx);
            let registration = registry
                .accept_registration(
                    &guard(),
                    &caller("tenant-a"),
                    &registration("tenant-a", &["charge"]),
                    tx,
                )
                .await?;
            ids.push(registration.worker_id());
            registrations.push(registration);
        }

        let sorted_membership = {
            let mut sorted = ids.clone();
            sorted.sort();
            sorted
        };

        let mut starts = Vec::new();
        for _ in 0..3 {
            let rotated = registry.workers_for("tenant-a", "charge")?;
            assert_eq!(rotated.len(), 3, "every call sees all three workers");
            let mut membership: Vec<Option<WorkerId>> =
                rotated.iter().map(|worker| Some(worker.id())).collect();
            membership.sort();
            assert_eq!(
                membership, sorted_membership,
                "membership is stable across rotations"
            );
            starts.push(rotated.first().map(WorkerHandle::id));
        }

        assert_eq!(
            starts.len(),
            3,
            "three calls recorded three starting workers"
        );
        let distinct: BTreeSet<_> = starts.iter().collect();
        assert_eq!(
            distinct.len(),
            3,
            "each successive call starts at a different worker: {starts:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn workers_for_stays_in_bounds_after_deregistering_rotation_position()
    -> Result<(), ServerError> {
        let registry = ConnectedWorkerRegistry::default();
        let (tx_a, _rx_a) = mpsc::channel(1);
        let (tx_b, _rx_b) = mpsc::channel(1);
        let worker_a = registry
            .accept_registration(
                &guard(),
                &caller("tenant-a"),
                &registration("tenant-a", &["charge"]),
                tx_a,
            )
            .await?;
        let worker_b = registry
            .accept_registration(
                &guard(),
                &caller("tenant-a"),
                &registration("tenant-a", &["charge"]),
                tx_b,
            )
            .await?;

        // Advance the rotation index to the second worker, then deregister the
        // worker sitting at that index. The next call must wrap the index back
        // into range rather than index out of bounds.
        let _ = registry.workers_for("tenant-a", "charge")?;
        let _ = registry.workers_for("tenant-a", "charge")?;
        worker_b.deregister()?;

        let remaining = registry.workers_for("tenant-a", "charge")?;
        assert_eq!(remaining.len(), 1, "only the surviving worker remains");
        assert_eq!(
            remaining.first().map(WorkerHandle::id),
            worker_a.worker_id(),
            "the surviving worker is returned"
        );
        worker_a.deregister()?;
        Ok(())
    }

    #[tokio::test]
    async fn rotation_cursor_is_pruned_when_last_worker_for_a_key_deregisters()
    -> Result<(), ServerError> {
        let registry = ConnectedWorkerRegistry::default();
        let (tx, _rx) = mpsc::channel(1);
        let worker = registry
            .accept_registration(
                &guard(),
                &caller("tenant-a"),
                &registration("tenant-a", &["charge"]),
                tx,
            )
            .await?;

        // Materialise a rotation cursor for the key.
        let _ = registry.workers_for("tenant-a", "charge")?;
        assert_eq!(
            registry.rotation_key_count()?,
            1,
            "cursor created for the key"
        );

        worker.deregister()?;
        assert_eq!(
            registry.rotation_key_count()?,
            0,
            "the rotation cursor is pruned once the key has no workers"
        );
        Ok(())
    }

    #[tokio::test]
    async fn denied_namespace_is_not_registered() -> Result<(), ServerError> {
        let registry = ConnectedWorkerRegistry::default();
        let (tx, _rx) = mpsc::channel(1);
        let denied = registry
            .accept_registration(
                &guard(),
                &caller("tenant-a"),
                &registration("tenant-b", &["charge"]),
                tx,
            )
            .await;

        assert!(denied.is_err());
        assert!(registry.workers_for("tenant-b", "charge")?.is_empty());
        Ok(())
    }
}
