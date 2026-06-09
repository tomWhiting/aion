//! `EngineBuilder` and build wiring.

use std::{path::PathBuf, sync::Arc};

use chrono::Utc;

use aion_core::{Event, Payload, RunId, status_from_events};
use aion_package::Package;
use aion_store::visibility::VisibilityStore;
use aion_store::{EventStore, InMemoryStore};

use crate::{
    CompletionNotifier, EngineError, HandleResidency, LoadedWorkflows, Registry, RuntimeConfig,
    RuntimeHandle, SignalDeliveryConfig, SupervisionTree, WorkflowHandle, WorkflowHandleParts,
    activity::bridge::{ActivityDispatcher, install_activity_dispatcher},
    durability::{
        ActiveWorkflowRecovery, ActiveWorkflowRecoverySeam, DeferredActiveWorkflowRecovery,
        Recorder,
    },
    runtime::{
        ChildNifBridge, ChildNifBridgeParts, NifEntry, NifRegistration, install_child_nif_bridge,
        install_nif_runtime_context, install_query_bridge, install_signal_nif_bridge,
        nif_determinism::{NifContextSource, install_nif_context_source},
    },
    signal::SignalResumeHandoff,
};

use super::api::{
    Engine, EngineComponents, schedule_coordinator_run_id, schedule_coordinator_workflow_id,
    schedule_coordinator_workflow_type,
};
use super::delegated::{DelegatedSeams, EventPublisher, QueryService, SignalRouter};

/// Source for a workflow package collected before `build()` performs fallible
/// loading and runtime registration.
#[derive(Clone, Debug)]
pub enum WorkflowPackageSource {
    /// Load a package from this `.aion` archive path during `build()`.
    Path(PathBuf),
    /// Use an already-loaded package value.
    Package(Box<Package>),
}

impl From<Package> for WorkflowPackageSource {
    fn from(package: Package) -> Self {
        Self::Package(Box::new(package))
    }
}

impl From<PathBuf> for WorkflowPackageSource {
    fn from(path: PathBuf) -> Self {
        Self::Path(path)
    }
}

impl From<&std::path::Path> for WorkflowPackageSource {
    fn from(path: &std::path::Path) -> Self {
        Self::Path(path.to_path_buf())
    }
}

impl From<&str> for WorkflowPackageSource {
    fn from(path: &str) -> Self {
        Self::Path(PathBuf::from(path))
    }
}

impl From<String> for WorkflowPackageSource {
    fn from(path: String) -> Self {
        Self::Path(PathBuf::from(path))
    }
}

type SignalRouterFactory = Arc<
    dyn Fn(Arc<RuntimeHandle>, Arc<SignalResumeHandoff>) -> Arc<dyn SignalRouter> + Send + Sync,
>;

/// Builder for the embedded, transport-agnostic workflow engine.
pub struct EngineBuilder {
    store: Option<Arc<dyn EventStore>>,
    visibility_store: Option<Arc<dyn VisibilityStore>>,
    scheduler_threads: Option<usize>,
    signal_delivery: SignalDeliveryConfig,
    workflow_sources: Vec<WorkflowPackageSource>,
    host_nifs: Vec<NifEntry>,
    recovery: Arc<dyn ActiveWorkflowRecoverySeam>,
    delegated: DelegatedSeams,
    signal_router_factory: Option<SignalRouterFactory>,
    activity_dispatcher: Option<Arc<dyn ActivityDispatcher>>,
    active_registry: Option<Arc<Registry>>,
}

impl Default for EngineBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl EngineBuilder {
    /// Create a builder with no store, no scheduler-thread override, no loaded
    /// workflows, and no host NIFs.
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: None,
            visibility_store: None,
            scheduler_threads: None,
            signal_delivery: SignalDeliveryConfig::default(),
            workflow_sources: Vec::new(),
            host_nifs: Vec::new(),
            recovery: Arc::new(DeferredActiveWorkflowRecovery),
            delegated: DelegatedSeams::default(),
            signal_router_factory: None,
            activity_dispatcher: None,
            active_registry: None,
        }
    }

    /// Supply the event store used by the engine.
    #[must_use]
    pub fn store<S>(mut self, store: S) -> Self
    where
        S: EventStore,
    {
        self.store = Some(Arc::new(store));
        self
    }

    /// Supply an already type-erased event store.
    #[must_use]
    pub fn store_arc(mut self, store: Arc<dyn EventStore>) -> Self {
        self.store = Some(store);
        self
    }

    /// Supply the visibility store used by the engine for workflow projections.
    #[must_use]
    pub fn visibility_store<S>(mut self, visibility_store: S) -> Self
    where
        S: VisibilityStore,
    {
        self.visibility_store = Some(Arc::new(visibility_store));
        self
    }

    /// Supply an already type-erased visibility store.
    #[must_use]
    pub fn visibility_store_arc(mut self, visibility_store: Arc<dyn VisibilityStore>) -> Self {
        self.visibility_store = Some(visibility_store);
        self
    }

    /// Explicitly opt in to an ephemeral in-memory visibility store.
    ///
    /// This is intended for tests and local scenarios that do not need durable
    /// visibility projections. Visibility data stored this way does not survive
    /// process restarts.
    #[must_use]
    pub fn in_memory_visibility(mut self) -> Self {
        self.visibility_store = Some(Arc::new(InMemoryStore::default()));
        self
    }

    /// Record the caller-supplied scheduler thread count.
    ///
    /// If this setter is never called, `None` is passed through to beamr.
    #[must_use]
    pub const fn scheduler_threads(mut self, threads: usize) -> Self {
        self.scheduler_threads = Some(threads);
        self
    }

    /// Record the caller-supplied signal delivery readiness and retry policy.
    #[must_use]
    pub const fn signal_delivery(mut self, signal_delivery: SignalDeliveryConfig) -> Self {
        self.signal_delivery = signal_delivery;
        self
    }

    /// Add one workflow package source to load during `build()`.
    #[must_use]
    pub fn load_workflows(mut self, source: impl Into<WorkflowPackageSource>) -> Self {
        self.workflow_sources.push(source.into());
        self
    }

    /// Add many workflow package sources to load during `build()`.
    #[must_use]
    pub fn load_workflow_sources<I, S>(mut self, sources: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<WorkflowPackageSource>,
    {
        self.workflow_sources
            .extend(sources.into_iter().map(Into::into));
        self
    }

    /// Collect host-supplied NIF entries to install before workflow modules load.
    #[must_use]
    pub fn register_nifs(mut self, entries: impl IntoIterator<Item = NifEntry>) -> Self {
        self.host_nifs.extend(entries);
        self
    }

    /// Override the AD recovery seam used while repopulating active workflows.
    #[must_use]
    pub fn recovery_seam(mut self, recovery: Arc<dyn ActiveWorkflowRecoverySeam>) -> Self {
        self.recovery = recovery;
        self
    }

    /// Override the AT signal-routing seam.
    #[must_use]
    pub fn signal_router(mut self, signal_router: Arc<dyn SignalRouter>) -> Self {
        self.signal_router_factory = None;
        self.delegated = DelegatedSeams::new(
            signal_router,
            self.delegated.query_service_arc(),
            self.delegated.event_publisher_arc(),
        );
        self
    }

    /// Override the AT signal-routing seam after the runtime is assembled.
    #[must_use]
    pub fn signal_router_factory<F>(mut self, factory: F) -> Self
    where
        F: Fn(Arc<RuntimeHandle>, Arc<SignalResumeHandoff>) -> Arc<dyn SignalRouter>
            + Send
            + Sync
            + 'static,
    {
        self.signal_router_factory = Some(Arc::new(factory));
        self
    }

    /// Override the AT query-dispatch seam.
    #[must_use]
    pub fn query_service(mut self, query_service: Arc<dyn QueryService>) -> Self {
        self.delegated = DelegatedSeams::new(
            self.delegated.signal_router_arc(),
            query_service,
            self.delegated.event_publisher_arc(),
        );
        self
    }

    /// Override the AD/AT live event-publisher seam.
    #[must_use]
    pub fn event_publisher(mut self, event_publisher: Arc<dyn EventPublisher>) -> Self {
        self.delegated = DelegatedSeams::new(
            self.delegated.signal_router_arc(),
            self.delegated.query_service_arc(),
            event_publisher,
        );
        self
    }

    /// Supply the activity dispatcher that backs `aion_flow_ffi:run_activity`.
    ///
    /// When set, the dispatcher is installed in the global bridge before
    /// workflow modules are loaded. Without a dispatcher, `run_activity`
    /// returns an error to workflow code instead of crashing the process.
    #[must_use]
    pub fn activity_dispatcher(mut self, dispatcher: Arc<dyn ActivityDispatcher>) -> Self {
        self.activity_dispatcher = Some(dispatcher);
        self
    }

    /// Supply the active workflow registry used by the built engine.
    ///
    /// Server-owned dispatchers that run behind raw NIFs use this to correlate a
    /// calling BEAM pid to the same workflow handle the engine registers.
    #[must_use]
    pub fn active_registry(mut self, registry: Arc<Registry>) -> Self {
        self.active_registry = Some(registry);
        self
    }

    /// Inspect the configured scheduler thread count.
    #[must_use]
    pub const fn scheduler_thread_count(&self) -> Option<usize> {
        self.scheduler_threads
    }

    /// Construct the live engine.
    ///
    /// # Errors
    ///
    /// Returns typed [`EngineError`] variants for missing store, runtime startup,
    /// NIF registration, package loading, store reads, registry/supervision lock
    /// poison, or deferred AD recovery failures for active histories.
    pub async fn build(self) -> Result<Engine, EngineError> {
        let store = self.store.ok_or(EngineError::MissingStore)?;
        let visibility_store = self
            .visibility_store
            .ok_or(EngineError::MissingVisibilityStore)?;

        let activity_dispatcher = self.activity_dispatcher;

        let runtime = Arc::new(RuntimeHandle::new(
            RuntimeConfig::new(self.scheduler_threads).with_signal_delivery(self.signal_delivery),
        )?);

        let mut nifs = NifRegistration::new();
        nifs.add_engine_nifs().add_host_nifs(self.host_nifs);
        runtime.install_nifs(nifs)?;

        let mut loaded_workflows = LoadedWorkflows::new();
        for source in self.workflow_sources {
            let package = package_from_source(source)?;
            let loaded_workflow = loaded_workflows.load_package(runtime.as_ref(), &package)?;
            tracing::info!(
                workflow_type = loaded_workflow.workflow_type(),
                content_hash = %loaded_workflow.version(),
                "loaded workflow package {}",
                loaded_workflow.workflow_type()
            );
        }

        let registry = self
            .active_registry
            .unwrap_or_else(|| Arc::new(Registry::default()));
        install_nif_runtime_context(Arc::clone(&registry), tokio::runtime::Handle::current());
        crate::runtime::nif_timer::install_timer_nif_bridge(
            Arc::clone(&registry),
            Arc::clone(&store),
            tokio::runtime::Handle::current(),
        );
        install_nif_context_source(Arc::new(NifContextSource::new(
            Arc::clone(&registry),
            tokio::runtime::Handle::current(),
            Arc::clone(&store),
        )));
        install_query_bridge(Arc::clone(&registry), tokio::runtime::Handle::current());
        if let Some(dispatcher) = activity_dispatcher {
            install_activity_dispatcher(dispatcher);
        }
        let supervision = Arc::new(SupervisionTree::new());
        bootstrap_schedule_coordinator(Arc::clone(&store)).await?;
        repopulate_active_workflows(
            Arc::clone(&store),
            Arc::clone(&visibility_store),
            &loaded_workflows,
            registry.as_ref(),
            supervision.as_ref(),
            self.recovery.as_ref(),
        )
        .await?;

        let signal_handoff = Arc::new(SignalResumeHandoff::new());

        let delegated = if let Some(factory) = self.signal_router_factory {
            DelegatedSeams::new(
                factory(Arc::clone(&runtime), Arc::clone(&signal_handoff)),
                self.delegated.query_service_arc(),
                self.delegated.event_publisher_arc(),
            )
        } else {
            self.delegated
        };

        install_signal_nif_bridge(Arc::new(crate::runtime::SignalNifBridge::new(
            Arc::clone(&registry),
            Arc::clone(&runtime),
            tokio::runtime::Handle::current(),
            delegated.signal_router_arc(),
        )));
        install_child_nif_bridge(Arc::new(ChildNifBridge::new(ChildNifBridgeParts {
            store: Arc::clone(&store),
            visibility_store: Arc::clone(&visibility_store),
            runtime: Arc::clone(&runtime),
            loaded_workflows: loaded_workflows.clone(),
            registry: Arc::clone(&registry),
            supervision: Arc::clone(&supervision),
            signal_handoff: Arc::clone(&signal_handoff),
            tokio_handle: tokio::runtime::Handle::current(),
        })));

        let engine = Engine::new(EngineComponents {
            store,
            visibility_store,
            runtime,
            loaded_workflows,
            registry,
            supervision,
            delegated,
            signal_handoff,
        });
        engine.catchup_schedule_coordinator().await?;
        engine.recover_schedules_on_startup(Utc::now()).await?;
        Ok(engine)
    }
}

fn package_from_source(source: WorkflowPackageSource) -> Result<Package, EngineError> {
    match source {
        WorkflowPackageSource::Path(path) => {
            Package::load_from_path(&path).map_err(|error| EngineError::Load {
                reason: format!(
                    "failed to load workflow package `{}`: {error}",
                    path.display()
                ),
            })
        }
        WorkflowPackageSource::Package(package) => Ok(*package),
    }
}

async fn bootstrap_schedule_coordinator(store: Arc<dyn EventStore>) -> Result<(), EngineError> {
    let workflow_id = schedule_coordinator_workflow_id();
    let history = store.as_ref().read_history(&workflow_id).await?;
    if !history.is_empty() {
        return Ok(());
    }

    let input = Payload::from_json(&serde_json::json!({})).map_err(|error| EngineError::Load {
        reason: format!("failed to build schedule coordinator input payload: {error}"),
    })?;
    let run_id = schedule_coordinator_run_id();
    let mut recorder = Recorder::new(workflow_id, store);
    recorder
        .record_workflow_started(
            Utc::now(),
            schedule_coordinator_workflow_type().to_owned(),
            input,
            run_id,
        )
        .await?;
    Ok(())
}

async fn repopulate_active_workflows(
    store: Arc<dyn EventStore>,
    visibility_store: Arc<dyn VisibilityStore>,
    loaded_workflows: &LoadedWorkflows,
    registry: &Registry,
    supervision: &SupervisionTree,
    recovery: &dyn ActiveWorkflowRecoverySeam,
) -> Result<(), EngineError> {
    for workflow_id in store.as_ref().list_active().await? {
        let history = store.as_ref().read_history(&workflow_id).await?;
        let workflow_type = started_workflow_type(&workflow_id, &history)?;
        let projected_status = status_from_events(&history);
        supervision.ensure_type_supervisor(workflow_type.clone())?;

        let recovered = recover_active_workflow(
            recovery,
            &workflow_id,
            &workflow_type,
            &history,
            loaded_workflows,
        )?;
        let history_len = u64::try_from(history.len()).unwrap_or(u64::MAX);
        match recovered {
            ActiveWorkflowRecovery::Resident {
                run_id,
                loaded_version,
                pid,
            } => {
                let recorder =
                    Recorder::resume_at(workflow_id.clone(), Arc::clone(&store), history_len)
                        .with_visibility(run_id.clone(), Arc::clone(&visibility_store));
                let completion = CompletionNotifier::new();
                let handle = WorkflowHandle::new(WorkflowHandleParts {
                    workflow_id: workflow_id.clone(),
                    run_id: run_id.clone(),
                    pid,
                    workflow_type: workflow_type.clone(),
                    loaded_version,
                    cached_status: projected_status,
                    residency: HandleResidency::Resident,
                    recorder,
                    completion,
                });
                registry.insert((workflow_id.clone(), run_id.clone()), handle)?;
                registry.reconcile(&workflow_id, &run_id, &history)?;
                supervision.place_workflow(workflow_type, pid)?;
            }
            ActiveWorkflowRecovery::ScheduleCoordinator { run_id } => {
                registry.reconcile(&workflow_id, &run_id, &history)?;
            }
        }
    }

    Ok(())
}

fn recover_active_workflow(
    recovery: &dyn ActiveWorkflowRecoverySeam,
    workflow_id: &aion_core::WorkflowId,
    workflow_type: &str,
    history: &[Event],
    loaded_workflows: &LoadedWorkflows,
) -> Result<ActiveWorkflowRecovery, EngineError> {
    if workflow_id == &schedule_coordinator_workflow_id()
        && workflow_type == schedule_coordinator_workflow_type()
    {
        let run_id = started_run_id(workflow_id, history)?;
        return Ok(ActiveWorkflowRecovery::ScheduleCoordinator { run_id });
    }

    recovery.recover_active_workflow(workflow_id, workflow_type, history, loaded_workflows)
}

fn started_workflow_type(
    workflow_id: &aion_core::WorkflowId,
    history: &[Event],
) -> Result<String, EngineError> {
    if let Some(workflow_type) = history.iter().find_map(|event| match event {
        Event::WorkflowStarted { workflow_type, .. } => Some(workflow_type.clone()),
        _ => None,
    }) {
        return Ok(workflow_type);
    }

    if workflow_id == &schedule_coordinator_workflow_id() {
        return Ok(schedule_coordinator_workflow_type().to_owned());
    }

    Err(EngineError::Load {
        reason: format!(
            "active workflow `{workflow_id}` has no WorkflowStarted event in durable history"
        ),
    })
}

fn started_run_id(
    workflow_id: &aion_core::WorkflowId,
    history: &[Event],
) -> Result<RunId, EngineError> {
    if let Some(run_id) = history.iter().find_map(|event| match event {
        Event::WorkflowStarted { run_id, .. } => Some(run_id.clone()),
        _ => None,
    }) {
        return Ok(run_id);
    }

    if workflow_id == &schedule_coordinator_workflow_id() {
        return Ok(schedule_coordinator_run_id());
    }

    Err(EngineError::Load {
        reason: format!(
            "active workflow `{workflow_id}` has no WorkflowStarted run id in durable history"
        ),
    })
}

#[cfg(test)]
mod tests {
    use std::{path::PathBuf, process::Command, sync::Arc, time::Duration};

    use aion_core::{Event, EventEnvelope, Payload, RunId, WorkflowId, WorkflowStatus};
    use aion_package::{
        BeamModule, BeamSet, CURRENT_FORMAT_VERSION, ContentHash, DeclaredActivity, Manifest,
        ManifestVersion, Package, PackageBuilder,
    };
    use aion_store::{InMemoryStore, ReadableEventStore, WritableEventStore, WriteToken};
    use chrono::Utc;
    use serde_json::json;

    use crate::durability::{ActiveWorkflowRecovery, ActiveWorkflowRecoverySeam};
    use crate::engine::api::{
        schedule_coordinator_run_id, schedule_coordinator_workflow_id,
        schedule_coordinator_workflow_type,
    };
    use crate::runtime::{Mfa, NifEntry};

    use super::{EngineBuilder, LoadedWorkflows};
    use crate::{EngineError, Pid};

    fn payload() -> Result<Payload, aion_core::PayloadError> {
        Payload::from_json(&json!({ "input": true }))
    }

    fn started(
        workflow_id: &WorkflowId,
        workflow_type: &str,
    ) -> Result<Event, aion_core::PayloadError> {
        Ok(Event::WorkflowStarted {
            envelope: EventEnvelope {
                seq: 1,
                recorded_at: Utc::now(),
                workflow_id: workflow_id.clone(),
            },
            workflow_type: workflow_type.to_owned(),
            input: payload()?,
            run_id: aion_core::RunId::new(uuid::Uuid::from_u128(1)),
            parent_run_id: None,
        })
    }

    fn hash(byte: u8) -> ContentHash {
        ContentHash::from_bytes([byte; 32])
    }

    fn package_manifest() -> Manifest {
        Manifest {
            entry_module: "counter".to_owned(),
            entry_function: "version".to_owned(),
            input_schema: json!({ "type": "object" }),
            output_schema: json!({ "type": "integer" }),
            timeout: Duration::from_secs(30),
            activities: vec![DeclaredActivity {
                activity_type: "activity/test".to_owned(),
            }],
            version: ManifestVersion::new("test"),
            format_version: CURRENT_FORMAT_VERSION,
        }
    }

    fn compile_counter_beam() -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let temp_dir =
            std::env::temp_dir().join(format!("aion-engine-builder-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&temp_dir)?;
        let source_path = temp_dir.join("counter.erl");
        let beam_path = temp_dir.join("counter.beam");
        std::fs::write(
            &source_path,
            "-module(counter).\n-export([version/0]).\nversion() -> 1.\n",
        )?;
        let status = Command::new("erlc")
            .arg("-o")
            .arg(&temp_dir)
            .arg(&source_path)
            .status()?;
        if !status.success() {
            let cleanup_result = std::fs::remove_dir_all(&temp_dir);
            drop(cleanup_result);
            return Err(format!("erlc failed with status {status}").into());
        }
        let bytes = std::fs::read(beam_path)?;
        std::fs::remove_dir_all(temp_dir)?;
        Ok(bytes)
    }

    fn fixture_package() -> Result<Package, Box<dyn std::error::Error>> {
        let beams = BeamSet::new(vec![BeamModule::new("counter", compile_counter_beam()?)])?;
        let archive = PackageBuilder::new(package_manifest(), beams).write_to_bytes()?;
        Ok(Package::load_from_bytes(archive)?)
    }

    fn write_fixture_package(package: &Package) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path =
            std::env::temp_dir().join(format!("aion-engine-builder-{}.aion", uuid::Uuid::new_v4()));
        PackageBuilder::new(package.manifest().clone(), package.beams().clone())
            .write_to_path(&path)?;
        Ok(path)
    }

    #[derive(Debug)]
    struct TestRecovery {
        run_id: RunId,
        version: ContentHash,
        pid: Pid,
    }

    impl ActiveWorkflowRecoverySeam for TestRecovery {
        fn recover_active_workflow(
            &self,
            workflow_id: &WorkflowId,
            workflow_type: &str,
            history: &[Event],
            loaded_workflows: &LoadedWorkflows,
        ) -> Result<ActiveWorkflowRecovery, EngineError> {
            let _ = (workflow_id, workflow_type, history, loaded_workflows);
            Ok(ActiveWorkflowRecovery::Resident {
                run_id: self.run_id.clone(),
                loaded_version: self.version.clone(),
                pid: self.pid,
            })
        }
    }

    #[tokio::test]
    async fn build_without_store_returns_missing_store() {
        let error = EngineBuilder::new().build().await.err();

        assert!(matches!(error, Some(EngineError::MissingStore)));
    }

    #[tokio::test]
    async fn build_without_visibility_store_returns_missing_visibility_store() {
        let error = EngineBuilder::new()
            .store(InMemoryStore::default())
            .build()
            .await
            .err();

        assert!(matches!(error, Some(EngineError::MissingVisibilityStore)));
    }

    #[tokio::test]
    async fn in_memory_visibility_allows_build_without_visibility_store() -> Result<(), EngineError>
    {
        let engine = EngineBuilder::new()
            .store(InMemoryStore::default())
            .in_memory_visibility()
            .build()
            .await?;

        engine.shutdown()?;
        Ok(())
    }

    #[test]
    fn scheduler_threads_are_only_set_by_caller() {
        assert_eq!(EngineBuilder::new().scheduler_thread_count(), None);
        assert_eq!(
            EngineBuilder::new()
                .scheduler_threads(4)
                .scheduler_thread_count(),
            Some(4)
        );
    }

    #[tokio::test]
    async fn duplicate_host_nif_mfa_returns_typed_error() {
        let mfa = Mfa::new("host", "zero", 0);
        let error = EngineBuilder::new()
            .store(InMemoryStore::default())
            .in_memory_visibility()
            .register_nifs([
                NifEntry::new(mfa.clone(), crate::runtime::nif::test_native_zero),
                NifEntry::dirty(mfa, crate::runtime::nif::test_native_zero),
            ])
            .build()
            .await
            .err();

        assert!(matches!(
            error,
            Some(EngineError::NifRegistration { reason }) if reason.contains("host:zero/0")
        ));
    }

    #[tokio::test]
    async fn empty_store_builds_coordinator_history_without_registry_or_supervision()
    -> Result<(), EngineError> {
        let store = Arc::new(InMemoryStore::default());
        let engine = EngineBuilder::new()
            .store_arc(store.clone())
            .in_memory_visibility()
            .build()
            .await?;

        assert!(engine.registry().list()?.is_empty());
        assert_eq!(engine.supervision().type_supervisor_count()?, 1);
        assert_eq!(engine.loaded_workflows().iter().count(), 0);

        let coordinator_id = schedule_coordinator_workflow_id();
        let active = store.list_active().await?;
        assert_eq!(active, vec![coordinator_id.clone()]);
        let history = store.read_history(&coordinator_id).await?;
        let [started] = history.as_slice() else {
            return Err(EngineError::Load {
                reason: format!(
                    "expected exactly one coordinator event, found {}",
                    history.len()
                ),
            });
        };
        match started {
            Event::WorkflowStarted {
                workflow_type,
                input,
                run_id,
                parent_run_id,
                ..
            } => {
                assert_eq!(workflow_type, schedule_coordinator_workflow_type());
                assert_eq!(
                    input,
                    &Payload::from_json(&json!({})).map_err(|error| {
                        EngineError::Load {
                            reason: format!("failed to build expected payload: {error}"),
                        }
                    })?
                );
                assert_eq!(run_id, &schedule_coordinator_run_id());
                assert!(parent_run_id.is_none());
            }
            other => {
                return Err(EngineError::Load {
                    reason: format!("expected coordinator WorkflowStarted, found {other:?}"),
                });
            }
        }

        engine.shutdown()?;
        let rebuilt = EngineBuilder::new()
            .store_arc(store.clone())
            .in_memory_visibility()
            .build()
            .await?;
        let rebuilt_history = store.read_history(&coordinator_id).await?;
        assert_eq!(rebuilt_history.len(), 1);
        rebuilt.shutdown()?;

        Ok(())
    }

    #[tokio::test]
    async fn build_loads_already_loaded_package() -> Result<(), Box<dyn std::error::Error>> {
        let package = fixture_package()?;
        let version = package.content_hash().clone();
        let deployed_entry_module = package.deployed_entry_module();

        let engine = EngineBuilder::new()
            .store(InMemoryStore::default())
            .in_memory_visibility()
            .load_workflows(package)
            .build()
            .await?;

        let loaded = engine
            .loaded_workflows()
            .get("counter", &version)
            .ok_or("loaded package record missing")?;
        assert_eq!(loaded.deployed_entry_module(), deployed_entry_module);
        assert!(
            engine
                .runtime()
                .has_registered_module(&deployed_entry_module)
        );
        Ok(())
    }

    #[tokio::test]
    async fn build_loads_package_from_path() -> Result<(), Box<dyn std::error::Error>> {
        let package = fixture_package()?;
        let version = package.content_hash().clone();
        let path = write_fixture_package(&package)?;

        let engine = EngineBuilder::new()
            .store(InMemoryStore::default())
            .in_memory_visibility()
            .load_workflows(path.as_path())
            .build()
            .await?;
        std::fs::remove_file(path)?;

        assert!(engine.loaded_workflows().get("counter", &version).is_some());
        Ok(())
    }

    #[tokio::test]
    async fn active_workflow_recovery_repopulates_registry_and_supervision()
    -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemoryStore::default();
        let workflow_id = WorkflowId::new_v4();
        store
            .append(
                WriteToken::recorder(),
                &workflow_id,
                &[started(&workflow_id, "checkout")?],
                0,
            )
            .await?;
        let run_id = RunId::new_v4();
        let version = hash(7);
        let pid = 42;

        let engine = EngineBuilder::new()
            .store(store)
            .in_memory_visibility()
            .recovery_seam(Arc::new(TestRecovery {
                run_id: run_id.clone(),
                version: version.clone(),
                pid,
            }))
            .build()
            .await?;

        let recovered = engine.registry().get(&workflow_id, &run_id)?;
        assert!(recovered.is_some_and(|handle| {
            handle.workflow_type() == "checkout"
                && handle.loaded_version() == &version
                && handle.cached_status() == WorkflowStatus::Running
                && handle.pid() == pid
        }));
        assert_eq!(engine.supervision().type_supervisor_count()?, 2);
        let type_supervisors = engine.supervision().type_supervisors()?;
        assert!(
            type_supervisors
                .iter()
                .any(|node| node.id().workflow_type() == "checkout")
        );
        assert!(
            type_supervisors
                .iter()
                .any(|node| node.id().workflow_type() == schedule_coordinator_workflow_type())
        );
        Ok(())
    }
}
