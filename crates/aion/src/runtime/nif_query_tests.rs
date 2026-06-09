use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use aion_core::{Event, WorkflowId, WorkflowStatus};
use aion_package::ContentHash;
use aion_store::{InMemoryStore, ReadableEventStore};
use beamr::term::Term;

use super::*;
use crate::Pid;
use crate::durability::Recorder;
use crate::engine_seam::{
    ChildWorkflowSpawnRequest, ChildWorkflowSpawnResult, EngineSeamError, TimerWheelEntry,
    WorkflowMailboxMessage, WorkflowProcessHandle, WorkflowResidency,
};
use crate::query::QueryResult;
use crate::registry::{CompletionNotifier, HandleResidency, WorkflowHandle, WorkflowHandleParts};

type TestResult = Result<(), Box<dyn std::error::Error>>;
type TestContext = (
    Arc<Registry>,
    Arc<InMemoryStore>,
    Arc<FakeEngine>,
    WorkflowId,
);

#[derive(Default)]
struct FakeEngine {
    residency: Mutex<HashMap<WorkflowId, WorkflowResidency>>,
    workflows: Mutex<HashMap<u64, HashMap<String, QueryResult>>>,
}

impl FakeEngine {
    fn set_workflow(
        &self,
        workflow_id: WorkflowId,
        pid: u64,
        handlers: HashMap<String, QueryResult>,
    ) -> Result<(), EngineSeamError> {
        self.residency
            .lock()
            .map_err(|_| EngineSeamError::Delivery {
                reason: "poisoned".to_owned(),
            })?
            .insert(
                workflow_id,
                WorkflowResidency::Resident(WorkflowProcessHandle::new(pid)),
            );
        self.workflows
            .lock()
            .map_err(|_| EngineSeamError::Delivery {
                reason: "poisoned".to_owned(),
            })?
            .insert(pid, handlers);
        Ok(())
    }
}

impl EngineHandle for FakeEngine {
    fn resolve_workflow(
        &self,
        workflow_id: &WorkflowId,
    ) -> Result<WorkflowResidency, EngineSeamError> {
        Ok(self
            .residency
            .lock()
            .map_err(|_| EngineSeamError::Delivery {
                reason: "poisoned".to_owned(),
            })?
            .get(workflow_id)
            .copied()
            .unwrap_or(WorkflowResidency::Unknown))
    }

    fn deliver_workflow_message(
        &self,
        process: WorkflowProcessHandle,
        message: WorkflowMailboxMessage,
    ) -> Result<(), EngineSeamError> {
        let WorkflowMailboxMessage::Query { name, reply_to, .. } = message else {
            return Err(EngineSeamError::Delivery {
                reason: "query only".to_owned(),
            });
        };
        let result = self
            .workflows
            .lock()
            .map_err(|_| EngineSeamError::Delivery {
                reason: "poisoned".to_owned(),
            })?
            .get(&process.pid())
            .and_then(|handlers| handlers.get(&name).cloned())
            .unwrap_or(Err(QueryError::UnknownQuery(name)));
        reply_to
            .send(result)
            .map_err(|_| EngineSeamError::Delivery {
                reason: "reply dropped".to_owned(),
            })
    }

    fn spawn_child_workflow(
        &self,
        request: ChildWorkflowSpawnRequest,
    ) -> Result<ChildWorkflowSpawnResult, EngineSeamError> {
        Err(EngineSeamError::ChildSpawn {
            reason: request.workflow_type,
        })
    }

    fn terminate_linked_child_workflow(
        &self,
        parent_workflow_id: &WorkflowId,
        child_process: WorkflowProcessHandle,
        correlation: u64,
    ) -> Result<(), EngineSeamError> {
        Err(EngineSeamError::ChildTermination {
            reason: format!("{parent_workflow_id}:{child_process:?}:{correlation}"),
        })
    }

    fn terminate_linked_activity(
        &self,
        parent_workflow_id: &WorkflowId,
        activity_process: Pid,
        correlation: u64,
    ) -> Result<(), EngineSeamError> {
        Err(EngineSeamError::ChildTermination {
            reason: format!("{parent_workflow_id}:{activity_process}:{correlation}"),
        })
    }

    fn arm_timer(&self, entry: TimerWheelEntry) -> Result<(), EngineSeamError> {
        Err(EngineSeamError::TimerWheel {
            reason: entry.timer_id.to_string(),
        })
    }

    fn disarm_timer(
        &self,
        process: WorkflowProcessHandle,
        timer_id: &aion_core::TimerId,
    ) -> Result<(), EngineSeamError> {
        Err(EngineSeamError::TimerWheel {
            reason: format!("{process:?}:{timer_id}"),
        })
    }

    fn record_workflow_event(
        &self,
        workflow_id: &WorkflowId,
        event: Event,
    ) -> Result<(), EngineSeamError> {
        Err(EngineSeamError::Recorder {
            reason: format!(
                "queries must not record event {} for {workflow_id}",
                event.seq()
            ),
        })
    }
}

fn hash() -> ContentHash {
    ContentHash::from_bytes([9; 32])
}

fn install(runtime: &tokio::runtime::Runtime) -> Result<TestContext, Box<dyn std::error::Error>> {
    let registry = Arc::new(Registry::default());
    let store = Arc::new(InMemoryStore::default());
    let workflow_id = WorkflowId::new_v4();
    let run_id = aion_core::RunId::new_v4();
    let recorder = Recorder::new(workflow_id.clone(), store.clone());
    let handle = WorkflowHandle::new(WorkflowHandleParts {
        workflow_id: workflow_id.clone(),
        run_id: run_id.clone(),
        pid: 42,
        workflow_type: "checkout".to_owned(),
        loaded_version: hash(),
        cached_status: WorkflowStatus::Running,
        residency: HandleResidency::Resident,
        recorder,
        completion: CompletionNotifier::new(),
    });
    registry.insert((workflow_id.clone(), run_id), handle)?;
    let engine = Arc::new(FakeEngine::default());
    install_query_bridge_with_engine(
        Arc::clone(&registry),
        engine.clone(),
        runtime.handle().clone(),
    );
    Ok((registry, store, engine, workflow_id))
}

#[test]
fn register_query_replaces_handler_for_workflow_pid() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let (_registry, _store, _engine, _workflow_id) = install(&runtime)?;
    let first = Term::small_int(1);
    let second = Term::small_int(2);

    assert_eq!(
        register_query_impl("state", first, "{}", Some(42))?,
        "registered"
    );
    assert_eq!(
        registered_handler(42, "state")?.map(|handler| handler.handler),
        Some(first)
    );
    assert_eq!(
        register_query_impl("state", second, "{}", Some(42))?,
        "registered"
    );
    let handler = registered_handler(42, "state")?.ok_or("missing handler")?;
    assert_eq!(handler.pid, 42);
    assert_eq!(handler.handler, second);
    Ok(())
}

#[test]
fn reply_query_delivers_pending_response_and_errors_for_missing_id() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let (_registry, _store, _engine, _workflow_id) = install(&runtime)?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    query_handlers()
        .lock_pending()?
        .insert("q-1".to_owned(), sender);

    assert_eq!(
        reply_query_impl("q-1", "{\"answer\":1}", Some(42))?,
        "replied"
    );
    let reply = runtime.block_on(receiver)??;
    assert_eq!(reply.bytes(), b"{\"answer\":1}");
    assert!(reply_query_impl("missing", "{}", Some(42)).is_err());
    Ok(())
}

#[test]
fn dispatch_query_round_trips_through_query_service() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let (_registry, store, engine, workflow_id) = install(&runtime)?;
    let target = WorkflowId::new_v4();
    let mut handlers = HashMap::new();
    handlers.insert(
        "state".to_owned(),
        Ok(payload_from_string("{\"visible\":true}")),
    );
    engine.set_workflow(target.clone(), 90, handlers)?;
    let config = serde_json::json!({
        "target_workflow_id": target,
        "payload": "{\"ask\":true}"
    })
    .to_string();

    let reply = dispatch_query_impl("state", &config, Some(42))?;

    assert_eq!(reply, "{\"visible\":true}");
    let history = runtime.block_on(store.read_history(&workflow_id))?;
    assert!(history.is_empty());
    Ok(())
}

#[test]
fn query_nifs_do_not_change_event_history() -> TestResult {
    let runtime = tokio::runtime::Runtime::new()?;
    let (_registry, store, engine, workflow_id) = install(&runtime)?;
    let target = WorkflowId::new_v4();
    let mut handlers = HashMap::new();
    handlers.insert("state".to_owned(), Ok(payload_from_string("{}")));
    engine.set_workflow(target.clone(), 91, handlers)?;
    let before = runtime.block_on(store.read_history(&workflow_id))?;

    register_query_impl("local", Term::small_int(7), "{}", Some(42))?;
    let (sender, receiver) = tokio::sync::oneshot::channel();
    query_handlers()
        .lock_pending()?
        .insert("q-2".to_owned(), sender);
    reply_query_impl("q-2", "{}", Some(42))?;
    let reply = runtime.block_on(receiver)??;
    assert_eq!(reply.bytes(), b"{}");
    let config = serde_json::json!({ "target_workflow_id": target, "payload": "{}" }).to_string();
    dispatch_query_impl("state", &config, Some(42))?;

    let after = runtime.block_on(store.read_history(&workflow_id))?;
    assert_eq!(before, after);
    assert!(after.is_empty());
    Ok(())
}
