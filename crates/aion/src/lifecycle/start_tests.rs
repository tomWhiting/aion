//! Tests for workflow start lifecycle.

use std::sync::Arc;

use aion_core::{Event, Payload};
use aion_package::ContentHash;
use aion_store::visibility::VisibilityStore;
use aion_store::{EventStore, InMemoryStore, ReadableEventStore};
use serde_json::json;

use super::{
    StartWorkflowContext, StartWorkflowOptions, start_workflow, start_workflow_with_options,
};
use crate::EngineError;
use crate::loader::LoadedWorkflows;
use crate::registry::{HandleResidency, Registry};
use crate::runtime::{RuntimeConfig, RuntimeHandle};
use crate::supervision::SupervisionTree;

fn payload(label: &str) -> Result<Payload, aion_core::PayloadError> {
    Payload::from_json(&json!({ "label": label }))
}

fn load_without_runtime_registration(workflow_type: &str) -> LoadedWorkflows {
    let mut loaded = LoadedWorkflows::new();
    loaded.note_loaded_workflow_for_test(
        workflow_type,
        format!("{workflow_type}__deployed"),
        "run",
        ContentHash::from_bytes([3; 32]),
    );
    loaded
}

fn context<'a>(
    store: Arc<dyn EventStore>,
    visibility_store: Arc<dyn VisibilityStore>,
    loaded_workflows: &'a LoadedWorkflows,
    runtime: Arc<RuntimeHandle>,
    supervision: &'a SupervisionTree,
    registry: Arc<Registry>,
) -> StartWorkflowContext<'a> {
    StartWorkflowContext {
        store,
        visibility_store,
        loaded_workflows,
        runtime,
        supervision,
        registry,
        signal_handoff: None,
    }
}

#[tokio::test]
async fn unknown_workflow_type_returns_not_found_and_appends_nothing()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::default());
    let loaded = LoadedWorkflows::new();
    let runtime = Arc::new(RuntimeHandle::new(RuntimeConfig::new(Some(1)))?);
    let supervision = SupervisionTree::new();
    let registry = Arc::new(Registry::default());
    let input = payload("input")?;

    let result = start_workflow(
        context(
            store.clone(),
            store.clone(),
            &loaded,
            Arc::clone(&runtime),
            &supervision,
            Arc::clone(&registry),
        ),
        "checkout",
        input,
    )
    .await;

    assert!(matches!(
        result,
        Err(EngineError::WorkflowNotFound { workflow_type }) if workflow_type == "checkout"
    ));
    assert_eq!(store.list_active().await?, Vec::new());
    assert_eq!(registry.list()?.len(), 0);
    runtime.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn recorder_append_happens_before_spawn_failure() -> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::default());
    let loaded = load_without_runtime_registration("checkout");
    let runtime = Arc::new(RuntimeHandle::new(RuntimeConfig::new(Some(1)))?);
    let supervision = SupervisionTree::new();
    let registry = Arc::new(Registry::default());
    let input = payload("input")?;

    let result = start_workflow(
        context(
            store.clone(),
            store.clone(),
            &loaded,
            Arc::clone(&runtime),
            &supervision,
            Arc::clone(&registry),
        ),
        "checkout",
        input.clone(),
    )
    .await;

    assert!(matches!(result, Err(EngineError::Runtime { .. })));
    let active = store.list_active().await?;
    assert_eq!(active.len(), 1);
    let history = store.read_history(&active[0]).await?;
    assert_eq!(history.len(), 1);
    match &history[0] {
        Event::WorkflowStarted {
            envelope,
            workflow_type,
            input: recorded_input,
            run_id: _,
            parent_run_id: None,
        } => {
            assert_eq!(envelope.seq, 1);
            assert_eq!(&envelope.workflow_id, &active[0]);
            assert_eq!(workflow_type, "checkout");
            assert_eq!(recorded_input, &input);
        }
        other => return Err(format!("expected WorkflowStarted, found {other:?}").into()),
    }
    assert_eq!(registry.list()?.len(), 0);
    runtime.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn successful_start_appends_spawns_places_registers_and_returns_handle()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::default());
    let deployed_module = "checkout__deployed";
    let loaded = load_without_runtime_registration("checkout");
    let runtime = Arc::new(RuntimeHandle::new(RuntimeConfig::new(Some(1)))?);
    runtime.register_waiting_test_module(deployed_module, "run");
    let supervision = SupervisionTree::new();
    let registry = Arc::new(Registry::default());
    let input = payload("input")?;

    let handle = start_workflow(
        context(
            store.clone(),
            store.clone(),
            &loaded,
            Arc::clone(&runtime),
            &supervision,
            Arc::clone(&registry),
        ),
        "checkout",
        input.clone(),
    )
    .await?;

    assert_eq!(handle.workflow_type(), "checkout");
    assert_eq!(handle.loaded_version(), &ContentHash::from_bytes([3; 32]));
    assert_eq!(handle.cached_status(), aion_core::WorkflowStatus::Running);
    assert_eq!(handle.residency(), HandleResidency::Resident);
    assert!(!handle.completion().is_completed());
    runtime.wait_for_process_ready(handle.pid())?;
    assert!(runtime.trap_exit(handle.pid())?);
    assert_eq!(
        supervision
            .workflow(handle.pid())?
            .map(|node| node.workflow_pid()),
        Some(handle.pid())
    );

    let registered = registry.get(handle.workflow_id(), handle.run_id())?;
    assert_eq!(registered, Some(handle.clone()));
    let active = store.list_active().await?;
    assert_eq!(active, vec![handle.workflow_id().clone()]);
    let history = store.read_history(handle.workflow_id()).await?;
    assert_eq!(history.len(), 1);
    match &history[0] {
        Event::WorkflowStarted {
            envelope,
            workflow_type,
            input: recorded_input,
            run_id: recorded_run_id,
            parent_run_id: None,
        } => {
            assert_eq!(envelope.seq, 1);
            assert_eq!(&envelope.workflow_id, handle.workflow_id());
            assert_eq!(workflow_type, "checkout");
            assert_eq!(recorded_input, &input);
            assert_eq!(recorded_run_id, handle.run_id());
        }
        other => return Err(format!("expected WorkflowStarted, found {other:?}").into()),
    }
    runtime.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn start_with_existing_workflow_id_resumes_history_sequence()
-> Result<(), Box<dyn std::error::Error>> {
    let store = Arc::new(InMemoryStore::default());
    let deployed_module = "checkout__deployed";
    let loaded = load_without_runtime_registration("checkout");
    let runtime = Arc::new(RuntimeHandle::new(RuntimeConfig::new(Some(1)))?);
    runtime.register_waiting_test_module(deployed_module, "run");
    let supervision = SupervisionTree::new();
    let registry = Arc::new(Registry::default());
    let workflow_id = aion_core::WorkflowId::new_v4();
    let parent_run_id = aion_core::RunId::new_v4();

    let mut recorder = crate::durability::Recorder::new(workflow_id.clone(), store.clone());
    recorder
        .record_workflow_started(
            chrono::Utc::now(),
            "checkout".to_owned(),
            payload("first")?,
            aion_core::RunId::new(uuid::Uuid::from_u128(1)),
        )
        .await?;
    recorder
        .record_workflow_continued_as_new(
            chrono::Utc::now(),
            payload("second")?,
            None,
            parent_run_id.clone(),
        )
        .await?;

    let handle = start_workflow_with_options(
        context(
            store.clone(),
            store.clone(),
            &loaded,
            Arc::clone(&runtime),
            &supervision,
            Arc::clone(&registry),
        ),
        "checkout",
        payload("second")?,
        StartWorkflowOptions {
            workflow_id: Some(workflow_id.clone()),
            parent_run_id: Some(parent_run_id.clone()),
        },
    )
    .await?;

    assert_eq!(handle.workflow_id(), &workflow_id);
    let history = store.read_history(&workflow_id).await?;
    assert_eq!(history.len(), 3);
    match &history[2] {
        Event::WorkflowStarted {
            envelope,
            workflow_type,
            run_id: _,
            parent_run_id: started_parent,
            ..
        } => {
            assert_eq!(envelope.seq, 3);
            assert_eq!(workflow_type, "checkout");
            assert_eq!(started_parent, &Some(parent_run_id));
        }
        other => {
            return Err(format!("expected replacement WorkflowStarted, found {other:?}").into());
        }
    }
    runtime.shutdown()?;
    Ok(())
}
