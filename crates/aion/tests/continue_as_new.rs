//! Continue-as-new lifecycle integration tests over `InMemoryStore`.

mod common;

use std::sync::Arc;

use aion::durability::{ActiveWorkflowRecovery, ActiveWorkflowRecoverySeam};
use aion::{EngineBuilder, EngineError, LoadedWorkflows, Pid};
use aion_core::{Event, Payload, RunId, WorkflowId, WorkflowStatus, status_from_events};
use aion_package::ContentHash;
use serde_json::json;

use common::{FIXTURE_MODULE, input_payload, payload};

#[tokio::test]
async fn continue_as_new_records_terminal_old_run_and_running_replacement()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, store) = common::engine_with_fixture("wait").await?;
    let handle = engine
        .start_workflow(FIXTURE_MODULE, input_payload()?)
        .await?;
    let old_workflow_id = handle.workflow_id().clone();
    let old_run_id = handle.run_id().clone();
    let carried = carried_payload("first")?;

    let replacement = engine
        .continue_as_new(handle.workflow_id(), handle.run_id(), carried.clone(), None)
        .await?;

    assert_eq!(replacement.workflow_id(), &old_workflow_id);
    assert_ne!(replacement.run_id(), &old_run_id);

    let history = store.read_history(&old_workflow_id).await?;
    assert_eq!(history.len(), 3);
    assert_eq!(
        status_from_events(&history[..2]),
        WorkflowStatus::ContinuedAsNew
    );
    assert_eq!(status_from_events(&history), WorkflowStatus::Running);

    match &history[1] {
        Event::WorkflowContinuedAsNew {
            input,
            workflow_type,
            parent_run_id,
            ..
        } => {
            assert_eq!(input, &carried);
            assert_eq!(workflow_type, &None);
            assert_eq!(parent_run_id, &old_run_id);
        }
        other => return Err(format!("expected WorkflowContinuedAsNew, found {other:?}").into()),
    }

    match &history[2] {
        Event::WorkflowStarted {
            workflow_type,
            input,
            parent_run_id,
            ..
        } => {
            assert_eq!(workflow_type, FIXTURE_MODULE);
            assert_eq!(input, &carried);
            assert_eq!(parent_run_id, &Some(old_run_id));
        }
        other => {
            return Err(format!("expected replacement WorkflowStarted, found {other:?}").into());
        }
    }

    engine.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn recovery_active_listing_contains_only_current_continuation_run()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, store) = common::engine_with_fixture("wait").await?;
    let continued = engine
        .start_workflow(FIXTURE_MODULE, input_payload()?)
        .await?;
    let untouched = engine
        .start_workflow(FIXTURE_MODULE, carried_payload("untouched")?)
        .await?;
    let old_run_id = continued.run_id().clone();

    let replacement = engine
        .continue_as_new(
            continued.workflow_id(),
            continued.run_id(),
            carried_payload("recovered")?,
            None,
        )
        .await?;

    let active = store.list_active().await?;
    assert_eq!(
        active
            .iter()
            .filter(|workflow_id| *workflow_id == replacement.workflow_id())
            .count(),
        1
    );
    assert_eq!(
        active
            .iter()
            .filter(|workflow_id| *workflow_id == untouched.workflow_id())
            .count(),
        1
    );
    let user_workflows: Vec<_> = active
        .iter()
        .filter(|id| *id == replacement.workflow_id() || *id == untouched.workflow_id())
        .collect();
    assert_eq!(user_workflows.len(), 2);

    assert!(
        engine
            .registry()
            .get(replacement.workflow_id(), &old_run_id)?
            .is_none()
    );
    assert!(
        engine
            .registry()
            .get(replacement.workflow_id(), replacement.run_id())?
            .is_some()
    );
    assert!(
        engine
            .registry()
            .get(untouched.workflow_id(), untouched.run_id())?
            .is_some()
    );

    let replacement_version = replacement.loaded_version().clone();
    let untouched_version = untouched.loaded_version().clone();
    let recovered = EngineBuilder::new()
        .store_arc(Arc::clone(&store))
        .scheduler_threads(1)
        .load_workflows(common::fixture_package("wait")?)
        .recovery_seam(Arc::new(TestRecovery {
            replacements: vec![
                RecoveryEntry {
                    workflow_id: replacement.workflow_id().clone(),
                    run_id: replacement.run_id().clone(),
                    version: replacement_version,
                    pid: 1,
                },
                RecoveryEntry {
                    workflow_id: untouched.workflow_id().clone(),
                    run_id: untouched.run_id().clone(),
                    version: untouched_version,
                    pid: 2,
                },
            ],
        }))
        .build()
        .await?;

    assert!(
        recovered
            .registry()
            .get(replacement.workflow_id(), &old_run_id)?
            .is_none()
    );
    assert!(
        recovered
            .registry()
            .get(replacement.workflow_id(), replacement.run_id())?
            .is_some()
    );
    assert!(
        recovered
            .registry()
            .get(untouched.workflow_id(), untouched.run_id())?
            .is_some()
    );

    recovered.shutdown()?;
    engine.shutdown()?;
    Ok(())
}

#[tokio::test]
async fn read_run_chain_returns_parent_links_in_chronological_order()
-> Result<(), Box<dyn std::error::Error>> {
    let (engine, store) = common::engine_with_fixture("wait").await?;
    let first = engine
        .start_workflow(FIXTURE_MODULE, input_payload()?)
        .await?;
    let second = engine
        .continue_as_new(
            first.workflow_id(),
            first.run_id(),
            carried_payload("second")?,
            None,
        )
        .await?;
    let third = engine
        .continue_as_new(
            second.workflow_id(),
            second.run_id(),
            carried_payload("third")?,
            None,
        )
        .await?;

    let chain = store.read_run_chain(first.workflow_id()).await?;

    assert_eq!(chain.len(), 3);
    assert_eq!(&chain[0].run_id, first.run_id());
    assert_eq!(chain[0].parent_run_id, None);
    assert_eq!(&chain[1].run_id, second.run_id());
    assert_eq!(chain[1].parent_run_id, Some(first.run_id().clone()));
    assert_eq!(&chain[2].run_id, third.run_id());
    assert_eq!(chain[2].parent_run_id, Some(second.run_id().clone()));
    assert!(chain[0].started_at <= chain[1].started_at);
    assert!(chain[1].started_at <= chain[2].started_at);

    engine.shutdown()?;
    Ok(())
}

fn carried_payload(label: &str) -> Result<Payload, aion_core::PayloadError> {
    payload(&json!({ "carried": label }))
}

#[derive(Debug)]
struct TestRecovery {
    replacements: Vec<RecoveryEntry>,
}

#[derive(Debug)]
struct RecoveryEntry {
    workflow_id: WorkflowId,
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
        let _ = (workflow_type, history, loaded_workflows);
        let entry = self
            .replacements
            .iter()
            .find(|entry| &entry.workflow_id == workflow_id)
            .ok_or_else(|| EngineError::Load {
                reason: format!("test recovery has no run metadata for workflow {workflow_id}"),
            })?;

        Ok(ActiveWorkflowRecovery::Resident {
            run_id: entry.run_id.clone(),
            loaded_version: entry.version.clone(),
            pid: entry.pid,
        })
    }
}
