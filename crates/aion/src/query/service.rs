//! Query dispatch service for live workflow processes.

use std::sync::Arc;
use std::time::Duration;

use aion_core::{Payload, WorkflowId};
use tokio::sync::oneshot;
use tokio::time;

use crate::engine_seam::{
    EngineHandle, EngineSeamError, WorkflowMailboxMessage, WorkflowResidency,
};

/// Result sent by workflow query handlers over a query reply channel.
pub type QueryResult = Result<Payload, QueryError>;

/// Result returned by [`QueryService::query`].
pub type QueryServiceResult = Result<Payload, QueryError>;

/// Typed failures surfaced by live workflow query dispatch.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum QueryError {
    /// The resident workflow has no registered handler for the requested query name.
    #[error("unknown query {0}")]
    UnknownQuery(String),

    /// No query reply arrived before the engine-configured timeout elapsed.
    #[error("query reply timed out")]
    Timeout,

    /// The workflow cannot answer a live query because it is not currently running.
    #[error("workflow {0} is not running")]
    NotRunning(WorkflowId),

    /// The engine does not know the requested workflow.
    #[error("workflow {0} is unknown")]
    Unknown(WorkflowId),

    /// The workflow query reply channel closed before a handler response was sent.
    #[error("query reply channel closed before a handler response was sent")]
    ReplyDropped,

    /// The engine seam failed while resolving or delivering the query.
    #[error("query engine seam failed: {0}")]
    Engine(#[from] EngineSeamError),
}

/// Non-recording, non-disruptive live workflow query dispatcher.
///
/// `QueryService` depends only on the engine seam's residency and mailbox-delivery operations. It
/// has no durable history dependency and no persistence method, so query dispatch is structurally a
/// read-only interaction. The delivered [`WorkflowMailboxMessage::Query`] is a distinct message
/// kind carrying a one-shot reply channel; AE/workflow processes answer it at deterministic yield
/// points from registered read-only handlers so in-progress workflow steps are not preempted or
/// mutated.
#[derive(Debug)]
pub struct QueryService<H: ?Sized> {
    engine: Arc<H>,
    query_timeout: Duration,
}

impl<H> QueryService<H>
where
    H: EngineHandle + ?Sized,
{
    /// Creates a query service with an engine-configured timeout.
    #[must_use]
    pub fn new(engine: Arc<H>, query_timeout: Duration) -> Self {
        Self {
            engine,
            query_timeout,
        }
    }

    /// Dispatches a read-only query to a resident workflow and returns the handler reply payload.
    ///
    /// # Errors
    ///
    /// Returns [`QueryError::Unknown`] for unknown workflows, [`QueryError::NotRunning`] for
    /// terminal or non-resident workflows, [`QueryError::Timeout`] when no handler reply arrives
    /// before the configured timeout, [`QueryError::UnknownQuery`] when the workflow replies that
    /// no handler exists, and [`QueryError::Engine`] for seam failures.
    pub async fn query(
        &self,
        workflow_id: &WorkflowId,
        name: impl Into<String>,
        args: Payload,
    ) -> QueryServiceResult {
        let process = match self.engine.resolve_workflow(workflow_id)? {
            WorkflowResidency::Resident(process) => process,
            WorkflowResidency::NonResident | WorkflowResidency::Terminal => {
                return Err(QueryError::NotRunning(workflow_id.clone()));
            }
            WorkflowResidency::Unknown => return Err(QueryError::Unknown(workflow_id.clone())),
        };

        let (reply_to, reply_from) = oneshot::channel();
        self.engine.deliver_workflow_message(
            process,
            WorkflowMailboxMessage::Query {
                name: name.into(),
                payload: args,
                reply_to,
            },
        )?;

        match time::timeout(self.query_timeout, reply_from).await {
            Ok(Ok(reply)) => reply,
            Ok(Err(_)) => Err(QueryError::ReplyDropped),
            Err(_) => Err(QueryError::Timeout),
        }
    }

    /// Returns the engine-configured timeout used for query replies.
    #[must_use]
    pub const fn query_timeout(&self) -> Duration {
        self.query_timeout
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, MutexGuard};
    use std::time::Duration;

    use aion_core::{ContentType, Event, Payload, TimerId, WorkflowId};
    use aion_store::{InMemoryStore, ReadableEventStore};

    use super::{QueryError, QueryService};
    use crate::Pid;
    use crate::engine_seam::{
        ChildWorkflowSpawnRequest, ChildWorkflowSpawnResult, EngineHandle, EngineSeamError,
        TimerWheelEntry, WorkflowMailboxMessage, WorkflowProcessHandle, WorkflowResidency,
    };

    const QUERY_TIMEOUT: Duration = Duration::from_millis(10);

    #[derive(Clone)]
    enum QueryBehavior {
        Reply(Payload),
        HoldSender,
    }

    #[derive(Default)]
    struct FakeQueryWorkflow {
        handlers: HashMap<String, QueryBehavior>,
        query_count: usize,
        last_payload: Option<Payload>,
    }

    #[derive(Default)]
    struct FakeQueryEngineState {
        residency: HashMap<WorkflowId, WorkflowResidency>,
        workflows: HashMap<WorkflowProcessHandle, FakeQueryWorkflow>,
        held_replies: Vec<crate::engine_seam::QueryReplySender>,
    }

    #[derive(Default)]
    struct FakeQueryEngine {
        state: Mutex<FakeQueryEngineState>,
    }

    impl FakeQueryEngine {
        fn set_resident_workflow(
            &self,
            workflow_id: WorkflowId,
            process: WorkflowProcessHandle,
            workflow: FakeQueryWorkflow,
        ) -> Result<(), EngineSeamError> {
            let mut state = self.state()?;
            state
                .residency
                .insert(workflow_id, WorkflowResidency::Resident(process));
            state.workflows.insert(process, workflow);
            Ok(())
        }

        fn set_residency(
            &self,
            workflow_id: WorkflowId,
            residency: WorkflowResidency,
        ) -> Result<(), EngineSeamError> {
            self.state()?.residency.insert(workflow_id, residency);
            Ok(())
        }

        fn query_count(&self, process: WorkflowProcessHandle) -> Result<usize, EngineSeamError> {
            Ok(self
                .state()?
                .workflows
                .get(&process)
                .map_or(0, |workflow| workflow.query_count))
        }

        fn last_payload(
            &self,
            process: WorkflowProcessHandle,
        ) -> Result<Option<Payload>, EngineSeamError> {
            Ok(self
                .state()?
                .workflows
                .get(&process)
                .and_then(|workflow| workflow.last_payload.clone()))
        }

        fn state(&self) -> Result<MutexGuard<'_, FakeQueryEngineState>, EngineSeamError> {
            self.state.lock().map_err(|_| EngineSeamError::Delivery {
                reason: "fake query engine state lock was poisoned".to_owned(),
            })
        }
    }

    impl EngineHandle for FakeQueryEngine {
        fn resolve_workflow(
            &self,
            workflow_id: &WorkflowId,
        ) -> Result<WorkflowResidency, EngineSeamError> {
            Ok(self
                .state()?
                .residency
                .get(workflow_id)
                .copied()
                .unwrap_or(WorkflowResidency::Unknown))
        }

        fn deliver_workflow_message(
            &self,
            process: WorkflowProcessHandle,
            message: WorkflowMailboxMessage,
        ) -> Result<(), EngineSeamError> {
            match message {
                WorkflowMailboxMessage::Query {
                    name,
                    payload,
                    reply_to,
                } => {
                    let mut state = self.state()?;
                    let behavior = {
                        let workflow = state.workflows.get_mut(&process).ok_or_else(|| {
                            EngineSeamError::Delivery {
                                reason: "query target process was not registered".to_owned(),
                            }
                        })?;
                        workflow.last_payload = Some(payload);
                        workflow.query_count += 1;
                        workflow.handlers.get(&name).cloned()
                    };

                    match behavior {
                        Some(QueryBehavior::Reply(payload)) => {
                            if reply_to.send(Ok(payload)).is_err() {
                                return Err(EngineSeamError::Delivery {
                                    reason: "query caller dropped reply receiver".to_owned(),
                                });
                            }
                        }
                        None => {
                            if reply_to.send(Err(QueryError::UnknownQuery(name))).is_err() {
                                return Err(EngineSeamError::Delivery {
                                    reason: "query caller dropped reply receiver".to_owned(),
                                });
                            }
                        }
                        Some(QueryBehavior::HoldSender) => state.held_replies.push(reply_to),
                    }
                    Ok(())
                }
                _ => Err(EngineSeamError::Delivery {
                    reason: "fake query engine only accepts query messages".to_owned(),
                }),
            }
        }

        fn spawn_child_workflow(
            &self,
            request: ChildWorkflowSpawnRequest,
        ) -> Result<ChildWorkflowSpawnResult, EngineSeamError> {
            Err(EngineSeamError::ChildSpawn {
                reason: format!(
                    "fake query engine does not spawn child workflow {}",
                    request.workflow_type
                ),
            })
        }

        fn terminate_linked_child_workflow(
            &self,
            parent_workflow_id: &WorkflowId,
            child_process: WorkflowProcessHandle,
            correlation: u64,
        ) -> Result<(), EngineSeamError> {
            Err(EngineSeamError::ChildTermination {
                reason: format!(
                    "fake query engine does not terminate child workflow process {} for parent {parent_workflow_id} with correlation {correlation}",
                    child_process.pid()
                ),
            })
        }

        fn terminate_linked_activity(
            &self,
            parent_workflow_id: &WorkflowId,
            activity_process: Pid,
            correlation: u64,
        ) -> Result<(), EngineSeamError> {
            Err(EngineSeamError::ChildTermination {
                reason: format!(
                    "fake query engine does not terminate activity process {activity_process} for parent {parent_workflow_id} with correlation {correlation}"
                ),
            })
        }

        fn arm_timer(&self, entry: TimerWheelEntry) -> Result<(), EngineSeamError> {
            Err(EngineSeamError::TimerWheel {
                reason: format!("fake query engine does not arm timer {}", entry.timer_id),
            })
        }

        fn disarm_timer(
            &self,
            process: WorkflowProcessHandle,
            timer_id: &TimerId,
        ) -> Result<(), EngineSeamError> {
            Err(EngineSeamError::TimerWheel {
                reason: format!(
                    "fake query engine does not disarm timer {timer_id} for process {}",
                    process.pid()
                ),
            })
        }

        fn record_workflow_event(
            &self,
            workflow_id: &WorkflowId,
            event: Event,
        ) -> Result<(), EngineSeamError> {
            Err(EngineSeamError::Recorder {
                reason: format!(
                    "queries must not record event {} for workflow {workflow_id}",
                    event.seq()
                ),
            })
        }
    }

    fn payload(label: &str) -> Payload {
        Payload::new(
            ContentType::Json,
            format!("{{\"label\":\"{label}\"}}").into_bytes(),
        )
    }

    fn known_workflow(reply: Payload) -> FakeQueryWorkflow {
        let mut handlers = HashMap::new();
        handlers.insert("state".to_owned(), QueryBehavior::Reply(reply));
        FakeQueryWorkflow {
            handlers,
            query_count: 0,
            last_payload: None,
        }
    }

    #[tokio::test]
    async fn query_returns_registered_handler_reply() -> Result<(), Box<dyn std::error::Error>> {
        let engine = Arc::new(FakeQueryEngine::default());
        let workflow_id = WorkflowId::new_v4();
        let process = WorkflowProcessHandle::new(7);
        let reply = payload("answer");
        engine.set_resident_workflow(
            workflow_id.clone(),
            process,
            known_workflow(reply.clone()),
        )?;
        let service = QueryService::new(Arc::clone(&engine), QUERY_TIMEOUT);

        let returned = service
            .query(&workflow_id, "state", payload("args"))
            .await?;

        assert_eq!(returned, reply);
        assert_eq!(engine.query_count(process)?, 1);
        assert_eq!(engine.last_payload(process)?, Some(payload("args")));
        Ok(())
    }

    #[tokio::test]
    async fn query_does_not_record_events() -> Result<(), Box<dyn std::error::Error>> {
        let store = InMemoryStore::default();
        let engine = Arc::new(FakeQueryEngine::default());
        let workflow_id = WorkflowId::new_v4();
        let process = WorkflowProcessHandle::new(8);
        engine.set_resident_workflow(
            workflow_id.clone(),
            process,
            known_workflow(payload("visible-state")),
        )?;
        let service = QueryService::new(engine, QUERY_TIMEOUT);

        let reply = service
            .query(&workflow_id, "state", payload("args"))
            .await?;
        assert_eq!(reply, payload("visible-state"));

        let history = store.read_history(&workflow_id).await?;
        assert!(history.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn unknown_query_returns_typed_error_and_workflow_remains_live()
    -> Result<(), Box<dyn std::error::Error>> {
        let engine = Arc::new(FakeQueryEngine::default());
        let workflow_id = WorkflowId::new_v4();
        let process = WorkflowProcessHandle::new(9);
        engine.set_resident_workflow(
            workflow_id.clone(),
            process,
            known_workflow(payload("known")),
        )?;
        let service = QueryService::new(Arc::clone(&engine), QUERY_TIMEOUT);

        let result = service
            .query(&workflow_id, "missing", payload("args"))
            .await;

        assert_eq!(result, Err(QueryError::UnknownQuery("missing".to_owned())));
        assert_eq!(
            engine.resolve_workflow(&workflow_id)?,
            WorkflowResidency::Resident(process)
        );
        assert_eq!(engine.query_count(process)?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn non_replying_workflow_times_out() -> Result<(), Box<dyn std::error::Error>> {
        let engine = Arc::new(FakeQueryEngine::default());
        let workflow_id = WorkflowId::new_v4();
        let process = WorkflowProcessHandle::new(10);
        let mut handlers = HashMap::new();
        handlers.insert("slow".to_owned(), QueryBehavior::HoldSender);
        engine.set_resident_workflow(
            workflow_id.clone(),
            process,
            FakeQueryWorkflow {
                handlers,
                query_count: 0,
                last_payload: None,
            },
        )?;
        let service = QueryService::new(engine, QUERY_TIMEOUT);

        let result = service.query(&workflow_id, "slow", payload("args")).await;

        assert_eq!(result, Err(QueryError::Timeout));
        Ok(())
    }

    #[tokio::test]
    async fn terminal_and_non_resident_workflows_are_not_running()
    -> Result<(), Box<dyn std::error::Error>> {
        let engine = Arc::new(FakeQueryEngine::default());
        let terminal_id = WorkflowId::new_v4();
        let non_resident_id = WorkflowId::new_v4();
        engine.set_residency(terminal_id.clone(), WorkflowResidency::Terminal)?;
        engine.set_residency(non_resident_id.clone(), WorkflowResidency::NonResident)?;
        let service = QueryService::new(engine, QUERY_TIMEOUT);

        let terminal_result = service.query(&terminal_id, "state", payload("args")).await;
        let non_resident_result = service
            .query(&non_resident_id, "state", payload("args"))
            .await;

        assert_eq!(terminal_result, Err(QueryError::NotRunning(terminal_id)));
        assert_eq!(
            non_resident_result,
            Err(QueryError::NotRunning(non_resident_id))
        );
        Ok(())
    }

    #[tokio::test]
    async fn unknown_workflow_returns_typed_unknown_error() -> Result<(), Box<dyn std::error::Error>>
    {
        let engine = Arc::new(FakeQueryEngine::default());
        let workflow_id = WorkflowId::new_v4();
        let service = QueryService::new(engine, QUERY_TIMEOUT);

        let result = service.query(&workflow_id, "state", payload("args")).await;

        assert_eq!(result, Err(QueryError::Unknown(workflow_id)));
        Ok(())
    }
}
