//! Module declarations.

/// Bridge from engine activity dispatch to connected workers.
pub mod bridge;
/// Activity completion handling and dispatch abstractions.
pub mod dispatch;
/// Worker heartbeat and liveness tracking.
pub mod heartbeat;
/// In-flight activity-result tracking and completion delivery.
pub mod pending;
/// Connected-worker registry and handles.
pub mod registry;

pub use bridge::WorkerActivityDispatcher;
pub use dispatch::{
    ActivityCompletion, ActivityCompletionOutcome, ActivityCompletionSink, handle_activity_result,
};
pub use heartbeat::{
    HeartbeatTracker, HeartbeatUpdate, InFlightActivity, LostWorkerReport, TaskLiveness,
};
pub use pending::PendingActivities;
pub use registry::{ConnectedWorkerRegistry, WorkerHandle, WorkerId, WorkerRegistration};
