//! Visibility-store contracts and query projection types.

use std::collections::HashMap;

use aion_core::{RunId, SearchAttributeValue, WorkflowId, WorkflowStatus};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::StoreError;

/// Durable visibility-query contract for workflow visibility backends.
#[async_trait]
pub trait VisibilityStore: Send + Sync + 'static {
    /// Upserts a workflow visibility record.
    ///
    /// Implementations should replace the visibility row for the workflow execution identified by
    /// `record.workflow_id` and `record.run_id`, preserving backend-specific atomicity guarantees.
    ///
    /// # Errors
    ///
    /// Returns backend or serialization errors encountered while recording the visibility entry.
    async fn record_visibility(&self, record: VisibilityRecord) -> Result<(), StoreError>;

    /// Lists workflow visibility summaries matching `filter`.
    ///
    /// A default filter has every predicate unset and matches all workflow summaries. Pagination is
    /// applied by backends after filtering and deterministic ordering.
    ///
    /// # Errors
    ///
    /// Returns backend or serialization errors encountered while querying visibility summaries.
    async fn list_workflows(
        &self,
        filter: ListWorkflowsFilter,
    ) -> Result<Vec<WorkflowSummary>, StoreError>;

    /// Counts workflow visibility summaries matching `filter`.
    ///
    /// # Errors
    ///
    /// Returns backend or serialization errors encountered while counting visibility summaries.
    async fn count_workflows(&self, filter: ListWorkflowsFilter) -> Result<u64, StoreError>;
}

/// Complete row payload needed to upsert a workflow visibility entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct VisibilityRecord {
    /// Logical workflow identifier.
    pub workflow_id: WorkflowId,
    /// Concrete run identifier for this workflow execution.
    pub run_id: RunId,
    /// Workflow type recorded when the execution started.
    pub workflow_type: String,
    /// Current workflow status for visibility queries.
    pub status: WorkflowStatus,
    /// Timestamp recorded when the workflow execution started.
    pub start_time: DateTime<Utc>,
    /// Timestamp recorded when the workflow execution closed, if terminal.
    pub close_time: Option<DateTime<Utc>>,
    /// Typed custom search attributes indexed for visibility queries.
    pub search_attributes: HashMap<String, SearchAttributeValue>,
}

impl From<VisibilityRecord> for WorkflowSummary {
    fn from(record: VisibilityRecord) -> Self {
        Self {
            workflow_id: record.workflow_id,
            run_id: record.run_id,
            workflow_type: record.workflow_type,
            status: record.status,
            start_time: record.start_time,
            close_time: record.close_time,
            search_attributes: record.search_attributes,
        }
    }
}

/// Lightweight workflow visibility projection returned by visibility queries.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct WorkflowSummary {
    /// Logical workflow identifier.
    pub workflow_id: WorkflowId,
    /// Concrete run identifier for this workflow execution.
    pub run_id: RunId,
    /// Workflow type recorded when the execution started.
    pub workflow_type: String,
    /// Current workflow status for visibility queries.
    pub status: WorkflowStatus,
    /// Timestamp recorded when the workflow execution started.
    pub start_time: DateTime<Utc>,
    /// Timestamp recorded when the workflow execution closed, if terminal.
    pub close_time: Option<DateTime<Utc>>,
    /// Typed custom search attributes indexed for visibility queries.
    pub search_attributes: HashMap<String, SearchAttributeValue>,
}

/// Query input for listing and counting workflow visibility summaries.
///
/// A default filter has every scalar predicate unset, no search-attribute predicates, and no
/// pagination, so it matches all workflow summaries. `closed_after` and `closed_before` only match
/// summaries with a `close_time` value.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq)]
pub struct ListWorkflowsFilter {
    /// Match workflows with this workflow type exactly.
    pub workflow_type: Option<String>,
    /// Match workflows whose visibility status equals this status.
    pub status: Option<WorkflowStatus>,
    /// Match workflows started at or after this timestamp.
    pub started_after: Option<DateTime<Utc>>,
    /// Match workflows started at or before this timestamp.
    pub started_before: Option<DateTime<Utc>>,
    /// Match workflows closed at or after this timestamp; running workflows do not match.
    pub closed_after: Option<DateTime<Utc>>,
    /// Match workflows closed at or before this timestamp; running workflows do not match.
    pub closed_before: Option<DateTime<Utc>>,
    /// Typed custom search-attribute predicates that must all match.
    pub search_attributes: Vec<SearchAttributePredicate>,
    /// Maximum number of summaries to return after filtering and ordering.
    pub limit: Option<u32>,
    /// Number of summaries to skip after filtering and ordering.
    pub offset: Option<u32>,
}

impl ListWorkflowsFilter {
    /// Returns whether a visibility summary satisfies all predicates in this filter.
    ///
    /// Pagination fields are intentionally ignored here because pagination applies after filtering
    /// and deterministic ordering in a backend.
    #[must_use]
    pub fn matches(&self, summary: &WorkflowSummary) -> bool {
        self.matches_workflow_type(summary)
            && self.matches_status(summary)
            && self.matches_started_after(summary)
            && self.matches_started_before(summary)
            && self.matches_closed_after(summary)
            && self.matches_closed_before(summary)
            && self.matches_search_attributes(summary)
    }

    fn matches_workflow_type(&self, summary: &WorkflowSummary) -> bool {
        self.workflow_type
            .as_ref()
            .is_none_or(|workflow_type| workflow_type == &summary.workflow_type)
    }

    fn matches_status(&self, summary: &WorkflowSummary) -> bool {
        self.status.is_none_or(|status| status == summary.status)
    }

    fn matches_started_after(&self, summary: &WorkflowSummary) -> bool {
        self.started_after
            .is_none_or(|started_after| summary.start_time >= started_after)
    }

    fn matches_started_before(&self, summary: &WorkflowSummary) -> bool {
        self.started_before
            .is_none_or(|started_before| summary.start_time <= started_before)
    }

    fn matches_closed_after(&self, summary: &WorkflowSummary) -> bool {
        self.closed_after.is_none_or(|closed_after| {
            summary
                .close_time
                .is_some_and(|close_time| close_time >= closed_after)
        })
    }

    fn matches_closed_before(&self, summary: &WorkflowSummary) -> bool {
        self.closed_before.is_none_or(|closed_before| {
            summary
                .close_time
                .is_some_and(|close_time| close_time <= closed_before)
        })
    }

    fn matches_search_attributes(&self, summary: &WorkflowSummary) -> bool {
        self.search_attributes
            .iter()
            .all(|predicate| predicate.matches(summary))
    }
}

/// Typed predicate over one custom search attribute.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum SearchAttributePredicate {
    /// Match when the stored attribute value equals `value` exactly.
    Equals {
        /// Search attribute name.
        name: String,
        /// Expected typed value.
        value: SearchAttributeValue,
    },
    /// Match when the stored ordered attribute value is greater than `value`.
    GreaterThan {
        /// Search attribute name.
        name: String,
        /// Exclusive lower bound for the stored typed value.
        value: SearchAttributeValue,
    },
    /// Match when the stored ordered attribute value is less than `value`.
    LessThan {
        /// Search attribute name.
        name: String,
        /// Exclusive upper bound for the stored typed value.
        value: SearchAttributeValue,
    },
    /// Match when the stored attribute is a keyword list containing `keyword`.
    Contains {
        /// Search attribute name.
        name: String,
        /// Keyword expected to be present in the stored keyword list.
        keyword: String,
    },
}

impl SearchAttributePredicate {
    /// Returns whether this predicate matches the corresponding attribute on `summary`.
    ///
    /// Missing attributes and mismatched typed comparisons do not match. Greater-than and less-than
    /// comparisons are supported for integer, float, and datetime values. `Contains` matches only
    /// stored [`SearchAttributeValue::KeywordList`] attributes.
    #[must_use]
    pub fn matches(&self, summary: &WorkflowSummary) -> bool {
        match self {
            Self::Equals { name, value } => summary
                .search_attributes
                .get(name)
                .is_some_and(|stored| stored == value),
            Self::GreaterThan { name, value } => summary
                .search_attributes
                .get(name)
                .is_some_and(|stored| stored_greater_than(stored, value)),
            Self::LessThan { name, value } => summary
                .search_attributes
                .get(name)
                .is_some_and(|stored| stored_less_than(stored, value)),
            Self::Contains { name, keyword } => {
                summary
                    .search_attributes
                    .get(name)
                    .is_some_and(|stored| match stored {
                        SearchAttributeValue::KeywordList(keywords) => keywords.contains(keyword),
                        _ => false,
                    })
            }
        }
    }
}

#[must_use]
fn stored_greater_than(stored: &SearchAttributeValue, value: &SearchAttributeValue) -> bool {
    match (stored, value) {
        (SearchAttributeValue::Int(stored), SearchAttributeValue::Int(value)) => stored > value,
        (SearchAttributeValue::Float(stored), SearchAttributeValue::Float(value)) => stored > value,
        (SearchAttributeValue::Datetime(stored), SearchAttributeValue::Datetime(value)) => {
            stored > value
        }
        _ => false,
    }
}

#[must_use]
fn stored_less_than(stored: &SearchAttributeValue, value: &SearchAttributeValue) -> bool {
    match (stored, value) {
        (SearchAttributeValue::Int(stored), SearchAttributeValue::Int(value)) => stored < value,
        (SearchAttributeValue::Float(stored), SearchAttributeValue::Float(value)) => stored < value,
        (SearchAttributeValue::Datetime(stored), SearchAttributeValue::Datetime(value)) => {
            stored < value
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use aion_core::{RunId, SearchAttributeValue, WorkflowId, WorkflowStatus};
    use chrono::{DateTime, Utc};

    use super::{ListWorkflowsFilter, SearchAttributePredicate, VisibilityStore, WorkflowSummary};

    #[test]
    fn visibility_store_is_object_safe() {
        let _: Option<Arc<dyn VisibilityStore>> = None;
    }

    #[test]
    fn default_filter_matches_all_workflows() {
        let summary = workflow_summary();

        assert!(ListWorkflowsFilter::default().matches(&summary));
    }

    #[test]
    fn mismatched_typed_search_attribute_predicates_do_not_match() {
        let summary = workflow_summary();

        let greater_than_string = SearchAttributePredicate::GreaterThan {
            name: String::from("customer"),
            value: SearchAttributeValue::String(String::from("a")),
        };
        let contains_non_keyword_list = SearchAttributePredicate::Contains {
            name: String::from("attempts"),
            keyword: String::from("vip"),
        };

        assert!(
            !ListWorkflowsFilter {
                search_attributes: vec![greater_than_string],
                ..ListWorkflowsFilter::default()
            }
            .matches(&summary)
        );
        assert!(
            !ListWorkflowsFilter {
                search_attributes: vec![contains_non_keyword_list],
                ..ListWorkflowsFilter::default()
            }
            .matches(&summary)
        );
    }

    #[test]
    fn search_attribute_predicates_match_supported_typed_operations() {
        let summary = workflow_summary();

        assert!(
            SearchAttributePredicate::Equals {
                name: String::from("customer"),
                value: SearchAttributeValue::String(String::from("cust-1")),
            }
            .matches(&summary)
        );
        assert!(
            SearchAttributePredicate::GreaterThan {
                name: String::from("attempts"),
                value: SearchAttributeValue::Int(2),
            }
            .matches(&summary)
        );
        assert!(
            SearchAttributePredicate::LessThan {
                name: String::from("attempts"),
                value: SearchAttributeValue::Int(4),
            }
            .matches(&summary)
        );
        assert!(
            SearchAttributePredicate::Contains {
                name: String::from("tags"),
                keyword: String::from("vip"),
            }
            .matches(&summary)
        );
    }

    #[test]
    fn workflow_type_filter_matches_exact_type_and_rejects_mismatch() {
        let summary = workflow_summary();

        let matching = ListWorkflowsFilter {
            workflow_type: Some(String::from("example")),
            ..ListWorkflowsFilter::default()
        };
        let non_matching = ListWorkflowsFilter {
            workflow_type: Some(String::from("other")),
            ..ListWorkflowsFilter::default()
        };

        assert!(matching.matches(&summary));
        assert!(!non_matching.matches(&summary));
    }

    #[test]
    fn status_filter_matches_exact_status_and_rejects_mismatch() {
        let summary = workflow_summary();

        let matching = ListWorkflowsFilter {
            status: Some(WorkflowStatus::Running),
            ..ListWorkflowsFilter::default()
        };
        let non_matching = ListWorkflowsFilter {
            status: Some(WorkflowStatus::Completed),
            ..ListWorkflowsFilter::default()
        };

        assert!(matching.matches(&summary));
        assert!(!non_matching.matches(&summary));
    }

    #[test]
    fn started_after_filter_matches_boundary_and_rejects_earlier() {
        let summary = workflow_summary();
        let start = summary.start_time;

        let at_boundary = ListWorkflowsFilter {
            started_after: Some(start),
            ..ListWorkflowsFilter::default()
        };
        let after_start = ListWorkflowsFilter {
            started_after: Some(start + chrono::Duration::seconds(1)),
            ..ListWorkflowsFilter::default()
        };

        assert!(at_boundary.matches(&summary));
        assert!(!after_start.matches(&summary));
    }

    #[test]
    fn started_before_filter_matches_boundary_and_rejects_later() {
        let summary = workflow_summary();
        let start = summary.start_time;

        let at_boundary = ListWorkflowsFilter {
            started_before: Some(start),
            ..ListWorkflowsFilter::default()
        };
        let before_start = ListWorkflowsFilter {
            started_before: Some(start - chrono::Duration::seconds(1)),
            ..ListWorkflowsFilter::default()
        };

        assert!(at_boundary.matches(&summary));
        assert!(!before_start.matches(&summary));
    }

    #[test]
    fn closed_after_filter_does_not_match_running_workflows() {
        let summary = workflow_summary();
        assert!(summary.close_time.is_none());

        let filter = ListWorkflowsFilter {
            closed_after: Some(DateTime::<Utc>::default()),
            ..ListWorkflowsFilter::default()
        };
        assert!(!filter.matches(&summary));
    }

    #[test]
    fn closed_after_filter_matches_closed_workflow_at_boundary() {
        let close_time = DateTime::<Utc>::default() + chrono::Duration::hours(1);
        let summary = closed_workflow_summary(close_time);

        let at_boundary = ListWorkflowsFilter {
            closed_after: Some(close_time),
            ..ListWorkflowsFilter::default()
        };
        let after_close = ListWorkflowsFilter {
            closed_after: Some(close_time + chrono::Duration::seconds(1)),
            ..ListWorkflowsFilter::default()
        };

        assert!(at_boundary.matches(&summary));
        assert!(!after_close.matches(&summary));
    }

    #[test]
    fn closed_before_filter_does_not_match_running_workflows() {
        let summary = workflow_summary();

        let filter = ListWorkflowsFilter {
            closed_before: Some(DateTime::<Utc>::default() + chrono::Duration::hours(24)),
            ..ListWorkflowsFilter::default()
        };
        assert!(!filter.matches(&summary));
    }

    #[test]
    fn closed_before_filter_matches_closed_workflow_at_boundary() {
        let close_time = DateTime::<Utc>::default() + chrono::Duration::hours(1);
        let summary = closed_workflow_summary(close_time);

        let at_boundary = ListWorkflowsFilter {
            closed_before: Some(close_time),
            ..ListWorkflowsFilter::default()
        };
        let before_close = ListWorkflowsFilter {
            closed_before: Some(close_time - chrono::Duration::seconds(1)),
            ..ListWorkflowsFilter::default()
        };

        assert!(at_boundary.matches(&summary));
        assert!(!before_close.matches(&summary));
    }

    #[test]
    fn combined_filters_require_all_predicates_to_match() {
        let close_time = DateTime::<Utc>::default() + chrono::Duration::hours(1);
        let summary = closed_workflow_summary(close_time);

        let all_match = ListWorkflowsFilter {
            workflow_type: Some(String::from("example")),
            status: Some(WorkflowStatus::Completed),
            started_before: Some(summary.start_time + chrono::Duration::seconds(1)),
            closed_after: Some(close_time - chrono::Duration::seconds(1)),
            ..ListWorkflowsFilter::default()
        };
        assert!(all_match.matches(&summary));

        let one_mismatches = ListWorkflowsFilter {
            workflow_type: Some(String::from("example")),
            status: Some(WorkflowStatus::Failed),
            started_before: Some(summary.start_time + chrono::Duration::seconds(1)),
            closed_after: Some(close_time - chrono::Duration::seconds(1)),
            ..ListWorkflowsFilter::default()
        };
        assert!(!one_mismatches.matches(&summary));
    }

    fn workflow_summary() -> WorkflowSummary {
        let mut search_attributes = HashMap::new();
        search_attributes.insert(
            String::from("customer"),
            SearchAttributeValue::String(String::from("cust-1")),
        );
        search_attributes.insert(String::from("attempts"), SearchAttributeValue::Int(3));
        search_attributes.insert(
            String::from("tags"),
            SearchAttributeValue::KeywordList(vec![String::from("vip"), String::from("west")]),
        );

        WorkflowSummary {
            workflow_id: WorkflowId::new_v4(),
            run_id: RunId::new_v4(),
            workflow_type: String::from("example"),
            status: WorkflowStatus::Running,
            start_time: DateTime::<Utc>::default(),
            close_time: None,
            search_attributes,
        }
    }

    fn closed_workflow_summary(close_time: DateTime<Utc>) -> WorkflowSummary {
        WorkflowSummary {
            status: WorkflowStatus::Completed,
            close_time: Some(close_time),
            ..workflow_summary()
        }
    }
}
