use crate::scanner::feed::FeedKind;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct FeedHealthEvent {
    pub feed_label: String,
    pub feed_url: String,
    pub status: String,
    pub detail: String,
    pub ts_ms: u64,
}

impl FeedHealthEvent {
    pub fn new(
        feed_label: impl Into<String>,
        feed_url: impl Into<String>,
        status: impl Into<String>,
        detail: impl Into<String>,
        ts_ms: u64,
    ) -> Self {
        Self {
            feed_label: feed_label.into(),
            feed_url: feed_url.into(),
            status: status.into(),
            detail: detail.into(),
            ts_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeedFirstHitEvent {
    pub event_key: String,
    pub event_type: String,
    pub mint: String,
    pub signature: String,
    pub slot: u64,
    pub feed_source: String,
    pub detected_at_ms: u64,
}

impl FeedFirstHitEvent {
    pub fn new(
        event_key: impl Into<String>,
        event_type: impl Into<String>,
        mint: impl Into<String>,
        signature: impl Into<String>,
        slot: u64,
        feed_source: impl Into<String>,
        detected_at_ms: u64,
    ) -> Self {
        Self {
            event_key: event_key.into(),
            event_type: event_type.into(),
            mint: mint.into(),
            signature: signature.into(),
            slot,
            feed_source: feed_source.into(),
            detected_at_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedRuntimeStatus {
    Connecting,
    Ready,
    Disconnected,
    Closed,
    Unknown,
}

impl FeedRuntimeStatus {
    pub fn from_status(status: &str) -> Self {
        match status {
            "connecting" => Self::Connecting,
            "ready" => Self::Ready,
            "disconnected" => Self::Disconnected,
            "closed" => Self::Closed,
            _ => Self::Unknown,
        }
    }

    fn score(self) -> i32 {
        match self {
            Self::Ready => 30,
            Self::Connecting => 20,
            Self::Disconnected => -20,
            Self::Closed => -30,
            Self::Unknown => 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeedRuntimeState {
    pub feed_label: String,
    pub feed_url: String,
    pub kind: FeedKind,
    pub status: FeedRuntimeStatus,
    pub detail: String,
    pub last_status_ms: u64,
    pub last_first_hit_ms: u64,
    pub first_hit_count: u64,
}

impl FeedRuntimeState {
    fn stale_penalty(&self, now_ms: u64, stale_after_ms: u64) -> i32 {
        let elapsed = now_ms.saturating_sub(self.last_status_ms.max(self.last_first_hit_ms));
        if elapsed > stale_after_ms {
            -25
        } else if elapsed > stale_after_ms / 2 {
            -10
        } else {
            0
        }
    }

    fn priority_score(&self, now_ms: u64, stale_after_ms: u64) -> i32 {
        self.status.score()
            + self.first_hit_count.min(100) as i32
            + self.stale_penalty(now_ms, stale_after_ms)
    }
}

#[derive(Debug, Clone)]
pub struct FeedSelectionChange {
    pub kind: FeedKind,
    pub previous_label: Option<String>,
    pub preferred_label: Option<String>,
    pub reason: String,
    pub ts_ms: u64,
}

#[derive(Debug, Default)]
pub struct FailoverController {
    stale_after_ms: u64,
    feeds: HashMap<String, FeedRuntimeState>,
    preferred_processed: Option<String>,
    preferred_deshred: Option<String>,
}

impl FailoverController {
    pub fn new(stale_after_ms: u64) -> Self {
        Self {
            stale_after_ms,
            ..Self::default()
        }
    }

    pub fn observe_health(
        &mut self,
        kind: FeedKind,
        event: &FeedHealthEvent,
    ) -> Option<FeedSelectionChange> {
        let entry = self
            .feeds
            .entry(event.feed_label.clone())
            .or_insert_with(|| FeedRuntimeState {
                feed_label: event.feed_label.clone(),
                feed_url: event.feed_url.clone(),
                kind,
                status: FeedRuntimeStatus::Unknown,
                detail: String::new(),
                last_status_ms: 0,
                last_first_hit_ms: 0,
                first_hit_count: 0,
            });
        entry.feed_url = event.feed_url.clone();
        entry.kind = kind;
        entry.status = FeedRuntimeStatus::from_status(&event.status);
        entry.detail = event.detail.clone();
        entry.last_status_ms = event.ts_ms;
        self.recompute_preferred(kind, event.ts_ms, format!("health:{}", event.status))
    }

    pub fn observe_first_hit(
        &mut self,
        feed_label: &str,
        detected_at_ms: u64,
    ) -> Option<FeedSelectionChange> {
        let Some(entry) = self.feeds.get_mut(feed_label) else {
            return None;
        };
        entry.first_hit_count = entry.first_hit_count.saturating_add(1);
        entry.last_first_hit_ms = detected_at_ms;
        self.recompute_preferred(entry.kind, detected_at_ms, "first_hit".to_string())
    }

    pub fn snapshot(&self) -> Vec<FeedRuntimeState> {
        let mut rows: Vec<_> = self.feeds.values().cloned().collect();
        rows.sort_by(|left, right| left.feed_label.cmp(&right.feed_label));
        rows
    }

    fn recompute_preferred(
        &mut self,
        kind: FeedKind,
        now_ms: u64,
        reason: String,
    ) -> Option<FeedSelectionChange> {
        let previous = match kind {
            FeedKind::Processed => self.preferred_processed.clone(),
            FeedKind::Deshred => self.preferred_deshred.clone(),
        };

        let next = self
            .feeds
            .values()
            .filter(|state| state.kind == kind)
            .max_by(|left, right| {
                left.priority_score(now_ms, self.stale_after_ms)
                    .cmp(&right.priority_score(now_ms, self.stale_after_ms))
                    .then_with(|| left.last_first_hit_ms.cmp(&right.last_first_hit_ms))
                    .then_with(|| left.feed_label.cmp(&right.feed_label))
            })
            .map(|state| state.feed_label.clone());

        match kind {
            FeedKind::Processed => self.preferred_processed = next.clone(),
            FeedKind::Deshred => self.preferred_deshred = next.clone(),
        }

        if previous != next {
            Some(FeedSelectionChange {
                kind,
                previous_label: previous,
                preferred_label: next,
                reason,
                ts_ms: now_ms,
            })
        } else {
            None
        }
    }
}
