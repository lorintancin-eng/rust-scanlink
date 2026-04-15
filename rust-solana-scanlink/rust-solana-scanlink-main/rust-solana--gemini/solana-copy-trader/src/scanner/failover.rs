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
