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
