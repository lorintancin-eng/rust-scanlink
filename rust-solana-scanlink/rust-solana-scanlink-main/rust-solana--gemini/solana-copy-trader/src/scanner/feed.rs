#[derive(Debug, Clone)]
pub struct FeedEndpoint {
    pub label: String,
    pub url: String,
    pub token: Option<String>,
}

impl FeedEndpoint {
    pub fn new(label: impl Into<String>, url: impl Into<String>, token: Option<String>) -> Self {
        Self {
            label: label.into(),
            url: url.into(),
            token,
        }
    }
}
