use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedKind {
    Processed,
    Deshred,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScannerMode {
    ProcessedOnly,
    DeshredOnly,
    Hybrid,
}

impl ScannerMode {
    pub fn allows_processed(self) -> bool {
        matches!(self, Self::ProcessedOnly | Self::Hybrid)
    }

    pub fn allows_deshred(self) -> bool {
        matches!(self, Self::DeshredOnly | Self::Hybrid)
    }
}

impl fmt::Display for ScannerMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::ProcessedOnly => "processed-only",
            Self::DeshredOnly => "deshred-only",
            Self::Hybrid => "hybrid",
        };
        f.write_str(value)
    }
}

impl FromStr for ScannerMode {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "processed" | "processed-only" | "processed_only" => Ok(Self::ProcessedOnly),
            "deshred" | "deshred-only" | "deshred_only" => Ok(Self::DeshredOnly),
            "hybrid" | "dual" | "all" => Ok(Self::Hybrid),
            other => Err(format!("unsupported SCANNER_MODE: {other}")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct FeedEndpoint {
    pub label: String,
    pub url: String,
    pub token: Option<String>,
    pub kind: FeedKind,
}

impl FeedEndpoint {
    pub fn processed(
        label: impl Into<String>,
        url: impl Into<String>,
        token: Option<String>,
    ) -> Self {
        Self {
            label: label.into(),
            url: url.into(),
            token,
            kind: FeedKind::Processed,
        }
    }

    pub fn deshred(
        label: impl Into<String>,
        url: impl Into<String>,
        token: Option<String>,
    ) -> Self {
        Self {
            label: label.into(),
            url: url.into(),
            token,
            kind: FeedKind::Deshred,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ScannerMode;
    use std::str::FromStr;

    #[test]
    fn parse_scanner_modes() {
        assert_eq!(
            ScannerMode::from_str("processed-only").unwrap(),
            ScannerMode::ProcessedOnly
        );
        assert_eq!(
            ScannerMode::from_str("deshred_only").unwrap(),
            ScannerMode::DeshredOnly
        );
        assert_eq!(
            ScannerMode::from_str("hybrid").unwrap(),
            ScannerMode::Hybrid
        );
    }
}
