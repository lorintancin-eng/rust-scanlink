use crate::config::AppConfig;
use crate::scanner::NewToken;
use reqwest::Url;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

const SUSPICIOUS_URI_HOSTS: &[&str] = &[
    "localhost",
    "127.0.0.1",
    "0.0.0.0",
    "example.com",
    "pastebin.com",
    "bit.ly",
    "tinyurl.com",
    "shorturl.at",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RiskSeverity {
    HardReject,
    StrongPenalty,
    WeakPenalty,
}

#[derive(Debug, Clone)]
pub struct RiskSignalSeed {
    pub signal_type: String,
    pub signal_value: String,
    pub score: i32,
    pub severity: RiskSeverity,
}

#[derive(Debug, Clone, Default)]
pub struct Gate1RiskProfile {
    pub uri_host: Option<String>,
    pub uri_pattern: Option<String>,
    pub template_hash: String,
    pub template_repeat_count: u32,
    pub penalty_score: u32,
    pub hard_reject_reason: Option<String>,
    pub signals: Vec<RiskSignalSeed>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RuntimeRiskInput {
    pub eligible_buyers: usize,
    pub unique_funders: usize,
    pub total_eligible_sol: f64,
    pub creator_funder_match_count: usize,
    pub creator_funder_match_sol: f64,
    pub max_single_buyer_share: f64,
}

#[derive(Debug, Clone, Default)]
pub struct RuntimeRiskProfile {
    pub penalty_score: u32,
    pub hard_reject_reason: Option<String>,
    pub signals: Vec<RiskSignalSeed>,
}

pub fn derive_template_identity(token: &NewToken) -> (Option<String>, Option<String>, String) {
    let uri_host = uri_host(&token.uri);
    let uri_pattern = uri_pattern(&token.uri);
    let template_hash = stable_template_hash(
        token.name.trim(),
        token.symbol.trim(),
        uri_pattern.as_deref().unwrap_or(token.uri.trim()),
    );
    (uri_host, uri_pattern, template_hash)
}

pub fn analyze_gate1_risk(
    token: &NewToken,
    uri_host: Option<String>,
    uri_pattern: Option<String>,
    template_hash: String,
    template_repeat_count: u32,
    config: &AppConfig,
) -> Gate1RiskProfile {
    let mut profile = Gate1RiskProfile {
        uri_host: uri_host.clone(),
        uri_pattern: uri_pattern.clone(),
        template_hash,
        template_repeat_count,
        penalty_score: 0,
        hard_reject_reason: None,
        signals: Vec::new(),
    };

    let mut add_penalty = |profile: &mut Gate1RiskProfile,
                           signal_type: &str,
                           signal_value: String,
                           severity: RiskSeverity,
                           score: i32,
                           penalty: u32| {
        profile.signals.push(RiskSignalSeed {
            signal_type: signal_type.to_string(),
            signal_value,
            score,
            severity,
        });
        profile.penalty_score = profile
            .penalty_score
            .saturating_add(penalty)
            .min(config.risk_penalty_cap);
    };

    if token.uri.trim().is_empty() {
        add_penalty(
            &mut profile,
            "missing_uri",
            token.mint.clone(),
            RiskSeverity::WeakPenalty,
            10,
            2,
        );
    } else if uri_pattern.is_none() {
        add_penalty(
            &mut profile,
            "invalid_uri",
            token.uri.clone(),
            RiskSeverity::StrongPenalty,
            config.risk_uri_penalty_score as i32,
            config.risk_uri_penalty_score,
        );
    } else if let Some(host) = uri_host.as_deref() {
        if SUSPICIOUS_URI_HOSTS
            .iter()
            .any(|candidate| host.eq_ignore_ascii_case(candidate))
        {
            add_penalty(
                &mut profile,
                "suspicious_uri_host",
                host.to_string(),
                RiskSeverity::StrongPenalty,
                config.risk_uri_penalty_score as i32,
                config.risk_uri_penalty_score,
            );
        }
    }

    if template_repeat_count >= config.risk_template_hard_reject_threshold {
        profile.hard_reject_reason = Some(format!(
            "gate1 reject: creator template repeat ({})",
            template_repeat_count
        ));
        profile.signals.push(RiskSignalSeed {
            signal_type: "template_repeat_hard".to_string(),
            signal_value: template_repeat_count.to_string(),
            score: 95,
            severity: RiskSeverity::HardReject,
        });
    } else if template_repeat_count >= config.risk_template_repeat_threshold {
        add_penalty(
            &mut profile,
            "template_repeat",
            template_repeat_count.to_string(),
            RiskSeverity::StrongPenalty,
            config.risk_template_penalty_score as i32,
            config.risk_template_penalty_score,
        );
    }

    profile
}

pub fn analyze_runtime_risk(
    gate1: &Gate1RiskProfile,
    input: RuntimeRiskInput,
    config: &AppConfig,
) -> RuntimeRiskProfile {
    let mut profile = RuntimeRiskProfile::default();
    let mut add_penalty = |profile: &mut RuntimeRiskProfile,
                           signal_type: &str,
                           signal_value: String,
                           severity: RiskSeverity,
                           score: i32,
                           penalty: u32| {
        profile.signals.push(RiskSignalSeed {
            signal_type: signal_type.to_string(),
            signal_value,
            score,
            severity,
        });
        profile.penalty_score = profile
            .penalty_score
            .saturating_add(penalty)
            .min(config.risk_penalty_cap);
    };

    if input.max_single_buyer_share >= 0.92 {
        add_penalty(
            &mut profile,
            "holder_concentration_proxy",
            format!("{:.4}", input.max_single_buyer_share),
            RiskSeverity::StrongPenalty,
            config.risk_concentration_penalty_score as i32,
            config.risk_concentration_penalty_score,
        );
    } else if input.max_single_buyer_share >= 0.80 {
        add_penalty(
            &mut profile,
            "holder_concentration_proxy",
            format!("{:.4}", input.max_single_buyer_share),
            RiskSeverity::WeakPenalty,
            (config.risk_concentration_penalty_score / 2).max(1) as i32,
            (config.risk_concentration_penalty_score / 2).max(1),
        );
    }

    if input.total_eligible_sol >= 2.5 && input.eligible_buyers <= 2 {
        add_penalty(
            &mut profile,
            "liquidity_spike_proxy",
            format!("{:.4}", input.total_eligible_sol),
            RiskSeverity::StrongPenalty,
            config.risk_liquidity_penalty_score as i32,
            config.risk_liquidity_penalty_score,
        );
    }

    if input.creator_funder_match_count >= 2 && input.creator_funder_match_sol >= 0.5 {
        add_penalty(
            &mut profile,
            "creator_funder_cluster_proxy",
            format!(
                "count={} sol={:.4}",
                input.creator_funder_match_count, input.creator_funder_match_sol
            ),
            RiskSeverity::StrongPenalty,
            config.risk_creator_funder_penalty_score as i32,
            config.risk_creator_funder_penalty_score,
        );
    }

    if input.eligible_buyers >= 3 && input.unique_funders <= 1 {
        add_penalty(
            &mut profile,
            "single_funder_cluster_proxy",
            input.unique_funders.to_string(),
            RiskSeverity::WeakPenalty,
            6,
            3,
        );
    }

    if gate1.template_repeat_count >= config.risk_template_repeat_threshold {
        add_penalty(
            &mut profile,
            "template_repeat_runtime",
            gate1.template_repeat_count.to_string(),
            RiskSeverity::WeakPenalty,
            (config.risk_template_penalty_score / 2).max(1) as i32,
            (config.risk_template_penalty_score / 2).max(1),
        );
    }

    profile.penalty_score = profile
        .penalty_score
        .saturating_add(gate1.penalty_score)
        .min(config.risk_penalty_cap);
    profile
}

fn uri_host(uri: &str) -> Option<String> {
    let uri = uri.trim();
    if uri.is_empty() {
        return None;
    }
    let url = Url::parse(uri).ok()?;
    match url.scheme() {
        "http" | "https" => url.host_str().map(|value| value.to_ascii_lowercase()),
        "ipfs" | "ar" | "arweave" => Some(url.scheme().to_ascii_lowercase()),
        _ => url.host_str().map(|value| value.to_ascii_lowercase()),
    }
}

fn uri_pattern(uri: &str) -> Option<String> {
    let uri = uri.trim();
    if uri.is_empty() {
        return None;
    }
    let url = Url::parse(uri).ok()?;
    let host = match url.scheme() {
        "http" | "https" => url.host_str().unwrap_or_default().to_ascii_lowercase(),
        "ipfs" | "ar" | "arweave" => url.scheme().to_ascii_lowercase(),
        _ => url.host_str().unwrap_or_default().to_ascii_lowercase(),
    };
    let mut segments = Vec::new();
    if let Some(parts) = url.path_segments() {
        for segment in parts.take(2) {
            let normalized = normalize_path_segment(segment);
            if !normalized.is_empty() {
                segments.push(normalized);
            }
        }
    }
    let path = if segments.is_empty() {
        "*".to_string()
    } else {
        segments.join("/")
    };
    Some(format!("{}://{}", host, path))
}

fn normalize_path_segment(segment: &str) -> String {
    let trimmed = segment.trim_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    if trimmed.len() >= 20 {
        return "{long}".to_string();
    }
    if trimmed.chars().all(|ch| ch.is_ascii_digit()) {
        return "{num}".to_string();
    }
    if trimmed
        .chars()
        .all(|ch| ch.is_ascii_hexdigit() || ch == '-' || ch == '_')
    {
        return "{id}".to_string();
    }
    trimmed.to_ascii_lowercase()
}

fn stable_template_hash(name: &str, symbol: &str, uri_pattern: &str) -> String {
    let mut hasher = DefaultHasher::new();
    (
        name.trim().to_ascii_lowercase(),
        symbol.trim().to_ascii_lowercase(),
        uri_pattern.trim().to_ascii_lowercase(),
    )
        .hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_token() -> NewToken {
        NewToken {
            mint: "mint".to_string(),
            bonding_curve: "bc".to_string(),
            creator: "creator".to_string(),
            feed_source: "feed".to_string(),
            name: "Agent First".to_string(),
            symbol: "AGENT".to_string(),
            uri: "https://example.com/meta/123".to_string(),
            is_v2: true,
            discovered_at_ms: 1,
            signature: "sig".to_string(),
            slot: 1,
        }
    }

    #[test]
    fn derives_template_identity_from_uri() {
        let token = sample_token();
        let (host, pattern, hash) = derive_template_identity(&token);
        assert_eq!(host.as_deref(), Some("example.com"));
        assert!(pattern.unwrap().starts_with("example.com://meta"));
        assert!(!hash.is_empty());
    }
}
