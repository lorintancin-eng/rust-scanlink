use crate::config::AppConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPrimary {
    Disabled,
    Rpc,
    Jito,
    ZeroSlot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionTier {
    Conservative,
    Balanced,
    Aggressive,
}

#[derive(Debug, Clone, Default)]
pub struct ExecutionFeedback {
    pub sample_count: usize,
    pub success_rate_bps: u32,
    pub recent_failure_streak: usize,
    pub prefer_fanout: bool,
    pub prefer_zero_slot: bool,
    pub fee_boost_bps: u32,
    pub tip_boost_bps: u32,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub primary: ExecutionPrimary,
    pub send_rpc: bool,
    pub send_jito: bool,
    pub send_zero_slot: bool,
}

impl ExecutionPlan {
    pub fn from_config(config: &AppConfig) -> Self {
        if !config.execution_enabled {
            return Self {
                primary: ExecutionPrimary::Disabled,
                send_rpc: false,
                send_jito: false,
                send_zero_slot: false,
            };
        }

        let send_zero_slot = !config.zero_slot_urls.is_empty();
        let send_jito = config.jito_enabled && !config.jito_block_engine_urls.is_empty();
        let send_rpc = !send_zero_slot;
        let primary = if send_zero_slot {
            ExecutionPrimary::ZeroSlot
        } else if send_jito {
            ExecutionPrimary::Jito
        } else {
            ExecutionPrimary::Rpc
        };

        Self {
            primary,
            send_rpc,
            send_jito,
            send_zero_slot,
        }
    }

    pub fn summary(&self) -> String {
        format!(
            "primary={} | rpc={} | jito={} | zero_slot={}",
            self.primary_label(),
            self.send_rpc,
            self.send_jito,
            self.send_zero_slot
        )
    }

    pub fn primary_label(&self) -> &'static str {
        match self.primary {
            ExecutionPrimary::Disabled => "disabled",
            ExecutionPrimary::Rpc => "rpc",
            ExecutionPrimary::Jito => "jito",
            ExecutionPrimary::ZeroSlot => "zero_slot",
        }
    }

    pub fn prefers_zero_slot(&self) -> bool {
        self.primary == ExecutionPrimary::ZeroSlot
    }

    pub fn prefers_jito(&self) -> bool {
        self.primary == ExecutionPrimary::Jito
    }

    pub fn profile_for_signal(
        &self,
        path: &str,
        quality_score: u32,
        urgency_score: u32,
        execution_confidence: u32,
        feedback: &ExecutionFeedback,
    ) -> ExecutionProfile {
        let mut tier =
            if execution_confidence >= 80 || (path == "fast" && urgency_score >= 18) {
                ExecutionTier::Aggressive
            } else if execution_confidence >= 60 || path == "fast" {
                ExecutionTier::Balanced
            } else {
                ExecutionTier::Conservative
            };

        if feedback.recent_failure_streak >= 2 && tier != ExecutionTier::Aggressive {
            tier = match tier {
                ExecutionTier::Conservative => ExecutionTier::Balanced,
                ExecutionTier::Balanced => ExecutionTier::Aggressive,
                ExecutionTier::Aggressive => ExecutionTier::Aggressive,
            };
        }

        let use_all_channels = !self.send_zero_slot
            && self.send_jito
            && (tier != ExecutionTier::Conservative || feedback.prefer_fanout);
        let allow_zero_slot = self.send_zero_slot
            && (self.primary == ExecutionPrimary::ZeroSlot
                || feedback.prefer_zero_slot
                || feedback.recent_failure_streak >= 3);

        let route_label = if allow_zero_slot {
            if use_all_channels {
                "zero_slot+jito".to_string()
            } else {
                "zero_slot".to_string()
            }
        } else if use_all_channels {
            "rpc+jito".to_string()
        } else {
            self.primary_label().to_string()
        };

        let (base_priority_fee_bps, base_tip_bps) = match tier {
            ExecutionTier::Aggressive => (15_000u32, 16_000u32),
            ExecutionTier::Balanced => (12_500u32, 12_500u32),
            ExecutionTier::Conservative => (10_000u32, 10_000u32),
        };

        ExecutionProfile {
            tier,
            route_label,
            use_all_channels,
            allow_zero_slot,
            priority_fee_bps: combine_bps(base_priority_fee_bps, feedback.fee_boost_bps),
            tip_bps: combine_bps(base_tip_bps, feedback.tip_boost_bps),
            signal_path: path.to_string(),
            quality_score,
            urgency_score,
            execution_confidence,
            feedback_sample_count: feedback.sample_count,
            feedback_success_rate_bps: feedback.success_rate_bps,
            feedback_failure_streak: feedback.recent_failure_streak,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionProfile {
    pub tier: ExecutionTier,
    pub route_label: String,
    pub use_all_channels: bool,
    pub allow_zero_slot: bool,
    pub priority_fee_bps: u32,
    pub tip_bps: u32,
    pub signal_path: String,
    pub quality_score: u32,
    pub urgency_score: u32,
    pub execution_confidence: u32,
    pub feedback_sample_count: usize,
    pub feedback_success_rate_bps: u32,
    pub feedback_failure_streak: usize,
}

impl ExecutionProfile {
    pub fn adjusted_priority_fee(&self, base: u64) -> u64 {
        scale_by_bps(base, self.priority_fee_bps)
    }

    pub fn adjusted_tip_lamports(&self, base: u64) -> u64 {
        scale_by_bps(base, self.tip_bps)
    }

    pub fn summary(&self) -> String {
        format!(
            "tier={:?} route={} fanout={} zero_slot={} quality={} urgency={} confidence={} fee_bps={} tip_bps={} feedback_samples={} success_bps={} failure_streak={}",
            self.tier,
            self.route_label,
            self.use_all_channels,
            self.allow_zero_slot,
            self.quality_score,
            self.urgency_score,
            self.execution_confidence,
            self.priority_fee_bps,
            self.tip_bps,
            self.feedback_sample_count,
            self.feedback_success_rate_bps,
            self.feedback_failure_streak,
        )
    }
}

fn scale_by_bps(base: u64, bps: u32) -> u64 {
    ((base as u128).saturating_mul(bps as u128) / 10_000u128) as u64
}

fn combine_bps(base_bps: u32, boost_bps: u32) -> u32 {
    let multiplier = boost_bps.max(10_000);
    ((base_bps as u128).saturating_mul(multiplier as u128) / 10_000u128) as u32
}
