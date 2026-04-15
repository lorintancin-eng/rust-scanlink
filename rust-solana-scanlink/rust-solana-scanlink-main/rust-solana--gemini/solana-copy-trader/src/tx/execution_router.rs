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
    ) -> ExecutionProfile {
        let tier = if execution_confidence >= 80 || (path == "fast" && urgency_score >= 18) {
            ExecutionTier::Aggressive
        } else if execution_confidence >= 60 || path == "fast" {
            ExecutionTier::Balanced
        } else {
            ExecutionTier::Conservative
        };

        let use_all_channels =
            !self.send_zero_slot && self.send_jito && tier != ExecutionTier::Conservative;
        let allow_zero_slot = self.send_zero_slot;
        let route_label = if allow_zero_slot {
            "zero_slot".to_string()
        } else if use_all_channels {
            "rpc+jito".to_string()
        } else {
            self.primary_label().to_string()
        };
        let (priority_fee_bps, tip_bps) = match tier {
            ExecutionTier::Aggressive => (15_000u32, 16_000u32),
            ExecutionTier::Balanced => (12_500u32, 12_500u32),
            ExecutionTier::Conservative => (10_000u32, 10_000u32),
        };

        ExecutionProfile {
            tier,
            route_label,
            use_all_channels,
            allow_zero_slot,
            priority_fee_bps,
            tip_bps,
            signal_path: path.to_string(),
            quality_score,
            urgency_score,
            execution_confidence,
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
            "tier={:?} route={} fanout={} zero_slot={} quality={} urgency={} confidence={} fee_bps={} tip_bps={}",
            self.tier,
            self.route_label,
            self.use_all_channels,
            self.allow_zero_slot,
            self.quality_score,
            self.urgency_score,
            self.execution_confidence,
            self.priority_fee_bps,
            self.tip_bps,
        )
    }
}

fn scale_by_bps(base: u64, bps: u32) -> u64 {
    ((base as u128).saturating_mul(bps as u128) / 10_000u128) as u64
}
