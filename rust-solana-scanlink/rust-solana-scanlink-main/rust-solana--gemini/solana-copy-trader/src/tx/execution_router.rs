use crate::config::AppConfig;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionPrimary {
    Disabled,
    Rpc,
    Jito,
    ZeroSlot,
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
}
