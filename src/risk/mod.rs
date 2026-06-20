use crate::{
    config::Config,
    types::{Action, Decision},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone)]
pub struct RiskLimits {
    pub max_buy_lamports: u64,
    pub max_buys_per_mint: u32,
    pub max_total_lamports_per_mint: u64,
    pub max_total_open_lamports: u64,
    pub max_open_positions: usize,
    pub max_daily_loss_lamports: i64,
    pub max_failed_txs_per_minute: u32,
    pub max_failed_fee_burn_lamports_per_hour: u64,
    pub max_slippage_bps: u32,
}

impl From<&Config> for RiskLimits {
    fn from(config: &Config) -> Self {
        Self {
            max_buy_lamports: config.base_buy_lamports,
            max_buys_per_mint: config.max_buys_per_mint,
            max_total_lamports_per_mint: config.max_total_lamports_per_mint,
            max_total_open_lamports: config.max_total_open_lamports,
            max_open_positions: config.max_open_positions,
            max_daily_loss_lamports: config.max_daily_loss_lamports,
            max_failed_txs_per_minute: config.max_failed_txs_per_minute,
            max_failed_fee_burn_lamports_per_hour: config.max_failed_fee_burn_lamports_per_hour,
            max_slippage_bps: config.max_slippage_bps,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RiskSnapshot {
    pub open_positions: usize,
    pub total_open_lamports: u64,
    pub exposure_for_mint: u64,
    pub buys_for_mint: u32,
    pub daily_realized_loss_lamports: i64,
    pub failed_txs_last_minute: u32,
    pub failed_fee_burn_lamports_last_hour: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskEvaluation {
    pub approved: bool,
    pub reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RiskEngine {
    limits: RiskLimits,
    kill_switch: bool,
}

impl RiskEngine {
    pub fn new(limits: RiskLimits) -> Self {
        Self {
            limits,
            kill_switch: false,
        }
    }

    pub fn trigger_kill_switch(&mut self) {
        self.kill_switch = true;
    }

    pub fn evaluate(&self, decision: &Decision, snapshot: &RiskSnapshot) -> RiskEvaluation {
        match decision.action {
            Action::Ignore | Action::Hold => RiskEvaluation {
                approved: false,
                reason: None,
            },
            Action::KillSwitch => RiskEvaluation {
                approved: false,
                reason: Some("kill_switch_decision".to_string()),
            },
            Action::Sell => RiskEvaluation {
                approved: true,
                reason: None,
            },
            Action::Buy => self.evaluate_buy(decision, snapshot),
        }
    }

    pub fn apply(&self, mut decision: Decision, snapshot: &RiskSnapshot) -> Decision {
        let evaluation = self.evaluate(&decision, snapshot);
        decision.risk_approved = evaluation.approved;
        decision.risk_veto_reason = evaluation.reason;
        decision
    }

    fn evaluate_buy(&self, decision: &Decision, snapshot: &RiskSnapshot) -> RiskEvaluation {
        if self.kill_switch {
            return veto("kill_switch_active");
        }

        let Some(requested) = decision.requested_lamports else {
            return veto("missing_requested_lamports");
        };

        if requested == 0 {
            return veto("zero_lamport_buy");
        }
        if requested > self.limits.max_buy_lamports {
            return veto("max_buy_lamports");
        }
        if snapshot.open_positions >= self.limits.max_open_positions
            && snapshot.exposure_for_mint == 0
        {
            return veto("max_open_positions");
        }
        if snapshot.buys_for_mint >= self.limits.max_buys_per_mint {
            return veto("max_buys_per_mint");
        }
        if snapshot.exposure_for_mint.saturating_add(requested)
            > self.limits.max_total_lamports_per_mint
        {
            return veto("max_total_lamports_per_mint");
        }
        if snapshot.total_open_lamports.saturating_add(requested)
            > self.limits.max_total_open_lamports
        {
            return veto("max_total_open_lamports");
        }
        if snapshot.daily_realized_loss_lamports >= self.limits.max_daily_loss_lamports {
            return veto("max_daily_loss_lamports");
        }
        if snapshot.failed_txs_last_minute >= self.limits.max_failed_txs_per_minute {
            return veto("max_failed_txs_per_minute");
        }
        if snapshot.failed_fee_burn_lamports_last_hour
            >= self.limits.max_failed_fee_burn_lamports_per_hour
        {
            return veto("max_failed_fee_burn_lamports_per_hour");
        }
        if self.limits.max_slippage_bps == 0 {
            return veto("max_slippage_bps_missing");
        }

        RiskEvaluation {
            approved: true,
            reason: None,
        }
    }
}

fn veto(reason: &str) -> RiskEvaluation {
    RiskEvaluation {
        approved: false,
        reason: Some(reason.to_string()),
    }
}
