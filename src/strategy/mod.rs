use crate::{
    config::{Config, PairScope},
    types::{now_ms, Action, Decision, DecodedTx, Mode, TokenClassification, TradeSide},
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct StrategySettings {
    pub mode: Mode,
    pub base_buy_lamports: u64,
    pub max_open_positions: usize,
    pub cooldown_seconds_per_mint: i64,
    pub burst_entry_seconds: i64,
    pub min_candidate_score: f64,
    pub require_mayhem_evidence: bool,
    pub allow_indirect_mayhem_candidates: bool,
    pub require_route_confirmation: bool,
    pub require_reference_wallet_signal: bool,
    pub require_discovery_signal: bool,
    pub require_fresh_mint_creation: bool,
    pub entry_deadline_ms: i64,
    pub follow_observed_sell_signals: bool,
    pub min_observed_buy_lamports: u64,
    pub max_observed_buy_lamports: Option<u64>,
    pub max_observed_buys_before_entry: Option<u64>,
    pub max_observed_sells_before_entry: Option<u64>,
}

impl From<&Config> for StrategySettings {
    fn from(config: &Config) -> Self {
        Self {
            mode: config.mode,
            base_buy_lamports: config.base_buy_lamports,
            max_open_positions: config.max_open_positions,
            cooldown_seconds_per_mint: config.cooldown_seconds_per_mint,
            burst_entry_seconds: config.burst_entry_seconds,
            min_candidate_score: 0.45,
            require_mayhem_evidence: config.require_mayhem_evidence
                && config.pair_scope != PairScope::AllPumpfun,
            allow_indirect_mayhem_candidates: config.allow_indirect_mayhem_candidates,
            require_route_confirmation: config.require_route_confirmation,
            require_reference_wallet_signal: config.require_reference_wallet_signal,
            require_discovery_signal: config.require_discovery_signal,
            require_fresh_mint_creation: config.require_fresh_mint_creation,
            entry_deadline_ms: config.entry_deadline_ms,
            follow_observed_sell_signals: config.follow_observed_sell_signals,
            min_observed_buy_lamports: config.min_observed_buy_lamports,
            max_observed_buy_lamports: config.max_observed_buy_lamports,
            max_observed_buys_before_entry: config.max_observed_buys_before_entry,
            max_observed_sells_before_entry: config.max_observed_sells_before_entry,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StrategyContext {
    pub open_positions: usize,
    pub has_position_for_mint: bool,
    pub buys_for_mint: u32,
    pub has_discovery_signal: bool,
    pub has_fresh_mint_discovery: bool,
    pub discovery_seen_ts_ms: Option<i64>,
    pub observed_buy_lamports: Option<u64>,
    pub observed_buys_for_mint: u64,
    pub observed_sells_for_mint: u64,
}

#[derive(Debug, Default)]
pub struct BurstStrategy {
    first_buy_ms_by_mint: HashMap<String, i64>,
    cooldown_until_ms: HashMap<String, i64>,
}

impl BurstStrategy {
    pub fn decide(
        &mut self,
        settings: &StrategySettings,
        decoded: &DecodedTx,
        classification: &TokenClassification,
        context: StrategyContext,
    ) -> Decision {
        let timestamp_ms = decoded.timestamp_ms.unwrap_or_else(now_ms);
        let mint = decoded.mint.clone();
        let id = make_decision_id(timestamp_ms, &decoded.signature);
        let mut reason_codes = Vec::new();

        if !decoded.ok {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["source_tx_failed".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        let Some(mint_value) = mint.clone() else {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["no_mint_decoded".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        };

        if settings.follow_observed_sell_signals
            && decoded.side == TradeSide::Sell
            && context.has_position_for_mint
        {
            self.cooldown_until_ms.insert(
                mint_value,
                timestamp_ms + settings.cooldown_seconds_per_mint * 1_000,
            );
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Sell,
                mode: settings.mode,
                reason_codes: vec!["observed_sell_for_open_position".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if decoded.side != TradeSide::Buy {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["not_buy_signal".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if settings.require_discovery_signal && !context.has_discovery_signal {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["discovery_signal_required".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if settings.require_fresh_mint_creation && !context.has_fresh_mint_discovery {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["fresh_mint_creation_required".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if settings.require_fresh_mint_creation && !classification.is_fresh_launch {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["fresh_entry_tx_creation_required".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if let Some(seen_ts_ms) = context.discovery_seen_ts_ms {
            let discovery_age_ms = timestamp_ms.saturating_sub(seen_ts_ms);
            if discovery_age_ms < 0 || discovery_age_ms > settings.entry_deadline_ms {
                return Decision {
                    id,
                    timestamp_ms,
                    source_signature: Some(decoded.signature.clone()),
                    mint,
                    action: Action::Ignore,
                    mode: settings.mode,
                    reason_codes: vec!["discovery_entry_window_elapsed".to_string()],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
            }
        }

        let mayhem_allowed = classification.has_verified_mayhem_evidence
            || (settings.allow_indirect_mayhem_candidates && classification.is_mayhem_candidate);
        if settings.require_mayhem_evidence && !mayhem_allowed {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["mayhem_evidence_required".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if settings.require_reference_wallet_signal && !classification.is_reference_wallet_seen {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["reference_wallet_signal_required".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if settings.require_route_confirmation && !classification.has_confirmed_execution_route {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["route_confirmation_required".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if let Some(observed_buy_lamports) = context.observed_buy_lamports {
            if settings.min_observed_buy_lamports > 0
                && observed_buy_lamports < settings.min_observed_buy_lamports
            {
                return Decision {
                    id,
                    timestamp_ms,
                    source_signature: Some(decoded.signature.clone()),
                    mint,
                    action: Action::Ignore,
                    mode: settings.mode,
                    reason_codes: vec![format!(
                        "observed_buy_below_min observed_lamports={observed_buy_lamports} min_lamports={}",
                        settings.min_observed_buy_lamports
                    )],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
            }
            if let Some(max_observed_buy_lamports) = settings.max_observed_buy_lamports {
                if observed_buy_lamports > max_observed_buy_lamports {
                    return Decision {
                        id,
                        timestamp_ms,
                        source_signature: Some(decoded.signature.clone()),
                        mint,
                        action: Action::Ignore,
                        mode: settings.mode,
                        reason_codes: vec![format!(
                            "observed_buy_above_max observed_lamports={observed_buy_lamports} max_lamports={max_observed_buy_lamports}"
                        )],
                        requested_lamports: None,
                        risk_approved: false,
                        risk_veto_reason: None,
                    };
                }
            }
        }

        if let Some(max_observed_buys_before_entry) = settings.max_observed_buys_before_entry {
            if !context.has_position_for_mint
                && context.observed_buys_for_mint > max_observed_buys_before_entry
            {
                return Decision {
                    id,
                    timestamp_ms,
                    source_signature: Some(decoded.signature.clone()),
                    mint,
                    action: Action::Ignore,
                    mode: settings.mode,
                    reason_codes: vec![format!(
                        "agent_buy_sequence_late observed_buys={} max_buys={}",
                        context.observed_buys_for_mint, max_observed_buys_before_entry
                    )],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
            }
        }

        if let Some(max_observed_sells_before_entry) = settings.max_observed_sells_before_entry {
            if !context.has_position_for_mint
                && context.observed_sells_for_mint > max_observed_sells_before_entry
            {
                return Decision {
                    id,
                    timestamp_ms,
                    source_signature: Some(decoded.signature.clone()),
                    mint,
                    action: Action::Ignore,
                    mode: settings.mode,
                    reason_codes: vec![format!(
                        "agent_sold_before_entry observed_sells={} max_sells={}",
                        context.observed_sells_for_mint, max_observed_sells_before_entry
                    )],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
            }
        }

        if let Some(cooldown_until) = self.cooldown_until_ms.get(&mint_value) {
            if timestamp_ms < *cooldown_until {
                return Decision {
                    id,
                    timestamp_ms,
                    source_signature: Some(decoded.signature.clone()),
                    mint,
                    action: Action::Ignore,
                    mode: settings.mode,
                    reason_codes: vec!["mint_cooldown_active".to_string()],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
            }
        }

        if context.open_positions >= settings.max_open_positions && !context.has_position_for_mint {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["strategy_max_open_positions".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if classification.score < settings.min_candidate_score {
            return Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint,
                action: Action::Ignore,
                mode: settings.mode,
                reason_codes: vec!["candidate_score_too_low".to_string()],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
        }

        if context.has_position_for_mint {
            let first_buy_ms = self
                .first_buy_ms_by_mint
                .entry(mint_value.clone())
                .or_insert(timestamp_ms);
            let burst_age_seconds = (timestamp_ms - *first_buy_ms) / 1_000;
            if burst_age_seconds > settings.burst_entry_seconds {
                return Decision {
                    id,
                    timestamp_ms,
                    source_signature: Some(decoded.signature.clone()),
                    mint,
                    action: Action::Ignore,
                    mode: settings.mode,
                    reason_codes: vec!["burst_entry_window_elapsed".to_string()],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
            }
            reason_codes.push("follow_existing_burst".to_string());
            reason_codes.push(format!("prior_buys_for_mint={}", context.buys_for_mint));
        } else {
            self.first_buy_ms_by_mint
                .insert(mint_value.clone(), timestamp_ms);
            reason_codes.push("first_burst_entry".to_string());
        }

        if classification.is_pumpfun_bonding_curve {
            reason_codes.push("pumpfun_buy_signal".to_string());
        }
        if classification.is_pumpswap {
            reason_codes.push("pumpswap_buy_signal".to_string());
        }
        if classification.is_mayhem_candidate {
            reason_codes.push("mayhem_candidate".to_string());
        }
        if classification.has_verified_mayhem_evidence {
            reason_codes.push("verified_mayhem_evidence".to_string());
        }
        if classification.is_token_2022 {
            reason_codes.push("token_2022".to_string());
        }
        if classification.has_confirmed_execution_route {
            reason_codes.push("confirmed_axiom_pump_route".to_string());
        }

        Decision {
            id,
            timestamp_ms,
            source_signature: Some(decoded.signature.clone()),
            mint,
            action: Action::Buy,
            mode: settings.mode,
            reason_codes,
            requested_lamports: Some(settings.base_buy_lamports),
            risk_approved: false,
            risk_veto_reason: None,
        }
    }

    pub fn mark_exit(&mut self, mint: &str, timestamp_ms: i64, cooldown_seconds: i64) {
        self.cooldown_until_ms.insert(
            mint.to_string(),
            timestamp_ms.saturating_add(cooldown_seconds.saturating_mul(1_000)),
        );
        self.first_buy_ms_by_mint.remove(mint);
    }
}

fn make_decision_id(timestamp_ms: i64, signature: &str) -> String {
    let prefix: String = signature.chars().take(12).collect();
    format!("decision-{timestamp_ms}-{prefix}")
}
