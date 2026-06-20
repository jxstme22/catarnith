use crate::types::{lamports_to_sol, TradeSide};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{BTreeMap, HashMap},
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabTradeEvent {
    pub signature: String,
    pub slot: u64,
    pub timestamp_ms: Option<i64>,
    pub ok: bool,
    pub side: TradeSide,
    pub mint: Option<String>,
    pub token_delta_raw: Option<i128>,
    pub token_decimals: Option<u8>,
    pub sol_delta_lamports: Option<i64>,
    pub fee_lamports: u64,
    pub compute_units_consumed: Option<u64>,
    pub compute_unit_limit: Option<u32>,
    pub compute_unit_price_micro_lamports: Option<u64>,
    pub instruction_names: Vec<String>,
    pub program_ids: Vec<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurvivalSettings {
    pub observation_window_ms: i64,
    pub entry_deadline_ms: i64,
    pub max_hold_ms: i64,
    pub min_successful_buys_in_window: u64,
    pub max_failed_events_in_window: u64,
    pub min_buy_spend_lamports_in_window: i64,
}

impl Default for SurvivalSettings {
    fn default() -> Self {
        Self {
            observation_window_ms: 5_000,
            entry_deadline_ms: 550,
            max_hold_ms: 5_000,
            min_successful_buys_in_window: 1,
            max_failed_events_in_window: 8,
            min_buy_spend_lamports_in_window: 13_025_001,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PulseMint {
    pub mint: String,
    pub seen_ts_ms: Option<i64>,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SurvivalLabSummary {
    pub wallet: String,
    pub source_mode: String,
    pub settings: SurvivalSettings,
    pub rows_seen: u64,
    pub parsed_events: u64,
    pub candidate_mints: usize,
    pub evaluated_mints: usize,
    pub paper_entries: usize,
    pub paper_wins: usize,
    pub paper_losses: usize,
    pub win_rate: Option<f64>,
    pub total_profit_sol: f64,
    pub median_hold_ms: Option<f64>,
    pub instant_loss_mints: usize,
    pub stale_skips: usize,
    pub high_failure_skips: usize,
    pub weak_flow_skips: usize,
    pub top_paper_winners: Vec<MintSurvivalReport>,
    pub worst_paper_losers: Vec<MintSurvivalReport>,
    pub reports: Vec<MintSurvivalReport>,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintSurvivalReport {
    pub mint: String,
    pub source: String,
    pub seen_ts_ms: Option<i64>,
    pub first_event_ms: Option<i64>,
    pub first_buy_ms: Option<i64>,
    pub first_sell_ms: Option<i64>,
    pub age_to_first_buy_ms: Option<i64>,
    pub observed_duration_ms: Option<i64>,
    pub events_in_window: u64,
    pub successful_buys_in_window: u64,
    pub successful_sells_in_window: u64,
    pub failed_events_in_window: u64,
    pub buy_spend_sol_in_window: f64,
    pub sell_receive_sol_in_window: f64,
    pub net_sol_flow_sol_in_window: f64,
    pub total_buy_spend_sol: f64,
    pub total_sell_receive_sol: f64,
    pub total_profit_sol: f64,
    pub roi_x: Option<f64>,
    pub paper_exit_ms: Option<i64>,
    pub paper_buy_spend_sol: f64,
    pub paper_sell_receive_sol: f64,
    pub paper_profit_sol: f64,
    pub paper_roi_x: Option<f64>,
    pub hold_ms: Option<i64>,
    pub instant_loss: bool,
    pub paper_enter: bool,
    pub reason_codes: Vec<String>,
}

#[derive(Debug, Default)]
struct MintEventAgg {
    events: Vec<LabTradeEvent>,
}

#[derive(Debug, Default)]
struct LamportAgg {
    buy_spend: i64,
    sell_receive: i64,
}

pub fn summarize_survival_events(
    wallet: &str,
    rows_seen: u64,
    events: Vec<LabTradeEvent>,
    pulse_mints: BTreeMap<String, PulseMint>,
    settings: SurvivalSettings,
) -> SurvivalLabSummary {
    let parsed_events = events.len() as u64;
    let source_mode = if pulse_mints.is_empty() {
        "wallet_first_seen_proxy".to_string()
    } else {
        "pulse_mint_feed".to_string()
    };

    let mut by_mint = HashMap::<String, MintEventAgg>::new();
    for event in events {
        if let Some(mint) = &event.mint {
            by_mint.entry(mint.clone()).or_default().events.push(event);
        }
    }

    let candidates: Vec<PulseMint> = if pulse_mints.is_empty() {
        by_mint
            .keys()
            .map(|mint| PulseMint {
                mint: mint.clone(),
                seen_ts_ms: None,
                source: "wallet_first_seen_proxy".to_string(),
            })
            .collect()
    } else {
        pulse_mints.values().cloned().collect()
    };

    let mut reports = candidates
        .iter()
        .filter_map(|pulse| {
            let events = by_mint.get(&pulse.mint)?.events.clone();
            Some(summarize_mint_survival(pulse, events, &settings))
        })
        .collect::<Vec<_>>();
    reports.sort_by(|a, b| {
        b.paper_profit_sol
            .partial_cmp(&a.paper_profit_sol)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    let entries: Vec<_> = reports.iter().filter(|report| report.paper_enter).collect();
    let paper_wins = entries
        .iter()
        .filter(|report| report.paper_profit_sol > 0.0)
        .count();
    let paper_losses = entries
        .iter()
        .filter(|report| report.paper_profit_sol <= 0.0)
        .count();
    let total_profit_sol = entries
        .iter()
        .map(|report| report.paper_profit_sol)
        .sum::<f64>();
    let median_hold_ms = median(
        entries
            .iter()
            .filter_map(|report| report.hold_ms.map(|value| value as f64))
            .collect(),
    );
    let win_rate = (!entries.is_empty()).then_some(paper_wins as f64 / entries.len() as f64);
    let mut worst_paper_losers = entries
        .iter()
        .map(|report| (*report).clone())
        .collect::<Vec<_>>();
    worst_paper_losers.sort_by(|a, b| {
        a.paper_profit_sol
            .partial_cmp(&b.paper_profit_sol)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    SurvivalLabSummary {
        wallet: wallet.to_string(),
        source_mode,
        settings,
        rows_seen,
        parsed_events,
        candidate_mints: candidates.len(),
        evaluated_mints: reports.len(),
        paper_entries: entries.len(),
        paper_wins,
        paper_losses,
        win_rate,
        total_profit_sol,
        median_hold_ms,
        instant_loss_mints: reports.iter().filter(|report| report.instant_loss).count(),
        stale_skips: reports
            .iter()
            .filter(|report| has_reason(report, "stale_entry"))
            .count(),
        high_failure_skips: reports
            .iter()
            .filter(|report| has_reason(report, "high_failure_pressure"))
            .count(),
        weak_flow_skips: reports
            .iter()
            .filter(|report| has_reason(report, "weak_buy_flow"))
            .count(),
        top_paper_winners: entries
            .iter()
            .take(10)
            .map(|report| (*report).clone())
            .collect(),
        worst_paper_losers: worst_paper_losers.into_iter().take(10).collect(),
        reports,
        recommendations: vec![
            "Treat Mayhem status as discovery input only; never buy solely because a mint is Mayhem."
                .to_string(),
            "Skip stale mints aggressively. The default entry deadline is 550ms from Pulse/first-seen time."
                .to_string(),
            "Track failed transactions as a first-class cost; fast Mayhem flows can burn fees before price data stabilizes."
                .to_string(),
            "Use this lab with a real Axiom Pulse Mayhem feed before enabling any live executor."
                .to_string(),
        ],
    }
}

pub fn read_pulse_mints(path: &Path) -> Result<BTreeMap<String, PulseMint>> {
    let file = File::open(path)
        .with_context(|| format!("failed to open Pulse mint feed {}", path.display()))?;
    let reader = BufReader::new(file);
    let mut out = BTreeMap::<String, PulseMint>::new();

    for line in reader.lines() {
        let line = line.with_context(|| format!("failed to read {}", path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let pulse = parse_pulse_mint_line(trimmed)?;
        if let Some(pulse) = pulse {
            out.insert(pulse.mint.clone(), pulse);
        }
    }

    Ok(out)
}

fn summarize_mint_survival(
    pulse: &PulseMint,
    mut events: Vec<LabTradeEvent>,
    settings: &SurvivalSettings,
) -> MintSurvivalReport {
    events.sort_by(|a, b| {
        a.timestamp_ms
            .unwrap_or_default()
            .cmp(&b.timestamp_ms.unwrap_or_default())
            .then_with(|| a.slot.cmp(&b.slot))
            .then_with(|| a.signature.cmp(&b.signature))
    });

    let first_event_ms = events.iter().filter_map(|event| event.timestamp_ms).min();
    let last_event_ms = events.iter().filter_map(|event| event.timestamp_ms).max();
    let seen_ts_ms = pulse.seen_ts_ms.or(first_event_ms);
    let first_buy_ms = first_event_time(&events, TradeSide::Buy);
    let first_sell_ms = first_event_time(&events, TradeSide::Sell);
    let age_to_first_buy_ms = seen_ts_ms.zip(first_buy_ms).map(|(seen, buy)| buy - seen);
    let observed_duration_ms = first_event_ms
        .zip(last_event_ms)
        .map(|(first, last)| last - first);
    let window_end_ms = seen_ts_ms.map(|seen| seen + settings.observation_window_ms);
    let window_events = events
        .iter()
        .filter(|event| {
            event
                .timestamp_ms
                .zip(seen_ts_ms)
                .zip(window_end_ms)
                .is_some_and(|((ts, seen), end)| ts >= seen && ts <= end)
        })
        .collect::<Vec<_>>();

    let window_lamports = lamport_agg(&window_events);
    let total_lamports = lamport_agg(&events.iter().collect::<Vec<_>>());
    let paper_end_ms = first_buy_ms.map(|buy| buy + settings.max_hold_ms);
    let paper_events = events
        .iter()
        .filter(|event| {
            event
                .timestamp_ms
                .zip(first_buy_ms)
                .zip(paper_end_ms)
                .is_some_and(|((ts, buy), end)| ts >= buy && ts <= end)
        })
        .collect::<Vec<_>>();
    let paper_lamports = lamport_agg(&paper_events);
    let paper_profit_lamports = paper_lamports.sell_receive - paper_lamports.buy_spend;
    let paper_exit_ms = first_ok_sell_time(&paper_events).or(paper_end_ms);
    let successful_buys_in_window = count_ok_side(&window_events, TradeSide::Buy);
    let successful_sells_in_window = count_ok_side(&window_events, TradeSide::Sell);
    let failed_events_in_window = window_events.iter().filter(|event| !event.ok).count() as u64;
    let total_profit_lamports = total_lamports.sell_receive - total_lamports.buy_spend;
    let hold_ms = first_buy_ms
        .zip(first_sell_ms)
        .map(|(buy, sell)| sell - buy);
    let instant_loss = hold_ms.is_some_and(|hold| hold <= settings.observation_window_ms)
        && paper_lamports.buy_spend > 0
        && paper_profit_lamports <= 0;

    let mut reason_codes = Vec::new();
    if first_buy_ms.is_none() {
        reason_codes.push("no_buy_flow".to_string());
    }
    if age_to_first_buy_ms.is_some_and(|age| age < 0) {
        reason_codes.push("buy_before_seen_ts".to_string());
    }
    if age_to_first_buy_ms.is_none_or(|age| age > settings.entry_deadline_ms) {
        reason_codes.push("stale_entry".to_string());
    }
    if successful_buys_in_window < settings.min_successful_buys_in_window {
        reason_codes.push("weak_buy_flow".to_string());
    }
    if failed_events_in_window > settings.max_failed_events_in_window {
        reason_codes.push("high_failure_pressure".to_string());
    }
    if window_lamports.buy_spend < settings.min_buy_spend_lamports_in_window {
        reason_codes.push("insufficient_buy_spend".to_string());
    }

    let paper_enter = reason_codes.is_empty();
    if paper_enter {
        reason_codes.push("paper_entry_survival_filter_passed".to_string());
    }

    MintSurvivalReport {
        mint: pulse.mint.clone(),
        source: pulse.source.clone(),
        seen_ts_ms,
        first_event_ms,
        first_buy_ms,
        first_sell_ms,
        age_to_first_buy_ms,
        observed_duration_ms,
        events_in_window: window_events.len() as u64,
        successful_buys_in_window,
        successful_sells_in_window,
        failed_events_in_window,
        buy_spend_sol_in_window: lamports_to_sol(window_lamports.buy_spend),
        sell_receive_sol_in_window: lamports_to_sol(window_lamports.sell_receive),
        net_sol_flow_sol_in_window: lamports_to_sol(
            window_lamports.sell_receive - window_lamports.buy_spend,
        ),
        total_buy_spend_sol: lamports_to_sol(total_lamports.buy_spend),
        total_sell_receive_sol: lamports_to_sol(total_lamports.sell_receive),
        total_profit_sol: lamports_to_sol(total_profit_lamports),
        roi_x: (total_lamports.buy_spend > 0)
            .then_some(total_lamports.sell_receive as f64 / total_lamports.buy_spend as f64),
        paper_exit_ms,
        paper_buy_spend_sol: lamports_to_sol(paper_lamports.buy_spend),
        paper_sell_receive_sol: lamports_to_sol(paper_lamports.sell_receive),
        paper_profit_sol: lamports_to_sol(paper_profit_lamports),
        paper_roi_x: (paper_lamports.buy_spend > 0)
            .then_some(paper_lamports.sell_receive as f64 / paper_lamports.buy_spend as f64),
        hold_ms,
        instant_loss,
        paper_enter,
        reason_codes,
    }
}

pub fn parse_pulse_mint_line(line: &str) -> Result<Option<PulseMint>> {
    if line.trim().is_empty() || line.trim().starts_with('#') {
        return Ok(None);
    }
    if line.trim().starts_with('{') {
        return parse_json_pulse_mint(line).map(Some);
    }
    Ok(parse_delimited_pulse_mint(line))
}

fn parse_json_pulse_mint(line: &str) -> Result<PulseMint> {
    let value: Value = serde_json::from_str(line).context("failed to parse Pulse JSONL row")?;
    let mint = value
        .get("mint")
        .or_else(|| value.get("tokenMint"))
        .or_else(|| value.get("address"))
        .and_then(Value::as_str)
        .context("Pulse JSON row is missing mint/tokenMint/address")?;
    let seen_ts_ms = value
        .get("seen_ts_ms")
        .or_else(|| value.get("timestamp_ms"))
        .or_else(|| value.get("created_at_ms"))
        .and_then(Value::as_i64)
        .or_else(|| {
            value
                .get("created_at")
                .and_then(Value::as_i64)
                .map(|seconds| seconds * 1_000)
        });
    let source = value
        .get("source")
        .and_then(Value::as_str)
        .unwrap_or("pulse_jsonl")
        .to_string();

    Ok(PulseMint {
        mint: mint.to_string(),
        seen_ts_ms,
        source,
    })
}

fn parse_delimited_pulse_mint(line: &str) -> Option<PulseMint> {
    let mut parts = line.split(',').map(str::trim);
    let mint = parts.next().unwrap_or_default().to_string();
    let mint_key = mint.to_ascii_lowercase();
    if mint.is_empty() || matches!(mint_key.as_str(), "mint" | "tokenmint" | "address") {
        return None;
    }
    let seen_ts_ms = parts.next().and_then(|value| value.parse::<i64>().ok());
    Some(PulseMint {
        mint,
        seen_ts_ms,
        source: "pulse_delimited".to_string(),
    })
}

fn lamport_agg(events: &[&LabTradeEvent]) -> LamportAgg {
    let mut agg = LamportAgg::default();
    for event in events {
        if !event.ok {
            continue;
        }
        match event.side {
            TradeSide::Buy => {
                if let Some(delta) = event.sol_delta_lamports.filter(|delta| *delta < 0) {
                    agg.buy_spend += -delta;
                }
            }
            TradeSide::Sell => {
                if let Some(delta) = event.sol_delta_lamports.filter(|delta| *delta > 0) {
                    agg.sell_receive += delta;
                }
            }
            _ => {}
        }
    }
    agg
}

fn first_event_time(events: &[LabTradeEvent], side: TradeSide) -> Option<i64> {
    events
        .iter()
        .filter(|event| event.ok && event.side == side)
        .filter_map(|event| event.timestamp_ms)
        .min()
}

fn count_ok_side(events: &[&LabTradeEvent], side: TradeSide) -> u64 {
    events
        .iter()
        .filter(|event| event.ok && event.side == side)
        .count() as u64
}

fn first_ok_sell_time(events: &[&LabTradeEvent]) -> Option<i64> {
    events
        .iter()
        .filter(|event| event.ok && event.side == TradeSide::Sell)
        .filter_map(|event| event.timestamp_ms)
        .min()
}

fn has_reason(report: &MintSurvivalReport, reason: &str) -> bool {
    report.reason_codes.iter().any(|item| item == reason)
}

fn median(mut values: Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((values.len() - 1) as f64 * 0.5).round() as usize;
    values.get(idx).copied()
}
