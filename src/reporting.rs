use crate::{
    analytics::EntryFeatures,
    curve::{sell_quote_from_state, BondingCurveState},
    position::{Position, PositionState},
    types::lamports_to_sol,
};
use anyhow::{Context, Result};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::{self, File},
    io::{BufRead, BufReader},
    path::Path,
};

const HORIZONS_MS: [i64; 4] = [1_000, 2_000, 3_000, 5_000];

#[derive(Debug, Serialize)]
struct PaperReport {
    journal_path: String,
    since_ms: Option<i64>,
    closed_round_trips: usize,
    wins: usize,
    losses: usize,
    win_rate: Option<f64>,
    total_profit_sol: f64,
    gross_profit_sol: f64,
    gross_loss_sol: f64,
    profit_factor: Option<f64>,
    average_profit_sol: Option<f64>,
    median_profit_sol: Option<f64>,
    total_profit_excluding_top_3_sol: f64,
    minimum_validation_trades: usize,
    minimum_validation_sample_met: bool,
    profitable_excluding_top_3: bool,
    best_trade: Option<RoundTrip>,
    worst_trade: Option<RoundTrip>,
    round_trips: Vec<RoundTrip>,
}

#[derive(Debug, Clone, Serialize)]
struct RoundTrip {
    mint: String,
    exit_ts_ms: Option<i64>,
    profit_lamports: i64,
    profit_sol: f64,
    buy_count: u32,
    fee_lamports: u64,
}

#[derive(Debug, Serialize)]
struct HorizonReport {
    journal_dir: String,
    slippage_bps: u32,
    exit_fee_lamports: u64,
    max_quote_age_ms: i64,
    all_horizons_profitable: bool,
    horizons: Vec<HorizonSummary>,
}

#[derive(Debug, Serialize)]
struct HorizonSummary {
    horizon_ms: i64,
    entries: usize,
    priced: usize,
    unavailable: usize,
    wins: usize,
    losses: usize,
    win_rate: Option<f64>,
    total_profit_sol: f64,
    average_profit_sol: Option<f64>,
    profit_factor: Option<f64>,
}

pub fn refresh_reports(
    journal_dir: &Path,
    paper_report_path: Option<&Path>,
    horizon_report_path: Option<&Path>,
    slippage_bps: u32,
    exit_fee_lamports: u64,
    max_quote_age_ms: i64,
) -> Result<()> {
    if let Some(path) = paper_report_path {
        let report = build_paper_report(&journal_dir.join("positions.jsonl"), None)?;
        write_json_atomic(path, &report)?;
    }
    if let Some(path) = horizon_report_path {
        let report = build_horizon_report(
            journal_dir,
            slippage_bps,
            exit_fee_lamports,
            max_quote_age_ms,
        )?;
        write_json_atomic(path, &report)?;
    }
    Ok(())
}

fn build_paper_report(path: &Path, since_ms: Option<i64>) -> Result<PaperReport> {
    let mut previous = HashMap::<String, Position>::new();
    let mut cycles = HashMap::<String, CycleStart>::new();
    let mut round_trips = Vec::<RoundTrip>::new();

    for position in read_jsonl::<Position>(path)? {
        let prior = previous.get(&position.mint);
        let became_open = matches!(
            position.state,
            PositionState::Open | PositionState::PartiallyExited
        ) && prior.is_none_or(|prior| {
            !matches!(
                prior.state,
                PositionState::Open | PositionState::PartiallyExited
            )
        });
        if became_open {
            cycles.insert(
                position.mint.clone(),
                CycleStart {
                    realized_lamports: prior
                        .map(|prior| prior.realized_lamports)
                        .unwrap_or_default(),
                    fee_lamports: prior.map(|prior| prior.fee_lamports).unwrap_or_default(),
                    entry_count: prior
                        .map(|prior| prior.entry_order_ids.len())
                        .unwrap_or_default(),
                },
            );
        }
        if position.state == PositionState::Closed {
            if let Some(cycle) = cycles.remove(&position.mint) {
                if since_ms
                    .is_none_or(|since| position.last_update_ts_ms.is_some_and(|ts| ts >= since))
                {
                    let profit_lamports = position
                        .realized_lamports
                        .saturating_sub(cycle.realized_lamports);
                    round_trips.push(RoundTrip {
                        mint: position.mint.clone(),
                        exit_ts_ms: position.last_update_ts_ms,
                        profit_lamports,
                        profit_sol: lamports_to_sol(profit_lamports),
                        buy_count: position
                            .entry_order_ids
                            .len()
                            .saturating_sub(cycle.entry_count)
                            .min(u32::MAX as usize) as u32,
                        fee_lamports: position.fee_lamports.saturating_sub(cycle.fee_lamports),
                    });
                }
            }
        }
        previous.insert(position.mint.clone(), position);
    }

    let wins = round_trips
        .iter()
        .filter(|trade| trade.profit_lamports > 0)
        .count();
    let losses = round_trips.len().saturating_sub(wins);
    let gross_profit_sol = round_trips
        .iter()
        .filter(|trade| trade.profit_sol > 0.0)
        .map(|trade| trade.profit_sol)
        .sum::<f64>();
    let gross_loss_sol = round_trips
        .iter()
        .filter(|trade| trade.profit_sol < 0.0)
        .map(|trade| trade.profit_sol.abs())
        .sum::<f64>();
    let total_profit_sol = round_trips.iter().map(|trade| trade.profit_sol).sum();
    let mut sorted_profits = round_trips
        .iter()
        .map(|trade| trade.profit_sol)
        .collect::<Vec<_>>();
    sorted_profits.sort_by(f64::total_cmp);
    let median_profit_sol = median(&sorted_profits);
    let retained = sorted_profits.len().saturating_sub(3);
    let total_profit_excluding_top_3_sol = sorted_profits[..retained].iter().sum::<f64>();
    let minimum_validation_trades = 500;

    Ok(PaperReport {
        journal_path: path.display().to_string(),
        since_ms,
        closed_round_trips: round_trips.len(),
        wins,
        losses,
        win_rate: (!round_trips.is_empty()).then_some(wins as f64 / round_trips.len() as f64),
        total_profit_sol,
        gross_profit_sol,
        gross_loss_sol,
        profit_factor: (gross_loss_sol > 0.0).then_some(gross_profit_sol / gross_loss_sol),
        average_profit_sol: (!round_trips.is_empty())
            .then_some(total_profit_sol / round_trips.len() as f64),
        median_profit_sol,
        total_profit_excluding_top_3_sol,
        minimum_validation_trades,
        minimum_validation_sample_met: round_trips.len() >= minimum_validation_trades,
        profitable_excluding_top_3: total_profit_excluding_top_3_sol > 0.0,
        best_trade: round_trips
            .iter()
            .cloned()
            .max_by_key(|trade| trade.profit_lamports),
        worst_trade: round_trips
            .iter()
            .cloned()
            .min_by_key(|trade| trade.profit_lamports),
        round_trips,
    })
}

fn build_horizon_report(
    journal_dir: &Path,
    slippage_bps: u32,
    exit_fee_lamports: u64,
    max_quote_age_ms: i64,
) -> Result<HorizonReport> {
    let entries = read_jsonl::<EntryFeatures>(&journal_dir.join("entry_features.jsonl"))?;
    let states = read_jsonl::<BondingCurveState>(&journal_dir.join("curve_states.jsonl"))?;
    let mut states_by_mint = HashMap::<String, Vec<BondingCurveState>>::new();
    for state in states {
        states_by_mint
            .entry(state.mint.clone())
            .or_default()
            .push(state);
    }
    for states in states_by_mint.values_mut() {
        states.sort_by_key(|state| (state.observed_at_ms, state.slot));
    }
    let horizons: Vec<HorizonSummary> = HORIZONS_MS
        .into_iter()
        .map(|horizon_ms| {
            summarize_horizon(
                horizon_ms,
                &entries,
                &states_by_mint,
                slippage_bps,
                exit_fee_lamports,
                max_quote_age_ms,
            )
        })
        .collect();
    let all_horizons_profitable = horizons
        .iter()
        .all(|summary| summary.priced > 0 && summary.total_profit_sol > 0.0);
    Ok(HorizonReport {
        journal_dir: journal_dir.display().to_string(),
        slippage_bps,
        exit_fee_lamports,
        max_quote_age_ms,
        all_horizons_profitable,
        horizons,
    })
}

fn summarize_horizon(
    horizon_ms: i64,
    entries: &[EntryFeatures],
    states_by_mint: &HashMap<String, Vec<BondingCurveState>>,
    slippage_bps: u32,
    exit_fee_lamports: u64,
    max_quote_age_ms: i64,
) -> HorizonSummary {
    let mut profits = Vec::<i64>::new();
    for entry in entries {
        let target_ms = entry.entry_ts_ms.saturating_add(horizon_ms);
        let state = states_by_mint
            .get(&entry.mint)
            .and_then(|states| {
                states
                    .iter()
                    .filter(|state| {
                        state.observed_at_ms >= entry.entry_ts_ms
                            && state.observed_at_ms <= target_ms
                            && entry
                                .curve_slot
                                .is_none_or(|entry_slot| state.slot >= entry_slot)
                    })
                    .max_by_key(|state| (state.observed_at_ms, state.slot))
            })
            .filter(|state| target_ms.saturating_sub(state.observed_at_ms) <= max_quote_age_ms);
        let Some(state) = state else {
            continue;
        };
        let Ok(quote) = sell_quote_from_state(state, entry.filled_token_amount_raw) else {
            continue;
        };
        let proceeds = quote
            .sol_lamports
            .saturating_mul((10_000u32.saturating_sub(slippage_bps)) as u128)
            / 10_000;
        let proceeds = proceeds.min(i64::MAX as u128) as i64;
        let cost = entry
            .filled_lamports
            .saturating_add(entry.fee_lamports)
            .saturating_add(exit_fee_lamports)
            .min(i64::MAX as u64) as i64;
        profits.push(proceeds.saturating_sub(cost));
    }
    let wins = profits.iter().filter(|profit| **profit > 0).count();
    let losses = profits.len().saturating_sub(wins);
    let gross_profit = profits
        .iter()
        .filter(|profit| **profit > 0)
        .map(|profit| *profit as i128)
        .sum::<i128>();
    let gross_loss = profits
        .iter()
        .filter(|profit| **profit < 0)
        .map(|profit| profit.unsigned_abs() as u128)
        .sum::<u128>();
    let total = profits.iter().map(|profit| *profit as i128).sum::<i128>();
    let total_profit_sol = lamports_to_sol(total.clamp(i64::MIN as i128, i64::MAX as i128) as i64);
    HorizonSummary {
        horizon_ms,
        entries: entries.len(),
        priced: profits.len(),
        unavailable: entries.len().saturating_sub(profits.len()),
        wins,
        losses,
        win_rate: (!profits.is_empty()).then_some(wins as f64 / profits.len() as f64),
        total_profit_sol,
        average_profit_sol: (!profits.is_empty())
            .then_some(total_profit_sol / profits.len() as f64),
        profit_factor: (gross_loss > 0).then_some(gross_profit as f64 / gross_loss as f64),
    }
}

fn median(sorted: &[f64]) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        Some((sorted[middle - 1] + sorted[middle]) / 2.0)
    } else {
        Some(sorted[middle])
    }
}

fn read_jsonl<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
    BufReader::new(file)
        .lines()
        .filter_map(|line| match line {
            Ok(line) if line.trim().is_empty() => None,
            other => Some(other),
        })
        .map(|line| {
            let line = line.with_context(|| format!("failed to read {}", path.display()))?;
            serde_json::from_str(&line).context("invalid report JSONL row")
        })
        .collect()
}

fn write_json_atomic(path: &Path, value: &impl Serialize) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    fs::rename(&temporary, path)
        .with_context(|| format!("failed to replace {}", path.display()))?;
    Ok(())
}

struct CycleStart {
    realized_lamports: i64,
    fee_lamports: u64,
    entry_count: usize,
}
