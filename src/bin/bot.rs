use anyhow::{Context, Result};
use catarnith::survival::read_pulse_mints;
use catarnith::{
    analytics::EntryFeatures,
    classifier::{candidate_source, classify_token, ClassifierConfig},
    config::{Config, CopyTradeBuyPolicy, CopyTradeSizing, Market},
    curve::{
        buy_quote_from_state, curve_state_key, sell_quote_from_state, BondingCurveState,
        CurveQuoteClient,
    },
    curve_stream::spawn_curve_watch,
    decoder::{
        decode_live_transaction, extract_instruction_names, extract_pump_create_event_mint,
        extract_pump_trade_observation, has_pump_create_signal, logs_have_pump_create_signal,
    },
    discovery::{candidate_from_classification, DiscoveryRegistry, DiscoverySignal},
    executor::{order_from_decision, Order, PaperExecutionSettings, PaperExecutor},
    ingest::{spawn_streams, StreamConfig, StreamEvent},
    journal::{Journal, JournalKind},
    live::LivePumpExecutor,
    market::{MarketQuote, MarketTracker},
    mayhem::{apply_mayhem_evidence, MayhemEvidence, MayhemEvidenceClient, MayhemEvidenceConfig},
    position::{Position, PositionManager},
    pulse::spawn_pulse_tail,
    quote_policy::{causal_curve_exit_quote, causal_trade_exit_quote, validate_entry_curve_slot},
    reporting::refresh_reports,
    risk::{RiskEngine, RiskLimits, RiskSnapshot},
    strategy::{BurstStrategy, StrategyContext, StrategySettings},
    tx_fetcher::TxFetcher,
    types::{
        now_ms, Action, Decision, DecodedTx, ExecutionReport, ExecutionStatus, Mode, SellOrder,
        TokenClassification,
    },
};
use futures_util::{stream::FuturesUnordered, StreamExt};
use serde::Serialize;
use std::{
    collections::{HashMap, HashSet},
    env,
    future::Future,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

const EXIT_CURVE_FETCH_MIN_INTERVAL_MS: i64 = 1_000;
const EXIT_CURVE_FETCH_RATE_LIMIT_BACKOFF_MS: i64 = 5_000;
const EXIT_CURVE_FETCH_MAX_BACKOFF_MS: i64 = 20_000;
const CREATE_SLOT_CACHE_TTL_MS: i64 = 250;
const STALE_STREAM_LOG_EVERY: u64 = 100;
const STALE_CREATE_LOG_EVERY: u64 = 25;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let config_path = parse_config_path();
    let cfg = Config::load(&config_path)?;
    cfg.validate_for_bot()?;
    let live_executor = if cfg.mode == Mode::Live {
        Some(Arc::new(LivePumpExecutor::new(&cfg).await?))
    } else {
        None
    };
    if cfg.mode == Mode::Live && env_flag("MAYHEM_LIVE_STARTUP_CHECK_ONLY") {
        info!("live executor startup check passed; exiting without streams or broadcasts");
        return Ok(());
    }
    info!("starting mayhem bot config={}", cfg.redacted());

    let journal = Journal::open(&cfg.journal_dir, &cfg.sqlite_path)?;
    let restored_positions = journal.load_latest_positions()?;
    let mut positions = PositionManager::restore(restored_positions);
    if positions.open_positions() > 0 {
        info!(
            "restored paper positions open_positions={}",
            positions.open_positions()
        );
    }

    let tx_fetcher = TxFetcher::new(cfg.rpc_url());
    let curve_quote_client = CurveQuoteClient::new(cfg.rpc_url(), &cfg.pumpfun_program)?;
    let (curve_state_tx, mut curve_state_rx) = mpsc::channel::<BondingCurveState>(1_024);
    let mut curve_states = HashMap::<String, BondingCurveState>::new();
    let mut curve_watches = HashMap::<String, CurveWatch>::new();
    if cfg.enable_curve_exit_quotes {
        for position in positions
            .positions()
            .filter(|position| positions.has_open_position(&position.mint))
        {
            let watch_until_ms = position
                .first_entry_ts_ms
                .unwrap_or_else(now_ms)
                .saturating_add(cfg.curve_observation_seconds.saturating_mul(1_000));
            ensure_curve_watch(
                &cfg,
                &curve_quote_client,
                &curve_state_tx,
                &mut curve_watches,
                &position.mint,
                watch_until_ms,
            )?;
        }
    }
    let watched_wallets = cfg.watched_wallets();
    let mut account_include = if cfg.subscribe_programs {
        vec![
            cfg.pumpfun_program.clone(),
            cfg.pumpswap_program.clone(),
            cfg.mayhem_program.clone(),
            cfg.mayhem_agent_wallet.clone(),
        ]
    } else {
        Vec::new()
    };
    if cfg.subscribe_programs
        && cfg.require_route_confirmation
        && !account_include
            .iter()
            .any(|account| account == &cfg.axiom_route_program)
    {
        account_include.push(cfg.axiom_route_program.clone());
    }
    for wallet in &watched_wallets {
        if !account_include.iter().any(|account| account == wallet) {
            account_include.push(wallet.clone());
        }
    }
    let stream_cfg = StreamConfig {
        ws_url: cfg.ws_url(),
        rpc_url: cfg.rpc_url(),
        commitment: cfg.subscribe_commitment.clone(),
        account_include,
        watched_wallets,
        logs_mentions: if cfg.subscribe_programs {
            vec![cfg.mayhem_agent_wallet.clone()]
        } else {
            Vec::new()
        },
        enable_transaction_subscribe: cfg.enable_transaction_subscribe,
        enable_logs_fallback: cfg.enable_logs_fallback,
        backfill_limit: cfg.backfill_limit,
    };
    let mut rx = spawn_streams(stream_cfg);
    let mut discoveries = DiscoveryRegistry::default();
    let mut pulse_rx = if cfg.pulse_mints_path.trim().is_empty() {
        None
    } else {
        let pulse_path = PathBuf::from(&cfg.pulse_mints_path);
        if pulse_path.exists() {
            for pulse in read_pulse_mints(&pulse_path)?.into_values() {
                register_discovery(&journal, &mut discoveries, DiscoverySignal::from(pulse))?;
            }
        }
        Some(spawn_pulse_tail(pulse_path, Duration::from_millis(100)))
    };

    let classifier_cfg = ClassifierConfig {
        pumpfun_program: cfg.pumpfun_program.clone(),
        pumpswap_program: cfg.pumpswap_program.clone(),
        mayhem_program: cfg.mayhem_program.clone(),
        mayhem_agent_wallet: cfg.mayhem_agent_wallet.clone(),
        token_2022_program: cfg.token_2022_program.clone(),
        axiom_route_program: cfg.axiom_route_program.clone(),
        axiom_jito_marker: cfg.axiom_jito_marker.clone(),
        reference_wallet: cfg
            .copy_trade_wallet()
            .map(str::to_string)
            .or_else(|| cfg.target_wallet.clone()),
    };
    let strategy_settings = StrategySettings::from(&cfg);
    let mayhem_evidence_client = MayhemEvidenceClient::new(MayhemEvidenceConfig::from(&cfg))?;
    let mut strategy = BurstStrategy::default();
    let risk = RiskEngine::new(RiskLimits::from(&cfg));
    let executor = PaperExecutor;
    let paper_execution_settings = PaperExecutionSettings {
        slippage_bps: cfg.paper_slippage_bps,
        fee_lamports_floor: cfg.paper_fee_lamports_floor,
    };
    let mut seen = HashSet::<String>::new();
    let mut curve_lookup_after_ms = HashMap::<String, i64>::new();
    let mut create_slot_cache = SlotCache::default();
    let mut market = MarketTracker::default();
    let mut stale_exit_logged = HashSet::<String>::new();
    let mut exit_curve_fetch_throttle = HashMap::<String, ExitQuoteThrottle>::new();
    let mut live_executions = FuturesUnordered::<LiveExecutionFuture>::new();
    let mut pending_live_orders = HashMap::<String, PendingLiveOrder>::new();
    let started_at_ms = now_ms();
    let mut stream_events = 0u64;
    let mut live_events = 0u64;
    let mut backfill_events = 0u64;
    let mut stale_stream_events = 0u64;
    let mut stale_log_limiter = StaleLogLimiter::default();
    let mut last_live_event_ms = None::<i64>;
    let default_wallet_for_delta = cfg
        .target_wallet
        .as_deref()
        .or_else(|| cfg.watched_wallets.first().map(String::as_str))
        .or(Some(cfg.mayhem_agent_wallet.as_str()));
    let mut exit_tick = tokio::time::interval(Duration::from_millis(cfg.exit_check_interval_ms));
    exit_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut heartbeat_tick = tokio::time::interval(Duration::from_secs(15));
    heartbeat_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    refresh_live_reports(&cfg);

    loop {
        tokio::select! {
            signal = next_pulse(&mut pulse_rx) => {
                if let Some(signal) = signal {
                    register_discovery(&journal, &mut discoveries, signal)?;
                }
            }
            state = curve_state_rx.recv() => {
                if let Some(state) = state {
                    process_curve_state(
                        &journal,
                        &positions,
                        &mut market,
                        &mut curve_states,
                        state,
                    )?;
                }
            }
            completion = live_executions.next(), if !live_executions.is_empty() => {
                if let Some(completion) = completion {
                    process_live_execution_completion(
                        &cfg,
                        &journal,
                        live_executor.as_ref(),
                        &mut live_executions,
                        &curve_quote_client,
                        &mut strategy,
                        &mut positions,
                        &discoveries,
                        &market,
                        &curve_state_tx,
                        &mut curve_watches,
                        &curve_states,
                        &mut stale_exit_logged,
                        &mut exit_curve_fetch_throttle,
                        &mut pending_live_orders,
                        completion,
                    )
                    .await?;
                }
            }
            _ = exit_tick.tick() => {
                process_exit_checks(
                    &cfg,
                    &journal,
                    &executor,
                    live_executor.as_ref(),
                    &mut live_executions,
                    &mut pending_live_orders,
                    &curve_quote_client,
                    paper_execution_settings,
                    &risk,
                    &mut strategy,
                    &mut positions,
                    &market,
                    &curve_states,
                    &mut stale_exit_logged,
                    &mut exit_curve_fetch_throttle,
                ).await?;
                cleanup_curve_watches(&positions, &mut curve_watches, now_ms());
            }
            _ = heartbeat_tick.tick() => {
                let current_ms = now_ms();
                let heartbeat = RuntimeHeartbeat {
                    timestamp_ms: current_ms,
                    uptime_seconds: current_ms.saturating_sub(started_at_ms) / 1_000,
                    stream_events,
                    live_events,
                    backfill_events,
                    stale_stream_events,
                    pending_live_orders: pending_live_orders.len(),
                    last_live_event_age_ms: last_live_event_ms
                        .map(|last| current_ms.saturating_sub(last)),
                    discoveries: discoveries.len(),
                    open_positions: positions.open_positions(),
                    active_curve_watches: curve_watches.len(),
                    live_single_lifecycle_enabled: cfg.live_single_lifecycle,
                    live_single_lifecycle_busy: active_live_lifecycle(
                        &cfg,
                        &positions,
                        &pending_live_orders,
                    )
                    .is_some(),
                };
                journal.record(JournalKind::MetricsSnapshot, &heartbeat)?;
                info!(
                    "heartbeat uptime_s={} stream_events={} live_events={} backfill_events={} stale_stream_events={} pending_live_orders={} last_live_age_ms={:?} discoveries={} open_positions={} curve_watches={} single_lifecycle_busy={}",
                    heartbeat.uptime_seconds,
                    heartbeat.stream_events,
                    heartbeat.live_events,
                    heartbeat.backfill_events,
                    heartbeat.stale_stream_events,
                    heartbeat.pending_live_orders,
                    heartbeat.last_live_event_age_ms,
                    heartbeat.discoveries,
                    heartbeat.open_positions,
                    heartbeat.active_curve_watches,
                    heartbeat.live_single_lifecycle_busy,
                );
                refresh_live_reports(&cfg);
            }
            event = rx.recv() => {
                let Some(event) = event else {
                    break;
                };
                stream_events = stream_events.saturating_add(1);
                if event.source.starts_with("backfill:") {
                    backfill_events = backfill_events.saturating_add(1);
                } else {
                    live_events = live_events.saturating_add(1);
                    last_live_event_ms = Some(now_ms());
                }
                process_stream_event(
                    event,
                    &cfg,
                    &journal,
                    &tx_fetcher,
                    &mut create_slot_cache,
                    &curve_quote_client,
                    default_wallet_for_delta,
                    &classifier_cfg,
                    &mayhem_evidence_client,
                    &strategy_settings,
                    &risk,
                    &executor,
                    live_executor.as_ref(),
                    &mut live_executions,
                    &mut pending_live_orders,
                    paper_execution_settings,
                    &mut strategy,
                    &mut positions,
                    &mut discoveries,
                    &mut market,
                    &mut seen,
                    &mut curve_lookup_after_ms,
                    &curve_state_tx,
                    &mut curve_watches,
                    &mut curve_states,
                    &mut stale_stream_events,
                    &mut stale_log_limiter,
                ).await?;
            }
        }
    }

    Ok(())
}

fn refresh_live_reports(cfg: &Config) {
    let paper_path = report_path(&cfg.paper_report_path);
    let horizon_path = report_path(&cfg.horizon_report_path);
    if paper_path.is_none() && horizon_path.is_none() {
        return;
    }
    let max_quote_age_ms = cfg
        .curve_observation_seconds
        .saturating_mul(1_000)
        .saturating_add(1_000);
    match refresh_reports(
        PathBuf::from(&cfg.journal_dir).as_path(),
        paper_path.as_deref(),
        horizon_path.as_deref(),
        cfg.paper_slippage_bps,
        cfg.paper_fee_lamports_floor,
        max_quote_age_ms,
    ) {
        Ok(()) => info!(
            "reports refreshed paper={} horizon={}",
            cfg.paper_report_path, cfg.horizon_report_path
        ),
        Err(err) => warn!("report refresh failed: {err:#}"),
    }
}

fn report_path(value: &str) -> Option<PathBuf> {
    (!value.trim().is_empty()).then(|| PathBuf::from(value))
}

fn should_skip_logs_before_fetch(
    event: &StreamEvent,
    cfg: &Config,
    _positions: &PositionManager,
) -> bool {
    if !event.source.starts_with("logsSubscribe:") {
        return false;
    }
    let instruction_names = extract_instruction_names(&event.logs);
    let has_buy = instruction_names.iter().any(|name| name.starts_with("Buy"));
    let has_create = logs_have_pump_create_signal(&event.logs);
    if has_buy || has_create {
        return false;
    }
    let has_sell = instruction_names
        .iter()
        .any(|name| name.starts_with("Sell"));
    if has_sell && cfg.follow_observed_sell_signals {
        return false;
    }
    true
}

#[derive(Debug, Serialize)]
struct RuntimeHeartbeat {
    timestamp_ms: i64,
    uptime_seconds: i64,
    stream_events: u64,
    live_events: u64,
    backfill_events: u64,
    stale_stream_events: u64,
    pending_live_orders: usize,
    last_live_event_age_ms: Option<i64>,
    discoveries: usize,
    open_positions: usize,
    active_curve_watches: usize,
    live_single_lifecycle_enabled: bool,
    live_single_lifecycle_busy: bool,
}

#[derive(Debug, Serialize)]
struct LiveLifecycleReport {
    cycle_id: String,
    mint: String,
    discovery_source: Option<String>,
    discovery_seen_ts_ms: Option<i64>,
    buy_order_id: Option<String>,
    buy_signature: Option<String>,
    entry_ts_ms: Option<i64>,
    entry_lamports: u64,
    filled_token_amount_raw: u128,
    sell_order_id: String,
    sell_signature: Option<String>,
    sell_status: ExecutionStatus,
    sell_confirmed_ts_ms: i64,
    exit_reason: String,
    hold_ms: Option<i64>,
    realized_lamports: i64,
    final_position_state: String,
    cycle_complete: bool,
}

#[derive(Debug, Clone)]
struct PendingLiveOrder {
    order: Order,
    source_slot: u64,
    submitted_at_ms: i64,
    exit_reason: Option<String>,
    attempt: u32,
    entry_origin_ts_ms: i64,
}

#[derive(Debug, Default)]
struct SlotCache {
    slot: Option<u64>,
    fetched_at_ms: i64,
}

impl SlotCache {
    fn fresh(&self, current_ms: i64) -> Option<u64> {
        self.slot
            .filter(|_| current_ms.saturating_sub(self.fetched_at_ms) <= CREATE_SLOT_CACHE_TTL_MS)
    }

    fn store(&mut self, slot: u64, current_ms: i64) {
        self.slot = Some(slot);
        self.fetched_at_ms = current_ms;
    }
}

#[derive(Debug, Default)]
struct StaleLogLimiter {
    stream_rejections: u64,
    create_rejections: u64,
}

impl StaleLogLimiter {
    fn stream_message(&mut self, stage: &str, age_ms: i64, max_age_ms: i64) -> Option<String> {
        self.stream_rejections = self.stream_rejections.saturating_add(1);
        if self.stream_rejections == 1
            || self
                .stream_rejections
                .is_multiple_of(STALE_STREAM_LOG_EVERY)
        {
            Some(format!(
                "stale stream events rejected count={} latest_stage={} latest_age_ms={} max_age_ms={}",
                self.stream_rejections, stage, age_ms, max_age_ms
            ))
        } else {
            None
        }
    }

    fn create_message(
        &mut self,
        reason: &str,
        slot_lag: Option<u64>,
        max_slot_lag: u64,
    ) -> Option<String> {
        self.create_rejections = self.create_rejections.saturating_add(1);
        if self.create_rejections == 1
            || self
                .create_rejections
                .is_multiple_of(STALE_CREATE_LOG_EVERY)
        {
            let lag = slot_lag
                .map(|lag| lag.to_string())
                .unwrap_or_else(|| "unknown".to_string());
            Some(format!(
                "stale create events rejected count={} latest_reason={} latest_slot_lag={} max_slot_lag={}",
                self.create_rejections, reason, lag, max_slot_lag
            ))
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct LiveExecutionCompletion {
    order_id: String,
    result: std::result::Result<ExecutionReport, String>,
}

type LiveExecutionFuture = Pin<Box<dyn Future<Output = LiveExecutionCompletion> + 'static>>;

#[allow(clippy::too_many_arguments)]
async fn process_stream_event(
    event: StreamEvent,
    cfg: &Config,
    journal: &Journal,
    tx_fetcher: &TxFetcher,
    create_slot_cache: &mut SlotCache,
    curve_quote_client: &CurveQuoteClient,
    default_wallet_for_delta: Option<&str>,
    classifier_cfg: &ClassifierConfig,
    mayhem_evidence_client: &MayhemEvidenceClient,
    strategy_settings: &StrategySettings,
    risk: &RiskEngine,
    executor: &PaperExecutor,
    live_executor: Option<&Arc<LivePumpExecutor>>,
    live_executions: &mut FuturesUnordered<LiveExecutionFuture>,
    pending_live_orders: &mut HashMap<String, PendingLiveOrder>,
    paper_execution_settings: PaperExecutionSettings,
    strategy: &mut BurstStrategy,
    positions: &mut PositionManager,
    discoveries: &mut DiscoveryRegistry,
    market: &mut MarketTracker,
    seen: &mut HashSet<String>,
    curve_lookup_after_ms: &mut HashMap<String, i64>,
    curve_state_tx: &mpsc::Sender<BondingCurveState>,
    curve_watches: &mut HashMap<String, CurveWatch>,
    curve_states: &mut HashMap<String, BondingCurveState>,
    stale_stream_events: &mut u64,
    stale_log_limiter: &mut StaleLogLimiter,
) -> Result<()> {
    let is_backfill = event.source.starts_with("backfill:");
    if seen.contains(&event.signature) {
        return Ok(());
    }
    if should_skip_logs_before_fetch(&event, cfg, positions) {
        seen.insert(event.signature);
        return Ok(());
    }
    if !journal.record_once(JournalKind::RawEvent, &event.signature, &event)? {
        seen.insert(event.signature);
        return Ok(());
    }
    seen.insert(event.signature.clone());

    let received_at_ms = if event.received_at_ms > 0 {
        event.received_at_ms
    } else {
        now_ms()
    };
    if !is_backfill
        && !accept_fresh_stream_event(
            journal,
            &event,
            received_at_ms,
            cfg.max_stream_event_age_ms,
            "queued_before_decode",
            stale_stream_events,
            stale_log_limiter,
        )?
    {
        return Ok(());
    }
    let create_event_mint = extract_pump_create_event_mint(&event.logs);
    let has_create_signal =
        create_event_mint.is_some() || logs_have_pump_create_signal(&event.logs);
    if cfg.mode == Mode::Live
        && has_create_signal
        && !accept_fresh_create_event_slot(
            journal,
            tx_fetcher,
            create_slot_cache,
            &event,
            cfg,
            stale_log_limiter,
        )
        .await?
    {
        return Ok(());
    }
    if let Some(decision) =
        early_single_lifecycle_decision(cfg, &event, received_at_ms, positions, pending_live_orders)
    {
        journal.record(JournalKind::Decision, &decision)?;
        return Ok(());
    }

    let inline_tx = event
        .raw
        .pointer("/params/result")
        .filter(|value| value.get("transaction").is_some())
        .cloned();
    let copy_trade_log_source = is_copy_trade_stream_source(cfg, &event);
    let wallet_for_delta = wallet_for_delta_for_event(cfg, &event, default_wallet_for_delta);
    let logs_have_pump_event = event.source.starts_with("logsSubscribe:")
        && (extract_pump_trade_observation(&event.logs).is_some() || has_create_signal);
    let tx = if is_backfill {
        Some(event.raw.clone())
    } else if inline_tx.is_some() {
        inline_tx
    } else if logs_have_pump_event && !copy_trade_log_source {
        None
    } else if cfg.fetch_full_transaction {
        match fetch_transaction_with_retry(tx_fetcher, &event.signature).await {
            Ok(tx) => Some(tx),
            Err(err) => {
                warn!("failed to fetch tx {}: {err:#}", event.signature);
                return Ok(());
            }
        }
    } else {
        None
    };

    if !is_backfill
        && !accept_fresh_stream_event(
            journal,
            &event,
            received_at_ms,
            cfg.max_stream_event_age_ms,
            "stale_after_transaction_fetch",
            stale_stream_events,
            stale_log_limiter,
        )?
    {
        return Ok(());
    }

    let mut decoded = decode_live_transaction(
        event.signature.clone(),
        event.slot,
        event.logs.clone(),
        tx.as_ref(),
        wallet_for_delta,
    );
    if !is_backfill {
        decoded.timestamp_ms = Some(received_at_ms);
        if event.source == format!("logsSubscribe:{}", cfg.mayhem_agent_wallet)
            && !decoded
                .account_keys
                .iter()
                .any(|account| account == &cfg.mayhem_agent_wallet)
        {
            decoded.account_keys.push(cfg.mayhem_agent_wallet.clone());
        }
    }
    let mut classification = classify_token(&decoded, classifier_cfg);
    journal.record(JournalKind::DecodedTransaction, &decoded)?;

    if let Some(mint) = decoded.mint.as_deref() {
        let evidence = mayhem_evidence_client
            .check_mint(mint, &decoded, &classification)
            .await;
        classification = apply_mayhem_evidence(classification, &evidence);
        journal.record(JournalKind::MayhemEvidence, &evidence)?;

        let has_executable_buy_observation = decoded.side.is_buy()
            && decoded.sol_delta_lamports.is_some_and(|delta| delta < 0)
            && decoded.token_delta_raw.is_some_and(|delta| delta > 0);
        let has_mint_creation = has_pump_create_signal(&decoded);
        let discovery_observation_allowed = if cfg.require_fresh_mint_creation {
            has_mint_creation
        } else {
            has_executable_buy_observation
        };
        if cfg.allow_onchain_mayhem_discovery
            && evidence.is_mayhem
            && !is_backfill
            && discovery_observation_allowed
        {
            let already_curve_confirmed = discoveries.get(mint).is_some_and(|signal| {
                signal.verified_mayhem && signal.source == "pump_curve_is_mayhem_mode"
            });
            let current_ms = now_ms();
            let lookup_ready = curve_lookup_after_ms
                .get(mint)
                .is_none_or(|retry_after| current_ms >= *retry_after);
            let mut curve_confirmed = already_curve_confirmed;
            if cfg.require_curve_mayhem_flag && !curve_confirmed && lookup_ready {
                match curve_quote_client.fetch_state(mint).await {
                    Ok(state) => {
                        curve_confirmed = state.is_mayhem_mode == Some(true);
                        let key = curve_state_key(&state);
                        journal.record_once(JournalKind::CurveState, &key, &state)?;
                        curve_states.insert(mint.to_string(), state.clone());
                        if curve_confirmed || state.is_mayhem_mode == Some(false) {
                            curve_lookup_after_ms.remove(mint);
                        } else {
                            curve_lookup_after_ms
                                .insert(mint.to_string(), current_ms.saturating_add(1_000));
                        }
                        if state.is_mayhem_mode == Some(false) {
                            debug!("direct Mayhem evidence rejected by curve flag mint={mint}");
                        }
                    }
                    Err(err) => {
                        curve_lookup_after_ms
                            .insert(mint.to_string(), current_ms.saturating_add(2_000));
                        warn!("Mayhem curve flag lookup failed mint={mint}: {err:#}");
                    }
                }
            }
            if curve_confirmed || !cfg.require_curve_mayhem_flag {
                register_discovery(
                    journal,
                    discoveries,
                    DiscoverySignal {
                        mint: mint.to_string(),
                        seen_ts_ms: decoded.timestamp_ms.unwrap_or_else(now_ms),
                        source: if has_mint_creation {
                            "pump_create_mayhem".to_string()
                        } else if curve_confirmed {
                            "pump_curve_is_mayhem_mode".to_string()
                        } else {
                            "onchain_mayhem".to_string()
                        },
                        verified_mayhem: true,
                    },
                )?;
            }
        }

        if let Some(discovery) = discoveries.get(mint) {
            classification.is_mayhem_candidate = true;
            classification.has_verified_mayhem_evidence |= discovery.verified_mayhem;
            classification.score += 1.0;
            classification
                .reasons
                .push(format!("discovery_source={}", discovery.source));
        }
    } else {
        let evidence = MayhemEvidence::rejected("", "no_mint_decoded");
        journal.record(JournalKind::MayhemEvidence, &evidence)?;
    }

    if let Some(mut quote) = MarketQuote::from_decoded(&decoded)
        .filter(|_| classification.is_pumpfun_bonding_curve || classification.is_pumpswap)
    {
        if is_backfill {
            quote.observed_at_ms = quote.timestamp_ms;
        }
        journal.record_once(JournalKind::MarketQuote, &quote.signature, &quote)?;
        market.update(quote);
    }

    journal.record(JournalKind::CandidateMint, &classification)?;
    if let Some(candidate) = candidate_from_classification(
        &classification,
        decoded.slot,
        decoded.timestamp_ms.unwrap_or_else(now_ms),
        candidate_source(&classification),
    ) {
        let key = format!("{}:{}", candidate.mint, candidate.first_seen_slot);
        journal.record_once(JournalKind::CandidateMint, &key, &candidate)?;
    }

    if is_backfill {
        return Ok(());
    }

    let mint = decoded.mint.as_deref();
    if cfg.mode == Mode::Live
        && mint.is_some_and(|mint| pending_order_for_mint(pending_live_orders, mint))
    {
        let decision = Decision {
            id: format!(
                "decision-pending-{}-{}",
                received_at_ms,
                mint_prefix(mint.unwrap())
            ),
            timestamp_ms: received_at_ms,
            source_signature: Some(decoded.signature.clone()),
            mint: mint.map(str::to_string),
            action: Action::Ignore,
            mode: cfg.mode,
            reason_codes: vec!["live_order_pending_for_mint".to_string()],
            requested_lamports: None,
            risk_approved: false,
            risk_veto_reason: None,
        };
        journal.record(JournalKind::Decision, &decision)?;
        return Ok(());
    }
    if let Some(decision) =
        observed_agent_buy_exit_decision(cfg, &event, &decoded, positions, received_at_ms)
    {
        let snapshot = positions.snapshot_for_mint(decision.mint.as_deref());
        let decision = risk.apply(decision, &snapshot);
        journal.record(JournalKind::Decision, &decision)?;
        if let Some(order) = order_from_decision(&decision) {
            journal.record(JournalKind::Order, &order)?;
            if cfg.mode == Mode::Live {
                submit_live_execution(
                    cfg,
                    live_executor.context("live executor unavailable")?,
                    live_executions,
                    pending_live_orders,
                    order,
                    None,
                    decoded.slot,
                    Some("observed_agent_buy_after_entry".to_string()),
                )?;
                return Ok(());
            }

            let held_token_amount_raw = positions.token_amount_for_mint(order.mint());
            let report = executor.force_close_unpriced(
                &order,
                held_token_amount_raw,
                paper_execution_settings,
            );
            journal.record(JournalKind::Execution, &report)?;
            if is_filled_execution(report.status) {
                positions.record_order_report(&order, &report);
                if let Some(position) = positions.position_for_mint(order.mint()) {
                    journal.record(JournalKind::Position, position)?;
                }
            }
        }
        return Ok(());
    }
    if decoded.side.is_buy() {
        if let Some(active) = active_live_lifecycle(cfg, positions, pending_live_orders) {
            let decision = Decision {
                id: format!(
                    "decision-single-lifecycle-{}-{}",
                    received_at_ms,
                    mint.map(mint_prefix)
                        .unwrap_or_else(|| "unknown".to_string())
                ),
                timestamp_ms: received_at_ms,
                source_signature: Some(decoded.signature.clone()),
                mint: mint.map(str::to_string),
                action: Action::Ignore,
                mode: cfg.mode,
                reason_codes: vec![
                    "live_single_lifecycle_active".to_string(),
                    format!("active_mint={}", active.mint),
                    format!("active_reason={}", active.reason),
                ],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            };
            journal.record(JournalKind::Decision, &decision)?;
            return Ok(());
        }
    }
    let discovery = mint.and_then(|mint| discoveries.get(mint));
    let market_stats = mint.map(|mint| market.stats(mint)).unwrap_or_default();
    let observed_buy_lamports = (decoded.side.is_buy())
        .then(|| {
            decoded
                .sol_delta_lamports
                .filter(|delta| *delta < 0)
                .and_then(|delta| delta.checked_abs())
                .map(|delta| delta as u64)
        })
        .flatten();
    let context = StrategyContext {
        open_positions: positions
            .open_positions()
            .saturating_add(pending_buy_count(pending_live_orders)),
        has_position_for_mint: mint.is_some_and(|mint| positions.has_open_position(mint)),
        buys_for_mint: positions.snapshot_for_mint(mint).buys_for_mint,
        has_discovery_signal: discovery.is_some(),
        has_fresh_mint_discovery: discovery
            .is_some_and(|signal| signal.source == "pump_create_mayhem"),
        discovery_seen_ts_ms: discovery.map(|signal| signal.seen_ts_ms),
        observed_buy_lamports,
        observed_buys_for_mint: market_stats.observed_buys,
        observed_sells_for_mint: market_stats.observed_sells,
    };
    let decision = copy_trade_decision(cfg, &event, &decoded, &classification, context)
        .unwrap_or_else(|| strategy.decide(strategy_settings, &decoded, &classification, context));
    let mut snapshot = positions.snapshot_for_mint(mint);
    apply_pending_live_risk(&mut snapshot, mint, pending_live_orders);
    let decision = risk.apply(decision, &snapshot);
    if decision.risk_veto_reason.is_some() {
        journal.record(JournalKind::RiskVeto, &decision)?;
    }
    journal.record(JournalKind::Decision, &decision)?;
    if decision
        .reason_codes
        .iter()
        .any(|reason| reason.starts_with("copy_trade_"))
    {
        info!(
            "copy trade decision action={:?} mint={:?} lamports={:?} reasons={}",
            decision.action,
            decision.mint.as_deref().map(mint_prefix),
            decision.requested_lamports,
            decision.reason_codes.join(",")
        );
    }

    if let Some(mut order) = order_from_decision(&decision) {
        let held_token_amount_raw = positions.token_amount_for_mint(order.mint());
        if cfg.mode == Mode::Live {
            journal.record(JournalKind::Order, &order)?;
            submit_live_execution(
                cfg,
                live_executor.context("live executor unavailable")?,
                live_executions,
                pending_live_orders,
                order,
                None,
                decoded.slot,
                None,
            )?;
            return Ok(());
        }

        let report = if let Order::Buy(buy) = &order {
            if cfg.use_observed_entry_fill {
                executor.execute_observed(
                    &order,
                    &decoded,
                    held_token_amount_raw,
                    paper_execution_settings,
                )?
            } else {
                let mint = buy.mint.clone();
                let lamports = buy.lamports;
                match fetch_entry_curve_state(
                    curve_quote_client,
                    &mint,
                    decoded.slot,
                    cfg.max_entry_curve_slot_ahead,
                )
                .await
                {
                    Ok(state) => {
                        if let Order::Buy(buy) = &mut order {
                            buy.timestamp_ms = state.observed_at_ms;
                        }
                        let key = curve_state_key(&state);
                        journal.record_once(JournalKind::CurveState, &key, &state)?;
                        curve_states.insert(mint, state.clone());
                        match buy_quote_from_state(&state, lamports as u128) {
                            Ok(quote) => {
                                journal.record_once(
                                    JournalKind::MarketQuote,
                                    &quote.signature,
                                    &quote,
                                )?;
                                executor.execute_quote(
                                    &order,
                                    &quote,
                                    held_token_amount_raw,
                                    paper_execution_settings,
                                )?
                            }
                            Err(err) => executor
                                .reject(&order, &format!("entry_curve_quote_unavailable:{err}")),
                        }
                    }
                    Err(err) => {
                        executor.reject(&order, &format!("entry_curve_state_unavailable:{err}"))
                    }
                }
            }
        } else if let Some(entry_slot) = positions
            .position_for_mint(order.mint())
            .and_then(|position| position.entry_quote_slot)
        {
            if decoded.slot < entry_slot {
                executor.reject(
                    &order,
                    &format!(
                        "observed_exit_slot_before_entry entry_slot={entry_slot} exit_slot={}",
                        decoded.slot
                    ),
                )
            } else {
                executor.execute_observed(
                    &order,
                    &decoded,
                    held_token_amount_raw,
                    paper_execution_settings,
                )?
            }
        } else {
            executor.reject(&order, "observed_exit_missing_entry_slot")
        };
        journal.record(JournalKind::Order, &order)?;
        journal.record(JournalKind::Execution, &report)?;
        if is_filled_execution(report.status) {
            positions.record_order_report(&order, &report);
            if let Some(position) = positions.position_for_mint(order.mint()) {
                journal.record(JournalKind::Position, position)?;
            }
            if let Order::Buy(buy) = &order {
                let watch_until_ms = buy
                    .timestamp_ms
                    .saturating_add(cfg.curve_observation_seconds.saturating_mul(1_000));
                if cfg.enable_curve_exit_quotes {
                    ensure_curve_watch(
                        cfg,
                        curve_quote_client,
                        curve_state_tx,
                        curve_watches,
                        &buy.mint,
                        watch_until_ms,
                    )?;
                }
                if let Some(discovery) = discoveries.get(&buy.mint) {
                    let features = EntryFeatures::build(
                        buy.mint.clone(),
                        buy.id.clone(),
                        buy.source_signature.clone(),
                        discovery.source.clone(),
                        discovery.seen_ts_ms,
                        decoded.slot,
                        buy.timestamp_ms,
                        report.filled_lamports.unwrap_or(buy.lamports),
                        report.filled_token_amount_raw.unwrap_or_default(),
                        report.fee_lamports.unwrap_or_default(),
                        curve_states.get(&buy.mint),
                        market.stats(&buy.mint),
                    );
                    journal.record_once(JournalKind::EntryFeatures, &buy.id, &features)?;
                }
            }
            info!(
                "execution fill mint={} mode={:?} side={:?} filled_lamports={:?} filled_token_raw={:?}",
                order.mint(),
                cfg.mode,
                decoded.side,
                report.filled_lamports,
                report.filled_token_amount_raw
            );
        } else {
            warn!(
                "execution rejected mint={} mode={:?} reason={}",
                order.mint(),
                cfg.mode,
                report.error.as_deref().unwrap_or("unknown")
            );
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_exit_checks(
    cfg: &Config,
    journal: &Journal,
    executor: &PaperExecutor,
    live_executor: Option<&Arc<LivePumpExecutor>>,
    live_executions: &mut FuturesUnordered<LiveExecutionFuture>,
    pending_live_orders: &mut HashMap<String, PendingLiveOrder>,
    curve_quote_client: &CurveQuoteClient,
    paper_settings: PaperExecutionSettings,
    risk: &RiskEngine,
    strategy: &mut BurstStrategy,
    positions: &mut PositionManager,
    market: &MarketTracker,
    curve_states: &HashMap<String, BondingCurveState>,
    stale_exit_logged: &mut HashSet<String>,
    exit_curve_fetch_throttle: &mut HashMap<String, ExitQuoteThrottle>,
) -> Result<()> {
    let current_ms = now_ms();
    let open_mints = positions
        .positions()
        .filter(|position| positions.has_open_position(&position.mint))
        .map(|position| position.mint.clone())
        .collect::<Vec<_>>();

    for mint in open_mints {
        if cfg.mode == Mode::Live && pending_order_for_mint(pending_live_orders, &mint) {
            continue;
        }
        let Some(position) = positions.position_for_mint(&mint).cloned() else {
            continue;
        };
        if cfg.mode == Mode::Live
            && position.entry_confirmation_pending
            && position.last_update_ts_ms.is_some_and(|last_check_ms| {
                current_ms.saturating_sub(last_check_ms) < cfg.ambiguous_inventory_recheck_ms as i64
            })
        {
            continue;
        }
        let Some(entry_ms) = position.first_entry_ts_ms else {
            continue;
        };
        let hold_ms = current_ms.saturating_sub(entry_ms);
        let exit_policy = exit_policy_for_position(cfg, &position);
        let max_hold_ms = exit_policy.max_hold_seconds.saturating_mul(1_000);
        let max_hold_elapsed = hold_ms >= max_hold_ms;
        let force_unpriced_exit = hold_ms >= max_hold_ms.saturating_add(cfg.unpriced_exit_grace_ms);
        let mut quote = curve_states.get(&mint).and_then(|state| {
            causal_curve_exit_quote(
                state,
                position.entry_quote_slot,
                position.token_amount_raw,
                current_ms,
                cfg.market_quote_max_age_ms,
            )
        });
        if max_hold_elapsed
            && cfg.enable_curve_exit_quotes
            && exit_curve_fetch_allowed(exit_curve_fetch_throttle, &mint, current_ms)
        {
            match curve_quote_client.fetch_state(&mint).await {
                Ok(state) => {
                    record_exit_curve_fetch_success(exit_curve_fetch_throttle, &mint, current_ms);
                    let key = curve_state_key(&state);
                    journal.record_once(JournalKind::CurveState, &key, &state)?;
                    quote = causal_curve_exit_quote(
                        &state,
                        position.entry_quote_slot,
                        position.token_amount_raw,
                        current_ms,
                        cfg.market_quote_max_age_ms,
                    );
                    if let Some(curve_quote) = quote.as_ref() {
                        journal.record_once(
                            JournalKind::MarketQuote,
                            &curve_quote.signature,
                            curve_quote,
                        )?;
                    }
                }
                Err(err) => {
                    let cooldown_ms = record_exit_curve_fetch_error(
                        exit_curve_fetch_throttle,
                        &mint,
                        current_ms,
                        &err,
                    );
                    info!("curve exit quote unavailable mint={mint} cooldown_ms={cooldown_ms}: {err:#}");
                }
            }
        }
        if quote.is_none() {
            quote = market.latest_sell(&mint).and_then(|trade| {
                causal_trade_exit_quote(
                    trade,
                    position.entry_quote_slot,
                    position.token_amount_raw,
                    current_ms,
                    cfg.market_quote_max_age_ms,
                )
            });
        }
        let Some(quote) = quote.as_ref() else {
            if force_unpriced_exit {
                let decision = Decision {
                    id: format!("decision-exit-unpriced-{current_ms}-{}", mint_prefix(&mint)),
                    timestamp_ms: current_ms,
                    source_signature: None,
                    mint: Some(mint.clone()),
                    action: Action::Sell,
                    mode: cfg.mode,
                    reason_codes: vec![
                        "max_hold_unpriced_zero_exit".to_string(),
                        format!("hold_ms={hold_ms}"),
                        format!("unpriced_exit_grace_ms={}", cfg.unpriced_exit_grace_ms),
                    ],
                    requested_lamports: None,
                    risk_approved: false,
                    risk_veto_reason: None,
                };
                let snapshot = positions.snapshot_for_mint(Some(&mint));
                let decision = risk.apply(decision, &snapshot);
                journal.record(JournalKind::Decision, &decision)?;
                if let Some(order) = order_from_decision(&decision) {
                    journal.record(JournalKind::Order, &order)?;
                    if cfg.mode == Mode::Live {
                        submit_live_execution(
                            cfg,
                            live_executor.context("live executor unavailable")?,
                            live_executions,
                            pending_live_orders,
                            order,
                            (!cfg.live_single_lifecycle).then_some(position.token_amount_raw),
                            position.entry_quote_slot.unwrap_or_default(),
                            Some("max_hold_unpriced_exit".to_string()),
                        )?;
                        continue;
                    }
                    let report = executor.force_close_unpriced(
                        &order,
                        position.token_amount_raw,
                        paper_settings,
                    );
                    journal.record(JournalKind::Execution, &report)?;
                    if is_filled_execution(report.status) {
                        positions.record_order_report(&order, &report);
                        if let Some(position) = positions.position_for_mint(&mint) {
                            journal.record(JournalKind::Position, position)?;
                            info!(
                                "execution exit forced mint={} mode={:?} reason=max_hold_unpriced_exit state={:?} pnl_lamports={} hold_ms={}",
                                mint, cfg.mode, position.state, position.realized_lamports, hold_ms
                            );
                        }
                        strategy.mark_exit(&mint, current_ms, cfg.cooldown_seconds_per_mint);
                        stale_exit_logged.remove(&mint);
                        exit_curve_fetch_throttle.remove(&mint);
                    }
                }
                continue;
            }
            if max_hold_elapsed {
                record_stale_exit_once(
                    journal,
                    stale_exit_logged,
                    &mint,
                    current_ms,
                    "market_and_curve_quote_unavailable_at_exit",
                )?;
            }
            continue;
        };

        let preview_order = Order::Sell(SellOrder {
            id: format!("preview-sell-{current_ms}-{}", mint_prefix(&mint)),
            timestamp_ms: current_ms,
            mint: mint.clone(),
            source_decision_id: "paper-exit-preview".to_string(),
            source_signature: Some(quote.signature.clone()),
        });
        let preview = executor.execute_quote(
            &preview_order,
            quote,
            position.token_amount_raw,
            paper_settings,
        )?;
        if preview.status != ExecutionStatus::PaperFilled {
            continue;
        }
        let net_proceeds = preview
            .filled_lamports
            .unwrap_or_default()
            .saturating_sub(preview.fee_lamports.unwrap_or_default());
        let pnl_lamports = net_proceeds as i128 - position.entry_lamports as i128;
        let pnl_bps = if position.entry_lamports == 0 {
            0
        } else {
            (pnl_lamports.saturating_mul(10_000) / position.entry_lamports as i128)
                .clamp(i64::MIN as i128, i64::MAX as i128) as i64
        };

        let reason = if max_hold_elapsed {
            Some("max_hold_elapsed")
        } else if exit_policy.take_profit_enabled
            && !position.has_taken_profit
            && pnl_bps >= exit_policy.take_profit_bps
        {
            Some("take_profit_reached")
        } else if exit_policy.stop_loss_enabled && pnl_bps <= -exit_policy.stop_loss_bps {
            Some("stop_loss_reached")
        } else {
            None
        };
        let Some(reason) = reason else {
            continue;
        };
        let sell_token_amount_raw = if reason == "take_profit_reached" {
            position
                .token_amount_raw
                .saturating_mul(exit_policy.take_profit_sell_bps as u128)
                .checked_div(10_000)
                .unwrap_or_default()
                .max(1)
                .min(position.token_amount_raw)
        } else {
            position.token_amount_raw
        };

        let decision = Decision {
            id: format!("decision-exit-{current_ms}-{}", mint_prefix(&mint)),
            timestamp_ms: current_ms,
            source_signature: Some(quote.signature.clone()),
            mint: Some(mint.clone()),
            action: Action::Sell,
            mode: cfg.mode,
            reason_codes: vec![
                reason.to_string(),
                format!("hold_ms={hold_ms}"),
                format!("mark_pnl_bps={pnl_bps}"),
                format!("quote_age_ms={}", quote.age_ms(current_ms)),
            ],
            requested_lamports: None,
            risk_approved: false,
            risk_veto_reason: None,
        };
        let snapshot = positions.snapshot_for_mint(Some(&mint));
        let decision = risk.apply(decision, &snapshot);
        journal.record(JournalKind::Decision, &decision)?;
        let Some(order) = order_from_decision(&decision) else {
            continue;
        };
        journal.record(JournalKind::Order, &order)?;
        if cfg.mode == Mode::Live {
            submit_live_execution(
                cfg,
                live_executor.context("live executor unavailable")?,
                live_executions,
                pending_live_orders,
                order,
                (!cfg.live_single_lifecycle).then_some(sell_token_amount_raw),
                quote.slot,
                Some(reason.to_string()),
            )?;
            continue;
        }
        let report =
            executor.execute_quote(&order, quote, sell_token_amount_raw, paper_settings)?;
        journal.record(JournalKind::Execution, &report)?;
        if is_filled_execution(report.status) {
            positions.record_order_report(&order, &report);
            if let Some(position) = positions.position_for_mint(&mint) {
                journal.record(JournalKind::Position, position)?;
                info!(
                    "execution exit mint={} mode={:?} reason={} state={:?} pnl_lamports={} hold_ms={}",
                    mint, cfg.mode, reason, position.state, position.realized_lamports, hold_ms
                );
                if !positions.has_open_position(&mint) {
                    strategy.mark_exit(&mint, current_ms, cfg.cooldown_seconds_per_mint);
                }
            }
            stale_exit_logged.remove(&mint);
            exit_curve_fetch_throttle.remove(&mint);
        }
    }
    exit_curve_fetch_throttle.retain(|mint, _| positions.has_open_position(mint));

    Ok(())
}

#[derive(Debug, Clone, Copy)]
struct ExitPolicy {
    max_hold_seconds: i64,
    take_profit_enabled: bool,
    take_profit_bps: i64,
    take_profit_sell_bps: u32,
    stop_loss_enabled: bool,
    stop_loss_bps: i64,
}

fn exit_policy_for_position(cfg: &Config, position: &Position) -> ExitPolicy {
    if position.copy_trade_entry {
        ExitPolicy {
            max_hold_seconds: cfg.copy_trade_max_hold_seconds,
            take_profit_enabled: cfg.copy_trade_take_profit_bps > 0,
            take_profit_bps: cfg.copy_trade_take_profit_bps,
            take_profit_sell_bps: cfg.copy_trade_take_profit_sell_bps,
            stop_loss_enabled: cfg.copy_trade_stop_loss_bps > 0,
            stop_loss_bps: cfg.copy_trade_stop_loss_bps,
        }
    } else {
        ExitPolicy {
            max_hold_seconds: cfg.max_hold_seconds,
            take_profit_enabled: cfg.enable_take_profit_exit,
            take_profit_bps: cfg.take_profit_bps,
            take_profit_sell_bps: cfg.take_profit_sell_bps,
            stop_loss_enabled: cfg.enable_stop_loss_exit,
            stop_loss_bps: cfg.stop_loss_bps,
        }
    }
}

fn accept_fresh_stream_event(
    journal: &Journal,
    event: &StreamEvent,
    received_at_ms: i64,
    max_age_ms: i64,
    stage: &str,
    stale_stream_events: &mut u64,
    stale_log_limiter: &mut StaleLogLimiter,
) -> Result<bool> {
    let age_ms = now_ms().saturating_sub(received_at_ms).max(0);
    if age_ms <= max_age_ms {
        return Ok(true);
    }

    *stale_stream_events = stale_stream_events.saturating_add(1);
    let metric = StreamFreshnessMetric {
        timestamp_ms: now_ms(),
        signature: event.signature.clone(),
        source: event.source.clone(),
        slot: event.slot,
        received_at_ms,
        age_ms,
        max_age_ms,
        stage: stage.to_string(),
        accepted: false,
    };
    journal.record(JournalKind::StreamFreshness, &metric)?;
    if let Some(message) = stale_log_limiter.stream_message(stage, age_ms, max_age_ms) {
        info!("{message}");
    } else {
        debug!(
            "stale stream event rejected signature={} source={} slot={} age_ms={} max_age_ms={} stage={}",
            event.signature, event.source, event.slot, age_ms, max_age_ms, stage
        );
    }
    Ok(false)
}

async fn accept_fresh_create_event_slot(
    journal: &Journal,
    tx_fetcher: &TxFetcher,
    create_slot_cache: &mut SlotCache,
    event: &StreamEvent,
    cfg: &Config,
    stale_log_limiter: &mut StaleLogLimiter,
) -> Result<bool> {
    if event.slot == 0 {
        record_create_slot_freshness(
            journal,
            event,
            0,
            None,
            cfg.max_create_event_slot_lag,
            false,
        )?;
        if let Some(message) = stale_log_limiter.create_message(
            "missing_event_slot",
            None,
            cfg.max_create_event_slot_lag,
        ) {
            info!("{message}");
        } else {
            debug!(
                "stale create event rejected signature={} source={} slot=0 reason=missing_event_slot",
                event.signature, event.source
            );
        }
        return Ok(false);
    }

    let now = now_ms();
    let current_slot = if let Some(slot) = create_slot_cache.fresh(now) {
        slot
    } else {
        match tx_fetcher.get_slot().await {
            Ok(slot) => {
                create_slot_cache.store(slot, now_ms());
                slot
            }
            Err(err) => {
                record_create_slot_freshness(
                    journal,
                    event,
                    0,
                    None,
                    cfg.max_create_event_slot_lag,
                    false,
                )?;
                if let Some(message) = stale_log_limiter.create_message(
                    "current_slot_unavailable",
                    None,
                    cfg.max_create_event_slot_lag,
                ) {
                    info!("{message}");
                } else {
                    debug!(
                        "stale create event rejected signature={} source={} slot={} reason=current_slot_unavailable error={err:#}",
                        event.signature, event.source, event.slot
                    );
                }
                return Ok(false);
            }
        }
    };

    let slot_lag = current_slot.saturating_sub(event.slot);
    let accepted = current_slot < event.slot || slot_lag <= cfg.max_create_event_slot_lag;
    if !accepted {
        record_create_slot_freshness(
            journal,
            event,
            current_slot,
            Some(slot_lag),
            cfg.max_create_event_slot_lag,
            false,
        )?;
        if let Some(message) = stale_log_limiter.create_message(
            "slot_lag_exceeded",
            Some(slot_lag),
            cfg.max_create_event_slot_lag,
        ) {
            info!("{message}");
        } else {
            debug!(
                "stale create event rejected signature={} source={} event_slot={} current_slot={} slot_lag={} max_slot_lag={}",
                event.signature,
                event.source,
                event.slot,
                current_slot,
                slot_lag,
                cfg.max_create_event_slot_lag
            );
        }
    }
    Ok(accepted)
}

fn record_create_slot_freshness(
    journal: &Journal,
    event: &StreamEvent,
    current_slot: u64,
    slot_lag: Option<u64>,
    max_slot_lag: u64,
    accepted: bool,
) -> Result<()> {
    let metric = CreateSlotFreshnessMetric {
        timestamp_ms: now_ms(),
        signature: event.signature.clone(),
        source: event.source.clone(),
        event_slot: event.slot,
        current_slot,
        slot_lag,
        max_slot_lag,
        accepted,
    };
    journal.record(JournalKind::StreamFreshness, &metric)
}

#[derive(Debug, Serialize)]
struct StreamFreshnessMetric {
    timestamp_ms: i64,
    signature: String,
    source: String,
    slot: u64,
    received_at_ms: i64,
    age_ms: i64,
    max_age_ms: i64,
    stage: String,
    accepted: bool,
}

#[derive(Debug, Serialize)]
struct CreateSlotFreshnessMetric {
    timestamp_ms: i64,
    signature: String,
    source: String,
    event_slot: u64,
    current_slot: u64,
    slot_lag: Option<u64>,
    max_slot_lag: u64,
    accepted: bool,
}

fn pending_order_for_mint(
    pending_live_orders: &HashMap<String, PendingLiveOrder>,
    mint: &str,
) -> bool {
    pending_live_orders
        .values()
        .any(|pending| pending.order.mint() == mint)
}

fn pending_buy_count(pending_live_orders: &HashMap<String, PendingLiveOrder>) -> usize {
    pending_live_orders
        .values()
        .filter(|pending| matches!(&pending.order, Order::Buy(_)))
        .count()
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ActiveLiveLifecycle {
    mint: String,
    reason: &'static str,
}

fn early_single_lifecycle_decision(
    cfg: &Config,
    event: &StreamEvent,
    timestamp_ms: i64,
    positions: &PositionManager,
    pending_live_orders: &HashMap<String, PendingLiveOrder>,
) -> Option<Decision> {
    let active = active_live_lifecycle(cfg, positions, pending_live_orders)?;
    let trade = extract_pump_trade_observation(&event.logs);
    let create_mint = extract_pump_create_event_mint(&event.logs);
    let mint = trade
        .as_ref()
        .map(|trade| trade.mint.clone())
        .or(create_mint);
    let relevant_sell_for_active = trade
        .as_ref()
        .is_some_and(|trade| !trade.is_buy && trade.mint == active.mint);
    if relevant_sell_for_active {
        return None;
    }
    let relevant_agent_buy_exit_for_active = cfg.exit_on_observed_agent_buy
        && active.reason == "open_position"
        && is_mayhem_agent_stream_event(cfg, event)
        && trade
            .as_ref()
            .is_some_and(|trade| trade.is_buy && trade.mint == active.mint);
    if relevant_agent_buy_exit_for_active {
        return None;
    }
    let instruction_names = extract_instruction_names(&event.logs);
    let is_buy_or_create = trade.as_ref().is_some_and(|trade| trade.is_buy)
        || logs_have_pump_create_signal(&event.logs)
        || instruction_names.iter().any(|name| name.starts_with("Buy"));
    let is_other_sell = trade
        .as_ref()
        .is_some_and(|trade| !trade.is_buy && trade.mint != active.mint);
    if !is_buy_or_create && !is_other_sell {
        return None;
    }

    Some(Decision {
        id: format!(
            "decision-single-lifecycle-early-{}-{}",
            timestamp_ms,
            mint.as_deref()
                .map(mint_prefix)
                .unwrap_or_else(|| "unknown".to_string())
        ),
        timestamp_ms,
        source_signature: Some(event.signature.clone()),
        mint,
        action: Action::Ignore,
        mode: cfg.mode,
        reason_codes: vec![
            "live_single_lifecycle_active".to_string(),
            "early_stream_rejection".to_string(),
            format!("active_mint={}", active.mint),
            format!("active_reason={}", active.reason),
        ],
        requested_lamports: None,
        risk_approved: false,
        risk_veto_reason: None,
    })
}

fn copy_trade_decision(
    cfg: &Config,
    event: &StreamEvent,
    decoded: &DecodedTx,
    classification: &TokenClassification,
    context: StrategyContext,
) -> Option<Decision> {
    let wallet = cfg.copy_trade_wallet()?;
    if !is_copy_trade_event(wallet, event, decoded) {
        return None;
    }

    let timestamp_ms = decoded.timestamp_ms.unwrap_or_else(now_ms);
    let mint = decoded.mint.clone();
    let id = format!(
        "decision-copy-{}-{}",
        timestamp_ms,
        mint.as_deref()
            .map(mint_prefix)
            .unwrap_or_else(|| "unknown".to_string())
    );
    let ignore = |reason: &str| Decision {
        id: id.clone(),
        timestamp_ms,
        source_signature: Some(decoded.signature.clone()),
        mint: mint.clone(),
        action: Action::Ignore,
        mode: cfg.mode,
        reason_codes: vec![reason.to_string()],
        requested_lamports: None,
        risk_approved: false,
        risk_veto_reason: None,
    };

    if !decoded.ok {
        return Some(ignore("copy_trade_source_tx_failed"));
    }
    let Some(mint_value) = mint.clone() else {
        return Some(ignore("copy_trade_no_mint_decoded"));
    };
    let route_supported = classification.is_pumpfun_bonding_curve
        || (cfg.copy_trade_allow_pumpswap && classification.is_pumpswap && cfg.mode == Mode::Paper);
    if !route_supported {
        return Some(ignore("copy_trade_route_unsupported"));
    }

    if decoded.side.is_sell() {
        if cfg.copy_trade_follow_sells && context.has_position_for_mint {
            return Some(Decision {
                id,
                timestamp_ms,
                source_signature: Some(decoded.signature.clone()),
                mint: Some(mint_value),
                action: Action::Sell,
                mode: cfg.mode,
                reason_codes: vec![
                    "copy_trade_source_sell".to_string(),
                    format!("copy_wallet={}", mint_prefix(wallet)),
                ],
                requested_lamports: None,
                risk_approved: false,
                risk_veto_reason: None,
            });
        }
        return Some(ignore("copy_trade_sell_without_open_position"));
    }

    if !decoded.side.is_buy() {
        return Some(ignore("copy_trade_not_buy_or_sell"));
    }

    let mayhem_detected = classification.is_mayhem_candidate
        || classification.is_mayhem_direct
        || classification.has_verified_mayhem_evidence;
    let mayhem_allowed = classification.has_verified_mayhem_evidence
        || (cfg.allow_indirect_mayhem_candidates && classification.is_mayhem_candidate);
    match cfg.market {
        Market::NonMayhemOnly if mayhem_detected => {
            return Some(ignore("copy_trade_non_mayhem_market_only"));
        }
        Market::MayhemOnly if !mayhem_allowed => {
            return Some(ignore("copy_trade_mayhem_evidence_required"));
        }
        _ => {}
    }

    let observed_lamports = decoded
        .sol_delta_lamports
        .filter(|delta| *delta < 0)
        .and_then(|delta| delta.checked_abs())
        .map(|value| value as u64);

    if cfg.copy_trade_min_source_buy_lamports > 0 {
        let Some(observed) = observed_lamports else {
            return Some(ignore("copy_trade_source_buy_size_unknown"));
        };
        if observed < cfg.copy_trade_min_source_buy_lamports {
            return Some(ignore("copy_trade_source_buy_too_small"));
        }
    }

    match cfg.copy_trade_buy_policy {
        CopyTradeBuyPolicy::FirstOnly
            if context.buys_for_mint > 0 || context.has_position_for_mint =>
        {
            return Some(ignore("copy_trade_first_buy_only"));
        }
        CopyTradeBuyPolicy::Accumulate
            if context.buys_for_mint >= cfg.copy_trade_max_buys_per_mint =>
        {
            return Some(ignore("copy_trade_max_buys_per_mint"));
        }
        _ => {}
    }

    let requested_lamports = copy_trade_buy_lamports(cfg, observed_lamports);
    if requested_lamports == 0 {
        return Some(ignore("copy_trade_zero_buy_size"));
    }

    let mut reasons = vec![
        "copy_trade_source_buy".to_string(),
        format!("copy_wallet={}", mint_prefix(wallet)),
        format!("copy_sizing={}", cfg.copy_trade_sizing.as_str()),
        format!("copy_buy_policy={}", cfg.copy_trade_buy_policy.as_str()),
        format!("copy_max_lamports={}", cfg.copy_trade_max_buy_lamports),
    ];
    if let Some(observed) = observed_lamports {
        reasons.push(format!("observed_lamports={observed}"));
    }
    if requested_lamports == cfg.copy_trade_max_buy_lamports {
        reasons.push("copy_size_capped".to_string());
    }

    Some(Decision {
        id,
        timestamp_ms,
        source_signature: Some(decoded.signature.clone()),
        mint: Some(mint_value),
        action: Action::Buy,
        mode: cfg.mode,
        reason_codes: reasons,
        requested_lamports: Some(requested_lamports),
        risk_approved: false,
        risk_veto_reason: None,
    })
}

fn copy_trade_buy_lamports(cfg: &Config, observed_lamports: Option<u64>) -> u64 {
    let base = match cfg.copy_trade_sizing {
        CopyTradeSizing::Fixed => cfg.base_buy_lamports,
        CopyTradeSizing::Mirror => observed_lamports.unwrap_or(cfg.base_buy_lamports),
        CopyTradeSizing::Scaled => {
            observed_lamports
                .unwrap_or(cfg.base_buy_lamports)
                .saturating_mul(cfg.copy_trade_scale_bps as u64)
                / 10_000
        }
    };
    base.min(cfg.copy_trade_max_buy_lamports)
}

fn is_copy_trade_stream_source(cfg: &Config, event: &StreamEvent) -> bool {
    let Some(wallet) = cfg.copy_trade_wallet() else {
        return false;
    };
    event
        .source
        .strip_prefix("logsSubscribe:")
        .or_else(|| event.source.strip_prefix("backfill:"))
        .is_some_and(|mention| mention == wallet)
}

fn wallet_for_delta_for_event<'a>(
    cfg: &'a Config,
    event: &'a StreamEvent,
    default_wallet: Option<&'a str>,
) -> Option<&'a str> {
    if is_copy_trade_stream_source(cfg, event) {
        return cfg.copy_trade_wallet();
    }
    event
        .source
        .strip_prefix("logsSubscribe:")
        .or_else(|| event.source.strip_prefix("backfill:"))
        .filter(|wallet| {
            cfg.target_wallet.as_deref() == Some(*wallet)
                || cfg.watched_wallets.iter().any(|watched| watched == wallet)
        })
        .or(default_wallet)
}

fn is_copy_trade_event(wallet: &str, event: &StreamEvent, decoded: &DecodedTx) -> bool {
    event
        .source
        .strip_prefix("logsSubscribe:")
        .or_else(|| event.source.strip_prefix("backfill:"))
        .is_some_and(|mention| mention == wallet)
        || decoded.signer.as_deref() == Some(wallet)
}

fn observed_agent_buy_exit_decision(
    cfg: &Config,
    event: &StreamEvent,
    decoded: &catarnith::types::DecodedTx,
    positions: &PositionManager,
    timestamp_ms: i64,
) -> Option<Decision> {
    if !cfg.exit_on_observed_agent_buy || !decoded.ok || !decoded.side.is_buy() {
        return None;
    }
    if !is_mayhem_agent_event(cfg, event, decoded) {
        return None;
    }
    let mint = decoded.mint.as_ref()?;
    if !positions.has_open_position(mint) {
        return None;
    }
    let position = positions.position_for_mint(mint)?;
    let entry_ms = position.first_entry_ts_ms?;
    let signal_ms = decoded.timestamp_ms.unwrap_or(timestamp_ms);
    if signal_ms <= entry_ms {
        return None;
    }
    let hold_ms = timestamp_ms.saturating_sub(entry_ms);
    let mut reason_codes = vec![
        "observed_agent_buy_after_entry".to_string(),
        format!("hold_ms={hold_ms}"),
        format!("signal_slot={}", decoded.slot),
    ];
    if let Some(lamports) = decoded
        .sol_delta_lamports
        .filter(|delta| *delta < 0)
        .and_then(|delta| delta.checked_abs())
    {
        reason_codes.push(format!("agent_buy_lamports={lamports}"));
    }

    Some(Decision {
        id: format!(
            "decision-exit-agent-buy-{timestamp_ms}-{}",
            mint_prefix(mint)
        ),
        timestamp_ms,
        source_signature: Some(decoded.signature.clone()),
        mint: Some(mint.clone()),
        action: Action::Sell,
        mode: cfg.mode,
        reason_codes,
        requested_lamports: None,
        risk_approved: false,
        risk_veto_reason: None,
    })
}

fn is_mayhem_agent_event(
    cfg: &Config,
    event: &StreamEvent,
    decoded: &catarnith::types::DecodedTx,
) -> bool {
    is_mayhem_agent_stream_event(cfg, event)
        || decoded
            .account_keys
            .iter()
            .any(|account| account == &cfg.mayhem_agent_wallet)
        || decoded.signer.as_deref() == Some(cfg.mayhem_agent_wallet.as_str())
}

fn is_mayhem_agent_stream_event(cfg: &Config, event: &StreamEvent) -> bool {
    event
        .source
        .strip_prefix("logsSubscribe:")
        .is_some_and(|mention| mention == cfg.mayhem_agent_wallet)
}

fn active_live_lifecycle(
    cfg: &Config,
    positions: &PositionManager,
    pending_live_orders: &HashMap<String, PendingLiveOrder>,
) -> Option<ActiveLiveLifecycle> {
    if cfg.mode != Mode::Live || !cfg.live_single_lifecycle {
        return None;
    }
    if let Some(pending) = pending_live_orders.values().next() {
        return Some(ActiveLiveLifecycle {
            mint: pending.order.mint().to_string(),
            reason: match &pending.order {
                Order::Buy(_) => "pending_buy",
                Order::Sell(_) => "pending_sell",
            },
        });
    }
    positions
        .positions()
        .find(|position| positions.has_open_position(&position.mint))
        .map(|position| ActiveLiveLifecycle {
            mint: position.mint.clone(),
            reason: "open_position",
        })
}

fn apply_pending_live_risk(
    snapshot: &mut RiskSnapshot,
    mint: Option<&str>,
    pending_live_orders: &HashMap<String, PendingLiveOrder>,
) {
    for pending in pending_live_orders.values() {
        let Order::Buy(buy) = &pending.order else {
            continue;
        };
        snapshot.open_positions = snapshot.open_positions.saturating_add(1);
        snapshot.total_open_lamports = snapshot.total_open_lamports.saturating_add(buy.lamports);
        if mint == Some(buy.mint.as_str()) {
            snapshot.exposure_for_mint = snapshot.exposure_for_mint.saturating_add(buy.lamports);
            snapshot.buys_for_mint = snapshot.buys_for_mint.saturating_add(1);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn submit_live_execution(
    cfg: &Config,
    live_executor: &Arc<LivePumpExecutor>,
    live_executions: &mut FuturesUnordered<LiveExecutionFuture>,
    pending_live_orders: &mut HashMap<String, PendingLiveOrder>,
    order: Order,
    sell_token_amount_raw: Option<u128>,
    source_slot: u64,
    exit_reason: Option<String>,
) -> Result<()> {
    let entry_origin_ts_ms = order_timestamp_ms(&order);
    submit_live_execution_attempt(
        cfg,
        live_executor,
        live_executions,
        pending_live_orders,
        order,
        sell_token_amount_raw,
        None,
        source_slot,
        exit_reason,
        0,
        entry_origin_ts_ms,
    )
}

#[allow(clippy::too_many_arguments)]
fn submit_live_execution_attempt(
    cfg: &Config,
    live_executor: &Arc<LivePumpExecutor>,
    live_executions: &mut FuturesUnordered<LiveExecutionFuture>,
    pending_live_orders: &mut HashMap<String, PendingLiveOrder>,
    order: Order,
    sell_token_amount_raw: Option<u128>,
    buy_slippage_bps: Option<u32>,
    source_slot: u64,
    exit_reason: Option<String>,
    attempt: u32,
    entry_origin_ts_ms: i64,
) -> Result<()> {
    validate_live_submission_slot(cfg, pending_live_orders, &order)?;

    let order_id = order.id().to_string();
    let pending = PendingLiveOrder {
        order: order.clone(),
        source_slot,
        submitted_at_ms: now_ms(),
        exit_reason,
        attempt,
        entry_origin_ts_ms,
    };
    pending_live_orders.insert(order_id.clone(), pending);

    let executor = Arc::clone(live_executor);
    live_executions.push(Box::pin(async move {
        let result = executor
            .execute(&order, sell_token_amount_raw, buy_slippage_bps)
            .await
            .map_err(|err| format!("{err:#}"));
        LiveExecutionCompletion { order_id, result }
    }));
    Ok(())
}

fn validate_live_submission_slot(
    cfg: &Config,
    pending_live_orders: &HashMap<String, PendingLiveOrder>,
    order: &Order,
) -> Result<()> {
    if pending_order_for_mint(pending_live_orders, order.mint()) {
        anyhow::bail!("live order already pending for mint {}", order.mint());
    }
    if cfg.live_single_lifecycle {
        if let Some(pending) = pending_live_orders.values().next() {
            anyhow::bail!(
                "live single lifecycle already has pending order {} for mint {}",
                pending.order.id(),
                pending.order.mint()
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn process_live_execution_completion(
    cfg: &Config,
    journal: &Journal,
    live_executor: Option<&Arc<LivePumpExecutor>>,
    live_executions: &mut FuturesUnordered<LiveExecutionFuture>,
    curve_quote_client: &CurveQuoteClient,
    strategy: &mut BurstStrategy,
    positions: &mut PositionManager,
    discoveries: &DiscoveryRegistry,
    market: &MarketTracker,
    curve_state_tx: &mpsc::Sender<BondingCurveState>,
    curve_watches: &mut HashMap<String, CurveWatch>,
    curve_states: &HashMap<String, BondingCurveState>,
    stale_exit_logged: &mut HashSet<String>,
    exit_curve_fetch_throttle: &mut HashMap<String, ExitQuoteThrottle>,
    pending_live_orders: &mut HashMap<String, PendingLiveOrder>,
    completion: LiveExecutionCompletion,
) -> Result<()> {
    let Some(pending) = pending_live_orders.remove(&completion.order_id) else {
        warn!(
            "live execution completion had no pending order order_id={}",
            completion.order_id
        );
        return Ok(());
    };
    let PendingLiveOrder {
        order,
        source_slot,
        submitted_at_ms,
        exit_reason,
        attempt,
        entry_origin_ts_ms,
    } = pending;
    let mut report = completion.result.unwrap_or_else(|error| ExecutionReport {
        order_id: order.id().to_string(),
        signature: None,
        quote_slot: None,
        status: ExecutionStatus::Errored,
        requested_lamports: match &order {
            Order::Buy(buy) => buy.lamports,
            Order::Sell(_) => 0,
        },
        filled_lamports: None,
        filled_token_amount_raw: None,
        fee_lamports: None,
        error: Some(error),
        latency_ms: Some(now_ms().saturating_sub(submitted_at_ms).max(0) as u64),
    });
    if let Some(error) = report.error.as_mut() {
        *error = redact_live_error(error, &cfg.helius_api_key);
    }
    let pending_inventory_sell = matches!(
        (&order, report.status),
        (Order::Sell(_), ExecutionStatus::LiveReconciled)
    ) && zero_inventory_without_sell_broadcast(&report)
        && positions
            .position_for_mint(order.mint())
            .is_some_and(|position| {
                position.entry_confirmation_pending
                    && position.first_entry_ts_ms.is_some_and(|entered_at| {
                        now_ms().saturating_sub(entered_at) < cfg.ambiguous_entry_expiry_ms
                    })
            });
    let provisional_buy_failure = if pending_inventory_sell {
        pending_provisional_buy_failure(live_executor, positions, order.mint()).await
    } else {
        None
    };
    let pending_inventory_sell = pending_inventory_sell && provisional_buy_failure.is_none();
    if pending_inventory_sell {
        report.status = ExecutionStatus::LiveSubmitted;
        report.error = Some(format!(
            "ambiguous_buy_inventory_not_visible; liquidation will retry until {}ms",
            cfg.ambiguous_entry_expiry_ms
        ));
    }
    let expired_provisional_buy = matches!(
        (&order, report.status),
        (Order::Sell(_), ExecutionStatus::LiveReconciled)
    ) && report.signature.is_none()
        && report.filled_lamports.unwrap_or_default() == 0
        && positions
            .position_for_mint(order.mint())
            .is_some_and(|position| {
                position.entry_confirmation_pending
                    && position.first_entry_ts_ms.is_some_and(|entered_at| {
                        now_ms().saturating_sub(entered_at) >= cfg.ambiguous_entry_expiry_ms
                    })
            });
    let unlanded_provisional_buy = provisional_buy_failure.is_some() || expired_provisional_buy;
    if unlanded_provisional_buy {
        report.error = Some(provisional_buy_failure.unwrap_or_else(|| {
            "ambiguous_buy_not_landed_after_expiry; provisional position closed without \
             realized trade loss"
                .to_string()
        }));
    }
    let external_inventory_depletion = matches!(
        (&order, report.status),
        (Order::Sell(_), ExecutionStatus::LiveReconciled)
    ) && report.signature.is_none()
        && report.filled_lamports.unwrap_or_default() == 0
        && positions
            .position_for_mint(order.mint())
            .is_some_and(|position| !position.entry_confirmation_pending);
    if external_inventory_depletion {
        report.error = Some(
            "inventory_already_zero_before_sell_broadcast; external_or_prior_sell_reconciled"
                .to_string(),
        );
    }
    journal.record(JournalKind::Execution, &report)?;

    let submitted_buy = matches!(
        (&order, report.status),
        (Order::Buy(_), ExecutionStatus::LiveSubmitted)
    );
    if should_retry_buy_slippage(cfg, &order, &report, attempt, entry_origin_ts_ms) {
        let retry_attempt = attempt.saturating_add(1);
        let retry_slippage_bps = retry_buy_slippage_bps(cfg, retry_attempt);
        let retry_order = refreshed_retry_buy_order(&order, retry_attempt);
        journal.record(JournalKind::Order, &retry_order)?;
        info!(
            "live buy slippage retry mint={} previous_signature={:?} attempt={}/{} slippage_bps={} age_ms={} max_age_ms={}",
            order.mint(),
            report.signature,
            retry_attempt,
            cfg.buy_slippage_retry_attempts,
            retry_slippage_bps,
            now_ms().saturating_sub(entry_origin_ts_ms),
            cfg.buy_slippage_retry_deadline_ms
        );
        submit_live_execution_attempt(
            cfg,
            live_executor.context("live executor unavailable for slippage retry")?,
            live_executions,
            pending_live_orders,
            retry_order,
            None,
            Some(retry_slippage_bps),
            source_slot,
            None,
            retry_attempt,
            entry_origin_ts_ms,
        )?;
        return Ok(());
    }
    if pending_inventory_sell {
        positions.defer_pending_inventory_recheck(order.mint(), now_ms());
        info!(
            "live sell deferred mint={} reason=ambiguous_buy_inventory_not_visible; retrying",
            order.mint()
        );
        return Ok(());
    }
    if !is_filled_execution(report.status) && !submitted_buy {
        warn!(
            "live execution failed mint={} order_id={} reason={}",
            order.mint(),
            order.id(),
            report.error.as_deref().unwrap_or("unknown")
        );
        return Ok(());
    }

    let position_before = positions.position_for_mint(order.mint()).cloned();
    if unlanded_provisional_buy {
        let Order::Sell(sell) = &order else {
            unreachable!("unlanded provisional reconciliation is sell-only");
        };
        if !positions.reconcile_unlanded_provisional_buy(sell) {
            warn!(
                "provisional buy reconciliation skipped mint={} reason=position_not_pending",
                sell.mint
            );
            positions.record_order_report(&order, &report);
        }
    } else if external_inventory_depletion {
        let Order::Sell(sell) = &order else {
            unreachable!("external inventory reconciliation is sell-only");
        };
        if !positions.reconcile_external_inventory_depletion(sell) {
            warn!(
                "external inventory reconciliation skipped mint={} reason=position_not_confirmed",
                sell.mint
            );
            positions.record_order_report(&order, &report);
        }
    } else {
        positions.record_order_report(&order, &report);
    }
    if let Some(position) = positions.position_for_mint(order.mint()) {
        journal.record(JournalKind::Position, position)?;
    }

    match &order {
        Order::Buy(buy) => {
            if report.status == ExecutionStatus::LiveSubmitted {
                info!(
                    "live buy submitted but not yet confirmed mint={} signature={:?}; \
                     provisional position will be liquidated on schedule",
                    buy.mint, report.signature
                );
            }
            let watch_until_ms = buy
                .timestamp_ms
                .saturating_add(cfg.curve_observation_seconds.saturating_mul(1_000));
            if cfg.enable_curve_exit_quotes {
                ensure_curve_watch(
                    cfg,
                    curve_quote_client,
                    curve_state_tx,
                    curve_watches,
                    &buy.mint,
                    watch_until_ms,
                )?;
            }
            if let Some(discovery) = discoveries.get(&buy.mint) {
                let features = EntryFeatures::build(
                    buy.mint.clone(),
                    buy.id.clone(),
                    buy.source_signature.clone(),
                    discovery.source.clone(),
                    discovery.seen_ts_ms,
                    source_slot,
                    buy.timestamp_ms,
                    report.filled_lamports.unwrap_or(buy.lamports),
                    report.filled_token_amount_raw.unwrap_or_default(),
                    report.fee_lamports.unwrap_or_default(),
                    curve_states.get(&buy.mint),
                    market.stats(&buy.mint),
                );
                journal.record_once(JournalKind::EntryFeatures, &buy.id, &features)?;
            }
            if report.status == ExecutionStatus::LiveSubmitted {
                info!(
                    "live buy pending confirmation mint={} signature={:?} execution_latency_ms={:?}",
                    buy.mint, report.signature, report.latency_ms
                );
            } else {
                info!(
                    "live buy confirmed mint={} signature={:?} execution_latency_ms={:?}",
                    buy.mint, report.signature, report.latency_ms
                );
            }
        }
        Order::Sell(sell) => {
            let closed = !positions.has_open_position(&sell.mint);
            if closed {
                let completed_at_ms = now_ms();
                let position_after = positions.position_for_mint(&sell.mint);
                let discovery = discoveries.get(&sell.mint);
                let lifecycle = LiveLifecycleReport {
                    cycle_id: format!(
                        "cycle-{}-{}",
                        position_before
                            .as_ref()
                            .and_then(|position| position.first_entry_ts_ms)
                            .unwrap_or(completed_at_ms),
                        mint_prefix(&sell.mint)
                    ),
                    mint: sell.mint.clone(),
                    discovery_source: discovery.map(|signal| signal.source.clone()),
                    discovery_seen_ts_ms: discovery.map(|signal| signal.seen_ts_ms),
                    buy_order_id: position_before
                        .as_ref()
                        .and_then(|position| position.entry_order_ids.first().cloned()),
                    buy_signature: position_before
                        .as_ref()
                        .and_then(|position| position.entry_signature.clone()),
                    entry_ts_ms: position_before
                        .as_ref()
                        .and_then(|position| position.first_entry_ts_ms),
                    entry_lamports: position_before
                        .as_ref()
                        .map(|position| position.entry_lamports)
                        .unwrap_or_default(),
                    filled_token_amount_raw: report
                        .filled_token_amount_raw
                        .or_else(|| {
                            position_before
                                .as_ref()
                                .map(|position| position.token_amount_raw)
                        })
                        .unwrap_or_default(),
                    sell_order_id: sell.id.clone(),
                    sell_signature: report.signature.clone(),
                    sell_status: report.status,
                    sell_confirmed_ts_ms: completed_at_ms,
                    exit_reason: if unlanded_provisional_buy {
                        "ambiguous_buy_not_landed".to_string()
                    } else if external_inventory_depletion {
                        "inventory_already_zero_external_or_prior_sell".to_string()
                    } else {
                        exit_reason
                            .clone()
                            .unwrap_or_else(|| "observed_agent_sell".to_string())
                    },
                    hold_ms: position_before
                        .as_ref()
                        .and_then(|position| position.first_entry_ts_ms)
                        .map(|entered_at| completed_at_ms.saturating_sub(entered_at)),
                    realized_lamports: position_after
                        .map(|position| position.realized_lamports)
                        .unwrap_or_default(),
                    final_position_state: position_after
                        .map(|position| format!("{:?}", position.state).to_lowercase())
                        .unwrap_or_else(|| "missing".to_string()),
                    cycle_complete: pending_live_orders.is_empty()
                        && positions.open_positions() == 0,
                };
                journal.record(JournalKind::LiveLifecycle, &lifecycle)?;
            }
            if closed {
                strategy.mark_exit(&sell.mint, now_ms(), cfg.cooldown_seconds_per_mint);
                refresh_live_reports(cfg);
            }
            stale_exit_logged.remove(&sell.mint);
            exit_curve_fetch_throttle.remove(&sell.mint);
            if unlanded_provisional_buy {
                info!(
                    "provisional live buy expired without landing mint={} closed={} \
                     execution_latency_ms={:?}",
                    sell.mint, closed, report.latency_ms
                );
            } else if external_inventory_depletion {
                warn!(
                    "live inventory was already zero before sell broadcast mint={} closed={} \
                     accounting=unknown external_wallet_activity_possible",
                    sell.mint, closed
                );
            } else {
                info!(
                    "live sell confirmed mint={} reason={} signature={:?} closed={} execution_latency_ms={:?}",
                    sell.mint,
                    exit_reason.as_deref().unwrap_or("observed_agent_sell"),
                    report.signature,
                    closed,
                    report.latency_ms
                );
            }
        }
    }
    Ok(())
}

fn should_retry_buy_slippage(
    cfg: &Config,
    order: &Order,
    report: &ExecutionReport,
    attempt: u32,
    entry_origin_ts_ms: i64,
) -> bool {
    let is_slippage_error = report.error.as_deref().is_some_and(|error| {
        error.contains("Custom(6042)") || error.contains("BuySlippageBelowMinTokensOut")
    });
    let retryable_status = matches!(
        report.status,
        ExecutionStatus::LiveFailed | ExecutionStatus::Errored
    );
    matches!(order, Order::Buy(_))
        && retryable_status
        && is_slippage_error
        && attempt < cfg.buy_slippage_retry_attempts
        && now_ms().saturating_sub(entry_origin_ts_ms) <= cfg.buy_slippage_retry_deadline_ms
}

fn retry_buy_slippage_bps(cfg: &Config, retry_attempt: u32) -> u32 {
    let base = cfg.max_slippage_bps;
    let step = cfg.buy_slippage_retry_step_bps;
    let escalated = base.saturating_add(step.saturating_mul(retry_attempt));
    escalated.min(cfg.buy_slippage_retry_max_bps).min(9_999)
}

async fn pending_provisional_buy_failure(
    live_executor: Option<&Arc<LivePumpExecutor>>,
    positions: &PositionManager,
    mint: &str,
) -> Option<String> {
    let signature = positions
        .position_for_mint(mint)
        .and_then(|position| position.entry_signature.clone())?;
    let live_executor = live_executor?;
    match live_executor
        .finalized_failure_for_signature(&signature)
        .await
    {
        Ok(Some(error)) => Some(format!("provisional_buy_failed_on_chain:{error}")),
        Ok(None) => None,
        Err(error) => {
            info!(
                "provisional buy signature status unavailable mint={} signature={} reason={:#}",
                mint, signature, error
            );
            None
        }
    }
}

fn refreshed_retry_buy_order(order: &Order, retry_attempt: u32) -> Order {
    let mut order = order.clone();
    if let Order::Buy(buy) = &mut order {
        buy.id = format!("{}-retry{retry_attempt}", buy.id);
        buy.timestamp_ms = now_ms();
    }
    order
}

fn order_timestamp_ms(order: &Order) -> i64 {
    match order {
        Order::Buy(order) => order.timestamp_ms,
        Order::Sell(order) => order.timestamp_ms,
    }
}

struct CurveWatch {
    handle: JoinHandle<()>,
    watch_until_ms: i64,
}

#[derive(Debug, Default)]
struct ExitQuoteThrottle {
    next_allowed_ms: i64,
    consecutive_failures: u32,
}

fn exit_curve_fetch_allowed(
    throttles: &HashMap<String, ExitQuoteThrottle>,
    mint: &str,
    current_ms: i64,
) -> bool {
    throttles
        .get(mint)
        .is_none_or(|throttle| current_ms >= throttle.next_allowed_ms)
}

fn record_exit_curve_fetch_success(
    throttles: &mut HashMap<String, ExitQuoteThrottle>,
    mint: &str,
    current_ms: i64,
) {
    throttles.insert(
        mint.to_string(),
        ExitQuoteThrottle {
            next_allowed_ms: current_ms.saturating_add(EXIT_CURVE_FETCH_MIN_INTERVAL_MS),
            consecutive_failures: 0,
        },
    );
}

fn record_exit_curve_fetch_error(
    throttles: &mut HashMap<String, ExitQuoteThrottle>,
    mint: &str,
    current_ms: i64,
    err: &anyhow::Error,
) -> i64 {
    let rate_limited = is_rate_limited_error(err);
    let failures = throttles
        .get(mint)
        .map(|throttle| throttle.consecutive_failures.saturating_add(1))
        .unwrap_or(1);
    let cooldown_ms = if rate_limited {
        let multiplier = 1_i64 << failures.saturating_sub(1).min(2);
        EXIT_CURVE_FETCH_RATE_LIMIT_BACKOFF_MS
            .saturating_mul(multiplier)
            .min(EXIT_CURVE_FETCH_MAX_BACKOFF_MS)
    } else {
        EXIT_CURVE_FETCH_MIN_INTERVAL_MS
    };
    throttles.insert(
        mint.to_string(),
        ExitQuoteThrottle {
            next_allowed_ms: current_ms.saturating_add(cooldown_ms),
            consecutive_failures: failures,
        },
    );
    cooldown_ms
}

fn is_rate_limited_error(err: &anyhow::Error) -> bool {
    let text = format!("{err:#}");
    text.contains("HTTP status 429") || text.contains("Too Many Requests")
}

fn redact_live_error(text: &str, helius_api_key: &str) -> String {
    let redacted = if helius_api_key.is_empty() {
        text.to_string()
    } else {
        text.replace(helius_api_key, "<redacted>")
    };
    ["api-key=", "api_key="]
        .into_iter()
        .fold(redacted, |value, marker| redact_query_value(&value, marker))
}

fn redact_query_value(text: &str, marker: &str) -> String {
    let mut output = String::with_capacity(text.len());
    let mut remaining = text;
    while let Some(marker_index) = remaining.find(marker) {
        let value_start = marker_index + marker.len();
        output.push_str(&remaining[..value_start]);
        output.push_str("<redacted>");
        let tail = &remaining[value_start..];
        let value_end = tail
            .find(|ch: char| {
                ch == '&' || ch.is_whitespace() || matches!(ch, ')' | ']' | '}' | '"' | '\'')
            })
            .unwrap_or(tail.len());
        remaining = &tail[value_end..];
    }
    output.push_str(remaining);
    output
}

fn ensure_curve_watch(
    cfg: &Config,
    curve_quote_client: &CurveQuoteClient,
    curve_state_tx: &mpsc::Sender<BondingCurveState>,
    curve_watches: &mut HashMap<String, CurveWatch>,
    mint: &str,
    watch_until_ms: i64,
) -> Result<()> {
    if let Some(watch) = curve_watches.get_mut(mint) {
        watch.watch_until_ms = watch.watch_until_ms.max(watch_until_ms);
        return Ok(());
    }
    let account = curve_quote_client.bonding_curve_address(mint)?.to_string();
    let handle = spawn_curve_watch(
        cfg.ws_url(),
        cfg.subscribe_commitment.clone(),
        mint.to_string(),
        account,
        curve_state_tx.clone(),
    );
    curve_watches.insert(
        mint.to_string(),
        CurveWatch {
            handle,
            watch_until_ms,
        },
    );
    Ok(())
}

fn cleanup_curve_watches(
    positions: &PositionManager,
    curve_watches: &mut HashMap<String, CurveWatch>,
    current_ms: i64,
) {
    let expired = curve_watches
        .iter()
        .filter(|(mint, watch)| {
            current_ms >= watch.watch_until_ms && !positions.has_open_position(mint)
        })
        .map(|(mint, _)| mint.clone())
        .collect::<Vec<_>>();
    for mint in expired {
        if let Some(watch) = curve_watches.remove(&mint) {
            watch.handle.abort();
        }
    }
}

fn process_curve_state(
    journal: &Journal,
    positions: &PositionManager,
    market: &mut MarketTracker,
    curve_states: &mut HashMap<String, BondingCurveState>,
    state: BondingCurveState,
) -> Result<()> {
    let key = curve_state_key(&state);
    journal.record_once(JournalKind::CurveState, &key, &state)?;
    if let Some(position) = positions.position_for_mint(&state.mint) {
        if positions.has_open_position(&state.mint) && position.token_amount_raw > 0 {
            match sell_quote_from_state(&state, position.token_amount_raw) {
                Ok(quote) => {
                    journal.record_once(JournalKind::MarketQuote, &quote.signature, &quote)?;
                    market.update(quote);
                }
                Err(err) => {
                    debug!("streamed curve quote rejected mint={}: {err:#}", state.mint);
                }
            }
        }
    }
    curve_states.insert(state.mint.clone(), state);
    Ok(())
}

fn register_discovery(
    journal: &Journal,
    discoveries: &mut DiscoveryRegistry,
    signal: DiscoverySignal,
) -> Result<()> {
    if discoveries.register(signal.clone()) {
        let key = format!("{}:{}:{}", signal.mint, signal.seen_ts_ms, signal.source);
        journal.record_once(JournalKind::DiscoverySignal, &key, &signal)?;
        info!(
            "registered Mayhem discovery mint={} source={} seen_ts_ms={}",
            signal.mint, signal.source, signal.seen_ts_ms
        );
    }
    Ok(())
}

fn record_stale_exit_once(
    journal: &Journal,
    stale_exit_logged: &mut HashSet<String>,
    mint: &str,
    timestamp_ms: i64,
    reason: &str,
) -> Result<()> {
    if stale_exit_logged.insert(mint.to_string()) {
        let decision = Decision {
            id: format!("decision-exit-stale-{timestamp_ms}-{}", mint_prefix(mint)),
            timestamp_ms,
            source_signature: None,
            mint: Some(mint.to_string()),
            action: Action::Hold,
            mode: Mode::Paper,
            reason_codes: vec![reason.to_string()],
            requested_lamports: None,
            risk_approved: false,
            risk_veto_reason: Some(reason.to_string()),
        };
        journal.record(JournalKind::RiskVeto, &decision)?;
        info!("exit delayed mint={mint} reason={reason}");
    }
    Ok(())
}

fn is_filled_execution(status: ExecutionStatus) -> bool {
    matches!(
        status,
        ExecutionStatus::PaperFilled
            | ExecutionStatus::LiveConfirmed
            | ExecutionStatus::LiveReconciled
    )
}

fn zero_inventory_without_sell_broadcast(report: &ExecutionReport) -> bool {
    report.signature.is_none()
        && report.filled_lamports.unwrap_or_default() == 0
        && report.filled_token_amount_raw.unwrap_or_default() == 0
}

async fn fetch_transaction_with_retry(
    fetcher: &TxFetcher,
    signature: &str,
) -> Result<serde_json::Value> {
    let mut last_error = None;
    for delay_ms in [0u64, 100, 250, 500] {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        match fetcher.get_transaction(signature).await {
            Ok(tx) if !tx.is_null() => return Ok(tx),
            Ok(_) => last_error = Some(anyhow::anyhow!("transaction not yet available")),
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("transaction fetch failed")))
}

async fn fetch_entry_curve_state(
    curve_quote_client: &CurveQuoteClient,
    mint: &str,
    signal_slot: u64,
    max_ahead_slots: u64,
) -> Result<BondingCurveState> {
    let mut last_error = None;
    for delay_ms in [125u64, 250, 500] {
        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        match curve_quote_client.fetch_state(mint).await {
            Ok(state) => {
                match validate_entry_curve_slot(signal_slot, state.slot, max_ahead_slots) {
                    Ok(()) => return Ok(state),
                    Err(err) if state.slot > signal_slot => return Err(err),
                    Err(err) => last_error = Some(err),
                }
            }
            Err(err) => last_error = Some(err),
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("entry curve lookup failed")))
}

async fn next_pulse(
    receiver: &mut Option<mpsc::Receiver<DiscoverySignal>>,
) -> Option<DiscoverySignal> {
    match receiver {
        Some(receiver) => receiver.recv().await,
        None => std::future::pending().await,
    }
}

fn mint_prefix(mint: &str) -> String {
    mint.chars().take(8).collect()
}

fn parse_config_path() -> PathBuf {
    let args: Vec<String> = env::args().collect();
    let mut idx = 1;
    while idx < args.len() {
        if args[idx] == "--config" && idx + 1 < args.len() {
            return PathBuf::from(&args[idx + 1]);
        }
        idx += 1;
    }
    PathBuf::from("config.toml")
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use catarnith::types::{BuyOrder, DecodedTx, TradeSide};

    fn live_cfg(single_lifecycle: bool) -> Config {
        Config {
            mode: Mode::Live,
            live_single_lifecycle: single_lifecycle,
            max_open_positions: 1,
            max_buys_per_mint: 1,
            max_total_lamports_per_mint: 13_500_000,
            max_total_open_lamports: 13_500_000,
            ..Config::default()
        }
    }

    fn buy_order(mint: &str) -> BuyOrder {
        BuyOrder {
            id: format!("order-buy-1000-{}", mint_prefix(mint)),
            timestamp_ms: 1_000,
            mint: mint.to_string(),
            lamports: 13_025_001,
            source_decision_id: "decision".to_string(),
            source_signature: None,
        }
    }

    fn execution_report(status: ExecutionStatus) -> ExecutionReport {
        ExecutionReport {
            order_id: "order".to_string(),
            signature: Some("signature".to_string()),
            quote_slot: Some(1),
            status,
            requested_lamports: 13_025_001,
            filled_lamports: Some(13_025_001),
            filled_token_amount_raw: Some(1),
            fee_lamports: Some(0),
            error: None,
            latency_ms: Some(1),
        }
    }

    fn decoded_agent_buy(cfg: &Config, mint: &str, timestamp_ms: i64) -> DecodedTx {
        DecodedTx {
            signature: format!("agent-buy-{timestamp_ms}"),
            slot: 42,
            timestamp_ms: Some(timestamp_ms),
            ok: true,
            side: TradeSide::Buy,
            instruction_names: vec!["BuyExactSolIn".to_string()],
            program_ids: vec![cfg.pumpfun_program.clone()],
            account_keys: vec![cfg.mayhem_agent_wallet.clone()],
            mint: Some(mint.to_string()),
            signer: Some(cfg.mayhem_agent_wallet.clone()),
            sol_delta_lamports: Some(-50_000_000),
            token_delta_raw: Some(1),
            fee_lamports: Some(5_000),
            logs: Vec::new(),
            err: None,
        }
    }

    fn copy_cfg() -> Config {
        Config {
            copy_trade_enabled: true,
            copy_trade_wallet: "11111111111111111111111111111111".to_string(),
            copy_trade_max_buy_lamports: 10_000_000,
            market: Market::AllPumpfun,
            ..live_cfg(false)
        }
    }

    fn copy_classification(mint: &str) -> TokenClassification {
        TokenClassification {
            mint: Some(mint.to_string()),
            is_pumpfun_bonding_curve: true,
            is_pumpswap: false,
            is_mayhem_direct: false,
            is_mayhem_candidate: false,
            has_verified_mayhem_evidence: false,
            is_axiom_route: false,
            is_axiom_jito_route: false,
            has_confirmed_execution_route: false,
            is_token_2022: false,
            is_fresh_launch: false,
            is_reference_wallet_seen: true,
            score: 1.0,
            reasons: vec!["test".to_string()],
        }
    }

    fn copy_event(cfg: &Config) -> StreamEvent {
        StreamEvent {
            source: format!("logsSubscribe:{}", cfg.copy_trade_wallet),
            signature: "copy-signature".to_string(),
            slot: 42,
            received_at_ms: 1_500,
            logs: Vec::new(),
            raw: serde_json::json!({}),
        }
    }

    fn copy_decoded(
        cfg: &Config,
        mint: &str,
        side: TradeSide,
        sol_delta: Option<i64>,
    ) -> DecodedTx {
        DecodedTx {
            signature: "copy-signature".to_string(),
            slot: 42,
            timestamp_ms: Some(1_500),
            ok: true,
            side,
            instruction_names: vec![],
            program_ids: vec![cfg.pumpfun_program.clone()],
            account_keys: vec![cfg.copy_trade_wallet.clone()],
            mint: Some(mint.to_string()),
            signer: Some(cfg.copy_trade_wallet.clone()),
            sol_delta_lamports: sol_delta,
            token_delta_raw: Some(1),
            fee_lamports: Some(5_000),
            logs: Vec::new(),
            err: None,
        }
    }

    fn copy_context(has_position: bool) -> StrategyContext {
        copy_context_with_buys(has_position, 0)
    }

    fn copy_context_with_buys(has_position: bool, buys_for_mint: u32) -> StrategyContext {
        StrategyContext {
            open_positions: usize::from(has_position),
            has_position_for_mint: has_position,
            buys_for_mint,
            has_discovery_signal: false,
            has_fresh_mint_discovery: false,
            discovery_seen_ts_ms: None,
            observed_buy_lamports: None,
            observed_buys_for_mint: 0,
            observed_sells_for_mint: 0,
        }
    }

    #[test]
    fn live_single_lifecycle_tracks_pending_and_open_work() {
        let disabled = live_cfg(false);
        let enabled = live_cfg(true);
        let mut positions = PositionManager::default();
        let mut pending = HashMap::new();

        assert!(active_live_lifecycle(&disabled, &positions, &pending).is_none());
        assert!(active_live_lifecycle(&enabled, &positions, &pending).is_none());

        let mint = "FreshMint111111111111111111111111111111111111";
        let order = Order::Buy(buy_order(mint));
        pending.insert(
            order.id().to_string(),
            PendingLiveOrder {
                order,
                source_slot: 1,
                submitted_at_ms: 1_000,
                exit_reason: None,
                attempt: 0,
                entry_origin_ts_ms: 1_000,
            },
        );
        assert_eq!(
            active_live_lifecycle(&enabled, &positions, &pending),
            Some(ActiveLiveLifecycle {
                mint: mint.to_string(),
                reason: "pending_buy",
            })
        );
        let second = Order::Buy(buy_order("SecondMint11111111111111111111111111111111111"));
        assert!(validate_live_submission_slot(&enabled, &pending, &second)
            .expect_err("single lifecycle must reject a second pending order")
            .to_string()
            .contains("live single lifecycle"));
        let event = StreamEvent {
            source: "logsSubscribe:pump".to_string(),
            signature: "candidate-signature".to_string(),
            slot: 2,
            received_at_ms: 1_100,
            logs: vec!["Program log: Instruction: BuyExactSolIn".to_string()],
            raw: serde_json::json!({}),
        };
        let early = early_single_lifecycle_decision(
            &enabled,
            &event,
            event.received_at_ms,
            &positions,
            &pending,
        )
        .expect("blocked buy should be rejected before full decode");
        assert_eq!(early.action, Action::Ignore);
        assert!(early
            .reason_codes
            .iter()
            .any(|reason| reason == "early_stream_rejection"));

        pending.clear();
        let buy = buy_order(mint);
        positions.record_buy(&buy, &execution_report(ExecutionStatus::LiveConfirmed));
        assert_eq!(
            active_live_lifecycle(&enabled, &positions, &pending),
            Some(ActiveLiveLifecycle {
                mint: mint.to_string(),
                reason: "open_position",
            })
        );
    }

    #[test]
    fn observed_agent_buy_after_entry_triggers_v2_sell_decision() {
        let mut cfg = live_cfg(true);
        cfg.exit_on_observed_agent_buy = true;
        let mint = "FreshMint111111111111111111111111111111111111";
        let mut positions = PositionManager::default();
        let buy = buy_order(mint);
        positions.record_buy(&buy, &execution_report(ExecutionStatus::LiveConfirmed));
        let event = StreamEvent {
            source: format!("logsSubscribe:{}", cfg.mayhem_agent_wallet),
            signature: "agent-buy-signature".to_string(),
            slot: 42,
            received_at_ms: 1_500,
            logs: Vec::new(),
            raw: serde_json::json!({}),
        };

        let before_entry = decoded_agent_buy(&cfg, mint, 1_000);
        assert!(
            observed_agent_buy_exit_decision(&cfg, &event, &before_entry, &positions, 1_000,)
                .is_none()
        );

        let after_entry = decoded_agent_buy(&cfg, mint, 1_500);
        let decision =
            observed_agent_buy_exit_decision(&cfg, &event, &after_entry, &positions, 1_500)
                .expect("post-entry Mayhem agent buy should trigger v2 sell");
        assert_eq!(decision.action, Action::Sell);
        assert_eq!(decision.mint.as_deref(), Some(mint));
        assert!(decision
            .reason_codes
            .iter()
            .any(|reason| reason == "observed_agent_buy_after_entry"));

        cfg.exit_on_observed_agent_buy = false;
        assert!(
            observed_agent_buy_exit_decision(&cfg, &event, &after_entry, &positions, 1_500,)
                .is_none()
        );
    }

    #[test]
    fn copy_trade_buy_uses_configured_sizing_and_cap() {
        let mut cfg = copy_cfg();
        let mint = "FreshMint111111111111111111111111111111111111";
        let event = copy_event(&cfg);
        let decoded = copy_decoded(&cfg, mint, TradeSide::Buy, Some(-8_000_000));
        let classification = copy_classification(mint);

        cfg.copy_trade_sizing = CopyTradeSizing::Fixed;
        cfg.base_buy_lamports = 5_000_000;
        let fixed =
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false))
                .expect("copy wallet buy should produce decision");
        assert_eq!(fixed.action, Action::Buy);
        assert_eq!(fixed.requested_lamports, Some(5_000_000));

        cfg.copy_trade_sizing = CopyTradeSizing::Mirror;
        cfg.copy_trade_max_buy_lamports = 6_000_000;
        let mirrored =
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false))
                .expect("copy wallet buy should produce decision");
        assert_eq!(mirrored.requested_lamports, Some(6_000_000));
        assert!(mirrored
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_size_capped"));
    }

    #[test]
    fn copy_trade_respects_market_filter_for_buys() {
        let mut cfg = copy_cfg();
        let mint = "FreshMint111111111111111111111111111111111111";
        let event = copy_event(&cfg);
        let decoded = copy_decoded(&cfg, mint, TradeSide::Buy, Some(-8_000_000));
        let mut classification = copy_classification(mint);
        classification.is_mayhem_candidate = true;

        cfg.market = Market::NonMayhemOnly;
        let non_mayhem =
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false))
                .expect("copy wallet buy should be recorded");
        assert_eq!(non_mayhem.action, Action::Ignore);
        assert!(non_mayhem
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_non_mayhem_market_only"));

        cfg.market = Market::MayhemOnly;
        classification.is_mayhem_candidate = false;
        classification.has_verified_mayhem_evidence = false;
        let mayhem_only =
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false))
                .expect("copy wallet buy should be recorded");
        assert_eq!(mayhem_only.action, Action::Ignore);
        assert!(mayhem_only
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_mayhem_evidence_required"));
    }

    #[test]
    fn copy_trade_source_sell_follows_open_position_only() {
        let cfg = copy_cfg();
        let mint = "FreshMint111111111111111111111111111111111111";
        let event = copy_event(&cfg);
        let decoded = copy_decoded(&cfg, mint, TradeSide::Sell, Some(7_000_000));
        let classification = copy_classification(mint);

        let sell = copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(true))
            .expect("copy wallet sell should produce decision");
        assert_eq!(sell.action, Action::Sell);
        assert!(sell
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_source_sell"));

        let ignored =
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false))
                .expect("copy wallet sell without position should be recorded as ignore");
        assert_eq!(ignored.action, Action::Ignore);
        assert!(ignored
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_sell_without_open_position"));
    }

    #[test]
    fn copy_trade_first_only_blocks_repeat_buys() {
        let mut cfg = copy_cfg();
        cfg.copy_trade_buy_policy = CopyTradeBuyPolicy::FirstOnly;
        let mint = "FreshMint111111111111111111111111111111111111";
        let event = copy_event(&cfg);
        let decoded = copy_decoded(&cfg, mint, TradeSide::Buy, Some(-8_000_000));
        let classification = copy_classification(mint);

        let first =
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false))
                .expect("first source buy should produce a decision");
        assert_eq!(first.action, Action::Buy);

        let repeat = copy_trade_decision(
            &cfg,
            &event,
            &decoded,
            &classification,
            copy_context_with_buys(true, 1),
        )
        .expect("repeat source buy should be recorded");
        assert_eq!(repeat.action, Action::Ignore);
        assert!(repeat
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_first_buy_only"));
    }

    #[test]
    fn copy_trade_accumulate_respects_copy_buy_limit_and_source_min() {
        let mut cfg = copy_cfg();
        cfg.copy_trade_buy_policy = CopyTradeBuyPolicy::Accumulate;
        cfg.copy_trade_max_buys_per_mint = 2;
        cfg.copy_trade_min_source_buy_lamports = 7_000_000;
        let mint = "FreshMint111111111111111111111111111111111111";
        let event = copy_event(&cfg);
        let classification = copy_classification(mint);

        let tiny = copy_decoded(&cfg, mint, TradeSide::Buy, Some(-6_000_000));
        let ignored_small =
            copy_trade_decision(&cfg, &event, &tiny, &classification, copy_context(false))
                .expect("small source buy should be recorded");
        assert_eq!(ignored_small.action, Action::Ignore);
        assert!(ignored_small
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_source_buy_too_small"));

        let qualifying = copy_decoded(&cfg, mint, TradeSide::Buy, Some(-8_000_000));
        let second = copy_trade_decision(
            &cfg,
            &event,
            &qualifying,
            &classification,
            copy_context_with_buys(true, 1),
        )
        .expect("second copied buy should be allowed");
        assert_eq!(second.action, Action::Buy);

        let third = copy_trade_decision(
            &cfg,
            &event,
            &qualifying,
            &classification,
            copy_context_with_buys(true, 2),
        )
        .expect("over-limit source buy should be recorded");
        assert_eq!(third.action, Action::Ignore);
        assert!(third
            .reason_codes
            .iter()
            .any(|reason| reason == "copy_trade_max_buys_per_mint"));
    }

    #[test]
    fn copy_trade_ignores_non_signer_account_key_mentions() {
        let cfg = copy_cfg();
        let mint = "FreshMint111111111111111111111111111111111111";
        let event = StreamEvent {
            source: "logsSubscribe:program".to_string(),
            signature: "program-signature".to_string(),
            slot: 42,
            received_at_ms: 1_500,
            logs: Vec::new(),
            raw: serde_json::json!({}),
        };
        let mut decoded = copy_decoded(&cfg, mint, TradeSide::Buy, Some(-8_000_000));
        decoded.signer = Some("OtherSigner111111111111111111111111111111".to_string());
        decoded.account_keys = vec![cfg.copy_trade_wallet.clone()];
        let classification = copy_classification(mint);

        assert!(
            copy_trade_decision(&cfg, &event, &decoded, &classification, copy_context(false),)
                .is_none()
        );
    }

    #[test]
    fn copy_wallet_delta_is_selected_only_for_copy_wallet_streams() {
        let mut cfg = copy_cfg();
        cfg.target_wallet = Some("TargetWallet111111111111111111111111111111".to_string());
        cfg.watched_wallets = vec!["WatchWallet1111111111111111111111111111111".to_string()];

        let program_event = StreamEvent {
            source: "logsSubscribe:program".to_string(),
            signature: "program-signature".to_string(),
            slot: 42,
            received_at_ms: 1_500,
            logs: Vec::new(),
            raw: serde_json::json!({}),
        };
        assert_eq!(
            wallet_for_delta_for_event(&cfg, &program_event, Some("DefaultWallet")),
            Some("DefaultWallet")
        );

        let copy_event = copy_event(&cfg);
        assert_eq!(
            wallet_for_delta_for_event(&cfg, &copy_event, Some("DefaultWallet")),
            cfg.copy_trade_wallet()
        );

        let target_event = StreamEvent {
            source: "logsSubscribe:TargetWallet111111111111111111111111111111".to_string(),
            signature: "target-signature".to_string(),
            slot: 42,
            received_at_ms: 1_500,
            logs: Vec::new(),
            raw: serde_json::json!({}),
        };
        assert_eq!(
            wallet_for_delta_for_event(&cfg, &target_event, Some("DefaultWallet")),
            cfg.target_wallet.as_deref()
        );
    }

    #[test]
    fn copy_trade_positions_use_copy_exit_policy() {
        let mut cfg = copy_cfg();
        cfg.max_hold_seconds = 999;
        cfg.take_profit_bps = 9_999;
        cfg.take_profit_sell_bps = 5_000;
        cfg.stop_loss_bps = 9_999;
        cfg.copy_trade_max_hold_seconds = 7;
        cfg.copy_trade_take_profit_bps = 1_500;
        cfg.copy_trade_take_profit_sell_bps = 10_000;
        cfg.copy_trade_stop_loss_bps = 800;

        let mint = "FreshMint111111111111111111111111111111111111";
        let mut positions = PositionManager::default();
        let mut buy = buy_order(mint);
        buy.source_decision_id = "decision-copy-1500-FreshMin".to_string();
        positions.record_buy(&buy, &execution_report(ExecutionStatus::LiveConfirmed));

        let position = positions
            .position_for_mint(mint)
            .expect("copy buy should open position");
        assert!(position.copy_trade_entry);
        let policy = exit_policy_for_position(&cfg, position);
        assert_eq!(policy.max_hold_seconds, 7);
        assert_eq!(policy.take_profit_bps, 1_500);
        assert_eq!(policy.take_profit_sell_bps, 10_000);
        assert_eq!(policy.stop_loss_bps, 800);
    }
}

fn env_flag(name: &str) -> bool {
    env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use catarnith::types::BuyOrder;

    #[test]
    fn redacts_rpc_query_keys_from_live_errors() {
        let error = "request failed for https://mainnet.helius-rpc.com/?api-key=secret-value&x=1";
        let redacted = redact_live_error(error, "secret-value");
        assert!(!redacted.contains("secret-value"));
        assert!(redacted.contains("api-key=<redacted>&x=1"));
    }

    #[test]
    fn retries_only_atomic_buy_slippage_inside_retry_window() {
        let now = now_ms();
        let mut cfg = Config {
            buy_slippage_retry_attempts: 1,
            buy_slippage_retry_deadline_ms: 3_000,
            ..Config::default()
        };
        let order = Order::Buy(BuyOrder {
            id: "buy".to_string(),
            timestamp_ms: now,
            mint: "mint".to_string(),
            lamports: 13_025_001,
            source_decision_id: "decision".to_string(),
            source_signature: None,
        });
        let report = ExecutionReport {
            order_id: "buy".to_string(),
            signature: Some("signature".to_string()),
            quote_slot: None,
            status: ExecutionStatus::LiveFailed,
            requested_lamports: 13_025_001,
            filled_lamports: Some(0),
            filled_token_amount_raw: Some(0),
            fee_lamports: None,
            error: Some(
                "transaction failed on-chain via primary: InstructionError(3, Custom(6042))"
                    .to_string(),
            ),
            latency_ms: Some(1),
        };

        assert!(should_retry_buy_slippage(&cfg, &order, &report, 0, now));
        assert!(!should_retry_buy_slippage(&cfg, &order, &report, 1, now));
        assert!(!should_retry_buy_slippage(
            &cfg,
            &order,
            &report,
            0,
            now.saturating_sub(3_500)
        ));

        cfg.buy_slippage_retry_attempts = 0;
        assert!(!should_retry_buy_slippage(&cfg, &order, &report, 0, now));

        // Pre-broadcast simulation failures are also retryable.
        let mut sim_report = report.clone();
        sim_report.status = ExecutionStatus::Errored;
        sim_report.error =
            Some("pre_broadcast_simulation_failed:InstructionError(3, Custom(6042))".to_string());
        cfg.buy_slippage_retry_attempts = 1;
        assert!(should_retry_buy_slippage(&cfg, &order, &sim_report, 0, now));
    }

    #[test]
    fn retry_slippage_escalates_and_caps() {
        let cfg = Config {
            max_slippage_bps: 1_000,
            buy_slippage_retry_step_bps: 500,
            buy_slippage_retry_max_bps: 2_000,
            ..Config::default()
        };
        assert_eq!(retry_buy_slippage_bps(&cfg, 1), 1_500);
        assert_eq!(retry_buy_slippage_bps(&cfg, 2), 2_000);
        assert_eq!(retry_buy_slippage_bps(&cfg, 3), 2_000);
    }

    #[test]
    fn pending_inventory_defer_requires_no_sell_broadcast() {
        let mut report = ExecutionReport {
            order_id: "sell".to_string(),
            signature: None,
            quote_slot: None,
            status: ExecutionStatus::LiveReconciled,
            requested_lamports: 0,
            filled_lamports: Some(0),
            filled_token_amount_raw: None,
            fee_lamports: Some(0),
            error: None,
            latency_ms: Some(1),
        };
        assert!(zero_inventory_without_sell_broadcast(&report));

        report.signature = Some("sell-signature".to_string());
        report.filled_lamports = Some(12_703_394);
        report.filled_token_amount_raw = Some(505_470_592_937);
        assert!(!zero_inventory_without_sell_broadcast(&report));
    }

    #[test]
    fn stale_log_limiter_summarizes_stream_rejections() {
        let mut limiter = StaleLogLimiter::default();
        assert!(limiter.stream_message("queued", 600, 500).is_some());
        for _ in 2..STALE_STREAM_LOG_EVERY {
            assert!(limiter.stream_message("queued", 600, 500).is_none());
        }
        let message = limiter
            .stream_message("after_fetch", 900, 500)
            .expect("every Nth stale stream rejection should summarize");
        assert!(message.contains("count=100"));
        assert!(message.contains("latest_stage=after_fetch"));
    }

    #[test]
    fn stale_log_limiter_summarizes_create_rejections() {
        let mut limiter = StaleLogLimiter::default();
        assert!(limiter
            .create_message("missing_event_slot", None, 4)
            .is_some());
        for _ in 2..STALE_CREATE_LOG_EVERY {
            assert!(limiter
                .create_message("missing_event_slot", None, 4)
                .is_none());
        }
        let message = limiter
            .create_message("slot_lag_exceeded", Some(12), 4)
            .expect("every Nth stale create rejection should summarize");
        assert!(message.contains("count=25"));
        assert!(message.contains("latest_reason=slot_lag_exceeded"));
        assert!(message.contains("latest_slot_lag=12"));
    }
}
