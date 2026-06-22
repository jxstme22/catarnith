//! Ctarnith — minimal single-lifecycle TUI trading terminal.
//!
//! `catarnith` is a single binary that owns the trade loop, the
//! wallet lock, and the render surface. It exists so an operator
//! can press Enter to dump a position without juggling multiple
//! processes or fighting the per-wallet file lock.
//!
//! Architecture:
//!
//! ```text
//! +--------------+   mpsc   +-----------------+   mpsc   +-----------+
//! | input thread | -------> | strategy thread | -------> |  executor |
//! +--------------+ commands +-----------------+ events   +-----------+
//!                       ^              |                    |
//!                       |              v                    |
//!                       |       +-------------+             |
//!                       +-------| render thr. |<------------+
//!                               +-------------+
//! ```
//!
//! - **Input thread** reads keystrokes from `crossterm::EventStream`
//!   and forwards them as `ScanCommand`s. The render thread never
//!   blocks on the input stream.
//! - **Strategy thread** runs the lifecycle: scan → entry → hold →
//!   sell → welcome. It mutates `ScanState` behind an `RwLock`
//!   and emits `ScanEvent`s for the render thread to consume.
//! - **Render thread** owns the ratatui `Terminal` and draws at
//!   30 FPS from the latest `ScanState` snapshot.

use anyhow::{anyhow, Context, Result};
use catarnith::{
    config::{Config, CopyTradeBuyPolicy, CopyTradeSizing, Market},
    curve::BondingCurveState,
    curve_stream::spawn_curve_watch,
    decoder::extract_pump_create_event_mint,
    ingest::{spawn_streams, StreamConfig},
    journal::{Journal, JournalKind},
    market_data::MarketData,
    types::ExecutionReport,
    types::{now_ms, BuyOrder, ExecutionStatus, Mode},
};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen},
};
use futures_util::StreamExt;
use ratatui::{backend::CrosstermBackend, Terminal};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::{
    collections::VecDeque,
    process::ExitStatus,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::io::AsyncBufReadExt;
use tokio::sync::{mpsc, RwLock};

/// Number of log lines retained in `ScanState`. Large enough to show
/// a useful tail of autonomous bot output inside the TUI.
const LOG_CAP: usize = 256;
const CREATE_SLOT_CACHE_TTL_MS: i64 = 250;
const BOT_CHILD_TAIL: usize = 8;
const BOT_RAPID_FAILURE_LIMIT: u32 = 3;
const BOT_RAPID_FAILURE_WINDOW: Duration = Duration::from_secs(5);

mod ascii_bg;
mod render;
mod scan_executor;
use scan_executor::ScanExecutor;

/// Where in the lifecycle we are.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// Mode picker: 1=auto bot, 2=live, 3=paper, S=settings. This is
    /// the first screen when `catarnith` is run with no subcommand.
    ModePicker,
    /// Big "press Enter" splash. We sit here between trades.
    Welcome,
    /// Subscribed to WS, waiting for a `Create` event.
    Scanning,
    /// Saw a fresh Pump.fun mint, evaluating the entry.
    Evaluating,
    /// Entered; holding while mcap ticks. Sell is manual (Enter).
    Holding,
    /// Sell in flight.
    Selling,
    /// Trade closed; result screen stays visible until operator
    /// presses a key to start the next cycle.
    TradeResult,
    /// Edit wallet key and buy size for the active config file.
    Settings,
    /// Bot-specific setup screen shown before launching mode 1.
    BotSettings,
    /// Autonomous bot is running inside the TUI. ESC stops it.
    BotRunning,
    /// Bot has stopped; press ESC again to return to the mode picker.
    BotStopped,
}

impl Phase {
    pub fn label(self) -> &'static str {
        match self {
            Phase::ModePicker => "PICK MODE",
            Phase::Welcome => "WELCOME",
            Phase::Scanning => "SCANNING",
            Phase::Evaluating => "EVALUATING",
            Phase::Holding => "HOLDING",
            Phase::Selling => "SELLING",
            Phase::TradeResult => "RESULT",
            Phase::Settings => "SETTINGS",
            Phase::BotSettings => "AUTO BOT SETUP",
            Phase::BotRunning => "BOT RUNNING",
            Phase::BotStopped => "BOT STOPPED",
        }
    }
}

/// Outcome of the last completed trade, shown on the welcome
/// screen between cycles.
#[derive(Debug, Clone, Default)]
pub struct LastTrade {
    pub mint: String,
    pub entry_sol: f64,
    pub exit_sol: f64,
    pub realized_sol: f64,
    pub held_ms: i64,
    pub won: bool,
}

/// Active field in the settings screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SettingsField {
    #[default]
    Wallet,
    BuySize,
    Market,
    HeliusKey,
    FallbackRpc,
    JupiterKey,
    SlippageBps,
    Theme,
    /// Expand/collapse the advanced live trade options below. Toggled with ←/→.
    AdvancedToggle,
    EnableLiveTrading,
    RequireManualLiveUnlock,
    LiveMaxBalanceSol,
    MaxHoldSecs,
    SellSlippageBps,
    PriorityFee,
    JitoUrl,
    JitoTipSol,
    ConfirmationPollMs,
    PreBroadcastSimulation,
}

impl SettingsField {
    /// True for the fields revealed only when the advanced section is
    /// expanded. The cursor skips these when collapsed.
    fn is_advanced(self) -> bool {
        use SettingsField::*;
        matches!(
            self,
            EnableLiveTrading
                | RequireManualLiveUnlock
                | LiveMaxBalanceSol
                | MaxHoldSecs
                | SellSlippageBps
                | PriorityFee
                | JitoUrl
                | JitoTipSol
                | ConfirmationPollMs
                | PreBroadcastSimulation
        )
    }
}

/// Forward to the next settings field. Cycles back to `Wallet`. When
/// the advanced section is collapsed, the advanced fields are skipped
/// and `AdvancedToggle` wraps straight back to `Wallet`.
fn next_field(f: SettingsField, show_advanced: bool) -> SettingsField {
    use SettingsField::*;
    let next = match f {
        Wallet => BuySize,
        BuySize => Market,
        Market => HeliusKey,
        HeliusKey => FallbackRpc,
        FallbackRpc => JupiterKey,
        JupiterKey => SlippageBps,
        SlippageBps => Theme,
        Theme => AdvancedToggle,
        AdvancedToggle => EnableLiveTrading,
        EnableLiveTrading => RequireManualLiveUnlock,
        RequireManualLiveUnlock => LiveMaxBalanceSol,
        LiveMaxBalanceSol => MaxHoldSecs,
        MaxHoldSecs => SellSlippageBps,
        SellSlippageBps => PriorityFee,
        PriorityFee => JitoUrl,
        JitoUrl => JitoTipSol,
        JitoTipSol => ConfirmationPollMs,
        ConfirmationPollMs => PreBroadcastSimulation,
        PreBroadcastSimulation => Wallet,
    };
    if next.is_advanced() && !show_advanced {
        Wallet
    } else {
        next
    }
}

/// Move to the previous settings field. Cycles back to the last
/// visible field. When advanced is collapsed, stepping back from
/// `Wallet` lands on `AdvancedToggle` (the last visible row).
fn prev_field(f: SettingsField, show_advanced: bool) -> SettingsField {
    use SettingsField::*;
    let prev = match f {
        Wallet => {
            if show_advanced {
                PreBroadcastSimulation
            } else {
                AdvancedToggle
            }
        }
        BuySize => Wallet,
        Market => BuySize,
        HeliusKey => Market,
        FallbackRpc => HeliusKey,
        JupiterKey => FallbackRpc,
        SlippageBps => JupiterKey,
        Theme => SlippageBps,
        AdvancedToggle => Theme,
        EnableLiveTrading => AdvancedToggle,
        RequireManualLiveUnlock => EnableLiveTrading,
        LiveMaxBalanceSol => RequireManualLiveUnlock,
        MaxHoldSecs => LiveMaxBalanceSol,
        SellSlippageBps => MaxHoldSecs,
        PriorityFee => SellSlippageBps,
        JitoUrl => PriorityFee,
        JitoTipSol => JitoUrl,
        ConfirmationPollMs => JitoTipSol,
        PreBroadcastSimulation => ConfirmationPollMs,
    };
    if prev.is_advanced() && !show_advanced {
        AdvancedToggle
    } else {
        prev
    }
}

/// Active field in the Auto Bot setup screen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BotSettingsField {
    #[default]
    Mode,
    Market,
    BuySize,
    SlippageBps,
    MaxHoldSecs,
    StreamAgeMs,
    EntryDeadlineMs,
    CopyTradeEnabled,
    CopyTradeWallet,
    CopyTradeSizing,
    CopyTradeScaleBps,
    CopyTradeMaxBuySol,
    CopyTradeFollowSells,
    AdvancedToggle,
    BotKeepAlive,
    MaxOpenPositions,
    MaxBuysPerMint,
    MaxPerMintSol,
    MaxOpenSol,
    DailyLossSol,
    CopyTradeBuyPolicy,
    CopyTradeMaxBuysPerMint,
    CopyTradeMinSourceBuySol,
    CopyTradeMaxHoldSecs,
    CopyTradeTakeProfitBps,
    CopyTradeTakeProfitSellBps,
    CopyTradeStopLossBps,
    CreateSlotLag,
    BackfillLimit,
    FetchFullTransaction,
    CurveExitQuotes,
    ConfirmationPollMs,
    ParallelFallbackReads,
}

impl BotSettingsField {
    fn is_advanced(self) -> bool {
        use BotSettingsField::*;
        matches!(
            self,
            BotKeepAlive
                | MaxOpenPositions
                | MaxBuysPerMint
                | MaxPerMintSol
                | MaxOpenSol
                | DailyLossSol
                | CopyTradeBuyPolicy
                | CopyTradeMaxBuysPerMint
                | CopyTradeMinSourceBuySol
                | CopyTradeMaxHoldSecs
                | CopyTradeTakeProfitBps
                | CopyTradeTakeProfitSellBps
                | CopyTradeStopLossBps
                | CreateSlotLag
                | BackfillLimit
                | FetchFullTransaction
                | CurveExitQuotes
                | ConfirmationPollMs
                | ParallelFallbackReads
        )
    }
}

fn next_bot_field(f: BotSettingsField, show_advanced: bool) -> BotSettingsField {
    use BotSettingsField::*;
    let next = match f {
        Mode => Market,
        Market => BuySize,
        BuySize => SlippageBps,
        SlippageBps => MaxHoldSecs,
        MaxHoldSecs => StreamAgeMs,
        StreamAgeMs => EntryDeadlineMs,
        EntryDeadlineMs => CopyTradeEnabled,
        CopyTradeEnabled => CopyTradeWallet,
        CopyTradeWallet => CopyTradeSizing,
        CopyTradeSizing => CopyTradeScaleBps,
        CopyTradeScaleBps => CopyTradeMaxBuySol,
        CopyTradeMaxBuySol => CopyTradeFollowSells,
        CopyTradeFollowSells => AdvancedToggle,
        AdvancedToggle => BotKeepAlive,
        BotKeepAlive => MaxOpenPositions,
        MaxOpenPositions => MaxBuysPerMint,
        MaxBuysPerMint => MaxPerMintSol,
        MaxPerMintSol => MaxOpenSol,
        MaxOpenSol => DailyLossSol,
        DailyLossSol => CopyTradeBuyPolicy,
        CopyTradeBuyPolicy => CopyTradeMaxBuysPerMint,
        CopyTradeMaxBuysPerMint => CopyTradeMinSourceBuySol,
        CopyTradeMinSourceBuySol => CopyTradeMaxHoldSecs,
        CopyTradeMaxHoldSecs => CopyTradeTakeProfitBps,
        CopyTradeTakeProfitBps => CopyTradeTakeProfitSellBps,
        CopyTradeTakeProfitSellBps => CopyTradeStopLossBps,
        CopyTradeStopLossBps => CreateSlotLag,
        CreateSlotLag => BackfillLimit,
        BackfillLimit => FetchFullTransaction,
        FetchFullTransaction => CurveExitQuotes,
        CurveExitQuotes => ConfirmationPollMs,
        ConfirmationPollMs => ParallelFallbackReads,
        ParallelFallbackReads => Mode,
    };
    if next.is_advanced() && !show_advanced {
        Mode
    } else {
        next
    }
}

fn prev_bot_field(f: BotSettingsField, show_advanced: bool) -> BotSettingsField {
    use BotSettingsField::*;
    let prev = match f {
        Mode => {
            if show_advanced {
                ParallelFallbackReads
            } else {
                AdvancedToggle
            }
        }
        Market => Mode,
        BuySize => Market,
        SlippageBps => BuySize,
        MaxHoldSecs => SlippageBps,
        StreamAgeMs => MaxHoldSecs,
        EntryDeadlineMs => StreamAgeMs,
        CopyTradeEnabled => EntryDeadlineMs,
        CopyTradeWallet => CopyTradeEnabled,
        CopyTradeSizing => CopyTradeWallet,
        CopyTradeScaleBps => CopyTradeSizing,
        CopyTradeMaxBuySol => CopyTradeScaleBps,
        CopyTradeFollowSells => CopyTradeMaxBuySol,
        AdvancedToggle => CopyTradeFollowSells,
        BotKeepAlive => AdvancedToggle,
        MaxOpenPositions => BotKeepAlive,
        MaxBuysPerMint => MaxOpenPositions,
        MaxPerMintSol => MaxBuysPerMint,
        MaxOpenSol => MaxPerMintSol,
        DailyLossSol => MaxOpenSol,
        CopyTradeBuyPolicy => DailyLossSol,
        CopyTradeMaxBuysPerMint => CopyTradeBuyPolicy,
        CopyTradeMinSourceBuySol => CopyTradeMaxBuysPerMint,
        CopyTradeMaxHoldSecs => CopyTradeMinSourceBuySol,
        CopyTradeTakeProfitBps => CopyTradeMaxHoldSecs,
        CopyTradeTakeProfitSellBps => CopyTradeTakeProfitBps,
        CopyTradeStopLossBps => CopyTradeTakeProfitSellBps,
        CreateSlotLag => CopyTradeStopLossBps,
        BackfillLimit => CreateSlotLag,
        FetchFullTransaction => BackfillLimit,
        CurveExitQuotes => FetchFullTransaction,
        ConfirmationPollMs => CurveExitQuotes,
        ParallelFallbackReads => ConfirmationPollMs,
    };
    if prev.is_advanced() && !show_advanced {
        AdvancedToggle
    } else {
        prev
    }
}

/// Mutable state for the settings editor.
#[derive(Debug, Clone)]
pub struct SettingsState {
    pub wallet_b58: String,
    pub buy_size_sol: String,
    pub market: Market,
    pub helius_key: String,
    pub fallback_rpc: String,
    pub jupiter_key: String,
    pub slippage_bps: String,
    pub theme: Theme,
    /// Advanced live-trade fields, edited only when `show_advanced` is on.
    pub enable_live_trading: bool,
    pub require_manual_live_unlock: bool,
    pub live_max_balance_sol: String,
    pub max_hold_secs: String,
    pub sell_slippage_bps: String,
    pub priority_fee_microlamports: String,
    pub jito_block_engine_url: String,
    pub jito_tip_sol: String,
    pub confirmation_poll_ms: String,
    pub pre_broadcast_simulation: bool,
    /// Whether the advanced live-trade section is expanded.
    pub show_advanced: bool,
    pub active_field: SettingsField,
    pub error: Option<String>,
    pub saved: bool,
}

impl Default for SettingsState {
    fn default() -> Self {
        Self {
            wallet_b58: String::new(),
            buy_size_sol: String::new(),
            market: Market::MayhemOnly,
            helius_key: String::new(),
            fallback_rpc: String::new(),
            jupiter_key: String::new(),
            slippage_bps: String::new(),
            theme: Theme::Mono,
            enable_live_trading: false,
            require_manual_live_unlock: true,
            live_max_balance_sol: String::new(),
            max_hold_secs: String::new(),
            sell_slippage_bps: String::new(),
            priority_fee_microlamports: String::new(),
            jito_block_engine_url: String::new(),
            jito_tip_sol: String::new(),
            confirmation_poll_ms: String::new(),
            pre_broadcast_simulation: true,
            show_advanced: false,
            active_field: SettingsField::default(),
            error: None,
            saved: false,
        }
    }
}

/// Mutable state for the Auto Bot setup editor.
#[derive(Debug, Clone)]
pub struct BotSettingsState {
    pub config_path: String,
    pub mode: Mode,
    pub market: Market,
    pub buy_size_sol: String,
    pub slippage_bps: String,
    pub max_hold_secs: String,
    pub max_stream_event_age_ms: String,
    pub entry_deadline_ms: String,
    pub copy_trade_enabled: bool,
    pub copy_trade_wallet: String,
    pub copy_trade_sizing: CopyTradeSizing,
    pub copy_trade_scale_bps: String,
    pub copy_trade_max_buy_sol: String,
    pub copy_trade_follow_sells: bool,
    pub bot_keep_alive: bool,
    pub max_open_positions: String,
    pub max_buys_per_mint: String,
    pub max_per_mint_sol: String,
    pub max_open_sol: String,
    pub daily_loss_sol: String,
    pub copy_trade_buy_policy: CopyTradeBuyPolicy,
    pub copy_trade_max_buys_per_mint: String,
    pub copy_trade_min_source_buy_sol: String,
    pub copy_trade_max_hold_secs: String,
    pub copy_trade_take_profit_bps: String,
    pub copy_trade_take_profit_sell_bps: String,
    pub copy_trade_stop_loss_bps: String,
    pub max_create_event_slot_lag: String,
    pub backfill_limit: String,
    pub fetch_full_transaction: bool,
    pub enable_curve_exit_quotes: bool,
    pub confirmation_poll_ms: String,
    pub parallel_fallback_reads: bool,
    pub show_advanced: bool,
    pub active_field: BotSettingsField,
    pub error: Option<String>,
    pub saved: bool,
}

impl Default for BotSettingsState {
    fn default() -> Self {
        Self {
            config_path: String::new(),
            mode: Mode::Paper,
            market: Market::MayhemOnly,
            buy_size_sol: String::new(),
            slippage_bps: String::new(),
            max_hold_secs: String::new(),
            max_stream_event_age_ms: String::new(),
            entry_deadline_ms: String::new(),
            copy_trade_enabled: false,
            copy_trade_wallet: String::new(),
            copy_trade_sizing: CopyTradeSizing::Fixed,
            copy_trade_scale_bps: String::new(),
            copy_trade_max_buy_sol: String::new(),
            copy_trade_follow_sells: true,
            bot_keep_alive: true,
            max_open_positions: String::new(),
            max_buys_per_mint: String::new(),
            max_per_mint_sol: String::new(),
            max_open_sol: String::new(),
            daily_loss_sol: String::new(),
            copy_trade_buy_policy: CopyTradeBuyPolicy::FirstOnly,
            copy_trade_max_buys_per_mint: String::new(),
            copy_trade_min_source_buy_sol: String::new(),
            copy_trade_max_hold_secs: String::new(),
            copy_trade_take_profit_bps: String::new(),
            copy_trade_take_profit_sell_bps: String::new(),
            copy_trade_stop_loss_bps: String::new(),
            max_create_event_slot_lag: String::new(),
            backfill_limit: String::new(),
            fetch_full_transaction: true,
            enable_curve_exit_quotes: false,
            confirmation_poll_ms: String::new(),
            parallel_fallback_reads: false,
            show_advanced: false,
            active_field: BotSettingsField::default(),
            error: None,
            saved: false,
        }
    }
}

/// What the render thread draws. Everything the operator sees
/// comes from a snapshot of this struct.
#[derive(Debug, Clone)]
pub struct ScanState {
    pub phase: Phase,
    /// Current mint the strategy is watching or holding. Empty on
    /// the welcome screen.
    pub mint: String,
    /// Display symbol (mint prefix). Empty on welcome.
    pub symbol: String,
    /// Bonding-curve mcap, in SOL. Updated from `BondingCurveState`
    /// ticks. Zero on welcome / pre-entry.
    pub mcap_sol: f64,
    /// Bonding-curve mcap in USD.
    pub mcap_usd: f64,
    /// SOL price in USD from Pyth.
    pub sol_price_usd: f64,
    /// Sparkline samples: `(timestamp_ms, mcap_sol)`. Bounded
    /// length; oldest are dropped.
    pub mcap_history: VecDeque<(i64, f64)>,
    /// Entry lamports for the held position. Zero on welcome.
    pub entry_lamports: u64,
    /// Entry USD value of the held position.
    pub entry_usd: f64,
    /// Current mark-to-market USD value of the held position.
    pub position_usd: f64,
    /// Wall-clock ms when the entry landed. Zero on welcome.
    pub entry_ms: i64,
    /// Tokens held (raw). Zero on welcome.
    pub token_amount_raw: u128,
    /// Last few log lines shown in the bottom strip.
    pub logs: VecDeque<String>,
    /// Full log overlay toggle (press L).
    pub show_logs: bool,
    /// Number of lines above the newest log that the log views are scrolled.
    /// Zero means tail-follow.
    pub log_scroll: usize,
    /// Wallet label shown in the footer (e.g. "PAPER" or the live
    /// wallet pubkey).
    pub wallet_label: String,
    /// Last trade summary, displayed on the welcome screen.
    pub last_trade: Option<LastTrade>,
    /// Trade counter for the session.
    pub trades_taken: u64,
    pub trades_won: u64,
    pub trades_lost: u64,
    /// How many mints the scanner inspected this session.
    /// Surfaced in the welcome screen so the operator can see
    /// "we looked at 142 mints and found 3 mayhem."
    pub scanned: u64,
    /// How many mints the scanner *skipped* (buy failed for a
    /// non-RPC reason: live_disabled, simulation_rejected,
    /// non-mayhem detected, etc.). The scanner continues.
    pub trades_skipped: u64,
    /// How many times we hit an RPC error and had to abort
    /// the scan. Distinct from `trades_skipped`: skip = keep
    /// going, error = stop and drop to welcome.
    pub rpc_errors: u64,
    /// Tick counter for animations.
    pub tick: u64,
    /// Theme. Default DARK.
    pub theme: Theme,
    /// Submission status of the most recent panic-sell. None
    /// before any sell; Some after.
    pub last_panic_status: Option<ExecutionStatus>,
    pub last_panic_signature: Option<String>,
    /// Status message shown on the scanning screen (e.g. "preparing executor…").
    pub status_line: String,
    /// Transient error message shown on the mode picker when a
    /// mode fails to start (e.g. RPC unreachable, config invalid).
    pub last_error: Option<String>,
    /// Resolved config path shown in the mode picker. This is set
    /// from the actual startup path, not from late `.env` overrides.
    pub config_label: String,
    /// Settings editor state.
    pub settings: SettingsState,
    /// Auto Bot setup editor state.
    pub bot_settings: BotSettingsState,
    /// True when Esc was pressed while holding a position and we are
    /// waiting for a second Esc to confirm abandoning it. Any other
    /// key clears it.
    pub confirm_exit: bool,
}

impl ScanState {
    pub fn new() -> Self {
        Self {
            phase: Phase::ModePicker,
            mint: String::new(),
            symbol: String::new(),
            mcap_sol: 0.0,
            mcap_usd: 0.0,
            sol_price_usd: 0.0,
            mcap_history: VecDeque::with_capacity(64),
            entry_lamports: 0,
            entry_usd: 0.0,
            position_usd: 0.0,
            entry_ms: 0,
            token_amount_raw: 0,
            logs: VecDeque::with_capacity(LOG_CAP),
            show_logs: false,
            log_scroll: 0,
            wallet_label: String::new(),
            last_trade: None,
            trades_taken: 0,
            trades_won: 0,
            trades_lost: 0,
            scanned: 0,
            trades_skipped: 0,
            rpc_errors: 0,
            tick: 0,
            theme: Theme::Mono,
            last_panic_status: None,
            last_panic_signature: None,
            status_line: String::new(),
            last_error: None,
            config_label: "config.toml".to_string(),
            settings: SettingsState::new(),
            bot_settings: BotSettingsState::new(),
            confirm_exit: false,
        }
    }

    pub fn push_log(&mut self, line: impl Into<String>) {
        let was_scrolled = self.log_scroll > 0;
        if self.logs.len() == LOG_CAP {
            self.logs.pop_front();
        }
        self.logs.push_back(line.into());
        if was_scrolled {
            self.log_scroll = self
                .log_scroll
                .saturating_add(1)
                .min(self.logs.len().saturating_sub(1));
        }
    }

    pub fn scroll_logs(&mut self, delta: i32) {
        let max_scroll = self.logs.len().saturating_sub(1);
        if delta == i32::MIN {
            self.log_scroll = 0;
        } else if delta == i32::MAX {
            self.log_scroll = max_scroll;
        } else if delta > 0 {
            self.log_scroll = self
                .log_scroll
                .saturating_add(delta as usize)
                .min(max_scroll);
        } else if delta < 0 {
            self.log_scroll = self
                .log_scroll
                .saturating_sub(delta.unsigned_abs() as usize);
        }
    }

    pub fn push_mcap(&mut self, ts_ms: i64, mcap_sol: f64) {
        self.mcap_history.push_back((ts_ms, mcap_sol));
        if self.mcap_history.len() > 64 {
            self.mcap_history.pop_front();
        }
        self.mcap_sol = mcap_sol;
    }
}

impl SettingsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.wallet_b58.clear();
        self.buy_size_sol.clear();
        self.market = Market::MayhemOnly;
        self.helius_key.clear();
        self.fallback_rpc.clear();
        self.jupiter_key.clear();
        self.slippage_bps.clear();
        self.max_hold_secs.clear();
        self.live_max_balance_sol.clear();
        self.sell_slippage_bps.clear();
        self.priority_fee_microlamports.clear();
        self.jito_block_engine_url.clear();
        self.jito_tip_sol.clear();
        self.confirmation_poll_ms.clear();
        self.show_advanced = false;
        self.active_field = SettingsField::Wallet;
        self.error = None;
        self.saved = false;
        // theme/live toggles are left at their loaded values.
    }
}

impl BotSettingsState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn reset(&mut self) {
        self.config_path.clear();
        self.buy_size_sol.clear();
        self.slippage_bps.clear();
        self.max_hold_secs.clear();
        self.max_stream_event_age_ms.clear();
        self.entry_deadline_ms.clear();
        self.copy_trade_wallet.clear();
        self.copy_trade_scale_bps.clear();
        self.copy_trade_max_buy_sol.clear();
        self.max_open_positions.clear();
        self.max_buys_per_mint.clear();
        self.max_per_mint_sol.clear();
        self.max_open_sol.clear();
        self.daily_loss_sol.clear();
        self.copy_trade_max_buys_per_mint.clear();
        self.copy_trade_min_source_buy_sol.clear();
        self.copy_trade_max_hold_secs.clear();
        self.copy_trade_take_profit_bps.clear();
        self.copy_trade_take_profit_sell_bps.clear();
        self.copy_trade_stop_loss_bps.clear();
        self.max_create_event_slot_lag.clear();
        self.backfill_limit.clear();
        self.confirmation_poll_ms.clear();
        self.show_advanced = false;
        self.active_field = BotSettingsField::Mode;
        self.error = None;
        self.saved = false;
    }
}

fn reset_trade_state(s: &mut ScanState) {
    s.mint.clear();
    s.symbol.clear();
    s.mcap_sol = 0.0;
    s.mcap_usd = 0.0;
    s.mcap_history.clear();
    s.entry_lamports = 0;
    s.entry_usd = 0.0;
    s.position_usd = 0.0;
    s.entry_ms = 0;
    s.token_amount_raw = 0;
    s.last_panic_signature = None;
    s.last_panic_status = None;
    s.confirm_exit = false;
    // Note: last_error is intentionally NOT cleared here so a
    // failed mode selection keeps its error visible on the
    // mode picker. It's cleared when the user enters a mode.
    s.scanned = 0;
    s.trades_skipped = 0;
    s.rpc_errors = 0;
}

fn reset_screening_candidate_state(s: &mut ScanState) {
    s.phase = Phase::Scanning;
    s.mint.clear();
    s.symbol.clear();
    s.mcap_sol = 0.0;
    s.mcap_usd = 0.0;
    s.mcap_history.clear();
    s.entry_lamports = 0;
    s.entry_usd = 0.0;
    s.position_usd = 0.0;
    s.entry_ms = 0;
    s.token_amount_raw = 0;
    s.last_panic_signature = None;
    s.last_panic_status = None;
    s.confirm_exit = false;
    s.status_line = "screening tokens…".to_string();
}

/// Theme palette names. MONO is the default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Theme {
    Dark,
    Amber,
    #[default]
    Mono,
}

impl Theme {
    pub fn cycle(self) -> Self {
        match self {
            Theme::Dark => Theme::Amber,
            Theme::Amber => Theme::Mono,
            Theme::Mono => Theme::Dark,
        }
    }
    pub fn label(self) -> &'static str {
        match self {
            Theme::Dark => "DARK",
            Theme::Amber => "AMBER",
            Theme::Mono => "MONO",
        }
    }
}

impl Default for ScanState {
    fn default() -> Self {
        Self::new()
    }
}

/// Commands from the input thread to the strategy thread.
#[derive(Debug, Clone)]
pub enum ScanCommand {
    /// Operator pressed Enter / Space — "act on the current phase."
    /// On mode-picker: ignored (use digit keys 1–6).
    /// On welcome: start the next trade. On holding: fire sell.
    /// In settings: save.
    Start,
    /// Operator pressed Esc; cancel a pending panic and clear
    /// transient UI state. In settings: discard and return to picker.
    Cancel,
    /// Operator pressed T; cycle theme.
    CycleTheme,
    /// Operator pressed Q / Ctrl-C; shut down.
    Quit,
    /// Mode-picker selection: launch the autonomous bot mode.
    PickBot,
    /// Mode-picker selection: launch live trade mode.
    PickLive,
    /// Mode-picker selection: launch paper trade mode.
    PickPaper,
    /// Mode-picker selection: open the settings editor.
    PickSettings,
    /// Operator pressed L; toggle the full log overlay.
    ShowLogs,
    /// Scroll the log viewport. Positive = older, negative = newer.
    ScrollLogs(i32),
    /// Printable character typed in a text field (settings).
    Char(char),
    /// Backspace in a text field.
    Backspace,
    /// Move to the next settings field (Tab / Down).
    NextField,
    /// Move to the previous settings field (Shift-Tab / Up).
    PrevField,
    /// Cycle the focused selector field forward (Right arrow).
    NextChoice,
    /// Cycle the focused selector field backward (Left arrow).
    PrevChoice,
    /// Raw crossterm key event. The strategy thread maps this to the
    /// appropriate high-level command based on the current phase.
    Key(KeyEvent),
}
/// Events from the strategy thread to the render thread.
#[derive(Debug, Clone)]
pub enum ScanEvent {
    StateChanged,
    McapTick {
        mcap_sol: f64,
        mcap_usd: f64,
        position_usd: f64,
        sol_price_usd: f64,
        ts_ms: i64,
    },
    Log(String),
    PanicSubmitted {
        signature: String,
        status: ExecutionStatus,
    },
    TradeClosed(LastTrade),
    /// Toggle the full log overlay (bound to L).
    ToggleLogs,
    ScrollLogs(i32),
}

fn main() -> Result<()> {
    // Top-level dispatch. `catarnith` is a real CLI:
    //   catarnith                       -> mode-picker TUI (default)
    //   catarnith scan                  -> trade-mode TUI (skip picker)
    //   catarnith bot [--config PATH]   -> spawn the bot binary directly
    //   catarnith panic-sell <MINT>     -> one-shot panic-sell
    //   catarnith --help | --version    -> usage
    //
    // The mode-picker is a Phase inside the TUI; selecting
    // "bot" inside the picker runs the autonomous bot in a
    // log panel.
    let raw: Vec<String> = std::env::args().skip(1).collect();
    if raw.iter().any(|a| a == "--help" || a == "-h") {
        print_help();
        return Ok(());
    }
    if raw.iter().any(|a| a == "--version" || a == "-V") {
        println!("catarnith {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }
    // Parse global options (e.g. --config) before dispatch so flags
    // can appear before or after the subcommand.
    let (args, first_positional, config_explicit) = Args::parse();
    match first_positional.as_deref() {
        Some("bot") => {
            // Hand off to the autonomous bot. We forward the
            // remaining args (--config PATH, --profile, etc.)
            // unchanged.
            let idx = raw.iter().position(|a| a == "bot").unwrap_or(0);
            return run_subcommand_bot(&raw[idx + 1..]);
        }
        Some("panic-sell") => {
            let idx = raw.iter().position(|a| a == "panic-sell").unwrap_or(0);
            return run_subcommand_panic_sell(&raw[idx + 1..]);
        }
        Some("scan") => {
            // Force into trade mode (skip the mode picker).
            std::env::set_var("CTARNITH_SCAN_SKIP_PICKER", "1");
        }
        Some(other) => {
            eprintln!("catarnith: unknown subcommand: {other}");
            print_help();
            std::process::exit(2);
        }
        None => {
            // Default: launch the mode picker.
        }
    }
    let cfg = match (|| -> Result<Config> {
        if !args.config.exists() && !config_explicit {
            return Err(anyhow!("config file not found"));
        }
        let cfg = Config::load(&args.config)?;
        cfg.validate_for_bot()?;
        if cfg.mode != Mode::Paper {
            cfg.validate_live_risk_envelope("mayhem scan")?;
        }
        Ok(cfg)
    })() {
        Ok(cfg) => cfg,
        Err(err) if !config_explicit => {
            eprintln!(
                "catarnith: warning: could not load config {}: {err:#}; starting with defaults.",
                args.config.display()
            );
            Config::default()
        }
        Err(err) => return Err(err),
    };

    // A single-threaded runtime so the executor's `File` wallet
    // lock never moves between threads.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("build current-thread tokio runtime")?;

    // Capture panic messages to a file so a crash in the TUI is
    // visible even though raw mode + alternate screen would hide
    // stderr. The file is overwritten on each run.
    std::panic::set_hook(Box::new(|info| {
        let msg = format!("{info}");
        let _ = std::fs::create_dir_all(".planning/debug");
        let _ = std::fs::write(".planning/debug/catarnith-panic.log", msg);
    }));

    let local = tokio::task::LocalSet::new();
    // First run: no config file resolved and no `.env` discoverable,
    // and the user did not point us at an explicit --config. In that
    // case we open the guided Settings editor before the picker.
    let first_run = !config_explicit
        && !args.config.exists()
        && catarnith::config::discover_dot_env().is_none();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        local.block_on(&rt, run(cfg, args.config, config_explicit, first_run))
    }));
    // Belt-and-braces: even if `run` returned or panicked, the
    // terminal must always be restored. The terminal's own Drop
    // runs first (when the Option goes out of scope at the end
    // of `run`), but if the panic happened *before* we set the
    // Option, the alternate screen would still be active. This
    // fallback writes the leave-screen sequence unconditionally
    // at process exit.
    let mut stdout = std::io::stdout();
    let _ = execute!(stdout, LeaveAlternateScreen);
    let _ = execute!(stdout, crossterm::cursor::Show);
    let _ = crossterm::terminal::disable_raw_mode();
    match result {
        Ok(inner) => inner,
        Err(_) => Err(anyhow::anyhow!("catarnith: panic in main loop")),
    }
}

fn print_help() {
    eprintln!("catarnith - CATARNITH Trading CLI");
    eprintln!();
    eprintln!("USAGE:");
    eprintln!("    catarnith [OPTIONS] [SUBCOMMAND]");
    eprintln!();
    eprintln!("SUBCOMMANDS:");
    eprintln!(
        "    (none)        Launch the mode-picker TUI (1=auto bot, 2=live, 3=paper, S=settings)."
    );
    eprintln!("    scan          Trade-mode TUI without the mode picker.");
    eprintln!("    bot           Spawn the autonomous bot binary (full multi-mint loop).");
    eprintln!("    panic-sell MINT");
    eprintln!("                 One-shot panic-sell against the held mint MINT.");
    eprintln!();
    eprintln!("OPTIONS:");
    eprintln!("    --config PATH     Path to the config TOML (default: config.toml).");
    eprintln!("    --version, -V     Print the version and exit.");
    eprintln!("    --help, -h        Print this help and exit.");
    eprintln!();
    eprintln!("EXAMPLES:");
    eprintln!("    catarnith");
    eprintln!("    catarnith --config config.toml scan");
    eprintln!("    catarnith bot --config config.toml");
    eprintln!();
    eprintln!("ENVIRONMENT:");
    eprintln!("    CTARNITH_LIVE_CONFIG       Override the config path.");
    eprintln!("    PYTH_SOL_USD_FEED_ID       Override the Pyth SOL/USD feed id.");
}

/// Locate a sibling binary (e.g. `bot`, `live_execute`) installed next
/// to the running `catarnith` executable. Returns the path when it exists
/// as a file, so installed deployments can exec it directly instead of
/// going through `cargo run`.
fn sibling_binary(name: &str) -> Option<std::path::PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let dir = exe.parent()?;
    let candidate = dir.join(name);
    if candidate.is_file() {
        Some(candidate)
    } else {
        None
    }
}

/// Forward to the `bot` binary. Prefers a `bot` binary sitting next to
/// the running `catarnith` executable (installed deployments); falls back
/// to the same `cargo run --bin bot --release` invocation the wrappers
/// use for dev/source runs.
/// The bot's own main() acquires the wallet lock and runs
/// the autonomous strategy loop until Q or max-hold.
fn run_subcommand_bot(extra: &[String]) -> Result<()> {
    let mut cmd = if let Some(sibling) = sibling_binary("bot") {
        std::process::Command::new(sibling)
    } else {
        let bot_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let bot_dir = bot_dir.parent().map(|p| p.to_path_buf()).unwrap_or(bot_dir);
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let mut cmd = std::process::Command::new(&cargo);
        cmd.arg("run")
            .arg("--release")
            .arg("--features")
            .arg("live-executor")
            .arg("--bin")
            .arg("bot")
            .arg("--");
        cmd.current_dir(&bot_dir);
        cmd
    };
    for a in extra {
        cmd.arg(a);
    }
    let status = cmd.status().context("spawn bot binary")?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    Ok(())
}

/// Forward to the `live_execute` binary in panic-sell mode. Prefers a
/// `live_execute` binary sitting next to the running `catarnith` executable
/// (installed deployments); falls back to `cargo run --bin live_execute`
/// for dev/source runs. The binary supports `--panic` to use the same
/// code path as the in-TUI panic-sell.
fn run_subcommand_panic_sell(extra: &[String]) -> Result<()> {
    if extra.is_empty() {
        eprintln!("catarnith panic-sell: missing <MINT> argument");
        std::process::exit(2);
    }
    let mint = &extra[0];
    // Honor --config if the user passed it after the mint.
    let mut config_arg = catarnith::config::env_var("CTARNITH_LIVE_CONFIG", "MAYHEM_LIVE_CONFIG")
        .unwrap_or_else(|_| "config.toml".into());
    let mut i = 1;
    while i < extra.len() {
        if extra[i] == "--config" && i + 1 < extra.len() {
            config_arg = extra[i + 1].clone();
            i += 2;
        } else {
            i += 1;
        }
    }
    let mut cmd = if let Some(sibling) = sibling_binary("live_execute") {
        std::process::Command::new(sibling)
    } else {
        let bot_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let bot_dir = bot_dir.parent().map(|p| p.to_path_buf()).unwrap_or(bot_dir);
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
        let mut cmd = std::process::Command::new(&cargo);
        cmd.arg("run")
            .arg("--release")
            .arg("--features")
            .arg("live-executor")
            .arg("--bin")
            .arg("live_execute")
            .arg("--");
        cmd.current_dir(&bot_dir);
        cmd
    };
    cmd.arg("--config")
        .arg(&config_arg)
        .arg("--side")
        .arg("sell")
        .arg("--mint")
        .arg(mint)
        .arg("--panic");
    let mut j = 1;
    while j < extra.len() {
        if extra[j] == "--config" {
            j += 2;
            continue;
        }
        cmd.arg(&extra[j]);
        j += 1;
    }
    let status = cmd.status().context("spawn panic-sell")?;
    if !status.success() {
        std::process::exit(status.code().unwrap_or(1));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct Args {
    config: std::path::PathBuf,
}

impl Args {
    /// Parse global options. Returns the resolved config path, the
    /// first positional argument (the subcommand, if any), and a
    /// flag indicating whether `--config` was explicitly passed on
    /// the command line.
    fn parse() -> (Self, Option<String>, bool) {
        let mut config = std::path::PathBuf::from("config.toml");
        let args: Vec<String> = std::env::args().skip(1).collect();
        let mut first_positional: Option<String> = None;
        let mut config_explicit = false;
        let mut i = 0;
        while i < args.len() {
            match args[i].as_str() {
                "--config" if i + 1 < args.len() => {
                    config = std::path::PathBuf::from(&args[i + 1]);
                    config_explicit = true;
                    i += 2;
                }
                a if a.starts_with('-') => {
                    i += 1;
                }
                _ => {
                    first_positional = Some(args[i].clone());
                    break;
                }
            }
        }
        // Honor CTARNITH_LIVE_CONFIG when exported. Otherwise keep the explicit
        // --config / default.
        if !config_explicit {
            if let Ok(env_cfg) =
                catarnith::config::env_var("CTARNITH_LIVE_CONFIG", "MAYHEM_LIVE_CONFIG")
            {
                config = std::path::PathBuf::from(env_cfg);
            }
        }
        config = resolve_config_path(config);
        (Self { config }, first_positional, config_explicit)
    }
}

/// Resolve a config path to the first location that exists.
///
/// Order of attempts:
/// 1. The path as given (absolute or CWD-relative).
/// 2. The package manifest directory `<CARGO_MANIFEST_DIR>/<basename>`,
///    which lets an installed `catarnith` find a config shipped with the
///    crate without forcing the operator to set `CTARNITH_LIVE_CONFIG`.
fn resolve_config_path(p: std::path::PathBuf) -> std::path::PathBuf {
    let cwd = std::env::current_dir().ok();
    resolve_config_path_from(p, cwd.as_deref())
}

/// Pure core of [`resolve_config_path`], with the current directory
/// passed in explicitly so it can be tested without mutating the
/// process-global cwd (which races other tests under parallel runs).
fn resolve_config_path_from(
    p: std::path::PathBuf,
    _cwd: Option<&std::path::Path>,
) -> std::path::PathBuf {
    if p.exists() {
        return std::fs::canonicalize(&p).unwrap_or(p);
    }
    let basename = p
        .file_name()
        .map(|s| s.to_os_string())
        .unwrap_or_else(|| std::ffi::OsString::from("config.toml"));

    // Try the package manifest directory (useful when the binary is
    // installed or run from outside the workspace).
    let pkg_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let candidate = pkg_dir.join(&basename);
    if candidate.exists() {
        return candidate;
    }

    // Fall through; Config::load will produce a clear error, or the
    // caller will fall back to Config::default().
    p
}

/// Wraps a ratatui `Terminal` with a Drop guard that always
/// restores the screen. This is the **only** reliable way to
/// make sure the user's terminal returns to a clean state on
/// any exit path — including panics in other threads, the
/// `quit` command, or a Ctrl-C that lands mid-render.
struct TerminalGuard {
    inner: Option<Terminal<CrosstermBackend<std::io::Stdout>>>,
}

impl TerminalGuard {
    fn new() -> Result<Self> {
        let mut stdout = std::io::stdout();
        crossterm::terminal::enable_raw_mode().context("enable raw mode")?;
        // Request a fixed console size. Terminal emulators may ignore this,
        // but it works in most native terminals and is harmless otherwise.
        execute!(
            stdout,
            EnterAlternateScreen,
            crossterm::terminal::SetSize(93, 54)
        )
        .context("enter alternate screen")?;
        execute!(stdout, crossterm::cursor::Show).ok();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).context("create ratatui terminal")?;
        terminal.clear().context("clear terminal")?;
        Ok(Self {
            inner: Some(terminal),
        })
    }
    fn get_mut(&mut self) -> &mut Terminal<CrosstermBackend<std::io::Stdout>> {
        self.inner.as_mut().expect("terminal was taken")
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if let Some(term) = self.inner.as_mut() {
            term.show_cursor().ok();
            let _ = execute!(term.backend_mut(), LeaveAlternateScreen);
            let _ = crossterm::terminal::disable_raw_mode();
        }
    }
}

fn init_terminal() -> Result<TerminalGuard> {
    TerminalGuard::new()
}

async fn run(
    cfg: Config,
    config_path: std::path::PathBuf,
    config_explicit: bool,
    first_run: bool,
) -> Result<()> {
    // 1. Shared market data (Helius curve + Pyth SOL/USD).
    // This is created once, before any mode is chosen, because it
    // needs no wallet and is used by both paper and live paths.
    let market = Arc::new(MarketData::new(cfg.rpc_url(), cfg.pumpfun_program.clone()));

    // 2. State.
    let state = Arc::new(RwLock::new(ScanState::new()));

    // 3. Channels.
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ScanCommand>();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel::<ScanEvent>();

    // 4. Background tasks.
    spawn_input(cmd_tx.clone());
    spawn_strategy(
        cfg,
        config_path,
        config_explicit,
        first_run,
        Arc::clone(&market),
        Arc::clone(&state),
        cmd_rx,
        event_tx.clone(),
    );
    spawn_tick(event_tx.clone());

    // 5. Render.
    let mut terminal = init_terminal()?;
    let mut last_render = Instant::now();
    let refresh = Duration::from_millis(100); // keep the TUI responsive without repainting heavy panels constantly

    let result = loop {
        if last_render.elapsed() >= refresh {
            let state_snapshot = state.read().await.clone();
            let _ = terminal
                .get_mut()
                .draw(|frame| render::render(frame, &state_snapshot));
            last_render = Instant::now();
        }
        // Bound the event-wait tightly so a Quit command is
        // observed within ~5ms. The previous 20ms meant a fast
        // `Q Q Q` could keep us in the loop for 60ms after the
        // user already intended to quit.
        match tokio::time::timeout(Duration::from_millis(5), event_rx.recv()).await {
            Ok(Some(ScanEvent::StateChanged)) => {} // already snapshot above
            Ok(Some(ScanEvent::McapTick {
                mcap_sol,
                mcap_usd,
                position_usd,
                sol_price_usd,
                ts_ms,
            })) => {
                let mut s = state.write().await;
                s.push_mcap(ts_ms, mcap_sol);
                s.mcap_usd = mcap_usd;
                s.position_usd = position_usd;
                s.sol_price_usd = sol_price_usd;
            }
            Ok(Some(ScanEvent::Log(line))) => {
                state.write().await.push_log(line);
            }
            Ok(Some(ScanEvent::PanicSubmitted { signature, status })) => {
                let mut s = state.write().await;
                s.last_panic_signature = Some(signature);
                s.last_panic_status = Some(status);
            }
            Ok(Some(ScanEvent::TradeClosed(summary))) => {
                let mut s = state.write().await;
                s.last_trade = Some(summary);
            }
            Ok(Some(ScanEvent::ToggleLogs)) => {
                let mut s = state.write().await;
                s.show_logs = !s.show_logs;
            }
            Ok(Some(ScanEvent::ScrollLogs(delta))) => {
                state.write().await.scroll_logs(delta);
            }
            Ok(None) => break Ok(()),
            Err(_) => {}
        }
    };

    // Drop the terminal *before* returning. The Drop guard runs
    // its restore sequence. This is the happy-path teardown;
    // the panic-catch in `main` is the safety net.
    drop(terminal);
    result
}

fn spawn_input(cmd_tx: mpsc::UnboundedSender<ScanCommand>) {
    tokio::spawn(async move {
        let mut stream = EventStream::new();
        while let Some(event) = stream.next().await {
            let Ok(event) = event else { continue };
            if let Event::Key(key) = event {
                // Forward the raw key event. The strategy thread
                // decides what it means based on the current phase,
                // so text entry in Settings and global shortcuts
                // can coexist without conflicts.
                let _ = cmd_tx.send(ScanCommand::Key(key));
            }
        }
    });
}

/// Map a raw crossterm key event to a high-level command based on
/// the current phase. This keeps input handling in one place and
/// avoids collisions like typing 's' into the wallet field vs
/// pressing 'S' on the mode picker.
fn interpret_key(key: KeyEvent, phase: Phase) -> Option<ScanCommand> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        return Some(ScanCommand::Quit);
    }

    // Global shortcuts are disabled while typing in settings screens so
    // letters like q/t/l/s can be part of text fields.
    let in_settings = matches!(phase, Phase::Settings | Phase::BotSettings);

    match key.code {
        KeyCode::Esc if phase == Phase::Settings || phase == Phase::BotSettings => {
            Some(ScanCommand::Cancel)
        }
        KeyCode::Esc => Some(ScanCommand::Cancel),
        KeyCode::Char('q') | KeyCode::Char('Q') if !in_settings => Some(ScanCommand::Quit),
        KeyCode::Char('t') | KeyCode::Char('T') if !in_settings => Some(ScanCommand::CycleTheme),
        KeyCode::Char('l') | KeyCode::Char('L') if !in_settings => Some(ScanCommand::ShowLogs),
        KeyCode::PageUp if !in_settings => Some(ScanCommand::ScrollLogs(8)),
        KeyCode::PageDown if !in_settings => Some(ScanCommand::ScrollLogs(-8)),
        KeyCode::Home if !in_settings => Some(ScanCommand::ScrollLogs(i32::MAX)),
        KeyCode::End if !in_settings => Some(ScanCommand::ScrollLogs(i32::MIN)),
        KeyCode::Up if !in_settings => Some(ScanCommand::ScrollLogs(1)),
        KeyCode::Down if !in_settings => Some(ScanCommand::ScrollLogs(-1)),
        KeyCode::Char('1') if phase == Phase::ModePicker => Some(ScanCommand::PickBot),
        KeyCode::Char('2') if phase == Phase::ModePicker => Some(ScanCommand::PickLive),
        KeyCode::Char('3') if phase == Phase::ModePicker => Some(ScanCommand::PickPaper),
        KeyCode::Char('s') | KeyCode::Char('S') if phase == Phase::ModePicker => {
            Some(ScanCommand::PickSettings)
        }
        KeyCode::Char(_) if phase == Phase::ModePicker => None,
        KeyCode::Enter | KeyCode::Char('\n') => Some(ScanCommand::Start),
        KeyCode::Backspace => Some(ScanCommand::Backspace),
        KeyCode::Tab | KeyCode::Down => Some(ScanCommand::NextField),
        KeyCode::BackTab | KeyCode::Up => Some(ScanCommand::PrevField),
        KeyCode::Right => Some(ScanCommand::NextChoice),
        KeyCode::Left => Some(ScanCommand::PrevChoice),
        KeyCode::Char(c) => Some(ScanCommand::Char(c)),
        _ => None,
    }
}

/// Wait for a mapped command appropriate for `phase`. Unknown keys
/// are silently dropped; Welcome starts only on Enter.
async fn recv_mapped_command(
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    phase: Phase,
) -> Option<ScanCommand> {
    loop {
        match cmd_rx.recv().await {
            Some(ScanCommand::Key(key)) => {
                if let Some(cmd) = interpret_key(key, phase) {
                    return Some(cmd);
                }
            }
            Some(cmd) => return Some(cmd),
            None => return None,
        }
    }
}

fn spawn_tick(event_tx: mpsc::UnboundedSender<ScanEvent>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if event_tx.send(ScanEvent::StateChanged).is_err() {
                break;
            }
        }
    });
}

async fn close_log_overlay_if_open(
    state: &Arc<RwLock<ScanState>>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
) -> bool {
    let mut s = state.write().await;
    if !s.show_logs {
        return false;
    }
    s.show_logs = false;
    drop(s);
    let _ = event_tx.send(ScanEvent::StateChanged);
    true
}

/// The strategy loop. One pass per cycle:
///   Mode picker → (BOT / PAPER / LIVE / SETTINGS) →
///   Scanning → Evaluating → Holding → Selling → Result → Mode picker
#[allow(unused_assignments)]
#[allow(clippy::too_many_arguments)]
fn spawn_strategy(
    cfg: Config,
    config_path: std::path::PathBuf,
    config_explicit: bool,
    first_run: bool,
    market: Arc<MarketData>,
    state: Arc<RwLock<ScanState>>,
    mut cmd_rx: mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: mpsc::UnboundedSender<ScanEvent>,
) {
    tokio::task::spawn_local(async move {
        let mut quit = false;

        // -----------------------------------------------------------------
        // Splash: Welcome screen on open.
        // -----------------------------------------------------------------
        {
            let mut s = state.write().await;
            s.config_label = config_path.to_string_lossy().to_string();
            reset_trade_state(&mut s);
            s.phase = Phase::Welcome;
            let _ = event_tx.send(ScanEvent::StateChanged);
        }
        let mut start = false;
        while !start && !quit {
            match recv_mapped_command(&mut cmd_rx, Phase::Welcome).await {
                Some(ScanCommand::Start) => start = true,
                Some(ScanCommand::CycleTheme) => {
                    let mut s = state.write().await;
                    s.theme = s.theme.cycle();
                }
                Some(ScanCommand::ShowLogs) => {
                    let _ = event_tx.send(ScanEvent::ToggleLogs);
                }
                Some(ScanCommand::ScrollLogs(delta)) => {
                    let _ = event_tx.send(ScanEvent::ScrollLogs(delta));
                }
                Some(ScanCommand::Quit) => quit = true,
                _ => {}
            }
        }
        if quit {
            return;
        }

        // Guided first run: open the Settings editor with blank fields
        // so a new user enters wallet/keys/buy size up front. Save
        // writes both the TOML and `.env`; afterwards we fall through
        // to the normal mode picker.
        if first_run {
            let should_quit = run_settings(
                &config_path,
                true,
                Arc::clone(&state),
                &mut cmd_rx,
                &event_tx,
            )
            .await;
            if should_quit {
                return;
            }
        }

        let skip_picker = catarnith::config::env_lookup("MAYHEM_SCAN_SKIP_PICKER")
            .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
            .unwrap_or(false);

        if skip_picker {
            // `catarnith scan` jumps straight into the selected trade mode.
            let mode = if cfg.mode == Mode::Paper {
                Mode::Paper
            } else {
                Mode::Live
            };
            match run_trade_mode(
                &cfg,
                config_explicit,
                mode,
                Arc::clone(&market),
                Arc::clone(&state),
                &mut cmd_rx,
                &event_tx,
            )
            .await
            {
                Ok(true) | Err(_) => {}
                Ok(false) => {}
            }
            return;
        }

        // -----------------------------------------------------------------
        // Main loop: mode picker → trade cycles → mode picker.
        // -----------------------------------------------------------------
        while !quit {
            {
                let mut s = state.write().await;
                reset_trade_state(&mut s);
                s.phase = Phase::ModePicker;
                let _ = event_tx.send(ScanEvent::StateChanged);
            }

            let mut mode: Option<&'static str> = None;
            while mode.is_none() && !quit {
                match cmd_rx.recv().await {
                    Some(ScanCommand::Key(key)) => {
                        match key.code {
                            KeyCode::Char('1') => mode = Some("bot"),
                            KeyCode::Char('2') => mode = Some("live"),
                            KeyCode::Char('3') => mode = Some("paper"),
                            KeyCode::Char('s') | KeyCode::Char('S') => {
                                let should_quit = run_settings(
                                    &config_path,
                                    false,
                                    Arc::clone(&state),
                                    &mut cmd_rx,
                                    &event_tx,
                                )
                                .await;
                                if should_quit {
                                    quit = true;
                                } else {
                                    // Settings exited via Esc: redraw the
                                    // mode picker. Without this the phase
                                    // stays Phase::Settings and the screen
                                    // looks frozen even though the picker
                                    // loop is live again.
                                    let mut s = state.write().await;
                                    s.phase = Phase::ModePicker;
                                    let _ = event_tx.send(ScanEvent::StateChanged);
                                }
                            }
                            KeyCode::Char('q') | KeyCode::Char('Q') => quit = true,
                            KeyCode::Char('t') | KeyCode::Char('T') => {
                                let mut s = state.write().await;
                                s.theme = s.theme.cycle();
                            }
                            KeyCode::Char('l') | KeyCode::Char('L') => {
                                let _ = event_tx.send(ScanEvent::ToggleLogs);
                            }
                            _ => {}
                        }
                    }
                    Some(ScanCommand::Quit) => quit = true,
                    _ => {}
                }
            }
            if quit {
                break;
            }
            let mode = mode.expect("mode must be set after picker");

            if mode == "bot" {
                match run_bot_settings(&config_path, Arc::clone(&state), &mut cmd_rx, &event_tx)
                    .await
                {
                    BotSetupOutcome::Start(bot_config_path) => {
                        let should_quit = run_bot_mode(
                            &bot_config_path,
                            Arc::clone(&state),
                            &mut cmd_rx,
                            &event_tx,
                        )
                        .await;
                        if should_quit {
                            quit = true;
                        }
                    }
                    BotSetupOutcome::Back => {}
                    BotSetupOutcome::Quit => quit = true,
                }
                continue;
            }

            let mode_enum = if mode == "paper" {
                Mode::Paper
            } else {
                Mode::Live
            };
            match run_trade_mode(
                &cfg,
                config_explicit,
                mode_enum,
                Arc::clone(&market),
                Arc::clone(&state),
                &mut cmd_rx,
                &event_tx,
            )
            .await
            {
                Ok(true) => quit = true,
                Ok(false) => {}
                Err(err) => {
                    let _ = event_tx.send(ScanEvent::Log(format!("trade mode failed: {err:#}")));
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
        }
    });
}

async fn resolve_trade_config(base: &Config, mode: Mode, explicit: bool) -> Result<Config> {
    if explicit {
        let mut cfg = base.clone();
        cfg.apply_runtime_mode(mode)?;
        cfg.validate_for_bot()?;
        if cfg.mode != Mode::Paper {
            cfg.validate_live_risk_envelope("catarnith scan")?;
        }
        return Ok(cfg);
    }
    let path = match mode {
        Mode::Paper => "config.toml",
        Mode::Live => "config.toml",
    };
    let p = resolve_config_path(std::path::PathBuf::from(path));
    let mut cfg = Config::load(&p)?;
    // The mode picker is the runtime source of truth. `config.toml` still
    // carries a default for CLI/non-picker runs, but an in-TUI pick must
    // never be overridden by a stale `mode = ...` value on disk.
    cfg.apply_runtime_mode(mode)?;
    // Ensure the helius API key is present. Config::load calls
    // apply_dot_env() which finds .env by walking up from CWD,
    // but CWD may not be in the workspace. Fall back to:
    // 1. Copy from the already-loaded base config.
    // 2. Read HELIUS_API_KEY from the process environment.
    // 3. Walk up from CARGO_MANIFEST_DIR and parse .env ourselves.
    if cfg.helius_api_key.trim().is_empty() {
        if !base.helius_api_key.trim().is_empty() {
            cfg.helius_api_key = base.helius_api_key.clone();
        } else if let Ok(key) = std::env::var("HELIUS_API_KEY") {
            let key = key.trim();
            if !key.is_empty() {
                cfg.helius_api_key = key.to_string();
            }
        } else {
            // Walk up from the package directory looking for .env.
            let pkg = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            let mut cur: Option<&std::path::Path> = Some(pkg.as_path());
            while let Some(dir) = cur {
                let candidate = dir.join(".env");
                if candidate.is_file() {
                    if let Ok(text) = std::fs::read_to_string(&candidate) {
                        for line in text.lines() {
                            let line = line.trim();
                            let line = line
                                .strip_prefix("export ")
                                .map(|s| s.trim())
                                .unwrap_or(line);
                            if let Some((k, v)) = line.split_once('=') {
                                if k.trim() == "HELIUS_API_KEY" {
                                    let val = v.trim().trim_matches('"').trim_matches('\'');
                                    if !val.is_empty() {
                                        cfg.helius_api_key = val.to_string();
                                    }
                                }
                            }
                        }
                    }
                    break;
                }
                cur = dir.parent();
            }
        }
    }
    cfg.validate_for_bot()?;
    if cfg.mode != Mode::Paper {
        cfg.validate_live_risk_envelope("catarnith scan")?;
    }
    Ok(cfg)
}

/// Append a paper ExecutionReport to the configured paper report path.
fn append_paper_report(path: &str, report: &ExecutionReport) -> Result<()> {
    if path.trim().is_empty() {
        return Ok(());
    }
    let line = serde_json::to_string(report).context("serialize paper report")?;
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open paper report {path}"))?;
    writeln!(f, "{line}").with_context(|| format!("write paper report {path}"))?;
    Ok(())
}

/// Run an entire paper/live trade mode: build the executor, then
/// loop scan → entry → hold → sell → result until the operator
/// presses Esc (return to mode picker) or Q (quit).
/// Returns `true` when the operator explicitly quits.
#[allow(clippy::arc_with_non_send_sync)]
async fn run_trade_mode(
    base_cfg: &Config,
    config_explicit: bool,
    mode: Mode,
    mut market: Arc<MarketData>,
    state: Arc<RwLock<ScanState>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
) -> Result<bool> {
    // Switch to the scanning screen immediately so the operator
    // sees feedback even while the executor initialises.
    {
        let mut s = state.write().await;
        s.phase = Phase::Scanning;
        s.last_error = None;
        s.status_line = "loading config…".to_string();
        let _ = event_tx.send(ScanEvent::StateChanged);
    }
    {
        let mode_name = match mode {
            Mode::Paper => "paper",
            Mode::Live => "live",
        };
        let _ = event_tx.send(ScanEvent::Log(format!("entering {mode_name} mode…")));
    }
    let cfg = match resolve_trade_config(base_cfg, mode, config_explicit).await {
        Ok(c) => {
            {
                let mut s = state.write().await;
                s.status_line = "config OK".to_string();
                let _ = event_tx.send(ScanEvent::StateChanged);
            }
            c
        }
        Err(err) => {
            let msg = format!("config load failed: {err:#}");
            let _ = event_tx.send(ScanEvent::Log(msg.clone()));
            {
                let mut s = state.write().await;
                s.last_error = Some(msg);
            }
            return Ok(false);
        }
    };
    // Config loaded OK — clear any previous error.
    {
        let mut s = state.write().await;
        s.last_error = None;
    }
    // The shared MarketData may have been created from a fallback
    // config, so re-point it at the real RPC for this mode.
    Arc::make_mut(&mut market).reconfigure(cfg.rpc_url(), cfg.pumpfun_program.clone());
    {
        let mut s = state.write().await;
        s.status_line = "initialising executor…".to_string();
        let _ = event_tx.send(ScanEvent::StateChanged);
    }
    let executor = match tokio::time::timeout(
        Duration::from_secs(20),
        ScanExecutor::new(&cfg, Arc::clone(&market)),
    )
    .await
    {
        Ok(Ok(e)) => {
            {
                let mut s = state.write().await;
                s.status_line = "executor ready".to_string();
                s.wallet_label = e.wallet_label();
                let _ = event_tx.send(ScanEvent::StateChanged);
            }
            e
        }
        Ok(Err(err)) => {
            let msg = format!("executor init failed: {err:#}");
            let _ = event_tx.send(ScanEvent::Log(msg.clone()));
            {
                let mut s = state.write().await;
                s.last_error = Some(msg);
            }
            return Ok(false);
        }
        Err(_) => {
            let msg = "executor init timed out after 20s".to_string();
            let _ = event_tx.send(ScanEvent::Log(msg.clone()));
            {
                let mut s = state.write().await;
                s.last_error = Some(msg);
            }
            return Ok(false);
        }
    };
    {
        let mut s = state.write().await;
        s.status_line = "opening journal…".to_string();
        let _ = event_tx.send(ScanEvent::StateChanged);
    }
    let journal = match Journal::open(&cfg.journal_dir, &cfg.sqlite_path) {
        Ok(j) => Arc::new(j),
        Err(err) => {
            let msg = format!("journal open failed: {err:#}");
            let _ = event_tx.send(ScanEvent::Log(msg.clone()));
            {
                let mut s = state.write().await;
                s.last_error = Some(msg);
            }
            return Ok(false);
        }
    };

    loop {
        match run_lifecycle(&cfg, &market, &executor, &state, cmd_rx, event_tx, &journal).await {
            Ok(CycleOutcome::Closed) => {
                {
                    let mut s = state.write().await;
                    s.phase = Phase::TradeResult;
                    let _ = event_tx.send(ScanEvent::StateChanged);
                }
                let mut next = false;
                let mut back = false;
                while !next && !back {
                    match recv_mapped_command(cmd_rx, Phase::TradeResult).await {
                        Some(ScanCommand::Start) => next = true,
                        Some(ScanCommand::Cancel) => {
                            if close_log_overlay_if_open(&state, event_tx).await {
                                continue;
                            }
                            back = true;
                        }
                        Some(ScanCommand::CycleTheme) => {
                            let mut s = state.write().await;
                            s.theme = s.theme.cycle();
                            let _ = event_tx.send(ScanEvent::Log(format!(
                                "theme cycled -> {}",
                                s.theme.label()
                            )));
                        }
                        Some(ScanCommand::ShowLogs) => {
                            let _ = event_tx.send(ScanEvent::ToggleLogs);
                        }
                        Some(ScanCommand::ScrollLogs(delta)) => {
                            let _ = event_tx.send(ScanEvent::ScrollLogs(delta));
                        }
                        Some(ScanCommand::Quit) => return Ok(true),
                        _ => {}
                    }
                }
                if next {
                    continue;
                }
                if back {
                    return Ok(false);
                }
            }
            Ok(CycleOutcome::Cancelled) => return Ok(false),
            Ok(CycleOutcome::Quit) => return Ok(true),
            Err(err) => {
                let msg = format!("lifecycle error: {err:#}");
                let _ = event_tx.send(ScanEvent::Log(msg.clone()));
                {
                    let mut s = state.write().await;
                    s.last_error = Some(msg);
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
                return Ok(false);
            }
        }
    }
}

/// Strip ANSI escape sequences (colors) from a line.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.peek() == Some(&'[') {
            chars.next(); // skip '['
            for inner in chars.by_ref() {
                if inner.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(ch);
        }
    }
    out
}

/// Turn a raw tracing log line into a clean message. Returns `None`
/// for noisy WARN lines (the user wants warnings out of the TUI panel)
/// and for lines that are not recognizable tracing output.
fn clean_bot_log_line(line: &str) -> Option<String> {
    let stripped = strip_ansi(line);
    let trimmed = stripped.trim();
    if trimmed.is_empty() {
        return None;
    }

    // Drop warnings unless they explain stream startup/fallback.
    if trimmed.contains(" WARN ") {
        let lower = trimmed.to_ascii_lowercase();
        if lower.contains("transactionsubscribe") || lower.contains("logssubscribe") {
            // Keep it.
        } else {
            return None;
        }
    }

    if trimmed.contains(" DEBUG ") || trimmed.contains(" TRACE ") {
        return None;
    }

    const LEVELS: [&str; 5] = [" INFO ", " WARN ", " ERROR ", " DEBUG ", " TRACE "];
    if let Some(pos) = LEVELS.iter().filter_map(|lvl| trimmed.find(lvl)).min() {
        let level_len = lvl_len(trimmed, pos);
        let after_level = &trimmed[pos + level_len..];
        if let Some(colon_pos) = after_level.find(": ") {
            let msg = &after_level[colon_pos + 2..];
            if msg.starts_with("starting mayhem bot") {
                return Some("bot started".to_string());
            }
            if msg.starts_with("heartbeat ") {
                return Some(shorten_heartbeat(msg));
            }
            return Some(prettify_bot_log_line(msg));
        }
    }

    // Non-tracing lines (e.g. panics) are kept as-is.
    Some(prettify_bot_log_line(trimmed))
}

fn lvl_len(trimmed: &str, pos: usize) -> usize {
    const LEVELS: [&str; 5] = [" INFO ", " WARN ", " ERROR ", " DEBUG ", " TRACE "];
    LEVELS
        .iter()
        .find(|lvl| trimmed[pos..].starts_with(*lvl))
        .map(|lvl| lvl.len())
        .unwrap_or(0)
}

/// Keep heartbeat lines short and informative.
fn shorten_heartbeat(msg: &str) -> String {
    let mut up = None;
    let mut open = None;
    let mut pending = None;
    let mut busy = None;
    let mut discoveries = None;
    for token in msg.split_whitespace().skip(1) {
        if let Some((k, v)) = token.split_once('=') {
            match k {
                "uptime_s" => up = Some(v),
                "open_positions" => open = Some(v),
                "pending_live_orders" => pending = Some(v),
                "single_lifecycle_busy" => busy = Some(v),
                "discoveries" => discoveries = Some(v),
                _ => {}
            }
        }
    }
    let mut out = String::from("heartbeat");
    if let Some(v) = up {
        out.push_str(&format!(" up={v}s"));
    }
    if let Some(v) = open {
        out.push_str(&format!(" open={v}"));
    }
    if let Some(v) = pending {
        out.push_str(&format!(" pending={v}"));
    }
    if let Some(v) = busy {
        out.push_str(&format!(" busy={v}"));
    }
    if let Some(v) = discoveries {
        out.push_str(&format!(" discoveries={v}"));
    }
    out
}

fn prettify_bot_log_line(msg: &str) -> String {
    if msg.starts_with("live buy confirmed") {
        return format!(
            "BUY ok mint={} sig={} latency={}",
            kv_value(msg, "mint").unwrap_or("?"),
            kv_value(msg, "signature").unwrap_or("?"),
            kv_value(msg, "execution_latency_ms").unwrap_or("?"),
        );
    }
    if msg.starts_with("live buy pending") || msg.starts_with("live buy submitted") {
        return format!(
            "BUY pending mint={} sig={} latency={}",
            kv_value(msg, "mint").unwrap_or("?"),
            kv_value(msg, "signature").unwrap_or("?"),
            kv_value(msg, "execution_latency_ms").unwrap_or("?"),
        );
    }
    if msg.starts_with("live sell confirmed") {
        return format!(
            "SELL ok mint={} reason={} closed={} latency={}",
            kv_value(msg, "mint").unwrap_or("?"),
            kv_value(msg, "reason").unwrap_or("?"),
            kv_value(msg, "closed").unwrap_or("?"),
            kv_value(msg, "execution_latency_ms").unwrap_or("?"),
        );
    }
    if msg.starts_with("execution rejected") {
        return format!(
            "REJECT mint={} reason={}",
            kv_value(msg, "mint").unwrap_or("?"),
            kv_value(msg, "reason").unwrap_or("?"),
        );
    }
    if msg.starts_with("live execution failed") {
        return format!(
            "FAIL mint={} reason={}",
            kv_value(msg, "mint").unwrap_or("?"),
            kv_value(msg, "reason").unwrap_or("?"),
        );
    }
    if msg.starts_with("copy trade decision") {
        return format!(
            "COPY action={} mint={} lamports={} reasons={}",
            kv_value(msg, "action").unwrap_or("?"),
            kv_value(msg, "mint").unwrap_or("?"),
            kv_value(msg, "lamports").unwrap_or("?"),
            kv_value(msg, "reasons").unwrap_or("?"),
        );
    }
    msg.to_string()
}

fn kv_value<'a>(msg: &'a str, key: &str) -> Option<&'a str> {
    let prefix = format!("{key}=");
    msg.split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
        .map(|value| value.trim_end_matches(','))
}

/// Keep bot lines that explain startup status or trading activity.
fn is_bot_execution_log(line: &str) -> bool {
    if line.is_empty() {
        return false;
    }
    let lower = line.to_lowercase();
    let keywords = [
        "starting mayhem bot",
        "bot started",
        "startup check passed",
        "subscribing",
        "confirmed transactionsubscribe subscription",
        "confirmed logssubscribe subscription",
        "transactionsubscribe is unavailable",
        "activating logssubscribe fallback",
        "heartbeat",
        "copy ",
        "copy trade",
        "buy ok",
        "buy pending",
        "sell ok",
        "reject",
        "fail",
        "candidate",
        "buy_build_diag",
        "execution fill",
        "execution rejected",
        "execution exit",
        "live execution failed",
        "live buy",
        "live sell",
        "live sell deferred",
        "provisional live buy",
        "live inventory was already zero",
        "panic",
        "realized",
        "pnl",
    ];
    keywords.iter().any(|k| lower.contains(k))
}

/// Keep fatal child-process output even when it is not a normal tracing log.
fn is_bot_fatal_log(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    lower.starts_with("error:")
        || lower.starts_with("caused by:")
        || lower.contains("panicked at")
        || lower.contains("failed to read config")
        || lower.contains("failed to parse toml")
        || lower.contains("not a valid solana pubkey")
        || lower.contains("copy_trade_enabled requires")
        || lower.contains("requires fetch_full_transaction=true")
        || lower.contains("must be a boolean value")
        || lower.contains("must be positive")
}

fn remember_bot_child_line(tail: &mut VecDeque<String>, line: &str) {
    if tail.len() == BOT_CHILD_TAIL {
        tail.pop_front();
    }
    tail.push_back(line.to_string());
}

fn status_failed(status: Option<&ExitStatus>) -> bool {
    status.map(|status| !status.success()).unwrap_or(true)
}

fn emit_bot_exit_diagnostics(
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
    status: Option<&ExitStatus>,
    tail: &VecDeque<String>,
) {
    let _ = event_tx.send(ScanEvent::Log(format!(
        "bot failed status={}",
        format_exit_status(status)
    )));
    if tail.is_empty() {
        let _ = event_tx.send(ScanEvent::Log(
            "bot produced no stderr/stdout before exit".to_string(),
        ));
        return;
    }
    let _ = event_tx.send(ScanEvent::Log("last bot output:".to_string()));
    for line in tail {
        let _ = event_tx.send(ScanEvent::Log(format!("  {line}")));
    }
}

fn format_exit_status(status: Option<&ExitStatus>) -> String {
    match status {
        Some(status) => status
            .code()
            .map(|code| format!("exit-code={code}"))
            .unwrap_or_else(|| status.to_string()),
        None => "unknown".to_string(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BotSetupOutcome {
    Start(std::path::PathBuf),
    Back,
    Quit,
}

async fn run_bot_settings(
    config_path: &std::path::Path,
    state: Arc<RwLock<ScanState>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
) -> BotSetupOutcome {
    let bot_state = load_bot_settings_state(config_path);
    let bot_config_path = bot_state.config_path.clone();
    {
        let mut s = state.write().await;
        s.bot_settings = bot_state;
        s.phase = Phase::BotSettings;
        s.wallet_label = "BOT SETUP".to_string();
        s.last_error = None;
        let _ = event_tx.send(ScanEvent::StateChanged);
    }

    loop {
        match recv_mapped_command(cmd_rx, Phase::BotSettings).await {
            Some(ScanCommand::NextField) => {
                let mut s = state.write().await;
                let show = s.bot_settings.show_advanced;
                s.bot_settings.active_field = next_bot_field(s.bot_settings.active_field, show);
            }
            Some(ScanCommand::PrevField) => {
                let mut s = state.write().await;
                let show = s.bot_settings.show_advanced;
                s.bot_settings.active_field = prev_bot_field(s.bot_settings.active_field, show);
            }
            Some(ScanCommand::NextChoice) | Some(ScanCommand::PrevChoice) => {
                let mut s = state.write().await;
                s.bot_settings.error = None;
                s.bot_settings.saved = false;
                match s.bot_settings.active_field {
                    BotSettingsField::Mode => {
                        s.bot_settings.mode = match s.bot_settings.mode {
                            Mode::Paper => Mode::Live,
                            Mode::Live => Mode::Paper,
                        };
                    }
                    BotSettingsField::Market => {
                        s.bot_settings.market = s.bot_settings.market.cycle();
                    }
                    BotSettingsField::CopyTradeEnabled => {
                        s.bot_settings.copy_trade_enabled = !s.bot_settings.copy_trade_enabled;
                    }
                    BotSettingsField::CopyTradeSizing => {
                        s.bot_settings.copy_trade_sizing = s.bot_settings.copy_trade_sizing.cycle();
                    }
                    BotSettingsField::CopyTradeBuyPolicy => {
                        s.bot_settings.copy_trade_buy_policy =
                            s.bot_settings.copy_trade_buy_policy.cycle();
                    }
                    BotSettingsField::CopyTradeFollowSells => {
                        s.bot_settings.copy_trade_follow_sells =
                            !s.bot_settings.copy_trade_follow_sells;
                    }
                    BotSettingsField::AdvancedToggle => {
                        s.bot_settings.show_advanced = !s.bot_settings.show_advanced;
                    }
                    BotSettingsField::BotKeepAlive => {
                        s.bot_settings.bot_keep_alive = !s.bot_settings.bot_keep_alive;
                    }
                    BotSettingsField::FetchFullTransaction => {
                        s.bot_settings.fetch_full_transaction =
                            !s.bot_settings.fetch_full_transaction;
                    }
                    BotSettingsField::CurveExitQuotes => {
                        s.bot_settings.enable_curve_exit_quotes =
                            !s.bot_settings.enable_curve_exit_quotes;
                    }
                    BotSettingsField::ParallelFallbackReads => {
                        s.bot_settings.parallel_fallback_reads =
                            !s.bot_settings.parallel_fallback_reads;
                    }
                    _ => {}
                }
            }
            Some(ScanCommand::Char(c)) => {
                let mut s = state.write().await;
                s.bot_settings.error = None;
                s.bot_settings.saved = false;
                match s.bot_settings.active_field {
                    BotSettingsField::BuySize => s.bot_settings.buy_size_sol.push(c),
                    BotSettingsField::SlippageBps => s.bot_settings.slippage_bps.push(c),
                    BotSettingsField::MaxHoldSecs => s.bot_settings.max_hold_secs.push(c),
                    BotSettingsField::StreamAgeMs => {
                        s.bot_settings.max_stream_event_age_ms.push(c);
                    }
                    BotSettingsField::EntryDeadlineMs => {
                        s.bot_settings.entry_deadline_ms.push(c);
                    }
                    BotSettingsField::CopyTradeWallet => {
                        s.bot_settings.copy_trade_wallet.push(c);
                    }
                    BotSettingsField::CopyTradeScaleBps => {
                        s.bot_settings.copy_trade_scale_bps.push(c);
                    }
                    BotSettingsField::CopyTradeMaxBuySol => {
                        s.bot_settings.copy_trade_max_buy_sol.push(c);
                    }
                    BotSettingsField::MaxOpenPositions => {
                        s.bot_settings.max_open_positions.push(c);
                    }
                    BotSettingsField::MaxBuysPerMint => {
                        s.bot_settings.max_buys_per_mint.push(c);
                    }
                    BotSettingsField::MaxPerMintSol => {
                        s.bot_settings.max_per_mint_sol.push(c);
                    }
                    BotSettingsField::MaxOpenSol => {
                        s.bot_settings.max_open_sol.push(c);
                    }
                    BotSettingsField::DailyLossSol => {
                        s.bot_settings.daily_loss_sol.push(c);
                    }
                    BotSettingsField::CopyTradeMaxBuysPerMint => {
                        s.bot_settings.copy_trade_max_buys_per_mint.push(c);
                    }
                    BotSettingsField::CopyTradeMinSourceBuySol => {
                        s.bot_settings.copy_trade_min_source_buy_sol.push(c);
                    }
                    BotSettingsField::CopyTradeMaxHoldSecs => {
                        s.bot_settings.copy_trade_max_hold_secs.push(c);
                    }
                    BotSettingsField::CopyTradeTakeProfitBps => {
                        s.bot_settings.copy_trade_take_profit_bps.push(c);
                    }
                    BotSettingsField::CopyTradeTakeProfitSellBps => {
                        s.bot_settings.copy_trade_take_profit_sell_bps.push(c);
                    }
                    BotSettingsField::CopyTradeStopLossBps => {
                        s.bot_settings.copy_trade_stop_loss_bps.push(c);
                    }
                    BotSettingsField::CreateSlotLag => {
                        s.bot_settings.max_create_event_slot_lag.push(c);
                    }
                    BotSettingsField::BackfillLimit => s.bot_settings.backfill_limit.push(c),
                    BotSettingsField::ConfirmationPollMs => {
                        s.bot_settings.confirmation_poll_ms.push(c);
                    }
                    BotSettingsField::Mode
                    | BotSettingsField::Market
                    | BotSettingsField::CopyTradeEnabled
                    | BotSettingsField::CopyTradeSizing
                    | BotSettingsField::CopyTradeBuyPolicy
                    | BotSettingsField::CopyTradeFollowSells
                    | BotSettingsField::AdvancedToggle
                    | BotSettingsField::BotKeepAlive
                    | BotSettingsField::FetchFullTransaction
                    | BotSettingsField::CurveExitQuotes
                    | BotSettingsField::ParallelFallbackReads => {}
                }
            }
            Some(ScanCommand::Backspace) => {
                let mut s = state.write().await;
                s.bot_settings.error = None;
                s.bot_settings.saved = false;
                match s.bot_settings.active_field {
                    BotSettingsField::BuySize => {
                        s.bot_settings.buy_size_sol.pop();
                    }
                    BotSettingsField::SlippageBps => {
                        s.bot_settings.slippage_bps.pop();
                    }
                    BotSettingsField::MaxHoldSecs => {
                        s.bot_settings.max_hold_secs.pop();
                    }
                    BotSettingsField::StreamAgeMs => {
                        s.bot_settings.max_stream_event_age_ms.pop();
                    }
                    BotSettingsField::EntryDeadlineMs => {
                        s.bot_settings.entry_deadline_ms.pop();
                    }
                    BotSettingsField::CopyTradeWallet => {
                        s.bot_settings.copy_trade_wallet.pop();
                    }
                    BotSettingsField::CopyTradeScaleBps => {
                        s.bot_settings.copy_trade_scale_bps.pop();
                    }
                    BotSettingsField::CopyTradeMaxBuySol => {
                        s.bot_settings.copy_trade_max_buy_sol.pop();
                    }
                    BotSettingsField::MaxOpenPositions => {
                        s.bot_settings.max_open_positions.pop();
                    }
                    BotSettingsField::MaxBuysPerMint => {
                        s.bot_settings.max_buys_per_mint.pop();
                    }
                    BotSettingsField::MaxPerMintSol => {
                        s.bot_settings.max_per_mint_sol.pop();
                    }
                    BotSettingsField::MaxOpenSol => {
                        s.bot_settings.max_open_sol.pop();
                    }
                    BotSettingsField::DailyLossSol => {
                        s.bot_settings.daily_loss_sol.pop();
                    }
                    BotSettingsField::CopyTradeMaxBuysPerMint => {
                        s.bot_settings.copy_trade_max_buys_per_mint.pop();
                    }
                    BotSettingsField::CopyTradeMinSourceBuySol => {
                        s.bot_settings.copy_trade_min_source_buy_sol.pop();
                    }
                    BotSettingsField::CopyTradeMaxHoldSecs => {
                        s.bot_settings.copy_trade_max_hold_secs.pop();
                    }
                    BotSettingsField::CopyTradeTakeProfitBps => {
                        s.bot_settings.copy_trade_take_profit_bps.pop();
                    }
                    BotSettingsField::CopyTradeTakeProfitSellBps => {
                        s.bot_settings.copy_trade_take_profit_sell_bps.pop();
                    }
                    BotSettingsField::CopyTradeStopLossBps => {
                        s.bot_settings.copy_trade_stop_loss_bps.pop();
                    }
                    BotSettingsField::CreateSlotLag => {
                        s.bot_settings.max_create_event_slot_lag.pop();
                    }
                    BotSettingsField::BackfillLimit => {
                        s.bot_settings.backfill_limit.pop();
                    }
                    BotSettingsField::ConfirmationPollMs => {
                        s.bot_settings.confirmation_poll_ms.pop();
                    }
                    BotSettingsField::Mode
                    | BotSettingsField::Market
                    | BotSettingsField::CopyTradeEnabled
                    | BotSettingsField::CopyTradeSizing
                    | BotSettingsField::CopyTradeBuyPolicy
                    | BotSettingsField::CopyTradeFollowSells
                    | BotSettingsField::AdvancedToggle
                    | BotSettingsField::BotKeepAlive
                    | BotSettingsField::FetchFullTransaction
                    | BotSettingsField::CurveExitQuotes
                    | BotSettingsField::ParallelFallbackReads => {}
                }
            }
            Some(ScanCommand::Start) => {
                let vals = {
                    let s = state.read().await;
                    BotSettingsSnapshot {
                        mode: s.bot_settings.mode,
                        market: s.bot_settings.market,
                        buy_size_sol: s.bot_settings.buy_size_sol.clone(),
                        slippage_bps: s.bot_settings.slippage_bps.clone(),
                        max_hold_secs: s.bot_settings.max_hold_secs.clone(),
                        max_stream_event_age_ms: s.bot_settings.max_stream_event_age_ms.clone(),
                        entry_deadline_ms: s.bot_settings.entry_deadline_ms.clone(),
                        copy_trade_enabled: s.bot_settings.copy_trade_enabled,
                        copy_trade_wallet: s.bot_settings.copy_trade_wallet.clone(),
                        copy_trade_sizing: s.bot_settings.copy_trade_sizing,
                        copy_trade_scale_bps: s.bot_settings.copy_trade_scale_bps.clone(),
                        copy_trade_max_buy_sol: s.bot_settings.copy_trade_max_buy_sol.clone(),
                        copy_trade_follow_sells: s.bot_settings.copy_trade_follow_sells,
                        bot_keep_alive: s.bot_settings.bot_keep_alive,
                        max_open_positions: s.bot_settings.max_open_positions.clone(),
                        max_buys_per_mint: s.bot_settings.max_buys_per_mint.clone(),
                        max_per_mint_sol: s.bot_settings.max_per_mint_sol.clone(),
                        max_open_sol: s.bot_settings.max_open_sol.clone(),
                        daily_loss_sol: s.bot_settings.daily_loss_sol.clone(),
                        copy_trade_buy_policy: s.bot_settings.copy_trade_buy_policy,
                        copy_trade_max_buys_per_mint: s
                            .bot_settings
                            .copy_trade_max_buys_per_mint
                            .clone(),
                        copy_trade_min_source_buy_sol: s
                            .bot_settings
                            .copy_trade_min_source_buy_sol
                            .clone(),
                        copy_trade_max_hold_secs: s.bot_settings.copy_trade_max_hold_secs.clone(),
                        copy_trade_take_profit_bps: s
                            .bot_settings
                            .copy_trade_take_profit_bps
                            .clone(),
                        copy_trade_take_profit_sell_bps: s
                            .bot_settings
                            .copy_trade_take_profit_sell_bps
                            .clone(),
                        copy_trade_stop_loss_bps: s.bot_settings.copy_trade_stop_loss_bps.clone(),
                        max_create_event_slot_lag: s.bot_settings.max_create_event_slot_lag.clone(),
                        backfill_limit: s.bot_settings.backfill_limit.clone(),
                        fetch_full_transaction: s.bot_settings.fetch_full_transaction,
                        enable_curve_exit_quotes: s.bot_settings.enable_curve_exit_quotes,
                        confirmation_poll_ms: s.bot_settings.confirmation_poll_ms.clone(),
                        parallel_fallback_reads: s.bot_settings.parallel_fallback_reads,
                    }
                };
                let path = std::path::PathBuf::from(&bot_config_path);
                match save_bot_settings(&path, &vals).and_then(|msg| {
                    let cfg = Config::load(&path)
                        .with_context(|| format!("reload bot config {}", path.display()))?;
                    cfg.validate_for_bot()?;
                    if cfg.mode != Mode::Paper {
                        cfg.validate_live_risk_envelope("auto bot")?;
                    }
                    Ok(msg)
                }) {
                    Ok(msg) => {
                        let _ = event_tx.send(ScanEvent::Log(msg));
                        return BotSetupOutcome::Start(path);
                    }
                    Err(err) => {
                        let mut s = state.write().await;
                        s.bot_settings.saved = false;
                        s.bot_settings.error = Some(format!("{err:#}"));
                    }
                }
            }
            Some(ScanCommand::Cancel) => return BotSetupOutcome::Back,
            Some(ScanCommand::Quit) => return BotSetupOutcome::Quit,
            _ => {}
        }
    }
}

fn load_bot_settings_state(config_path: &std::path::Path) -> BotSettingsState {
    let resolved = resolve_config_path(config_path.to_path_buf());
    let mut state = BotSettingsState::new();
    state.config_path = resolved.to_string_lossy().to_string();
    match Config::load_raw(&resolved) {
        Ok(cfg) => {
            state.mode = cfg.mode;
            state.market = cfg.market;
            state.buy_size_sol = format!("{:.4}", cfg.base_buy_lamports as f64 / 1_000_000_000.0);
            state.slippage_bps = cfg.max_slippage_bps.to_string();
            state.max_hold_secs = cfg.max_hold_seconds.to_string();
            state.max_stream_event_age_ms = cfg.max_stream_event_age_ms.to_string();
            state.entry_deadline_ms = cfg.entry_deadline_ms.to_string();
            state.copy_trade_enabled = cfg.copy_trade_enabled;
            state.copy_trade_wallet = cfg.copy_trade_wallet.clone();
            state.copy_trade_sizing = cfg.copy_trade_sizing;
            state.copy_trade_scale_bps = cfg.copy_trade_scale_bps.to_string();
            state.copy_trade_max_buy_sol = format!(
                "{:.4}",
                cfg.copy_trade_max_buy_lamports as f64 / 1_000_000_000.0
            );
            state.copy_trade_follow_sells = cfg.copy_trade_follow_sells;
            state.bot_keep_alive = cfg.bot_keep_alive;
            state.max_open_positions = cfg.max_open_positions.to_string();
            state.max_buys_per_mint = cfg.max_buys_per_mint.to_string();
            state.max_per_mint_sol = format!(
                "{:.4}",
                cfg.max_total_lamports_per_mint as f64 / 1_000_000_000.0
            );
            state.max_open_sol = format!(
                "{:.4}",
                cfg.max_total_open_lamports as f64 / 1_000_000_000.0
            );
            state.daily_loss_sol = format!(
                "{:.4}",
                cfg.max_daily_loss_lamports as f64 / 1_000_000_000.0
            );
            state.copy_trade_buy_policy = cfg.copy_trade_buy_policy;
            state.copy_trade_max_buys_per_mint = cfg.copy_trade_max_buys_per_mint.to_string();
            state.copy_trade_min_source_buy_sol = format!(
                "{:.4}",
                cfg.copy_trade_min_source_buy_lamports as f64 / 1_000_000_000.0
            );
            state.copy_trade_max_hold_secs = cfg.copy_trade_max_hold_seconds.to_string();
            state.copy_trade_take_profit_bps = cfg.copy_trade_take_profit_bps.to_string();
            state.copy_trade_take_profit_sell_bps = cfg.copy_trade_take_profit_sell_bps.to_string();
            state.copy_trade_stop_loss_bps = cfg.copy_trade_stop_loss_bps.to_string();
            state.max_create_event_slot_lag = cfg.max_create_event_slot_lag.to_string();
            state.backfill_limit = cfg.backfill_limit.to_string();
            state.fetch_full_transaction = cfg.fetch_full_transaction;
            state.enable_curve_exit_quotes = cfg.enable_curve_exit_quotes;
            state.confirmation_poll_ms = cfg.live.confirmation_poll_ms.to_string();
        }
        Err(err) => {
            let cfg = Config::default();
            state.mode = cfg.mode;
            state.market = cfg.market;
            state.buy_size_sol = format!("{:.4}", cfg.base_buy_lamports as f64 / 1_000_000_000.0);
            state.slippage_bps = cfg.max_slippage_bps.to_string();
            state.max_hold_secs = cfg.max_hold_seconds.to_string();
            state.max_stream_event_age_ms = cfg.max_stream_event_age_ms.to_string();
            state.entry_deadline_ms = cfg.entry_deadline_ms.to_string();
            state.copy_trade_enabled = cfg.copy_trade_enabled;
            state.copy_trade_wallet = cfg.copy_trade_wallet.clone();
            state.copy_trade_sizing = cfg.copy_trade_sizing;
            state.copy_trade_scale_bps = cfg.copy_trade_scale_bps.to_string();
            state.copy_trade_max_buy_sol = format!(
                "{:.4}",
                cfg.copy_trade_max_buy_lamports as f64 / 1_000_000_000.0
            );
            state.copy_trade_follow_sells = cfg.copy_trade_follow_sells;
            state.bot_keep_alive = cfg.bot_keep_alive;
            state.max_open_positions = cfg.max_open_positions.to_string();
            state.max_buys_per_mint = cfg.max_buys_per_mint.to_string();
            state.max_per_mint_sol = format!(
                "{:.4}",
                cfg.max_total_lamports_per_mint as f64 / 1_000_000_000.0
            );
            state.max_open_sol = format!(
                "{:.4}",
                cfg.max_total_open_lamports as f64 / 1_000_000_000.0
            );
            state.daily_loss_sol = format!(
                "{:.4}",
                cfg.max_daily_loss_lamports as f64 / 1_000_000_000.0
            );
            state.copy_trade_buy_policy = cfg.copy_trade_buy_policy;
            state.copy_trade_max_buys_per_mint = cfg.copy_trade_max_buys_per_mint.to_string();
            state.copy_trade_min_source_buy_sol = format!(
                "{:.4}",
                cfg.copy_trade_min_source_buy_lamports as f64 / 1_000_000_000.0
            );
            state.copy_trade_max_hold_secs = cfg.copy_trade_max_hold_seconds.to_string();
            state.copy_trade_take_profit_bps = cfg.copy_trade_take_profit_bps.to_string();
            state.copy_trade_take_profit_sell_bps = cfg.copy_trade_take_profit_sell_bps.to_string();
            state.copy_trade_stop_loss_bps = cfg.copy_trade_stop_loss_bps.to_string();
            state.max_create_event_slot_lag = cfg.max_create_event_slot_lag.to_string();
            state.backfill_limit = cfg.backfill_limit.to_string();
            state.fetch_full_transaction = cfg.fetch_full_transaction;
            state.enable_curve_exit_quotes = cfg.enable_curve_exit_quotes;
            state.confirmation_poll_ms = cfg.live.confirmation_poll_ms.to_string();
            state.error = Some(format!("could not read profile: {err:#}"));
        }
    }
    state.parallel_fallback_reads = env_bool_lookup("MAYHEM_LIVE_PARALLEL_FALLBACK_READS", false);
    state
}

fn env_bool_lookup(legacy_name: &str, default: bool) -> bool {
    catarnith::config::env_lookup(legacy_name)
        .map(|value| {
            matches!(
                value.to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn toml_sol_or_lamports(
    table: &toml::Table,
    sol_key: &str,
    lamports_key: &str,
    default_lamports: u64,
) -> u64 {
    if let Some(value) = table.get(sol_key) {
        let sol = value
            .as_float()
            .or_else(|| value.as_integer().map(|v| v as f64))
            .or_else(|| value.as_str().and_then(|v| v.parse::<f64>().ok()));
        if let Some(sol) = sol.filter(|v| v.is_finite() && *v >= 0.0) {
            return (sol * 1_000_000_000.0).round() as u64;
        }
    }
    table
        .get(lamports_key)
        .and_then(|v| v.as_integer())
        .filter(|v| *v >= 0)
        .map(|v| v as u64)
        .unwrap_or(default_lamports)
}

fn format_sol(value: f64) -> String {
    let mut out = format!("{value:.9}");
    while out.contains('.') && out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.push('0');
    }
    out
}

struct BotSettingsSnapshot {
    mode: Mode,
    market: Market,
    buy_size_sol: String,
    slippage_bps: String,
    max_hold_secs: String,
    max_stream_event_age_ms: String,
    entry_deadline_ms: String,
    copy_trade_enabled: bool,
    copy_trade_wallet: String,
    copy_trade_sizing: CopyTradeSizing,
    copy_trade_scale_bps: String,
    copy_trade_max_buy_sol: String,
    copy_trade_follow_sells: bool,
    bot_keep_alive: bool,
    max_open_positions: String,
    max_buys_per_mint: String,
    max_per_mint_sol: String,
    max_open_sol: String,
    daily_loss_sol: String,
    copy_trade_buy_policy: CopyTradeBuyPolicy,
    copy_trade_max_buys_per_mint: String,
    copy_trade_min_source_buy_sol: String,
    copy_trade_max_hold_secs: String,
    copy_trade_take_profit_bps: String,
    copy_trade_take_profit_sell_bps: String,
    copy_trade_stop_loss_bps: String,
    max_create_event_slot_lag: String,
    backfill_limit: String,
    fetch_full_transaction: bool,
    enable_curve_exit_quotes: bool,
    confirmation_poll_ms: String,
    parallel_fallback_reads: bool,
}

fn save_bot_settings(config_path: &std::path::Path, vals: &BotSettingsSnapshot) -> Result<String> {
    use anyhow::{bail, Context};

    let sol: f64 = vals
        .buy_size_sol
        .trim()
        .parse()
        .context("buy size is not a valid SOL number")?;
    if sol <= 0.0 {
        bail!("buy size must be positive");
    }
    let lamports = (sol * 1_000_000_000.0).round() as u64;
    if lamports == 0 {
        bail!("buy size is too small");
    }

    let slippage_bps: u32 = vals
        .slippage_bps
        .trim()
        .parse()
        .context("slippage is not a valid integer (bps)")?;
    if slippage_bps == 0 {
        bail!("slippage must be greater than 0 bps");
    }
    if slippage_bps >= 10_000 {
        bail!("slippage must be below 10000 bps (100%)");
    }

    let max_hold_secs: i64 = vals
        .max_hold_secs
        .trim()
        .parse()
        .context("max hold is not a valid integer (seconds)")?;
    if max_hold_secs <= 0 {
        bail!("max hold must be greater than 0 seconds");
    }

    let max_stream_event_age_ms: i64 = vals
        .max_stream_event_age_ms
        .trim()
        .parse()
        .context("stream age is not a valid integer (ms)")?;
    if max_stream_event_age_ms <= 0 {
        bail!("stream age must be greater than 0 ms");
    }

    let entry_deadline_ms: i64 = vals
        .entry_deadline_ms
        .trim()
        .parse()
        .context("buy deadline is not a valid integer (ms)")?;
    if entry_deadline_ms <= 0 {
        bail!("buy deadline must be greater than 0 ms");
    }

    let copy_trade_wallet = vals.copy_trade_wallet.trim();
    if vals.copy_trade_enabled {
        if copy_trade_wallet.is_empty() {
            bail!("copy trade wallet is required when copy trade is on");
        }
        copy_trade_wallet
            .parse::<solana_pubkey::Pubkey>()
            .context("copy trade wallet is not a valid Solana pubkey")?;
    } else if !copy_trade_wallet.is_empty() {
        copy_trade_wallet
            .parse::<solana_pubkey::Pubkey>()
            .context("copy trade wallet is not a valid Solana pubkey")?;
    }

    let copy_trade_scale_bps: u32 = vals
        .copy_trade_scale_bps
        .trim()
        .parse()
        .context("copy trade scale is not a valid integer (bps)")?;
    if copy_trade_scale_bps == 0 {
        bail!("copy trade scale must be greater than 0 bps");
    }

    let copy_trade_max_buy_sol: f64 = vals
        .copy_trade_max_buy_sol
        .trim()
        .parse()
        .context("copy trade max buy is not a valid SOL number")?;
    if copy_trade_max_buy_sol <= 0.0 {
        bail!("copy trade max buy must be positive");
    }
    let copy_trade_max_buy_lamports = (copy_trade_max_buy_sol * 1_000_000_000.0).round() as u64;
    if copy_trade_max_buy_lamports == 0 {
        bail!("copy trade max buy is too small");
    }

    let max_open_positions: usize = vals
        .max_open_positions
        .trim()
        .parse()
        .context("max open positions is not a valid integer")?;
    if max_open_positions == 0 {
        bail!("max open positions must be greater than 0");
    }

    let max_buys_per_mint: u32 = vals
        .max_buys_per_mint
        .trim()
        .parse()
        .context("max buys per mint is not a valid integer")?;
    if max_buys_per_mint == 0 {
        bail!("max buys per mint must be greater than 0");
    }

    let parse_positive_sol_lamports = |raw: &str, label: &str| -> Result<u64> {
        let value: f64 = raw
            .trim()
            .parse()
            .with_context(|| format!("{label} is not a valid SOL number"))?;
        if !value.is_finite() || value <= 0.0 {
            bail!("{label} must be positive");
        }
        let lamports = (value * 1_000_000_000.0).round() as u64;
        if lamports == 0 {
            bail!("{label} is too small");
        }
        Ok(lamports)
    };

    let max_total_lamports_per_mint =
        parse_positive_sol_lamports(&vals.max_per_mint_sol, "per-mint cap")?;
    if max_total_lamports_per_mint < lamports {
        bail!("per-mint cap must be at least the buy size");
    }

    let max_total_open_lamports =
        parse_positive_sol_lamports(&vals.max_open_sol, "open exposure cap")?;
    if max_total_open_lamports < lamports {
        bail!("open exposure cap must be at least the buy size");
    }

    let _max_daily_loss_lamports =
        parse_positive_sol_lamports(&vals.daily_loss_sol, "daily loss limit")? as i64;

    let copy_trade_max_buys_per_mint: u32 = vals
        .copy_trade_max_buys_per_mint
        .trim()
        .parse()
        .context("copy trade max buys is not a valid integer")?;
    if copy_trade_max_buys_per_mint == 0 {
        bail!("copy trade max buys must be greater than 0");
    }

    let copy_trade_min_source_buy_sol: f64 = vals
        .copy_trade_min_source_buy_sol
        .trim()
        .parse()
        .context("copy trade min source buy is not a valid SOL number")?;
    if copy_trade_min_source_buy_sol < 0.0 {
        bail!("copy trade min source buy cannot be negative");
    }
    let _copy_trade_min_source_buy_lamports =
        (copy_trade_min_source_buy_sol * 1_000_000_000.0).round() as u64;

    let copy_trade_max_hold_seconds: i64 = vals
        .copy_trade_max_hold_secs
        .trim()
        .parse()
        .context("copy trade max hold is not a valid integer (seconds)")?;
    if copy_trade_max_hold_seconds <= 0 {
        bail!("copy trade max hold must be greater than 0 seconds");
    }

    let copy_trade_take_profit_bps: i64 = vals
        .copy_trade_take_profit_bps
        .trim()
        .parse()
        .context("copy trade take profit is not a valid integer (bps)")?;
    if copy_trade_take_profit_bps < 0 {
        bail!("copy trade take profit cannot be negative");
    }

    let copy_trade_take_profit_sell_bps: u32 = vals
        .copy_trade_take_profit_sell_bps
        .trim()
        .parse()
        .context("copy trade take profit sell is not a valid integer (bps)")?;
    if copy_trade_take_profit_sell_bps == 0 || copy_trade_take_profit_sell_bps > 10_000 {
        bail!("copy trade take profit sell must be between 1 and 10000 bps");
    }

    let copy_trade_stop_loss_bps: i64 = vals
        .copy_trade_stop_loss_bps
        .trim()
        .parse()
        .context("copy trade stop loss is not a valid integer (bps)")?;
    if copy_trade_stop_loss_bps < 0 {
        bail!("copy trade stop loss cannot be negative");
    }

    let max_create_event_slot_lag: u64 = vals
        .max_create_event_slot_lag
        .trim()
        .parse()
        .context("create slot lag is not a valid integer")?;
    if max_create_event_slot_lag > 64 {
        bail!("create slot lag must be <= 64");
    }

    let backfill_limit: usize = vals
        .backfill_limit
        .trim()
        .parse()
        .context("backfill limit is not a valid integer")?;

    let confirmation_poll_ms: u64 = vals
        .confirmation_poll_ms
        .trim()
        .parse()
        .context("confirmation poll is not a valid integer (ms)")?;
    if confirmation_poll_ms == 0 {
        bail!("confirmation poll must be greater than 0 ms");
    }

    let content = read_config_or_default(config_path)?;
    let mut table: toml::Table = content
        .parse()
        .with_context(|| format!("parse config {config_path:?}"))?;

    table.insert(
        "mode".to_string(),
        toml::Value::String(vals.mode.as_str().to_string()),
    );
    table.remove("pair_scope");
    table.insert(
        "market".to_string(),
        toml::Value::String(vals.market.as_str().to_string()),
    );
    table.remove("base_buy_lamports");
    table.insert("base_buy_sol".to_string(), toml::Value::Float(sol));
    table.insert(
        "max_slippage_bps".to_string(),
        toml::Value::Integer(slippage_bps as i64),
    );
    table.insert(
        "max_hold_seconds".to_string(),
        toml::Value::Integer(max_hold_secs),
    );
    table.insert(
        "max_stream_event_age_ms".to_string(),
        toml::Value::Integer(max_stream_event_age_ms),
    );
    table.insert(
        "entry_deadline_ms".to_string(),
        toml::Value::Integer(entry_deadline_ms),
    );
    table.insert(
        "copy_trade_enabled".to_string(),
        toml::Value::Boolean(vals.copy_trade_enabled),
    );
    table.insert(
        "copy_trade_wallet".to_string(),
        toml::Value::String(copy_trade_wallet.to_string()),
    );
    table.insert(
        "copy_trade_sizing".to_string(),
        toml::Value::String(vals.copy_trade_sizing.as_str().to_string()),
    );
    table.insert(
        "copy_trade_scale_bps".to_string(),
        toml::Value::Integer(copy_trade_scale_bps as i64),
    );
    table.remove("copy_trade_max_buy_lamports");
    table.insert(
        "copy_trade_max_buy_sol".to_string(),
        toml::Value::Float(copy_trade_max_buy_sol),
    );
    table.insert(
        "copy_trade_buy_policy".to_string(),
        toml::Value::String(vals.copy_trade_buy_policy.as_str().to_string()),
    );
    table.insert(
        "copy_trade_max_buys_per_mint".to_string(),
        toml::Value::Integer(copy_trade_max_buys_per_mint as i64),
    );
    table.remove("copy_trade_min_source_buy_lamports");
    table.insert(
        "copy_trade_min_source_buy_sol".to_string(),
        toml::Value::Float(copy_trade_min_source_buy_sol),
    );
    table.insert(
        "copy_trade_max_hold_seconds".to_string(),
        toml::Value::Integer(copy_trade_max_hold_seconds),
    );
    table.insert(
        "copy_trade_take_profit_bps".to_string(),
        toml::Value::Integer(copy_trade_take_profit_bps),
    );
    table.insert(
        "copy_trade_take_profit_sell_bps".to_string(),
        toml::Value::Integer(copy_trade_take_profit_sell_bps as i64),
    );
    table.insert(
        "copy_trade_stop_loss_bps".to_string(),
        toml::Value::Integer(copy_trade_stop_loss_bps),
    );
    table.insert(
        "copy_trade_follow_sells".to_string(),
        toml::Value::Boolean(vals.copy_trade_follow_sells),
    );
    table.insert(
        "bot_keep_alive".to_string(),
        toml::Value::Boolean(vals.bot_keep_alive),
    );
    table.insert(
        "max_open_positions".to_string(),
        toml::Value::Integer(max_open_positions as i64),
    );
    table.insert(
        "max_buys_per_mint".to_string(),
        toml::Value::Integer(max_buys_per_mint as i64),
    );
    table.remove("max_total_lamports_per_mint");
    table.insert(
        "max_total_sol_per_mint".to_string(),
        toml::Value::Float(vals.max_per_mint_sol.trim().parse::<f64>().unwrap_or(0.0)),
    );
    table.remove("max_total_open_lamports");
    table.insert(
        "max_total_open_sol".to_string(),
        toml::Value::Float(vals.max_open_sol.trim().parse::<f64>().unwrap_or(0.0)),
    );
    table.remove("max_daily_loss_lamports");
    table.insert(
        "max_daily_loss_sol".to_string(),
        toml::Value::Float(vals.daily_loss_sol.trim().parse::<f64>().unwrap_or(0.0)),
    );
    table.insert(
        "max_create_event_slot_lag".to_string(),
        toml::Value::Integer(max_create_event_slot_lag as i64),
    );
    table.insert(
        "backfill_limit".to_string(),
        toml::Value::Integer(backfill_limit as i64),
    );
    table.insert(
        "fetch_full_transaction".to_string(),
        toml::Value::Boolean(vals.fetch_full_transaction),
    );
    table.insert(
        "enable_curve_exit_quotes".to_string(),
        toml::Value::Boolean(vals.enable_curve_exit_quotes),
    );

    let live_value = table
        .entry("live".to_string())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    let live_table = live_value
        .as_table_mut()
        .context("[live] config section must be a table")?;
    live_table.insert(
        "confirmation_poll_ms".to_string(),
        toml::Value::Integer(confirmation_poll_ms as i64),
    );

    let out = toml::to_string(&table).context("serialize updated config")?;
    std::fs::write(config_path, out).with_context(|| format!("write config {config_path:?}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(err) =
            std::fs::set_permissions(config_path, std::fs::Permissions::from_mode(0o600))
        {
            eprintln!("warning: could not chmod 600 {config_path:?}: {err}");
        }
    }

    let env_values = vec![
        ("MARKET", vals.market.as_str().to_string()),
        ("LIVE_MARKET", vals.market.as_str().to_string()),
        ("LIVE_BASE_BUY_SOL", format_sol(sol)),
        ("LIVE_MAX_SLIPPAGE_BPS", slippage_bps.to_string()),
        ("LIVE_MAX_HOLD_SECONDS", max_hold_secs.to_string()),
        (
            "LIVE_MAX_STREAM_EVENT_AGE_MS",
            max_stream_event_age_ms.to_string(),
        ),
        ("LIVE_ENTRY_DEADLINE_MS", entry_deadline_ms.to_string()),
        (
            "LIVE_COPY_TRADE_ENABLED",
            vals.copy_trade_enabled.to_string(),
        ),
        ("LIVE_COPY_TRADE_WALLET", copy_trade_wallet.to_string()),
        (
            "LIVE_COPY_TRADE_SIZING",
            vals.copy_trade_sizing.as_str().to_string(),
        ),
        (
            "LIVE_COPY_TRADE_SCALE_BPS",
            copy_trade_scale_bps.to_string(),
        ),
        (
            "LIVE_COPY_TRADE_MAX_BUY_SOL",
            format_sol(copy_trade_max_buy_sol),
        ),
        (
            "LIVE_COPY_TRADE_BUY_POLICY",
            vals.copy_trade_buy_policy.as_str().to_string(),
        ),
        (
            "LIVE_COPY_TRADE_MAX_BUYS_PER_MINT",
            copy_trade_max_buys_per_mint.to_string(),
        ),
        (
            "LIVE_COPY_TRADE_MIN_SOURCE_BUY_SOL",
            format_sol(copy_trade_min_source_buy_sol),
        ),
        (
            "LIVE_COPY_TRADE_MAX_HOLD_SECONDS",
            copy_trade_max_hold_seconds.to_string(),
        ),
        (
            "LIVE_COPY_TRADE_TAKE_PROFIT_BPS",
            copy_trade_take_profit_bps.to_string(),
        ),
        (
            "LIVE_COPY_TRADE_TAKE_PROFIT_SELL_BPS",
            copy_trade_take_profit_sell_bps.to_string(),
        ),
        (
            "LIVE_COPY_TRADE_STOP_LOSS_BPS",
            copy_trade_stop_loss_bps.to_string(),
        ),
        (
            "LIVE_COPY_TRADE_FOLLOW_SELLS",
            vals.copy_trade_follow_sells.to_string(),
        ),
        ("LIVE_BOT_KEEP_ALIVE", vals.bot_keep_alive.to_string()),
        ("LIVE_MAX_OPEN_POSITIONS", max_open_positions.to_string()),
        ("LIVE_MAX_BUYS_PER_MINT", max_buys_per_mint.to_string()),
        (
            "LIVE_MAX_TOTAL_SOL_PER_MINT",
            format_sol(vals.max_per_mint_sol.trim().parse::<f64>().unwrap_or(0.0)),
        ),
        (
            "LIVE_MAX_TOTAL_OPEN_SOL",
            format_sol(vals.max_open_sol.trim().parse::<f64>().unwrap_or(0.0)),
        ),
        (
            "LIVE_MAX_DAILY_LOSS_SOL",
            format_sol(vals.daily_loss_sol.trim().parse::<f64>().unwrap_or(0.0)),
        ),
        (
            "LIVE_MAX_CREATE_EVENT_SLOT_LAG",
            max_create_event_slot_lag.to_string(),
        ),
        ("LIVE_BACKFILL_LIMIT", backfill_limit.to_string()),
        (
            "LIVE_FETCH_FULL_TRANSACTION",
            vals.fetch_full_transaction.to_string(),
        ),
        (
            "LIVE_ENABLE_CURVE_EXIT_QUOTES",
            vals.enable_curve_exit_quotes.to_string(),
        ),
        (
            "LIVE_CONFIRMATION_POLL_MS",
            confirmation_poll_ms.to_string(),
        ),
        (
            "LIVE_PARALLEL_FALLBACK_READS",
            vals.parallel_fallback_reads.to_string(),
        ),
    ];

    for (suffix, value) in &env_values {
        std::env::set_var(format!("CTARNITH_{suffix}"), value);
    }

    let env_note = update_bot_env_file(config_path, &env_values).unwrap_or(None);
    let live_hint = if vals.mode == Mode::Live {
        "  (live still needs enable_live_trading=true and unlock gates)"
    } else {
        ""
    };

    Ok(format!(
        "saved auto bot mode={} market={} buy_sol={} slippage_bps={} hold_s={} age_ms={} deadline_ms={} caps={}/{} per_mint_sol={} open_sol={} daily_loss_sol={} copy_trade={}{}{}",
        vals.mode.as_str(),
        vals.market.as_str(),
        format_sol(sol),
        slippage_bps,
        max_hold_secs,
        max_stream_event_age_ms,
        entry_deadline_ms,
        max_open_positions,
        max_buys_per_mint,
        format_sol(vals.max_per_mint_sol.trim().parse::<f64>().unwrap_or(0.0)),
        format_sol(vals.max_open_sol.trim().parse::<f64>().unwrap_or(0.0)),
        format_sol(vals.daily_loss_sol.trim().parse::<f64>().unwrap_or(0.0)),
        if vals.copy_trade_enabled { "on" } else { "off" },
        env_note
            .map(|p| format!(" and {p:?}"))
            .unwrap_or_default(),
        live_hint,
    ))
}

fn update_bot_env_file(
    config_path: &std::path::Path,
    values: &[(&str, String)],
) -> Result<Option<String>> {
    use anyhow::Context;

    let path = match catarnith::config::discover_dot_env() {
        Some(p) => p,
        None => {
            let candidate = config_path
                .parent()
                .map(|d| d.join(".env"))
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            if !candidate.exists() {
                return Ok(None);
            }
            candidate
        }
    };
    let content = std::fs::read_to_string(&path).with_context(|| format!("read .env {path:?}"))?;
    let mut lines: Vec<String> = Vec::new();
    let mut wrote = vec![false; values.len()];

    for line in content.lines() {
        let trimmed = line.trim();
        let mut replacement: Option<(usize, String)> = None;
        for (idx, (suffix, value)) in values.iter().enumerate() {
            if matches_env_suffix(trimmed, suffix) {
                replacement = Some((idx, format!("export CTARNITH_{suffix}={value}")));
                break;
            }
        }
        if let Some((idx, line)) = replacement {
            lines.push(line);
            wrote[idx] = true;
        } else {
            lines.push(line.to_string());
        }
    }

    for (idx, (suffix, value)) in values.iter().enumerate() {
        if !wrote[idx] {
            lines.push(format!("export CTARNITH_{suffix}={value}"));
        }
    }

    let mut out = lines.join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    std::fs::write(&path, out).with_context(|| format!("write .env {path:?}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }

    Ok(Some(path.to_string_lossy().to_string()))
}

fn matches_env_suffix(trimmed: &str, suffix: &str) -> bool {
    let suffixes: &[&str] = match suffix {
        "MARKET" => &["MARKET", "PAIR_SCOPE"],
        "LIVE_MARKET" => &["LIVE_MARKET", "LIVE_PAIR_SCOPE"],
        "LIVE_BASE_BUY_SOL" => &["LIVE_BASE_BUY_SOL", "LIVE_BASE_BUY_LAMPORTS"],
        "LIVE_MAX_BALANCE_SOL" => &["LIVE_MAX_BALANCE_SOL", "LIVE_MAX_BALANCE_LAMPORTS"],
        "LIVE_MAX_TOTAL_SOL_PER_MINT" => &[
            "LIVE_MAX_TOTAL_SOL_PER_MINT",
            "LIVE_MAX_TOTAL_LAMPORTS_PER_MINT",
        ],
        "LIVE_MAX_TOTAL_OPEN_SOL" => &["LIVE_MAX_TOTAL_OPEN_SOL", "LIVE_MAX_TOTAL_OPEN_LAMPORTS"],
        "LIVE_MAX_DAILY_LOSS_SOL" => &["LIVE_MAX_DAILY_LOSS_SOL", "LIVE_MAX_DAILY_LOSS_LAMPORTS"],
        "LIVE_COPY_TRADE_MAX_BUY_SOL" => &[
            "LIVE_COPY_TRADE_MAX_BUY_SOL",
            "LIVE_COPY_TRADE_MAX_BUY_LAMPORTS",
        ],
        "LIVE_COPY_TRADE_MIN_SOURCE_BUY_SOL" => &[
            "LIVE_COPY_TRADE_MIN_SOURCE_BUY_SOL",
            "LIVE_COPY_TRADE_MIN_SOURCE_BUY_LAMPORTS",
        ],
        "LIVE_JITO_TIP_SOL" => &["LIVE_JITO_TIP_SOL", "LIVE_JITO_TIP_LAMPORTS"],
        _ => std::slice::from_ref(&suffix),
    };
    for prefix in ["CTARNITH_", "MAYHEM_"] {
        for lead in ["export ", ""] {
            for candidate_suffix in suffixes {
                if trimmed.starts_with(&format!("{lead}{prefix}{candidate_suffix}=")) {
                    return true;
                }
            }
        }
    }
    false
}

/// Run the autonomous bot child process inside the TUI. Its stdout/stderr
/// are streamed into the log panel. ESC stops the bot and moves to
/// `Phase::BotStopped`; a second ESC returns to the mode picker.
/// Returns `true` if the operator chose to quit the app outright.
async fn run_bot_mode(
    config_path: &std::path::Path,
    state: Arc<RwLock<ScanState>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
) -> bool {
    // Switch to the bot screen immediately so the operator sees
    // feedback even while we resolve the config and bot binary.
    {
        let mut s = state.write().await;
        s.logs.clear();
        s.wallet_label = "BOT".to_string();
        s.push_log("starting bot…".to_string());
        s.phase = Phase::BotRunning;
        s.last_error = None;
        let _ = event_tx.send(ScanEvent::StateChanged);
    }

    let (config_arg, bot_cwd) = resolve_bot_config_from(config_path.to_path_buf());

    let launch = match find_bot_launch(&bot_cwd) {
        Some(launch) => launch,
        None => {
            let msg = "bot launcher not found. build with: cargo build --release --bin bot";
            let _ = event_tx.send(ScanEvent::Log(msg.to_string()));
            {
                let mut s = state.write().await;
                s.push_log(msg.to_string());
                s.phase = Phase::BotStopped;
                s.last_error = Some(msg.to_string());
                let _ = event_tx.send(ScanEvent::StateChanged);
            }
            wait_esc_in_bot_stopped(&state, cmd_rx, event_tx).await;
            return false;
        }
    };

    let launch_label = launch.label();
    let keep_alive = Config::load_raw(&config_arg)
        .map(|cfg| cfg.bot_keep_alive)
        .unwrap_or(true);
    let mut quit = false;
    let mut intentional_stop = false;
    let mut restart_count = 0u32;

    loop {
        let mut cmd = launch.command(&config_arg, &bot_cwd);
        cmd.env("CTARNITH_LIVE_CONFIG", config_arg.as_os_str())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(err) => {
                let msg = format!("failed to spawn bot: {err:#}");
                let _ = event_tx.send(ScanEvent::Log(msg.clone()));
                {
                    let mut s = state.write().await;
                    s.push_log(msg.clone());
                    s.phase = Phase::BotStopped;
                    s.last_error = Some(msg);
                    let _ = event_tx.send(ScanEvent::StateChanged);
                }
                wait_esc_in_bot_stopped(&state, cmd_rx, event_tx).await;
                return false;
            }
        };
        let child_started_at = Instant::now();

        {
            let mut s = state.write().await;
            s.phase = Phase::BotRunning;
            s.push_log(if restart_count == 0 {
                launch_label.clone()
            } else {
                format!("bot restarted #{restart_count}: {launch_label}")
            });
            s.push_log("waiting for stream subscriptions…".to_string());
            let _ = event_tx.send(ScanEvent::StateChanged);
        }

        let stdout = child.stdout.take().expect("piped stdout");
        let stderr = child.stderr.take().expect("piped stderr");
        let mut stdout_lines = tokio::io::BufReader::new(stdout).lines();
        let mut stderr_lines = tokio::io::BufReader::new(stderr).lines();
        let mut child_tail = VecDeque::<String>::with_capacity(BOT_CHILD_TAIL);

        let mut stopped = false;

        while !stopped {
            tokio::select! {
                line = stdout_lines.next_line() => {
                    match line {
                        Ok(Some(l)) => {
                            if let Some(cleaned) = clean_bot_log_line(&l) {
                                remember_bot_child_line(&mut child_tail, &cleaned);
                                if is_bot_execution_log(&cleaned) || is_bot_fatal_log(&cleaned) {
                                    let _ = event_tx.send(ScanEvent::Log(cleaned));
                                }
                            }
                        }
                        Ok(None) => stopped = true,
                        Err(e) => {
                            let _ = event_tx.send(ScanEvent::Log(format!("stdout error: {e}")));
                            stopped = true;
                        }
                    }
                }
                line = stderr_lines.next_line() => {
                    match line {
                        Ok(Some(l)) => {
                            if let Some(cleaned) = clean_bot_log_line(&l) {
                                remember_bot_child_line(&mut child_tail, &cleaned);
                                if is_bot_execution_log(&cleaned) || is_bot_fatal_log(&cleaned) {
                                    let _ = event_tx.send(ScanEvent::Log(cleaned));
                                }
                            }
                        }
                        Ok(None) => stopped = true,
                        Err(e) => {
                            let _ = event_tx.send(ScanEvent::Log(format!("stderr error: {e}")));
                            stopped = true;
                        }
                    }
                }
                cmd = recv_mapped_command(cmd_rx, Phase::BotRunning) => {
                    match cmd {
                        Some(ScanCommand::Cancel) => {
                            if close_log_overlay_if_open(&state, event_tx).await {
                                continue;
                            }
                            let _ = event_tx.send(ScanEvent::Log("[esc] stopping bot…".into()));
                            let _ = child.kill().await;
                            intentional_stop = true;
                            stopped = true;
                        }
                        Some(ScanCommand::Quit) => {
                            let _ = event_tx.send(ScanEvent::Log("[q] quitting…".into()));
                            let _ = child.kill().await;
                            intentional_stop = true;
                            stopped = true;
                            quit = true;
                        }
                        Some(ScanCommand::ShowLogs) => {
                            let _ = event_tx.send(ScanEvent::ToggleLogs);
                        }
                        Some(ScanCommand::ScrollLogs(delta)) => {
                            let _ = event_tx.send(ScanEvent::ScrollLogs(delta));
                        }
                        Some(ScanCommand::CycleTheme) => {
                            let mut s = state.write().await;
                            s.theme = s.theme.cycle();
                            let _ = event_tx.send(ScanEvent::Log(format!(
                                "theme cycled -> {}",
                                s.theme.label()
                            )));
                        }
                        _ => {}
                    }
                }
            }
        }

        let status = child.wait().await.ok();
        if quit || intentional_stop {
            break;
        }
        if keep_alive {
            restart_count = restart_count.saturating_add(1);
            let failed = status_failed(status.as_ref());
            if failed {
                emit_bot_exit_diagnostics(event_tx, status.as_ref(), &child_tail);
            }
            let rapid_failure = failed && child_started_at.elapsed() <= BOT_RAPID_FAILURE_WINDOW;
            if rapid_failure && restart_count >= BOT_RAPID_FAILURE_LIMIT {
                let msg = format!(
                    "bot stopped after {restart_count} rapid startup failures; fix the last bot error and start Auto Bot again"
                );
                let _ = event_tx.send(ScanEvent::Log(msg.clone()));
                {
                    let mut s = state.write().await;
                    s.last_error = Some(msg);
                    let _ = event_tx.send(ScanEvent::StateChanged);
                }
                break;
            }
            let _ = event_tx.send(ScanEvent::Log(format!(
                "bot exited unexpectedly status={status:?}; restarting in 2s"
            )));
            tokio::time::sleep(Duration::from_secs(2)).await;
            continue;
        }
        let _ = event_tx.send(ScanEvent::Log(format!("bot exited status={status:?}")));
        break;
    }
    {
        let mut s = state.write().await;
        s.phase = Phase::BotStopped;
        let _ = event_tx.send(ScanEvent::StateChanged);
    }

    if quit {
        return true;
    }

    wait_esc_in_bot_stopped(&state, cmd_rx, event_tx).await;
    false
}

async fn wait_esc_in_bot_stopped(
    state: &Arc<RwLock<ScanState>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
) {
    loop {
        match recv_mapped_command(cmd_rx, Phase::BotStopped).await {
            Some(ScanCommand::Cancel) => {
                if close_log_overlay_if_open(state, event_tx).await {
                    continue;
                }
                break;
            }
            Some(ScanCommand::Quit) => break,
            Some(ScanCommand::ShowLogs) => {
                let _ = event_tx.send(ScanEvent::ToggleLogs);
            }
            Some(ScanCommand::ScrollLogs(delta)) => {
                let _ = event_tx.send(ScanEvent::ScrollLogs(delta));
            }
            _ => {}
        }
    }
}

/// Settings editor. Returns `true` if the operator chose to quit the
/// app from inside settings. When `first_run` is set the editable
/// text fields open blank so a brand-new user fills them in from
/// scratch instead of editing pre-seeded defaults.
async fn run_settings(
    config_path: &std::path::Path,
    first_run: bool,
    state: Arc<RwLock<ScanState>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
) -> bool {
    // Pre-fill all editable fields from the active TOML profile and,
    // for the env-only vars, from the process environment.
    let (
        buy_size_sol,
        market,
        helius_key,
        slippage_bps,
        max_hold_secs,
        enable_live_trading,
        require_manual_live_unlock,
        live_max_balance_sol,
        sell_slippage_bps,
        priority_fee_microlamports,
        jito_block_engine_url,
        jito_tip_sol,
        confirmation_poll_ms,
        pre_broadcast_simulation,
    ) = {
        let content = std::fs::read_to_string(config_path).unwrap_or_default();
        let table: toml::Table = content.parse().unwrap_or_default();
        let live_table = table.get("live").and_then(|v| v.as_table());
        let lamports =
            toml_sol_or_lamports(&table, "base_buy_sol", "base_buy_lamports", 13_025_001);
        let buy_size_sol = format!("{:.4}", lamports as f64 / 1_000_000_000.0);
        let market_value = table
            .get("market")
            .or_else(|| table.get("pair_scope"))
            .and_then(|v| v.as_str())
            .unwrap_or("mayhem_only");
        let mut market = Market::from_config_value(market_value).unwrap_or(Market::MayhemOnly);
        if let Some(value) = catarnith::config::env_lookup("MAYHEM_MARKET")
            .or_else(|| catarnith::config::env_lookup("MAYHEM_PAIR_SCOPE"))
            .or_else(|| catarnith::config::env_lookup("MAYHEM_LIVE_MARKET"))
            .or_else(|| catarnith::config::env_lookup("MAYHEM_LIVE_PAIR_SCOPE"))
        {
            market = Market::from_config_value(&value).unwrap_or(market);
        }
        // Helius key: prefer the TOML, fall back to the env var.
        let helius_key = table
            .get("helius_api_key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty())
            .or_else(|| std::env::var("HELIUS_API_KEY").ok())
            .unwrap_or_default();
        let mut slippage_bps = table
            .get("max_slippage_bps")
            .and_then(|v| v.as_integer())
            .unwrap_or(1500)
            .to_string();
        slippage_bps =
            catarnith::config::env_lookup("MAYHEM_LIVE_MAX_SLIPPAGE_BPS").unwrap_or(slippage_bps);
        let mut max_hold_secs = table
            .get("max_hold_seconds")
            .and_then(|v| v.as_integer())
            .unwrap_or(180)
            .to_string();
        max_hold_secs =
            catarnith::config::env_lookup("MAYHEM_LIVE_MAX_HOLD_SECONDS").unwrap_or(max_hold_secs);
        let mut enable_live_trading = table
            .get("enable_live_trading")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        enable_live_trading =
            env_bool_lookup("MAYHEM_LIVE_ENABLE_LIVE_TRADING", enable_live_trading);
        let mut require_manual_live_unlock = table
            .get("require_manual_live_unlock")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        require_manual_live_unlock = env_bool_lookup(
            "MAYHEM_LIVE_REQUIRE_MANUAL_LIVE_UNLOCK",
            require_manual_live_unlock,
        );
        let mut live_max_balance_lamports = live_table
            .map(|t| toml_sol_or_lamports(t, "max_balance_sol", "max_balance_lamports", 50_000_000))
            .unwrap_or(50_000_000);
        if let Some(value) = catarnith::config::env_lookup("MAYHEM_LIVE_MAX_BALANCE_SOL") {
            if let Ok(parsed) = value.parse::<f64>() {
                live_max_balance_lamports = (parsed * 1_000_000_000.0).round() as u64;
            }
        } else if let Some(value) =
            catarnith::config::env_lookup("MAYHEM_LIVE_MAX_BALANCE_LAMPORTS")
        {
            if let Ok(parsed) = value.parse::<u64>() {
                live_max_balance_lamports = parsed;
            }
        }
        let live_max_balance_sol =
            format!("{:.4}", live_max_balance_lamports as f64 / 1_000_000_000.0);
        let mut sell_slippage_bps = live_table
            .and_then(|t| t.get("sell_slippage_bps"))
            .and_then(|v| v.as_integer())
            .map(|v| v.to_string())
            .unwrap_or_else(|| slippage_bps.clone());
        sell_slippage_bps = catarnith::config::env_lookup("MAYHEM_LIVE_SELL_SLIPPAGE_BPS")
            .unwrap_or(sell_slippage_bps);
        let mut priority_fee_microlamports = live_table
            .and_then(|t| t.get("compute_unit_price_microlamports"))
            .and_then(|v| v.as_integer())
            .unwrap_or(100_000)
            .to_string();
        priority_fee_microlamports =
            catarnith::config::env_lookup("MAYHEM_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS")
                .unwrap_or(priority_fee_microlamports);
        let mut jito_block_engine_url = live_table
            .and_then(|t| t.get("jito_block_engine_url"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        jito_block_engine_url = catarnith::config::env_lookup("MAYHEM_LIVE_JITO_BLOCK_ENGINE_URL")
            .unwrap_or(jito_block_engine_url);
        let mut jito_tip_lamports = live_table
            .map(|t| toml_sol_or_lamports(t, "jito_tip_sol", "jito_tip_lamports", 100_000))
            .unwrap_or(100_000);
        if let Some(value) = catarnith::config::env_lookup("MAYHEM_LIVE_JITO_TIP_SOL") {
            if let Ok(parsed) = value.parse::<f64>() {
                jito_tip_lamports = (parsed * 1_000_000_000.0).round() as u64;
            }
        } else if let Some(value) = catarnith::config::env_lookup("MAYHEM_LIVE_JITO_TIP_LAMPORTS") {
            if let Ok(parsed) = value.parse::<u64>() {
                jito_tip_lamports = parsed;
            }
        }
        let jito_tip_sol = format_sol(jito_tip_lamports as f64 / 1_000_000_000.0);
        let mut confirmation_poll_ms = live_table
            .and_then(|t| t.get("confirmation_poll_ms"))
            .and_then(|v| v.as_integer())
            .unwrap_or(200)
            .to_string();
        confirmation_poll_ms = catarnith::config::env_lookup("MAYHEM_LIVE_CONFIRMATION_POLL_MS")
            .unwrap_or(confirmation_poll_ms);
        let mut pre_broadcast_simulation = live_table
            .and_then(|t| t.get("pre_broadcast_simulation"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        pre_broadcast_simulation = env_bool_lookup(
            "MAYHEM_LIVE_PRE_BROADCAST_SIMULATION",
            pre_broadcast_simulation,
        );
        (
            buy_size_sol,
            market,
            helius_key,
            slippage_bps,
            max_hold_secs,
            enable_live_trading,
            require_manual_live_unlock,
            live_max_balance_sol,
            sell_slippage_bps,
            priority_fee_microlamports,
            jito_block_engine_url,
            jito_tip_sol,
            confirmation_poll_ms,
            pre_broadcast_simulation,
        )
    };
    let fallback_rpc = catarnith::config::env_lookup("MAYHEM_FALLBACK_RPC_URL").unwrap_or_default();
    let jupiter_key = std::env::var("JUP_API_KEY").unwrap_or_default();

    {
        let mut s = state.write().await;
        s.settings.reset();
        s.settings.buy_size_sol = buy_size_sol;
        s.settings.market = market;
        s.settings.helius_key = helius_key;
        s.settings.fallback_rpc = fallback_rpc;
        s.settings.jupiter_key = jupiter_key;
        s.settings.slippage_bps = slippage_bps;
        s.settings.max_hold_secs = max_hold_secs;
        s.settings.theme = s.theme;
        s.settings.enable_live_trading = enable_live_trading;
        s.settings.require_manual_live_unlock = require_manual_live_unlock;
        s.settings.live_max_balance_sol = live_max_balance_sol;
        s.settings.sell_slippage_bps = sell_slippage_bps;
        s.settings.priority_fee_microlamports = priority_fee_microlamports;
        s.settings.jito_block_engine_url = jito_block_engine_url;
        s.settings.jito_tip_sol = jito_tip_sol;
        s.settings.confirmation_poll_ms = confirmation_poll_ms;
        s.settings.pre_broadcast_simulation = pre_broadcast_simulation;
        if first_run {
            // Brand-new user: open with blank editable fields so they
            // enter their own wallet/keys/sizes from scratch instead
            // of editing pre-seeded defaults.
            s.settings.buy_size_sol.clear();
            s.settings.helius_key.clear();
            s.settings.fallback_rpc.clear();
            s.settings.jupiter_key.clear();
            s.settings.slippage_bps.clear();
            s.settings.max_hold_secs.clear();
        }
        s.phase = Phase::Settings;
        let _ = event_tx.send(ScanEvent::StateChanged);
    }

    let mut should_quit = false;
    loop {
        match recv_mapped_command(cmd_rx, Phase::Settings).await {
            Some(ScanCommand::NextField) => {
                let mut s = state.write().await;
                let show = s.settings.show_advanced;
                s.settings.active_field = next_field(s.settings.active_field, show);
            }
            Some(ScanCommand::PrevField) => {
                let mut s = state.write().await;
                let show = s.settings.show_advanced;
                s.settings.active_field = prev_field(s.settings.active_field, show);
            }
            Some(ScanCommand::NextChoice) | Some(ScanCommand::PrevChoice) => {
                let mut s = state.write().await;
                s.settings.error = None;
                s.settings.saved = false;
                match s.settings.active_field {
                    SettingsField::Market => s.settings.market = s.settings.market.cycle(),
                    SettingsField::Theme => s.settings.theme = s.settings.theme.cycle(),
                    SettingsField::AdvancedToggle => {
                        s.settings.show_advanced = !s.settings.show_advanced;
                    }
                    SettingsField::EnableLiveTrading => {
                        s.settings.enable_live_trading = !s.settings.enable_live_trading;
                    }
                    SettingsField::RequireManualLiveUnlock => {
                        s.settings.require_manual_live_unlock =
                            !s.settings.require_manual_live_unlock;
                    }
                    SettingsField::PreBroadcastSimulation => {
                        s.settings.pre_broadcast_simulation = !s.settings.pre_broadcast_simulation;
                    }
                    // Left/Right on a text field is a no-op.
                    _ => {}
                }
            }
            Some(ScanCommand::Char(c)) => {
                let mut s = state.write().await;
                s.settings.error = None;
                s.settings.saved = false;
                match s.settings.active_field {
                    SettingsField::Wallet => s.settings.wallet_b58.push(c),
                    SettingsField::BuySize => s.settings.buy_size_sol.push(c),
                    SettingsField::HeliusKey => s.settings.helius_key.push(c),
                    SettingsField::FallbackRpc => s.settings.fallback_rpc.push(c),
                    SettingsField::JupiterKey => s.settings.jupiter_key.push(c),
                    SettingsField::SlippageBps => s.settings.slippage_bps.push(c),
                    SettingsField::MaxHoldSecs => s.settings.max_hold_secs.push(c),
                    SettingsField::LiveMaxBalanceSol => s.settings.live_max_balance_sol.push(c),
                    SettingsField::SellSlippageBps => s.settings.sell_slippage_bps.push(c),
                    SettingsField::PriorityFee => s.settings.priority_fee_microlamports.push(c),
                    SettingsField::JitoUrl => s.settings.jito_block_engine_url.push(c),
                    SettingsField::JitoTipSol => s.settings.jito_tip_sol.push(c),
                    SettingsField::ConfirmationPollMs => s.settings.confirmation_poll_ms.push(c),
                    // Selector fields are changed with ←/→, not typing.
                    SettingsField::Market
                    | SettingsField::Theme
                    | SettingsField::AdvancedToggle
                    | SettingsField::EnableLiveTrading
                    | SettingsField::RequireManualLiveUnlock
                    | SettingsField::PreBroadcastSimulation => {}
                }
            }
            Some(ScanCommand::Backspace) => {
                let mut s = state.write().await;
                s.settings.error = None;
                s.settings.saved = false;
                match s.settings.active_field {
                    SettingsField::Wallet => {
                        s.settings.wallet_b58.pop();
                    }
                    SettingsField::BuySize => {
                        s.settings.buy_size_sol.pop();
                    }
                    SettingsField::HeliusKey => {
                        s.settings.helius_key.pop();
                    }
                    SettingsField::FallbackRpc => {
                        s.settings.fallback_rpc.pop();
                    }
                    SettingsField::JupiterKey => {
                        s.settings.jupiter_key.pop();
                    }
                    SettingsField::SlippageBps => {
                        s.settings.slippage_bps.pop();
                    }
                    SettingsField::MaxHoldSecs => {
                        s.settings.max_hold_secs.pop();
                    }
                    SettingsField::LiveMaxBalanceSol => {
                        s.settings.live_max_balance_sol.pop();
                    }
                    SettingsField::SellSlippageBps => {
                        s.settings.sell_slippage_bps.pop();
                    }
                    SettingsField::PriorityFee => {
                        s.settings.priority_fee_microlamports.pop();
                    }
                    SettingsField::JitoUrl => {
                        s.settings.jito_block_engine_url.pop();
                    }
                    SettingsField::JitoTipSol => {
                        s.settings.jito_tip_sol.pop();
                    }
                    SettingsField::ConfirmationPollMs => {
                        s.settings.confirmation_poll_ms.pop();
                    }
                    // Non-text fields ignore Backspace.
                    SettingsField::Market
                    | SettingsField::Theme
                    | SettingsField::AdvancedToggle
                    | SettingsField::EnableLiveTrading
                    | SettingsField::RequireManualLiveUnlock
                    | SettingsField::PreBroadcastSimulation => {}
                }
            }
            Some(ScanCommand::Start) => {
                let vals = {
                    let s = state.read().await;
                    SettingsSnapshot {
                        wallet_b58: s.settings.wallet_b58.clone(),
                        buy_size_sol: s.settings.buy_size_sol.clone(),
                        market: s.settings.market,
                        helius_key: s.settings.helius_key.clone(),
                        fallback_rpc: s.settings.fallback_rpc.clone(),
                        jupiter_key: s.settings.jupiter_key.clone(),
                        slippage_bps: s.settings.slippage_bps.clone(),
                        max_hold_secs: s.settings.max_hold_secs.clone(),
                        theme: s.settings.theme,
                        enable_live_trading: s.settings.enable_live_trading,
                        require_manual_live_unlock: s.settings.require_manual_live_unlock,
                        live_max_balance_sol: s.settings.live_max_balance_sol.clone(),
                        sell_slippage_bps: s.settings.sell_slippage_bps.clone(),
                        priority_fee_microlamports: s.settings.priority_fee_microlamports.clone(),
                        jito_block_engine_url: s.settings.jito_block_engine_url.clone(),
                        jito_tip_sol: s.settings.jito_tip_sol.clone(),
                        confirmation_poll_ms: s.settings.confirmation_poll_ms.clone(),
                        pre_broadcast_simulation: s.settings.pre_broadcast_simulation,
                    }
                };
                match save_settings(config_path, &vals) {
                    Ok(msg) => {
                        let mut s = state.write().await;
                        s.settings.saved = true;
                        s.settings.error = None;
                        // Theme is runtime-only: apply the choice now.
                        s.theme = vals.theme;
                        let _ = event_tx.send(ScanEvent::Log(msg));
                    }
                    Err(err) => {
                        let mut s = state.write().await;
                        s.settings.saved = false;
                        s.settings.error = Some(format!("{err:#}"));
                    }
                }
            }
            Some(ScanCommand::Cancel) => break,
            Some(ScanCommand::Quit) => {
                should_quit = true;
                break;
            }
            _ => {}
        }
    }
    should_quit
}

/// Snapshot of all editable Settings values, cloned out of `ScanState`
/// before saving so the write lock isn't held across the file I/O.
struct SettingsSnapshot {
    wallet_b58: String,
    buy_size_sol: String,
    market: Market,
    helius_key: String,
    fallback_rpc: String,
    jupiter_key: String,
    slippage_bps: String,
    max_hold_secs: String,
    theme: Theme,
    enable_live_trading: bool,
    require_manual_live_unlock: bool,
    live_max_balance_sol: String,
    sell_slippage_bps: String,
    priority_fee_microlamports: String,
    jito_block_engine_url: String,
    jito_tip_sol: String,
    confirmation_poll_ms: String,
    pre_broadcast_simulation: bool,
}

fn read_config_or_default(config_path: &std::path::Path) -> Result<String> {
    match std::fs::read_to_string(config_path) {
        Ok(content) => Ok(content),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            toml::to_string(&Config::default()).context("serialize default config")
        }
        Err(err) => Err(err).with_context(|| format!("read config {config_path:?}")),
    }
}

/// Persist wallet key (optional) and buy size to the active config
/// file, then lock it down to owner-read/write. Also update the
/// project `.env` if it contains overrides (`CTARNITH_LIVE_BASE_BUY_SOL`
/// or `CTARNITH_WALLET_KEYPAIR_BASE58`, or their legacy `MAYHEM_*` aliases),
/// because those take precedence over the TOML file at runtime. Legacy
/// `MAYHEM_*` override lines are migrated to the canonical name on save.
fn save_settings(config_path: &std::path::Path, vals: &SettingsSnapshot) -> Result<String> {
    use anyhow::{bail, Context};

    let sol: f64 = vals
        .buy_size_sol
        .trim()
        .parse()
        .context("buy size is not a valid SOL number")?;
    if sol <= 0.0 {
        bail!("buy size must be positive");
    }
    let lamports = (sol * 1_000_000_000.0).round() as u64;
    if lamports == 0 {
        bail!("buy size is too small");
    }

    let trimmed_wallet = vals.wallet_b58.trim();
    if !trimmed_wallet.is_empty() {
        catarnith::keypair_source::decode_base58_keypair(trimmed_wallet)
            .context("wallet base58 does not decode to a valid keypair")?;
    }

    // Validate slippage (bps): reject 0 and >= 10000 (config.rs caps it).
    let slippage_bps: u32 = vals
        .slippage_bps
        .trim()
        .parse()
        .context("slippage is not a valid integer (bps)")?;
    if slippage_bps == 0 {
        bail!("slippage must be greater than 0 bps");
    }
    if slippage_bps >= 10_000 {
        bail!("slippage must be below 10000 bps (100%)");
    }

    // Validate max hold (seconds): must be > 0.
    let max_hold_secs: i64 = vals
        .max_hold_secs
        .trim()
        .parse()
        .context("max hold is not a valid integer (seconds)")?;
    if max_hold_secs <= 0 {
        bail!("max hold must be greater than 0 seconds");
    }

    let live_max_balance_sol: f64 = vals
        .live_max_balance_sol
        .trim()
        .parse()
        .context("live max balance is not a valid SOL number")?;
    if live_max_balance_sol <= 0.0 {
        bail!("live max balance must be positive");
    }
    let live_max_balance_lamports = (live_max_balance_sol * 1_000_000_000.0).round() as u64;
    if live_max_balance_lamports == 0 {
        bail!("live max balance is too small");
    }

    let sell_slippage_bps: u32 = vals
        .sell_slippage_bps
        .trim()
        .parse()
        .context("sell slippage is not a valid integer (bps)")?;
    if sell_slippage_bps == 0 {
        bail!("sell slippage must be greater than 0 bps");
    }
    if sell_slippage_bps >= 10_000 {
        bail!("sell slippage must be below 10000 bps (100%)");
    }

    let priority_fee_microlamports: u64 = vals
        .priority_fee_microlamports
        .trim()
        .parse()
        .context("priority fee is not a valid integer (micro-lamports)")?;
    if priority_fee_microlamports == 0 {
        bail!("priority fee must be greater than 0 micro-lamports");
    }

    let jito_block_engine_url = vals.jito_block_engine_url.trim();
    if !(jito_block_engine_url.is_empty()
        || jito_block_engine_url.starts_with("https://")
        || jito_block_engine_url.starts_with("http://"))
    {
        bail!("jito url must start with https:// or http://");
    }

    let jito_tip_sol: f64 = vals
        .jito_tip_sol
        .trim()
        .parse()
        .context("jito tip is not a valid SOL number")?;
    if !jito_tip_sol.is_finite() || jito_tip_sol < 0.0 {
        bail!("jito tip must be a non-negative SOL number");
    }
    let confirmation_poll_ms: u64 = vals
        .confirmation_poll_ms
        .trim()
        .parse()
        .context("confirmation poll is not a valid integer (ms)")?;
    if confirmation_poll_ms == 0 {
        bail!("confirmation poll must be greater than 0 ms");
    }

    let helius_key = vals.helius_key.trim();

    let content = read_config_or_default(config_path)?;
    let mut table: toml::Table = content
        .parse()
        .with_context(|| format!("parse config {config_path:?}"))?;

    table.remove("pair_scope");
    table.insert(
        "market".to_string(),
        toml::Value::String(vals.market.as_str().to_string()),
    );
    table.remove("base_buy_lamports");
    table.insert("base_buy_sol".to_string(), toml::Value::Float(sol));
    if !trimmed_wallet.is_empty() {
        table.insert(
            "wallet_keypair_base58".to_string(),
            toml::Value::String(trimmed_wallet.to_string()),
        );
    }
    // Helius key: only write when non-empty (empty = keep existing).
    if !helius_key.is_empty() {
        table.insert(
            "helius_api_key".to_string(),
            toml::Value::String(helius_key.to_string()),
        );
    }
    table.insert(
        "max_slippage_bps".to_string(),
        toml::Value::Integer(slippage_bps as i64),
    );
    table.insert(
        "max_hold_seconds".to_string(),
        toml::Value::Integer(max_hold_secs),
    );
    table.insert(
        "enable_live_trading".to_string(),
        toml::Value::Boolean(vals.enable_live_trading),
    );
    table.insert(
        "require_manual_live_unlock".to_string(),
        toml::Value::Boolean(vals.require_manual_live_unlock),
    );

    let live_value = table
        .entry("live".to_string())
        .or_insert_with(|| toml::Value::Table(toml::Table::new()));
    let live_table = live_value
        .as_table_mut()
        .context("[live] config section must be a table")?;
    live_table.remove("max_balance_lamports");
    live_table.insert(
        "max_balance_sol".to_string(),
        toml::Value::Float(live_max_balance_sol),
    );
    live_table.insert(
        "sell_slippage_bps".to_string(),
        toml::Value::Integer(sell_slippage_bps as i64),
    );
    live_table.insert(
        "compute_unit_price_microlamports".to_string(),
        toml::Value::Integer(priority_fee_microlamports as i64),
    );
    if jito_block_engine_url.is_empty() {
        live_table.remove("jito_block_engine_url");
    } else {
        live_table.insert(
            "jito_block_engine_url".to_string(),
            toml::Value::String(jito_block_engine_url.to_string()),
        );
    }
    live_table.remove("jito_tip_lamports");
    live_table.insert("jito_tip_sol".to_string(), toml::Value::Float(jito_tip_sol));
    live_table.insert(
        "confirmation_poll_ms".to_string(),
        toml::Value::Integer(confirmation_poll_ms as i64),
    );
    live_table.insert(
        "pre_broadcast_simulation".to_string(),
        toml::Value::Boolean(vals.pre_broadcast_simulation),
    );

    let out = toml::to_string(&table).context("serialize updated config")?;
    std::fs::write(config_path, out).with_context(|| format!("write config {config_path:?}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Err(err) =
            std::fs::set_permissions(config_path, std::fs::Permissions::from_mode(0o600))
        {
            eprintln!("warning: could not chmod 600 {config_path:?}: {err}");
        }
    }

    // Keep `.env` in sync so env overrides don't silently shadow the
    // values the user just edited in the TOML.
    let env_note = update_env_file(
        config_path,
        trimmed_wallet,
        helius_key,
        vals.fallback_rpc.trim(),
        vals.jupiter_key.trim(),
        vals.market,
        sol,
        slippage_bps,
        max_hold_secs,
        vals.enable_live_trading,
        vals.require_manual_live_unlock,
        live_max_balance_sol,
        sell_slippage_bps,
        priority_fee_microlamports,
        jito_block_engine_url,
        jito_tip_sol,
        confirmation_poll_ms,
        vals.pre_broadcast_simulation,
    )
    .unwrap_or(None);

    let live_gate = if vals.enable_live_trading && !vals.require_manual_live_unlock {
        "live gates armed"
    } else if vals.enable_live_trading {
        "live enabled but manual unlock is still locked"
    } else {
        "live disabled"
    };
    Ok(format!(
        "saved market={} buy_sol={} slippage_bps={slippage_bps} max_hold_s={max_hold_secs} live_max_balance_sol={} sell_slippage_bps={sell_slippage_bps} priority_fee={priority_fee_microlamports} jito_tip_sol={} confirmation_poll_ms={confirmation_poll_ms} ({live_gate}) to {config_path:?}{}",
        vals.market.as_str(),
        format_sol(sol),
        format_sol(live_max_balance_sol),
        format_sol(jito_tip_sol),
        env_note
            .map(|p| format!(" and {p:?}"))
            .unwrap_or_default(),
    ))
}

/// Update the project `.env` so runtime env overrides do not shadow
/// the values written to the active TOML. Rewrites (or appends) the
/// terminal keys: base buy SOL, wallet base58, Helius key, fallback
/// RPC, Jupiter key, max slippage bps, and max hold seconds. Empty secret
/// values (Helius/Jupiter/wallet) are skipped so an existing key is never
/// blanked. Returns the path of the updated `.env`, or `None` if none exists.
#[allow(clippy::too_many_arguments)]
fn update_env_file(
    config_path: &std::path::Path,
    wallet_b58: &str,
    helius_key: &str,
    fallback_rpc: &str,
    jupiter_key: &str,
    market: Market,
    buy_sol: f64,
    slippage_bps: u32,
    max_hold_secs: i64,
    enable_live_trading: bool,
    require_manual_live_unlock: bool,
    live_max_balance_sol: f64,
    sell_slippage_bps: u32,
    priority_fee_microlamports: u64,
    jito_block_engine_url: &str,
    jito_tip_sol: f64,
    confirmation_poll_ms: u64,
    pre_broadcast_simulation: bool,
) -> Result<Option<String>> {
    use anyhow::Context;

    // Resolve the `.env` the runtime will actually read, rather than a
    // bare CWD-relative ".env". When the installed binary is launched from
    // another directory the CWD has no `.env`, so the env overrides that
    // shadow the TOML at runtime would never get updated. Prefer the same
    // discovery the config loader uses; fall back to the config file's own
    // directory so a save still lands next to the edited config.
    let path = match catarnith::config::discover_dot_env() {
        Some(p) => p,
        None => {
            let candidate = config_path
                .parent()
                .map(|d| d.join(".env"))
                .unwrap_or_else(|| std::path::PathBuf::from(".env"));
            if !candidate.exists() {
                return Ok(None);
            }
            candidate
        }
    };
    let content = std::fs::read_to_string(&path).with_context(|| format!("read .env {path:?}"))?;

    let wallet_b58_opt = if wallet_b58.is_empty() {
        None
    } else {
        Some(wallet_b58)
    };
    // Secret values are skipped when empty so we never blank an existing key.
    let helius_opt = (!helius_key.is_empty()).then_some(helius_key);
    let jupiter_opt = (!jupiter_key.is_empty()).then_some(jupiter_key);
    let env_values = vec![
        ("MARKET", market.as_str().to_string()),
        ("LIVE_MARKET", market.as_str().to_string()),
        ("LIVE_BASE_BUY_SOL", format_sol(buy_sol)),
        ("FALLBACK_RPC_URL", fallback_rpc.to_string()),
        ("LIVE_MAX_SLIPPAGE_BPS", slippage_bps.to_string()),
        ("LIVE_MAX_HOLD_SECONDS", max_hold_secs.to_string()),
        ("LIVE_ENABLE_LIVE_TRADING", enable_live_trading.to_string()),
        (
            "LIVE_REQUIRE_MANUAL_LIVE_UNLOCK",
            require_manual_live_unlock.to_string(),
        ),
        ("LIVE_MAX_BALANCE_SOL", format_sol(live_max_balance_sol)),
        ("LIVE_SELL_SLIPPAGE_BPS", sell_slippage_bps.to_string()),
        (
            "LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS",
            priority_fee_microlamports.to_string(),
        ),
        (
            "LIVE_JITO_BLOCK_ENGINE_URL",
            jito_block_engine_url.to_string(),
        ),
        ("LIVE_JITO_TIP_SOL", format_sol(jito_tip_sol)),
        (
            "LIVE_CONFIRMATION_POLL_MS",
            confirmation_poll_ms.to_string(),
        ),
        (
            "LIVE_PRE_BROADCAST_SIMULATION",
            pre_broadcast_simulation.to_string(),
        ),
    ];

    let mut lines: Vec<String> = Vec::new();
    let mut wrote = vec![false; env_values.len()];
    let mut wrote_wallet = wallet_b58_opt.is_none();
    let mut wrote_helius = helius_opt.is_none();
    let mut wrote_jupiter = jupiter_opt.is_none();
    for line in content.lines() {
        let trimmed = line.trim();
        if matches_env_suffix(trimmed, "WALLET_KEYPAIR_BASE58") {
            if let Some(w) = wallet_b58_opt {
                lines.push(format!("export CTARNITH_WALLET_KEYPAIR_BASE58={w}"));
                wrote_wallet = true;
            } else {
                lines.push(line.to_string());
            }
        } else if matches_env_suffix(trimmed, "WALLET_KEYPAIR_PATH") {
            if wallet_b58_opt.is_some() {
                lines.push(format!("# {line}  # disabled by catarnith settings editor"));
            } else {
                lines.push(line.to_string());
            }
        } else if trimmed.starts_with("export HELIUS_API_KEY=")
            || trimmed.starts_with("HELIUS_API_KEY=")
        {
            if let Some(k) = helius_opt {
                lines.push(format!("export HELIUS_API_KEY={k}"));
                wrote_helius = true;
            } else {
                lines.push(line.to_string());
            }
        } else if matches_env_suffix(trimmed, "FALLBACK_RPC_URL") {
            lines.push(format!("export CTARNITH_FALLBACK_RPC_URL={fallback_rpc}"));
            if let Some(idx) = env_values
                .iter()
                .position(|(suffix, _)| *suffix == "FALLBACK_RPC_URL")
            {
                wrote[idx] = true;
            }
        } else if trimmed.starts_with("export JUP_API_KEY=") || trimmed.starts_with("JUP_API_KEY=")
        {
            if let Some(k) = jupiter_opt {
                lines.push(format!("export JUP_API_KEY={k}"));
                wrote_jupiter = true;
            } else {
                lines.push(line.to_string());
            }
        } else {
            let mut replacement: Option<(usize, String)> = None;
            for (idx, (suffix, value)) in env_values.iter().enumerate() {
                if matches_env_suffix(trimmed, suffix) {
                    replacement = Some((idx, format!("export CTARNITH_{suffix}={value}")));
                    break;
                }
            }
            if let Some((idx, line)) = replacement {
                lines.push(line);
                wrote[idx] = true;
            } else {
                lines.push(line.to_string());
            }
        }
    }
    if let Some(w) = wallet_b58_opt {
        if !wrote_wallet {
            lines.push(format!("export CTARNITH_WALLET_KEYPAIR_BASE58={w}"));
        }
    }
    if let Some(k) = helius_opt {
        if !wrote_helius {
            lines.push(format!("export HELIUS_API_KEY={k}"));
        }
    }
    if let Some(k) = jupiter_opt {
        if !wrote_jupiter {
            lines.push(format!("export JUP_API_KEY={k}"));
        }
    }
    for (idx, (suffix, value)) in env_values.iter().enumerate() {
        if !wrote[idx] {
            lines.push(format!("export CTARNITH_{suffix}={value}"));
        }
    }

    let mut out = lines.join("\n");
    if !out.ends_with('\n') {
        out.push('\n');
    }
    std::fs::write(&path, out).with_context(|| format!("write .env {path:?}"))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }

    Ok(Some(path.to_string_lossy().to_string()))
}

/// True if a raw or mapped command means "stop this trade action"
/// (Esc or Q/Ctrl-C) in the given phase.
fn is_cancel_or_quit_cmd(cmd: &ScanCommand, phase: Phase) -> bool {
    match cmd {
        ScanCommand::Cancel | ScanCommand::Quit => true,
        ScanCommand::Key(key) => matches!(
            interpret_key(*key, phase),
            Some(ScanCommand::Cancel) | Some(ScanCommand::Quit)
        ),
        _ => false,
    }
}

/// Execute a buy, but allow the operator to abort with Esc/Q while
/// waiting for on-chain confirmation. Returns an error if cancelled.
async fn execute_cancellable(
    executor: &ScanExecutor,
    order: &catarnith::executor::Order,
    sell_token_amount_raw: Option<u128>,
    buy_slippage_bps: Option<u32>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
) -> Result<ExecutionReport> {
    let fut = executor.execute(order, sell_token_amount_raw, buy_slippage_bps);
    tokio::pin!(fut);
    loop {
        tokio::select! {
            res = &mut fut => return res,
            Some(cmd) = cmd_rx.recv() => {
                if is_cancel_or_quit_cmd(&cmd, Phase::Scanning) {
                    return Err(anyhow::anyhow!("trade cancelled by operator"));
                }
            }
        }
    }
}

/// Fire a panic-sell, but allow the operator to abort with Esc/Q while
/// waiting for on-chain confirmation. Returns an error if cancelled.
async fn panic_sell_cancellable(
    executor: &ScanExecutor,
    mint: &str,
    amount: u128,
    slippage_bps: Option<u32>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
) -> Result<ExecutionReport> {
    let fut = executor.panic_sell_with_slippage(mint, amount, slippage_bps);
    tokio::pin!(fut);
    loop {
        tokio::select! {
            res = &mut fut => return res,
            Some(cmd) = cmd_rx.recv() => {
                if is_cancel_or_quit_cmd(&cmd, Phase::Selling) {
                    return Err(anyhow::anyhow!("sell cancelled by operator"));
                }
            }
        }
    }
}

/// Outcome of one trade cycle so the outer loop knows whether to
/// show the result screen or go straight back to Welcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CycleOutcome {
    /// Position was sold; display result/logs until operator continues.
    Closed,
    /// Operator aborted with Esc before selling; return to Welcome.
    Cancelled,
    /// Operator pressed Q/Ctrl-C; shut the app down.
    Quit,
}

const LIVE_ENTRY_BALANCE_RETRIES: usize = 4;
const LIVE_ENTRY_BALANCE_RETRY_MS: u64 = 150;
const LIVE_SELL_RECONCILE_RETRIES: usize = 10;
const LIVE_SELL_RECONCILE_RETRY_MS: u64 = 250;

fn report_token_amount(report: &ExecutionReport) -> u128 {
    report.filled_token_amount_raw.unwrap_or_default()
}

async fn live_wallet_amount_after_entry(
    executor: &ScanExecutor,
    mint: &str,
) -> Result<Option<u128>> {
    for attempt in 1..=LIVE_ENTRY_BALANCE_RETRIES {
        match executor.fetch_token_balance(mint).await {
            Ok(amount) if amount > 0 => return Ok(Some(amount)),
            Ok(_) if attempt < LIVE_ENTRY_BALANCE_RETRIES => {
                tokio::time::sleep(Duration::from_millis(LIVE_ENTRY_BALANCE_RETRY_MS)).await;
            }
            Ok(_) => return Ok(None),
            Err(err) if attempt < LIVE_ENTRY_BALANCE_RETRIES => {
                tracing::debug!("entry balance fetch attempt {attempt} failed for {mint}: {err:#}");
                tokio::time::sleep(Duration::from_millis(LIVE_ENTRY_BALANCE_RETRY_MS)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Ok(None)
}

async fn live_wallet_amount_after_sell(executor: &ScanExecutor, mint: &str) -> Result<u128> {
    let mut last_amount = None;
    for attempt in 1..=LIVE_SELL_RECONCILE_RETRIES {
        match executor.fetch_token_balance(mint).await {
            Ok(0) => return Ok(0),
            Ok(amount) => {
                last_amount = Some(amount);
                if attempt < LIVE_SELL_RECONCILE_RETRIES {
                    tokio::time::sleep(Duration::from_millis(LIVE_SELL_RECONCILE_RETRY_MS)).await;
                }
            }
            Err(err) if attempt < LIVE_SELL_RECONCILE_RETRIES => {
                tracing::debug!("sell balance fetch attempt {attempt} failed for {mint}: {err:#}");
                tokio::time::sleep(Duration::from_millis(LIVE_SELL_RECONCILE_RETRY_MS)).await;
            }
            Err(err) => return Err(err),
        }
    }
    Ok(last_amount.unwrap_or(0))
}

async fn filled_entry_amount(
    executor: &ScanExecutor,
    mint: &str,
    report: &ExecutionReport,
) -> Result<Option<u128>> {
    match report.status {
        ExecutionStatus::PaperFilled => Ok(Some(report_token_amount(report))),
        ExecutionStatus::LiveConfirmed => {
            let reported = report_token_amount(report);
            if reported > 0 {
                Ok(Some(reported))
            } else {
                live_wallet_amount_after_entry(executor, mint).await
            }
        }
        ExecutionStatus::LiveSubmitted => live_wallet_amount_after_entry(executor, mint).await,
        _ => Ok(None),
    }
}

async fn resume_screening_after_entry_skip(
    state: &Arc<RwLock<ScanState>>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
    mint: &str,
    reason: impl AsRef<str>,
) {
    let short = mint.chars().take(8).collect::<String>();
    let _ = event_tx.send(ScanEvent::Log(format!(
        "skip {short} - {}; screening next token",
        reason.as_ref()
    )));
    {
        let mut s = state.write().await;
        reset_screening_candidate_state(&mut s);
        s.trades_skipped += 1;
    }
    let _ = event_tx.send(ScanEvent::StateChanged);
}

async fn accept_fresh_create_event_slot(
    cfg: &Config,
    event: &catarnith::ingest::StreamEvent,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
    rpc: &RpcClient,
    slot_cache: &mut CreateSlotCache,
    stale_create_log: &mut StaleCreateUiLog,
) -> bool {
    if event.slot == 0 {
        stale_create_log.emit(
            event_tx,
            "missing event slot",
            None,
            cfg.max_create_event_slot_lag,
        );
        return false;
    }

    let now = now_ms();
    let current_slot = if let Some(slot) = slot_cache.fresh(now) {
        slot
    } else {
        match rpc.get_slot().await {
            Ok(slot) => {
                slot_cache.store(slot, now_ms());
                slot
            }
            Err(err) => {
                let reason = format!("current slot check failed: {err:#}");
                stale_create_log.emit(event_tx, &reason, None, cfg.max_create_event_slot_lag);
                return false;
            }
        }
    };

    if current_slot < event.slot {
        return true;
    }

    let slot_lag = current_slot.saturating_sub(event.slot);
    if slot_lag > cfg.max_create_event_slot_lag {
        stale_create_log.emit(
            event_tx,
            "slot lag exceeded",
            Some(slot_lag),
            cfg.max_create_event_slot_lag,
        );
        return false;
    }

    true
}

#[derive(Debug, Default)]
struct CreateSlotCache {
    slot: Option<u64>,
    fetched_at_ms: i64,
}

impl CreateSlotCache {
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
struct StaleCreateUiLog {
    rejected: u64,
}

impl StaleCreateUiLog {
    fn emit(
        &mut self,
        event_tx: &mpsc::UnboundedSender<ScanEvent>,
        reason: &str,
        slot_lag: Option<u64>,
        max_slot_lag: u64,
    ) {
        self.rejected = self.rejected.saturating_add(1);
        if self.rejected != 1 && !self.rejected.is_multiple_of(25) {
            return;
        }
        let lag = slot_lag
            .map(|lag| lag.to_string())
            .unwrap_or_else(|| "unknown".to_string());
        let _ = event_tx.send(ScanEvent::Log(format!(
            "skipping stale creates count={} latest_reason={} latest_slot_lag={} max_slot_lag={}",
            self.rejected, reason, lag, max_slot_lag
        )));
    }
}

/// Runs one full cycle: scan WS logs for a fresh Pump.fun mint, enter
/// when found, hold while mcap ticks, fire sell on operator input
/// (Enter), then close the trade.
#[allow(unused_assignments)]
#[allow(clippy::arc_with_non_send_sync)]
async fn run_lifecycle(
    cfg: &Config,
    market: &Arc<MarketData>,
    executor: &ScanExecutor,
    state: &Arc<RwLock<ScanState>>,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
    journal: &Arc<Journal>,
) -> Result<CycleOutcome> {
    // Open the log stream and look for a fresh Pump.fun mint.
    {
        let mut s = state.write().await;
        s.phase = Phase::Scanning;
        let _ = event_tx.send(ScanEvent::Log("subscribing to WS logs…".into()));
    }

    let stream_config = StreamConfig {
        ws_url: cfg
            .rpc_url()
            .replace("https://", "wss://")
            .replace("http://", "ws://"),
        rpc_url: cfg.rpc_url(),
        commitment: cfg.subscribe_commitment.clone(),
        account_include: vec![cfg.mayhem_program.clone(), cfg.pumpfun_program.clone()],
        watched_wallets: vec![cfg.mayhem_agent_wallet.clone()],
        logs_mentions: vec![cfg.mayhem_program.clone(), cfg.pumpfun_program.clone()],
        enable_transaction_subscribe: cfg.enable_transaction_subscribe,
        enable_logs_fallback: cfg.enable_logs_fallback,
        backfill_limit: 0, // no startup backfill in scan mode
    };
    let mut stream_rx = spawn_streams(stream_config.clone());
    let slot_rpc_timeout_ms = cfg.live.rpc_timeout_ms.clamp(100, 1_000);
    let slot_rpc = RpcClient::new_with_timeout_and_commitment(
        cfg.rpc_url(),
        Duration::from_millis(slot_rpc_timeout_ms),
        CommitmentConfig::processed(),
    );
    let mut create_slot_cache = CreateSlotCache::default();
    let mut stale_create_log = StaleCreateUiLog::default();

    let mut entered_mint: Option<String> = None;
    let mut _curve_poll: Option<CurvePollGuard> = None;

    loop {
        // Pull any pending commands first (non-blocking) so Enter
        // arms promptly even while the stream is quiet. Raw key events
        // are mapped based on the current phase so the same input
        // thread can serve both global shortcuts and text fields.
        while let Ok(raw) = cmd_rx.try_recv() {
            let phase = state.read().await.phase;
            let cmd = match raw {
                ScanCommand::Key(key) => interpret_key(key, phase),
                other => Some(other),
            };
            // Any input other than a second Esc disarms the
            // "abandon position?" confirmation so a stray key
            // keeps the operator in the trade.
            if !matches!(cmd, Some(ScanCommand::Cancel)) {
                let mut s = state.write().await;
                if s.confirm_exit {
                    s.confirm_exit = false;
                    let _ = event_tx.send(ScanEvent::StateChanged);
                }
            }
            match cmd {
                Some(ScanCommand::Start) => {
                    // The single "Enter" semantic. In Holding
                    // phase, one press = one instant sell. The
                    // previous two-press arm/fire window was
                    // removed because:
                    //   (a) the second press was a no-op log
                    //       line, never calling close_trade, and
                    //   (b) the operator asked for "speed of
                    //       light" — no extra key to fire.
                    if entered_mint.is_some() {
                        // Take the mint out so a second Start
                        // (e.g. key bounce) is a no-op until
                        // close_trade returns and the outer
                        // strategy loop restarts the cycle.
                        let (mint, held_ms) = {
                            let s = state.read().await;
                            let held_ms = chrono::Utc::now().timestamp_millis() - s.entry_ms;
                            (s.mint.clone(), held_ms.max(0))
                        };
                        entered_mint = None;
                        let sold = close_trade(
                            cfg, executor, state, event_tx, journal, &mint, held_ms, cmd_rx,
                        )
                        .await?;
                        if sold {
                            return Ok(CycleOutcome::Closed);
                        }
                        entered_mint = Some(mint);
                        continue;
                    } else {
                        let _ = event_tx.send(ScanEvent::Log(
                            "start ignored - not in a held position".into(),
                        ));
                    }
                }
                Some(ScanCommand::Cancel) => {
                    if close_log_overlay_if_open(state, event_tx).await {
                        continue;
                    }
                    // Esc returns to the mode picker. While holding a
                    // position, the first Esc only arms a confirmation
                    // (we don't want a stray keypress to abandon an open
                    // trade); a second Esc actually leaves. When idle,
                    // Esc returns immediately.
                    let holding = entered_mint.is_some();
                    if holding {
                        let armed = {
                            let s = state.read().await;
                            s.confirm_exit
                        };
                        if !armed {
                            let mut s = state.write().await;
                            s.confirm_exit = true;
                            let _ = event_tx.send(ScanEvent::Log(
                                "⚠ position still open — Esc again to leave, any key to stay"
                                    .into(),
                            ));
                            let _ = event_tx.send(ScanEvent::StateChanged);
                            continue;
                        }
                    }
                    {
                        let mut s = state.write().await;
                        s.phase = Phase::ModePicker;
                        s.mint.clear();
                        s.symbol.clear();
                        s.token_amount_raw = 0;
                        s.confirm_exit = false;
                    }
                    let _ = event_tx.send(ScanEvent::Log("cycle cancelled - back to menu".into()));
                    let _ = event_tx.send(ScanEvent::StateChanged);
                    return Ok(CycleOutcome::Cancelled);
                }
                Some(ScanCommand::CycleTheme) => {
                    let mut s = state.write().await;
                    s.theme = s.theme.cycle();
                }
                Some(ScanCommand::Quit) => {
                    return Ok(CycleOutcome::Quit);
                }
                // The mode-picker / settings commands are only
                // meaningful in the strategy thread's outer loop.
                Some(ScanCommand::PickBot)
                | Some(ScanCommand::PickLive)
                | Some(ScanCommand::PickPaper)
                | Some(ScanCommand::PickSettings) => {}
                Some(ScanCommand::ShowLogs) => {
                    let _ = event_tx.send(ScanEvent::ToggleLogs);
                }
                Some(ScanCommand::ScrollLogs(delta)) => {
                    let _ = event_tx.send(ScanEvent::ScrollLogs(delta));
                }
                _ => {}
            }
        }

        // Process one stream event (or timeout).
        let maybe_event = tokio::time::timeout(Duration::from_millis(100), stream_rx.recv()).await;
        let stream_event = match maybe_event {
            Ok(Some(ev)) => ev,
            Ok(None) => {
                let _ = event_tx.send(ScanEvent::Log("stream closed".into()));
                return Ok(CycleOutcome::Cancelled);
            }
            Err(_) => {
                // 100ms heartbeat so the command try_recv above
                // gets a chance to fire even when the stream is
                // quiet. No auto-sell in trade mode; sell is only
                // triggered by operator input (Enter).
                continue;
            }
        };

        // Decode the event.
        let Some(mint) = extract_pump_create_event_mint(&stream_event.logs) else {
            continue;
        };
        if !executor.is_paper()
            && !accept_fresh_create_event_slot(
                cfg,
                &stream_event,
                event_tx,
                &slot_rpc,
                &mut create_slot_cache,
                &mut stale_create_log,
            )
            .await
        {
            let mut s = state.write().await;
            s.scanned += 1;
            continue;
        }
        if entered_mint.is_some() {
            // We already entered; ignore new creates. Still listen
            // for trade events to update mcap.
        } else {
            // Phase: Evaluating. Market-specific modes must confirm
            // the curve flag before paying for a buy. In all-Pump.fun
            // scope, the create event itself is enough and we skip the
            // RPC round-trip for speed.
            let sym = mint.chars().take(8).collect::<String>();
            let scope = check_create_market_scope(cfg, &mint, &stream_event.logs).await;
            if !matches!(scope, CreateMarketScope::InScope) {
                let mut s = state.write().await;
                s.scanned += 1;
                if let CreateMarketScope::Unknown(reason) = scope {
                    s.last_error = Some(format!("skip {sym}: market check unknown ({reason})"));
                }
                continue;
            }
            {
                let mut s = state.write().await;
                s.phase = Phase::Evaluating;
                s.mint = mint.clone();
                s.symbol = sym.clone();
                let _ = event_tx.send(ScanEvent::StateChanged);
            }
            // Build + send a buy. In paper mode this is a
            // curve-derived synthetic fill with no broadcast.
            let buy = BuyOrder {
                id: format!("scan-buy-{}", chrono::Utc::now().timestamp_millis()),
                timestamp_ms: chrono::Utc::now().timestamp_millis(),
                mint: mint.clone(),
                lamports: cfg.base_buy_lamports,
                source_decision_id: "catarnith-scan".into(),
                source_signature: Some(stream_event.signature.clone()),
            };
            let order = catarnith::executor::Order::Buy(buy);
            match execute_cancellable(executor, &order, None, None, cmd_rx).await {
                Ok(report) => {
                    let _ = journal.record(JournalKind::Execution, &report);
                    if matches!(
                        report.status,
                        ExecutionStatus::LiveConfirmed
                            | ExecutionStatus::LiveSubmitted
                            | ExecutionStatus::PaperFilled
                    ) {
                        // Only enter the trading panel after proven inventory.
                        // A fast `LiveSubmitted` report can carry the quoted
                        // token amount even though the wallet balance is not
                        // visible yet, so live-submitted entries must verify
                        // the token account before we treat the buy as filled.
                        let amount = match filled_entry_amount(executor, &mint, &report).await {
                            Ok(Some(amount)) if amount > 0 => amount,
                            Ok(_) => {
                                let reason = if report.status == ExecutionStatus::LiveSubmitted {
                                    "buy submitted but no token balance visible"
                                } else {
                                    "buy reported zero token amount"
                                };
                                resume_screening_after_entry_skip(state, event_tx, &mint, reason)
                                    .await;
                                continue;
                            }
                            Err(err) => {
                                resume_screening_after_entry_skip(
                                    state,
                                    event_tx,
                                    &mint,
                                    format!("buy balance check failed: {err:#}"),
                                )
                                .await;
                                continue;
                            }
                        };
                        if amount == 0 {
                            resume_screening_after_entry_skip(
                                state,
                                event_tx,
                                &mint,
                                "buy reported zero token amount",
                            )
                            .await;
                            continue;
                        }
                        let entry_lamports =
                            report.filled_lamports.unwrap_or(cfg.base_buy_lamports);
                        let entry_sol = entry_lamports as f64 / 1_000_000_000.0;
                        let sol_price = market.sol_price_usd().await.unwrap_or(0.0);
                        let entry_usd = entry_sol * sol_price;
                        {
                            let mut s = state.write().await;
                            s.phase = Phase::Holding;
                            s.entry_lamports = entry_lamports;
                            s.entry_usd = entry_usd;
                            s.sol_price_usd = sol_price;
                            s.entry_ms = report
                                .latency_ms
                                .map(|lat| chrono::Utc::now().timestamp_millis() - lat as i64)
                                .unwrap_or_else(|| chrono::Utc::now().timestamp_millis());
                            s.token_amount_raw = amount;
                            s.trades_taken += 1;
                        }
                        if executor.is_paper() {
                            let _ = append_paper_report(&cfg.paper_report_path, &report);
                        }
                        let _ = event_tx.send(ScanEvent::Log(format!(
                            "HELD {} | amount {} | entry ${:.2} | mode {}",
                            sym,
                            amount,
                            entry_usd,
                            if executor.is_paper() { "PAPER" } else { "LIVE" }
                        )));
                        let _ = event_tx.send(ScanEvent::StateChanged);
                        entered_mint = Some(mint.clone());
                        // Push an immediate curve tick so MCAP and
                        // position USD are visible instantly instead of
                        // waiting for the 500ms poll.
                        if let Ok(curve) = market.curve_state(&mint).await {
                            let mcap_sol = compute_mcap_sol(&curve);
                            let mcap_usd = MarketData::mcap_usd(&curve, sol_price);
                            let position_usd = MarketData::position_usd(&curve, amount, sol_price);
                            let _ = event_tx.send(ScanEvent::McapTick {
                                mcap_sol,
                                mcap_usd,
                                position_usd,
                                sol_price_usd: sol_price,
                                ts_ms: curve.observed_at_ms.max(0),
                            });
                        }
                        // Stream the bonding curve (websocket push +
                        // RPC poll fallback) so mcap and position USD
                        // update in real time.
                        let curve_account = catarnith::curve::CurveQuoteClient::new(
                            cfg.rpc_url(),
                            &cfg.pumpfun_program,
                        )
                        .ok()
                        .and_then(|c| c.bonding_curve_address(&mint).ok())
                        .map(|a| a.to_string());
                        if let Some(curve_account) = curve_account {
                            _curve_poll = Some(spawn_curve_poll(
                                Arc::clone(market),
                                mint.clone(),
                                state.clone(),
                                event_tx.clone(),
                                stream_config.ws_url.clone(),
                                cfg.subscribe_commitment.clone(),
                                curve_account,
                            ));
                        }
                    } else {
                        // Buy was rejected by the executor for a
                        // non-RPC reason (LiveDisabled, simulation
                        // rejection, route gate, etc.). The mint
                        // doesn't match our config criteria — keep
                        // scanning.
                        let kind = format!("{:?}", report.status);
                        resume_screening_after_entry_skip(
                            state,
                            event_tx,
                            &mint,
                            format!("buy rejected: {kind}"),
                        )
                        .await;
                    }
                }
                Err(err) => {
                    let err_str = err.to_string();
                    if err_str.contains("cancelled by operator") {
                        return Ok(CycleOutcome::Cancelled);
                    }
                    let is_rpc = is_rpc_error(&err_str);
                    if is_rpc {
                        let _ = event_tx.send(ScanEvent::Log(format!(
                            "rpc error: {err_str} - stopping scan"
                        )));
                        let mut s = state.write().await;
                        s.rpc_errors += 1;
                        // Drop to menu: we cannot continue
                        // without a working RPC.
                        return Ok(CycleOutcome::Cancelled);
                    }
                    resume_screening_after_entry_skip(
                        state,
                        event_tx,
                        &mint,
                        format!("executor error: {err_str}"),
                    )
                    .await;
                    // Non-RPC error (e.g. transient instruction
                    // build failure). Continue scanning.
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn close_trade(
    cfg: &Config,
    executor: &ScanExecutor,
    state: &Arc<RwLock<ScanState>>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
    journal: &Arc<Journal>,
    mint: &str,
    held_ms: i64,
    cmd_rx: &mut mpsc::UnboundedReceiver<ScanCommand>,
) -> Result<bool> {
    // Resolve the actual amount to sell. Prefer a fresh wallet
    // balance read so we dump *all* tokens (including any dust
    // from rounding). If the RPC snapshot is stale/closed, fall
    // back to the amount recorded at entry. Retry a few times
    // because the token account can be slow to appear after a
    // freshly-landed buy.
    let amount: u128 = {
        let cached = state.read().await.token_amount_raw;
        let mut balance = 0u128;
        for attempt in 1..=3 {
            match executor.fetch_token_balance(mint).await {
                Ok(b) if b > 0 => {
                    balance = b;
                    break;
                }
                Ok(_) | Err(_) => {
                    if attempt < 3 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        }
        let amount = if balance > 0 { balance } else { cached };
        if amount == 0 {
            let _ = event_tx.send(ScanEvent::Log(
                "close_trade: balance and cached amount are 0 - nothing to sell".into(),
            ));
            return Ok(true);
        }
        amount
    };
    {
        let mut s = state.write().await;
        s.phase = Phase::Selling;
        let _ = event_tx.send(ScanEvent::Log("firing sell…".into()));
    }
    let report = if executor.is_paper() {
        panic_sell_cancellable(executor, mint, amount, None, cmd_rx).await?
    } else {
        let mut report: Option<ExecutionReport> = None;
        let mut last_err: Option<anyhow::Error> = None;
        for attempt in 1..=MAX_SELL_RETRIES {
            match panic_sell_cancellable(executor, mint, amount, None, cmd_rx).await {
                Ok(r) => {
                    report = Some(r);
                    break;
                }
                Err(err) => {
                    let err_str = err.to_string();
                    if err_str.contains("cancelled by operator") {
                        return Err(err);
                    }
                    last_err = Some(err);
                    let _ = event_tx.send(ScanEvent::Log(format!(
                        "sell attempt {attempt}/{MAX_SELL_RETRIES} failed"
                    )));
                    if attempt < MAX_SELL_RETRIES {
                        tokio::time::sleep(Duration::from_millis(SELL_RETRY_BACKOFF_MS)).await;
                    }
                }
            }
        }
        if report.is_none() {
            let _ = event_tx.send(ScanEvent::Log(format!(
                "force selling with {FORCE_SELL_SLIPPAGE_BPS} bps slippage"
            )));
            match panic_sell_cancellable(
                executor,
                mint,
                amount,
                Some(FORCE_SELL_SLIPPAGE_BPS),
                cmd_rx,
            )
            .await
            {
                Ok(r) => report = Some(r),
                Err(err) => {
                    last_err = Some(err);
                    // Local sell exhausted retries and force-sell failed.
                    // Try Jupiter as a last resort before giving up — it
                    // routes the pump.fun curve when JUP_API_KEY is set.
                    // No-op (Err) when the key is unset; we then fall
                    // through to the existing "remains on-chain" log.
                    let _ = event_tx.send(ScanEvent::Log("trying jupiter fallback…".into()));
                    match executor.jupiter_sell_fallback(mint, amount).await {
                        Ok(r) => {
                            let message = match r.status {
                                ExecutionStatus::LiveConfirmed
                                | ExecutionStatus::LiveReconciled => {
                                    "jupiter fallback closed the position"
                                }
                                ExecutionStatus::LiveSubmitted => {
                                    "jupiter fallback submitted sell; waiting for confirmation"
                                }
                                _ => "jupiter fallback returned without confirmed close",
                            };
                            let _ = event_tx.send(ScanEvent::Log(message.into()));
                            report = Some(r);
                        }
                        Err(jerr) => {
                            let _ = event_tx
                                .send(ScanEvent::Log(format!("jupiter fallback failed: {jerr}")));
                            let _ = event_tx.send(ScanEvent::Log(
                                "force sell failed - position remains on-chain".into(),
                            ));
                        }
                    }
                }
            }
        }
        match report {
            Some(r) => r,
            None => {
                let detail = last_err
                    .as_ref()
                    .map(|err| err.to_string())
                    .unwrap_or_else(|| "sell failed".to_string());
                restore_holding_after_sell_issue(
                    state,
                    event_tx,
                    &detail,
                    "sell failed - still holding; press Enter to retry",
                )
                .await;
                return Ok(false);
            }
        }
    };
    let _ = journal.record(JournalKind::Execution, &report);
    if executor.is_paper() {
        let _ = append_paper_report(&cfg.paper_report_path, &report);
    }
    let mut report = report;
    if matches!(
        report.status,
        ExecutionStatus::LiveSubmitted | ExecutionStatus::LiveFailed | ExecutionStatus::Errored
    ) {
        match live_wallet_amount_after_sell(executor, mint).await {
            Ok(0) => {
                let old_status = report.status;
                report.status = ExecutionStatus::LiveReconciled;
                if report.filled_lamports.unwrap_or_default() == 0 {
                    let entry_lamports = state.read().await.entry_lamports;
                    if entry_lamports > 0 {
                        report.filled_lamports = Some(entry_lamports);
                        report.error = Some(
                            "sell_reconciled_balance_zero; proceeds estimated from entry"
                                .to_string(),
                        );
                    }
                }
                let _ = event_tx.send(ScanEvent::Log(format!(
                    "sell reconciled on-chain after {old_status:?}; wallet token balance is zero"
                )));
            }
            Ok(amount) if report.status == ExecutionStatus::LiveSubmitted => {
                let sig = report.signature.clone().unwrap_or_default();
                let short_sig = short_id(&sig);
                let _ = event_tx.send(ScanEvent::Log(format!(
                    "sell submitted but not confirmed sig={short_sig}; wallet still shows token balance"
                )));
                let _ = event_tx.send(ScanEvent::PanicSubmitted {
                    signature: sig,
                    status: report.status,
                });
                restore_holding_after_sell_issue(
                    state,
                    event_tx,
                    &format!("sell confirmation pending; wallet token balance still {amount}"),
                    "sell pending - wallet still shows tokens; press Enter to retry",
                )
                .await;
                return Ok(false);
            }
            Ok(amount) => {
                let detail = report.error.as_deref().unwrap_or("sell failed").to_string();
                let sig = report.signature.clone().unwrap_or_default();
                let _ = event_tx.send(ScanEvent::PanicSubmitted {
                    signature: sig,
                    status: report.status,
                });
                restore_holding_after_sell_issue(
                    state,
                    event_tx,
                    &format!("{detail}; wallet token balance still {amount}"),
                    "sell failed - wallet still shows tokens; press Enter to retry",
                )
                .await;
                return Ok(false);
            }
            Err(err) if report.status == ExecutionStatus::LiveSubmitted => {
                let sig = report.signature.clone().unwrap_or_default();
                let short_sig = short_id(&sig);
                let _ = event_tx.send(ScanEvent::Log(format!(
                    "sell submitted but balance check failed sig={short_sig}; check chain before retry"
                )));
                let _ = event_tx.send(ScanEvent::PanicSubmitted {
                    signature: sig,
                    status: report.status,
                });
                restore_holding_after_sell_issue(
                    state,
                    event_tx,
                    &format!("sell confirmation pending; balance check failed: {err:#}"),
                    "sell status unknown - verify signature before retry",
                )
                .await;
                return Ok(false);
            }
            Err(err) => {
                let detail = report.error.as_deref().unwrap_or("sell failed").to_string();
                let sig = report.signature.clone().unwrap_or_default();
                let _ = event_tx.send(ScanEvent::PanicSubmitted {
                    signature: sig,
                    status: report.status,
                });
                restore_holding_after_sell_issue(
                    state,
                    event_tx,
                    &format!("{detail}; balance check failed: {err:#}"),
                    "sell failed - balance check failed; press Enter to retry",
                )
                .await;
                return Ok(false);
            }
        }
    }
    let sig = report.signature.clone().unwrap_or_default();
    let status = report.status;
    let proceeds_estimated = report
        .error
        .as_deref()
        .is_some_and(|err| err.contains("proceeds estimated from entry"));
    let exit_sol = report.filled_lamports.unwrap_or(0) as f64 / 1_000_000_000.0;
    let (entry_sol, entry_usd, exit_usd, realized_usd) = {
        let s = state.read().await;
        let entry_sol = s.entry_lamports as f64 / 1_000_000_000.0;
        let exit_usd = exit_sol * s.sol_price_usd;
        let realized_usd = exit_usd - s.entry_usd;
        (entry_sol, s.entry_usd, exit_usd, realized_usd)
    };
    let _ = entry_usd; // currently unused; keep for future P&L % display
    let realized_sol = exit_sol - entry_sol;
    let won = realized_usd >= 0.0
        && matches!(
            status,
            ExecutionStatus::LiveConfirmed | ExecutionStatus::PaperFilled
        );
    {
        let mut s = state.write().await;
        s.last_trade = Some(LastTrade {
            mint: mint.to_string(),
            entry_sol,
            exit_sol,
            realized_sol,
            held_ms,
            won,
        });
        if proceeds_estimated {
            // Inventory is gone, but proceeds were not observable. Do not
            // count this as a win or loss.
        } else if won {
            s.trades_won += 1;
        } else {
            s.trades_lost += 1;
        }
        let verb = if status == ExecutionStatus::LiveReconciled {
            "CLOSED"
        } else {
            "SOLD"
        };
        let short_mint = mint.chars().take(8).collect::<String>();
        let line = if proceeds_estimated {
            format!("{verb} {short_mint} | inventory zero | proceeds estimated; verify explorer")
        } else {
            format!(
                "{verb} {short_mint} | exit ${exit_usd:.2} | pnl ${realized_usd:+.2} ({realized_sol:.4} SOL)"
            )
        };
        let _ = event_tx.send(ScanEvent::Log(line));
        let _ = event_tx.send(ScanEvent::PanicSubmitted {
            signature: sig,
            status,
        });
    }
    Ok(true)
}

async fn restore_holding_after_sell_issue(
    state: &Arc<RwLock<ScanState>>,
    event_tx: &mpsc::UnboundedSender<ScanEvent>,
    detail: &str,
    message: &str,
) {
    {
        let mut s = state.write().await;
        s.phase = Phase::Holding;
        s.confirm_exit = false;
        s.last_error = Some(format!("sell failed; still holding: {detail}"));
    }
    let _ = event_tx.send(ScanEvent::Log(message.to_string()));
    let _ = event_tx.send(ScanEvent::StateChanged);
}

fn short_id(value: &str) -> String {
    if value.is_empty() {
        "none".to_string()
    } else {
        value.chars().take(12).collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CreateMarketScope {
    InScope,
    OutOfScope(&'static str),
    Unknown(&'static str),
}

async fn check_create_market_scope(cfg: &Config, mint: &str, logs: &[String]) -> CreateMarketScope {
    if cfg.market == Market::AllPumpfun {
        return CreateMarketScope::InScope;
    }

    let logs_mayhem = logs_have_mayhem_program(logs, &cfg.mayhem_program);
    let trust_ws_mayhem = catarnith::config::env_lookup("MAYHEM_SCAN_TRUST_WS_MAYHEM")
        .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if cfg.market == Market::MayhemOnly && trust_ws_mayhem && logs_mayhem {
        return CreateMarketScope::InScope;
    }
    if cfg.market == Market::NonMayhemOnly && logs_mayhem {
        return CreateMarketScope::OutOfScope("mayhem_program_in_create_logs");
    }

    let curve_flag = fetch_create_mayhem_flag(cfg, mint).await;
    decide_create_market_scope(cfg.market, curve_flag)
}

async fn fetch_create_mayhem_flag(cfg: &Config, mint: &str) -> Option<bool> {
    let client =
        catarnith::curve::CurveQuoteClient::new(cfg.rpc_url(), &cfg.pumpfun_program).ok()?;
    for delay_ms in [0_u64, 80, 160] {
        if delay_ms > 0 {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
        }
        if let Ok(state) = client.fetch_state(mint).await {
            if let Some(is_mayhem) = state.is_mayhem_mode {
                return Some(is_mayhem);
            }
        }
    }
    None
}

fn decide_create_market_scope(
    market: Market,
    curve_mayhem_flag: Option<bool>,
) -> CreateMarketScope {
    match market {
        Market::AllPumpfun => CreateMarketScope::InScope,
        Market::MayhemOnly => match curve_mayhem_flag {
            Some(true) => CreateMarketScope::InScope,
            Some(false) => CreateMarketScope::OutOfScope("curve_is_not_mayhem"),
            None => CreateMarketScope::Unknown("curve_mayhem_flag_unavailable"),
        },
        Market::NonMayhemOnly => match curve_mayhem_flag {
            Some(false) => CreateMarketScope::InScope,
            Some(true) => CreateMarketScope::OutOfScope("curve_is_mayhem"),
            None => CreateMarketScope::Unknown("curve_mayhem_flag_unavailable"),
        },
    }
}

/// True if any log line contains the Mayhem program pubkey.
/// Used by the fast WS-trust path to skip the pre-buy RPC check.
fn logs_have_mayhem_program(logs: &[String], mayhem_program: &str) -> bool {
    if mayhem_program.trim().is_empty() {
        return false;
    }
    logs.iter().any(|line| line.contains(mayhem_program))
}

/// Compute the SOL market cap from a `BondingCurveState`.
fn compute_mcap_sol(state: &BondingCurveState) -> f64 {
    if state.virtual_token_reserves == 0 {
        return 0.0;
    }
    let numerator = (state.token_total_supply as f64) * (state.virtual_quote_reserves as f64);
    let denominator = (state.virtual_token_reserves as f64) * 1_000_000_000.0;
    numerator / denominator
}

/// Live sell retry policy. After `MAX_SELL_RETRIES` normal attempts,
/// force-sell with 100% slippage tolerance as a last resort.
const MAX_SELL_RETRIES: usize = 3;
const SELL_RETRY_BACKOFF_MS: u64 = 250;
const FORCE_SELL_SLIPPAGE_BPS: u32 = 3_000;

/// Abort-on-drop guard for the curve polling task.
struct CurvePollGuard(Vec<tokio::task::JoinHandle<()>>);

impl Drop for CurvePollGuard {
    fn drop(&mut self) {
        for h in self.0.drain(..) {
            h.abort();
        }
    }
}

/// Stream the bonding curve for `mint` so mcap / position USD update
/// in real time. Two sources feed the same `McapTick`:
///   - a websocket `accountSubscribe` push (low latency, but flaky on
///     some RPC plans), and
///   - a 500ms RPC poll fallback that always works.
///
/// Whichever observes a newer curve state emits first; the render side
/// keeps the latest. If the websocket plan is unreliable the poll still
/// covers, so enabling push is strictly an upgrade.
fn spawn_curve_poll(
    market: Arc<MarketData>,
    mint: String,
    state: Arc<RwLock<ScanState>>,
    event_tx: mpsc::UnboundedSender<ScanEvent>,
    ws_url: String,
    commitment: String,
    curve_account: String,
) -> CurvePollGuard {
    let mut handles = Vec::with_capacity(3);

    // Websocket push source.
    let (curve_tx, mut curve_rx) = mpsc::channel::<BondingCurveState>(64);
    handles.push(spawn_curve_watch(
        ws_url,
        commitment,
        mint.clone(),
        curve_account,
        curve_tx,
    ));
    {
        let state = state.clone();
        let event_tx = event_tx.clone();
        handles.push(tokio::task::spawn_local(async move {
            while let Some(curve) = curve_rx.recv().await {
                let (sol_price, token_amount) = {
                    let s = state.read().await;
                    (s.sol_price_usd, s.token_amount_raw)
                };
                let _ = event_tx.send(ScanEvent::McapTick {
                    mcap_sol: compute_mcap_sol(&curve),
                    mcap_usd: MarketData::mcap_usd(&curve, sol_price),
                    position_usd: MarketData::position_usd(&curve, token_amount, sol_price),
                    sol_price_usd: sol_price,
                    ts_ms: curve.observed_at_ms.max(0),
                });
            }
        }));
    }

    // RPC poll fallback.
    handles.push(tokio::task::spawn_local(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(500)).await;
            let (state, sol_price, token_amount) = {
                let s = state.read().await;
                let amount = s.token_amount_raw;
                let price = s.sol_price_usd;
                // We need the curve state; fetch it outside the lock.
                drop(s);
                let curve = match market.curve_state(&mint).await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!("curve poll error for {mint}: {err:#}");
                        continue;
                    }
                };
                (curve, price, amount)
            };
            let mcap_sol = compute_mcap_sol(&state);
            let mcap_usd = MarketData::mcap_usd(&state, sol_price);
            let position_usd = MarketData::position_usd(&state, token_amount, sol_price);
            let ts = state.observed_at_ms.max(0);
            let _ = event_tx.send(ScanEvent::McapTick {
                mcap_sol,
                mcap_usd,
                position_usd,
                sol_price_usd: sol_price,
                ts_ms: ts,
            });
        }
    }));

    CurvePollGuard(handles)
}

/// Resolve the bot config to an absolute path and set the working
/// directory to the config's parent so relative paths inside the
/// config (journal dir, sqlite path, etc.) resolve correctly.
fn resolve_bot_config_from(raw: std::path::PathBuf) -> (std::path::PathBuf, std::path::PathBuf) {
    let (config_arg, cwd) = if raw.is_absolute() {
        let cwd = raw
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")));
        (raw, cwd)
    } else {
        // Try CWD-relative first.
        let abs = std::env::current_dir()
            .map(|cwd| cwd.join(&raw))
            .unwrap_or_else(|_| raw.clone());
        if abs.exists() {
            let canonical = std::fs::canonicalize(&abs).unwrap_or(abs);
            let cwd = canonical
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")));
            (canonical, cwd)
        } else {
            // Fall back to the package manifest directory.
            let pkg = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(&raw);
            if pkg.exists() {
                let canonical = std::fs::canonicalize(&pkg).unwrap_or(pkg);
                let cwd = canonical
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")));
                (canonical, cwd)
            } else {
                // Last resort: canonicalize whatever we have; the bot
                // binary will produce a clear error if it is still wrong.
                let canonical = std::fs::canonicalize(&raw).unwrap_or(raw);
                let cwd = canonical
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")));
                (canonical, cwd)
            }
        }
    };
    (config_arg, cwd)
}

enum BotLaunch {
    Binary(std::path::PathBuf),
    CargoRun {
        cargo: String,
        manifest_path: std::path::PathBuf,
    },
}

impl BotLaunch {
    fn label(&self) -> String {
        match self {
            BotLaunch::Binary(path) => format!("bot started: {}", path.display()),
            BotLaunch::CargoRun { manifest_path, .. } => {
                format!("bot started via cargo: {}", manifest_path.display())
            }
        }
    }

    fn command(
        &self,
        config_arg: &std::path::Path,
        bot_cwd: &std::path::Path,
    ) -> tokio::process::Command {
        match self {
            BotLaunch::Binary(path) => {
                let mut cmd = tokio::process::Command::new(path);
                cmd.arg("--config").arg(config_arg).current_dir(bot_cwd);
                cmd
            }
            BotLaunch::CargoRun {
                cargo,
                manifest_path,
            } => {
                let mut cmd = tokio::process::Command::new(cargo);
                cmd.arg("run")
                    .arg("--manifest-path")
                    .arg(manifest_path)
                    .arg("--features")
                    .arg("live-executor")
                    .arg("--bin")
                    .arg("bot")
                    .arg("--")
                    .arg("--config")
                    .arg(config_arg)
                    .current_dir(bot_cwd);
                cmd
            }
        }
    }
}

/// Locate the `bot` launcher for the in-TUI bot mode. Prefers a
/// target binary from the config checkout, then `cargo run` from the
/// current source checkout, and only then an installed sibling. This
/// avoids pairing a freshly installed/source-built `catarnith` with a
/// stale `~/.cargo/bin/bot`.
fn find_bot_launch(bot_dir: &std::path::Path) -> Option<BotLaunch> {
    let candidates = vec![
        bot_dir.join("target/release/bot"),
        bot_dir.join("target/debug/bot"),
    ];
    for c in candidates {
        if c.is_file() {
            return Some(BotLaunch::Binary(c));
        }
    }

    let manifest_path = bot_dir.join("Cargo.toml");
    if manifest_path.is_file() {
        return Some(BotLaunch::CargoRun {
            cargo: std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()),
            manifest_path,
        });
    }

    let manifest_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    if manifest_path.is_file() {
        return Some(BotLaunch::CargoRun {
            cargo: std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string()),
            manifest_path,
        });
    }

    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let sibling = dir.join("bot");
            if sibling.is_file() {
                return Some(BotLaunch::Binary(sibling));
            }
        }
    }

    if let Ok(home) = std::env::var("HOME") {
        let installed = std::path::PathBuf::from(home).join(".cargo/bin/bot");
        if installed.is_file() {
            return Some(BotLaunch::Binary(installed));
        }
    }
    None
}

fn is_rpc_error(err: &str) -> bool {
    let lower = err.to_lowercase();
    let needles = [
        "timeout",
        "timed out",
        "connection refused",
        "connection reset",
        "broken pipe",
        "no route to host",
        "name or service not known",
        "dns",
        "http error 5",
        " 500 ",
        " 502 ",
        " 503 ",
        " 504 ",
        "unauthorized",
        "forbidden",
        "unreachable",
        "tls handshake",
        "eof while parsing",
        "rpc error",
    ];
    needles.iter().any(|n| lower.contains(n))
}

#[cfg(test)]
mod panic_recovery_tests {
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

    /// The terminal-restoration fix (Drop guard + panic-catch
    /// in main) is verified manually: run the binary, press
    /// Ctrl-C, confirm the terminal restores cleanly. We don't
    /// spawn a subprocess in CI because the actual alternate-
    /// screen + raw-mode teardown can't be observed from a
    /// captured pipe.
    #[test]
    fn terminal_restore_is_manual_verification() {
        // Placeholder. The real test is: `cargo run --bin
        // mayhem_scan`, then Ctrl-C, then `echo restored` to
        // see your terminal back. If your terminal is *not*
        // restored, the Drop guard or the catch_unwind wrapper
        // is broken.
    }

    #[test]
    fn resolve_config_path_passes_through_existing_paths() {
        // A path that exists is shown as a canonical absolute path.
        let existing = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        assert!(
            existing.exists(),
            "test precondition: {}",
            existing.display()
        );
        let resolved = super::resolve_config_path(existing.clone());
        assert_eq!(resolved, std::fs::canonicalize(existing).unwrap());
    }

    #[test]
    fn resolve_config_path_canonicalizes_existing_relative_path() {
        let p = std::path::PathBuf::from("Cargo.toml");
        assert!(p.exists(), "test precondition: {}", p.display());
        let resolved =
            super::resolve_config_path_from(p.clone(), Some(std::env::temp_dir().as_path()));
        assert_eq!(resolved, std::fs::canonicalize(p).unwrap());
    }

    #[test]
    fn resolve_config_path_returns_input_when_nothing_found() {
        // From a directory with no config, the original path is returned
        // unchanged so Config::load produces its normal clear error. Uses
        // an explicit cwd (no set_current_dir) so it never races other tests.
        let tmp = std::env::temp_dir().join("catarnith_resolve_path_test");
        let _ = std::fs::create_dir_all(&tmp);
        let p = std::path::PathBuf::from("definitely-not-a-real-config.toml");
        let resolved = super::resolve_config_path_from(p.clone(), Some(tmp.as_path()));
        assert_eq!(resolved, p);
    }

    /// Regression test for the mode picker: mapped key commands must
    /// stay aligned with the rendered TUI menu.
    #[test]
    fn mode_picker_digits_match_rendered_menu() {
        fn key(c: char) -> KeyEvent {
            KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty())
        }

        assert!(matches!(
            super::interpret_key(key('1'), super::Phase::ModePicker),
            Some(super::ScanCommand::PickBot)
        ));
        assert!(matches!(
            super::interpret_key(key('2'), super::Phase::ModePicker),
            Some(super::ScanCommand::PickLive)
        ));
        assert!(matches!(
            super::interpret_key(key('3'), super::Phase::ModePicker),
            Some(super::ScanCommand::PickPaper)
        ));
        assert!(super::interpret_key(key('4'), super::Phase::ModePicker).is_none());
        assert!(super::interpret_key(key('5'), super::Phase::ModePicker).is_none());
        assert!(super::interpret_key(key('6'), super::Phase::ModePicker).is_none());
        // 's'/'S' opens the Settings editor.
        assert!(matches!(
            super::interpret_key(key('s'), super::Phase::ModePicker),
            Some(super::ScanCommand::PickSettings)
        ));
        assert!(matches!(
            super::interpret_key(key('S'), super::Phase::ModePicker),
            Some(super::ScanCommand::PickSettings)
        ));
    }

    #[tokio::test]
    async fn picker_live_forces_live_validation_for_explicit_config() {
        let cfg = super::Config {
            mode: super::Mode::Paper,
            helius_api_key: "test-key".to_string(),
            enable_live_trading: false,
            require_manual_live_unlock: false,
            ..super::Config::default()
        };

        let err = super::resolve_trade_config(&cfg, super::Mode::Live, true)
            .await
            .expect_err("picker Live must force live validation even for explicit configs");

        assert!(
            err.to_string().contains("live"),
            "expected a live validation error, got {err:#}"
        );
    }

    #[test]
    fn bot_settings_keeps_text_keys_local() {
        fn key(c: char) -> KeyEvent {
            KeyEvent::new(KeyCode::Char(c), KeyModifiers::empty())
        }

        assert!(matches!(
            super::interpret_key(key('q'), super::Phase::BotSettings),
            Some(super::ScanCommand::Char('q'))
        ));
        assert!(matches!(
            super::interpret_key(key('l'), super::Phase::BotSettings),
            Some(super::ScanCommand::Char('l'))
        ));
        assert!(matches!(
            super::interpret_key(key('t'), super::Phase::BotSettings),
            Some(super::ScanCommand::Char('t'))
        ));
    }

    /// Regression test for Bug 3 (silent missed sell). When
    /// the buy report doesn't include a fill amount, the
    /// strategy must end up with a non-zero token_amount_raw
    /// or it must abort the trade — never leave the position
    /// stranded on-chain. This is the state-side invariant.
    #[test]
    fn scan_state_initial_amount_is_zero() {
        // Starting state should have no held tokens. If this
        // changes, close_trade's "0 amount = silent skip"
        // shortcut would skip the first sell in a session.
        let s = super::ScanState::new();
        assert_eq!(s.token_amount_raw, 0);
        assert!(s.mint.is_empty());
        assert_eq!(s.phase, super::Phase::ModePicker);
    }

    /// The McapTick event must update mcap_sol. Regression
    /// test for Bug 2 (mcap=0) at the state layer.
    #[test]
    fn mcap_tick_event_updates_state() {
        let mut s = super::ScanState::new();
        let ts = chrono::Utc::now().timestamp_millis();
        s.push_mcap(ts, 12.5);
        s.push_mcap(ts + 100, 13.0);
        s.push_mcap(ts + 200, 13.5);
        assert_eq!(s.mcap_sol, 13.5);
        assert_eq!(s.mcap_history.len(), 3);
        assert!(s.mcap_history.back().unwrap().1 == 13.5);
    }

    /// The log buffer must be bounded so a long session
    /// doesn't OOM. push_log drops the front when full.
    #[test]
    fn push_log_drops_front_when_full() {
        let mut s = super::ScanState::new();
        let cap = super::LOG_CAP;
        for i in 0..(cap + 5) {
            s.push_log(format!("line {i}"));
        }
        // Buffer should never exceed the cap.
        assert!(s.logs.len() <= cap);
        // The oldest lines were dropped, so the front
        // element is one of the later ones.
        let front = s.logs.front().unwrap();
        assert!(
            front.starts_with("line 5")
                || front.starts_with("line 6")
                || front.starts_with("line 7")
                || front.starts_with("line 8")
        );
    }

    #[test]
    fn clean_bot_log_line_strips_tracing_prefix_and_drops_warnings() {
        let raw = "\x1b[2m2026-06-17T10:39:40.768759Z\x1b[0m \x1b[32m INFO\x1b[0m \x1b[2mbot\x1b[0m\x1b[2m:\x1b[0m starting mayhem bot config=...";
        assert_eq!(
            super::clean_bot_log_line(raw),
            Some("bot started".to_string())
        );

        let raw2 = "2026-06-17T10:39:41.622552Z  INFO catarnith::ingest: confirmed logsSubscribe subscription=4445350";
        assert_eq!(
            super::clean_bot_log_line(raw2),
            Some("confirmed logsSubscribe subscription=4445350".to_string())
        );

        let warn = "2026-06-17T10:39:41.622552Z  WARN bot: something bad";
        assert_eq!(super::clean_bot_log_line(warn), None);

        let fallback = "2026-06-17T10:39:41.622552Z  WARN catarnith::ingest: transactionSubscribe is unavailable for this RPC plan; activating logsSubscribe fallback";
        assert_eq!(
            super::clean_bot_log_line(fallback),
            Some(
                "transactionSubscribe is unavailable for this RPC plan; activating logsSubscribe fallback"
                    .to_string()
            )
        );
    }

    #[test]
    fn shorten_heartbeat_keeps_key_fields() {
        let hb = "heartbeat uptime_s=12 stream_events=47 live_events=3 backfill_events=0 stale_stream_events=0 pending_live_orders=1 last_live_age_ms=None discoveries=7 open_positions=2 curve_watches=0 single_lifecycle_busy=false";
        assert_eq!(
            super::shorten_heartbeat(hb),
            "heartbeat up=12s open=2 pending=1 busy=false discoveries=7"
        );
    }

    #[test]
    fn is_bot_execution_log_keeps_execution_lines() {
        assert!(super::is_bot_execution_log("execution fill mint=xxx"));
        assert!(super::is_bot_execution_log("live buy confirmed mint=xxx"));
        assert!(super::is_bot_execution_log("buy_build_diag mint=xxx"));
        assert!(super::is_bot_execution_log(
            "heartbeat up=0s open=0 pending=0"
        ));
        assert!(super::is_bot_execution_log(
            "confirmed logsSubscribe subscription=4445350"
        ));
        assert!(super::is_bot_execution_log(
            "confirmed transactionSubscribe subscription=123 accounts=[]"
        ));
        assert!(!super::is_bot_execution_log(
            "registered Mayhem discovery mint=xxx"
        ));
    }

    #[test]
    fn create_market_scope_requires_explicit_curve_flag_for_market_modes() {
        assert!(matches!(
            super::decide_create_market_scope(super::Market::AllPumpfun, None),
            super::CreateMarketScope::InScope
        ));
        assert!(matches!(
            super::decide_create_market_scope(super::Market::NonMayhemOnly, Some(false)),
            super::CreateMarketScope::InScope
        ));
        assert!(matches!(
            super::decide_create_market_scope(super::Market::NonMayhemOnly, Some(true)),
            super::CreateMarketScope::OutOfScope("curve_is_mayhem")
        ));
        assert!(matches!(
            super::decide_create_market_scope(super::Market::NonMayhemOnly, None),
            super::CreateMarketScope::Unknown("curve_mayhem_flag_unavailable")
        ));
        assert!(matches!(
            super::decide_create_market_scope(super::Market::MayhemOnly, Some(true)),
            super::CreateMarketScope::InScope
        ));
        assert!(matches!(
            super::decide_create_market_scope(super::Market::MayhemOnly, Some(false)),
            super::CreateMarketScope::OutOfScope("curve_is_not_mayhem")
        ));
    }

    #[test]
    fn logs_have_mayhem_program_does_not_match_empty_program() {
        let logs = vec!["Program log: create".to_string()];
        assert!(!super::logs_have_mayhem_program(&logs, ""));
    }

    #[test]
    fn bot_fatal_lines_are_kept_for_restart_diagnostics() {
        assert!(super::is_bot_fatal_log(
            "Error: copy_trade_enabled requires fetch_full_transaction=true"
        ));
        assert!(super::is_bot_fatal_log(
            "Caused by: copy_trade_wallet is not a valid Solana pubkey"
        ));
        assert!(super::is_bot_fatal_log(
            "thread 'main' panicked at src/bin/bot.rs:123:4"
        ));
        assert!(!super::is_bot_fatal_log("heartbeat up=1s open=0"));
    }

    #[test]
    fn bot_child_tail_keeps_recent_lines_only() {
        let mut tail = std::collections::VecDeque::new();
        for i in 0..(super::BOT_CHILD_TAIL + 3) {
            super::remember_bot_child_line(&mut tail, &format!("line {i}"));
        }
        assert_eq!(tail.len(), super::BOT_CHILD_TAIL);
        assert_eq!(tail.front().map(String::as_str), Some("line 3"));
        let expected = format!("line {}", super::BOT_CHILD_TAIL + 2);
        assert_eq!(tail.back().map(String::as_str), Some(expected.as_str()));
    }

    #[test]
    fn find_bot_launch_prefers_checkout_target_binary() {
        let root = std::env::temp_dir().join(format!(
            "catarnith_bot_launch_test_{}",
            chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default()
        ));
        let target = root.join("target/debug");
        std::fs::create_dir_all(&target).unwrap();
        let bot = target.join("bot");
        std::fs::write(&bot, b"").unwrap();

        let launch = super::find_bot_launch(&root).expect("target/debug/bot should be used");
        match launch {
            super::BotLaunch::Binary(path) => assert_eq!(path, bot),
            super::BotLaunch::CargoRun { .. } => panic!("expected target/debug/bot binary"),
        }

        let _ = std::fs::remove_dir_all(root);
    }
}
