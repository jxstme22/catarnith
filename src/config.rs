use crate::types::Mode;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::{env, fs, path::Path};

/// Load `.env`-style files into the current process's
/// environment.
///
/// Looks for a `.env` in the current working directory and, if
/// not found, walks up parents. Each line of the form
/// `export KEY=value` (or `KEY=value`) sets `KEY` in the
/// process environment if it is not already set — so an
/// already-exported shell variable wins, which matches the
/// behavior expected from shell-sourced local env files.
///
/// Recognized syntax:
///   * leading `export ` is stripped
///   * `KEY=value`, `KEY="value"`, `KEY='value'`
///   * `value` may reference `$HOME`, `$FOO`, or `${FOO}` from
///     the current process environment
///   * `# comments` and blank lines are ignored
///
/// Private key material (e.g. `CTARNITH_WALLET_KEYPAIR_BASE58`)
/// is *not* treated specially here — it flows through
/// `Config::load_inner` and `LivePumpExecutor` the same way
/// local env files would deliver it. Operators who do not
/// want the key on disk should `unset` the env var after
/// invoking the binary, or use a secret manager.
pub fn apply_dot_env() {
    if let Some(path) = discover_dot_env() {
        if let Ok(text) = fs::read_to_string(&path) {
            for (key, value) in parse_dot_env(&text) {
                // Always override: the .env file is the single source of
                // truth. Stale shell exports from a previous `source .env`
                // must never block a deliberate config change.
                env::set_var(&key, &value);
            }
        }
    }
}

/// Find a `.env` file. Order:
///   1. CWD and its parents
///   2. Install directory and its parents
///   3. CARGO_MANIFEST_DIR (compile-time source path) and its parents
///
/// Returns the first match.
pub fn discover_dot_env() -> Option<std::path::PathBuf> {
    let mut roots: Vec<std::path::PathBuf> = Vec::new();
    if let Ok(cwd) = env::current_dir() {
        roots.push(cwd);
    }
    if let Ok(exe) = env::current_exe() {
        if let Some(dir) = exe.parent() {
            roots.push(dir.to_path_buf());
        }
    }
    roots.push(Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf());

    for root in roots {
        let mut cur: Option<&Path> = Some(root.as_path());
        while let Some(dir) = cur {
            let candidate = dir.join(".env");
            if candidate.is_file() {
                return Some(candidate);
            }
            cur = dir.parent();
        }
    }
    None
}

/// Parse the body of a `.env` file into `KEY=value` pairs. The
/// parser is intentionally simple: it does not support
/// multiline values, command substitution, or `unset`. That is
/// sufficient for the project's `.env` shape.
pub fn parse_dot_env(text: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for raw_line in text.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim();
        let Some(eq) = line.find('=') else {
            continue;
        };
        let key = line[..eq].trim().to_string();
        if key.is_empty() || !is_valid_env_key(&key) {
            continue;
        }
        let raw_value = line[eq + 1..].trim();
        let value = unquote(raw_value);
        let value = expand_env(&value);
        out.push((key, value));
    }
    out
}

fn is_valid_env_key(s: &str) -> bool {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn unquote(s: &str) -> String {
    if s.len() >= 2 {
        let bytes = s.as_bytes();
        let first = bytes[0];
        let last = bytes[bytes.len() - 1];
        if (first == b'"' && last == b'"') || (first == b'\'' && last == b'\'') {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

/// Expand `$HOME`, `$FOO`, `${FOO}` in `s` using the current
/// process environment. Unknown variables expand to the empty
/// string (matches `bash` non-strict mode).
fn expand_env(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' {
            // ${FOO}
            if i + 1 < bytes.len() && bytes[i + 1] == b'{' {
                if let Some(close) = s[i + 2..].find('}') {
                    let name = &s[i + 2..i + 2 + close];
                    if let Ok(v) = env::var(name) {
                        out.push_str(&v);
                    }
                    i += 2 + close + 1;
                    continue;
                }
            }
            // $FOO
            let rest = &s[i + 1..];
            let name_len = rest
                .chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .count();
            if name_len > 0 {
                let name = &rest[..name_len];
                if let Ok(v) = env::var(name) {
                    out.push_str(&v);
                }
                i += 1 + name_len;
                continue;
            }
        }
        // Push one char (handles multi-byte UTF-8).
        let ch_end = s[i..]
            .char_indices()
            .nth(1)
            .map(|(off, _)| i + off)
            .unwrap_or(s.len());
        out.push_str(&s[i..ch_end]);
        i = ch_end;
    }
    out
}

pub const PUMPFUN_BONDING_CURVE_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const PUMPSWAP_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
pub const MAYHEM_PROGRAM: &str = "MAyhSmzXzV1pTf7LsNkrNwkWKTo4ougAJ1PPg47MD4e";
pub const MAYHEM_AGENT_WALLET: &str = "BwWK17cbHxwWBKZkUYvzxLcNQ1YVyaFezduWbtm2de6s";
pub const TOKEN_2022_PROGRAM: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
pub const AXIOM_ROUTE_PROGRAM: &str = "FLASHX8DrLbgeR8FcfNV1F5krxYcYMUdBkrP1EPBtxB9";
pub const AXIOM_JITO_MARKER: &str = "jitodontfrontB1111111TradeWithAxiomDotTrade";
pub const DEFAULT_LIVE_MAX_ENTRY_LAMPORTS: u64 = 13_025_001;
pub const DEFAULT_LIVE_MAX_OPEN_POSITIONS: usize = 2;
pub const DEFAULT_LIVE_MAX_TOTAL_OPEN_LAMPORTS: u64 = 27_000_000;
pub const DEFAULT_LIVE_MAX_DAILY_LOSS_LAMPORTS: i64 = 50_000_000;
pub const DEFAULT_LIVE_MAX_SLIPPAGE_BPS: u32 = 1_000;
pub const DEFAULT_LIVE_MAX_HOLD_SECONDS: i64 = 4;
pub const DEFAULT_LIVE_MAX_UNPRICED_EXIT_GRACE_MS: i64 = 0;
pub const DEFAULT_LIVE_MAX_EXIT_CHECK_INTERVAL_MS: u64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Market {
    #[default]
    MayhemOnly,
    NonMayhemOnly,
    AllPumpfun,
}

impl Market {
    pub fn as_str(self) -> &'static str {
        match self {
            Market::MayhemOnly => "mayhem_only",
            Market::NonMayhemOnly => "non_mayhem_only",
            Market::AllPumpfun => "all_pumpfun",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Market::MayhemOnly => "Mayhem only",
            Market::NonMayhemOnly => "Non-Mayhem only",
            Market::AllPumpfun => "All Pump.fun",
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            Market::MayhemOnly => Market::NonMayhemOnly,
            Market::NonMayhemOnly => Market::AllPumpfun,
            Market::AllPumpfun => Market::MayhemOnly,
        }
    }

    pub fn from_config_value(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "mayhem" | "mayhem_only" | "mayhem-only" => Ok(Market::MayhemOnly),
            "non_mayhem" | "non_mayhem_only" | "non-mayhem" | "non-mayhem-only" | "not_mayhem"
            | "not-mayhem" => Ok(Market::NonMayhemOnly),
            "all" | "all_pumpfun" | "all-pumpfun" | "pumpfun" | "pump.fun" => {
                Ok(Market::AllPumpfun)
            }
            other => anyhow::bail!(
                "market must be mayhem_only, non_mayhem_only, or all_pumpfun, got {other:?}"
            ),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LiveRiskEnvelope {
    pub max_entry_lamports: u64,
    pub max_open_positions: usize,
    pub max_total_open_lamports: u64,
    pub max_daily_loss_lamports: i64,
    pub max_slippage_bps: u32,
    pub max_hold_seconds: i64,
    pub max_unpriced_exit_grace_ms: i64,
    pub max_exit_check_interval_ms: u64,
    pub require_backfill_disabled: bool,
    pub require_real_curve_fills: bool,
}

impl LiveRiskEnvelope {
    pub fn from_env() -> Result<Self> {
        let mut envelope = Self::default();
        env_envelope_u64(
            "MAYHEM_LIVE_MAX_ENTRY_LAMPORTS",
            "MAYHEM_LIVE_BASE_BUY_LAMPORTS",
            &mut envelope.max_entry_lamports,
        )?;
        env_envelope_sol_lamports(
            "MAYHEM_LIVE_MAX_ENTRY_SOL",
            "MAYHEM_LIVE_BASE_BUY_SOL",
            &mut envelope.max_entry_lamports,
        )?;
        env_envelope_usize(
            "MAYHEM_LIVE_MAX_OPEN_POSITIONS_CEILING",
            "MAYHEM_LIVE_MAX_OPEN_POSITIONS",
            &mut envelope.max_open_positions,
        )?;
        env_envelope_u64(
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_LAMPORTS_CEILING",
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_LAMPORTS",
            &mut envelope.max_total_open_lamports,
        )?;
        env_envelope_sol_lamports(
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL_CEILING",
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL",
            &mut envelope.max_total_open_lamports,
        )?;
        env_envelope_i64(
            "MAYHEM_LIVE_MAX_DAILY_LOSS_CEILING_LAMPORTS",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_LAMPORTS",
            &mut envelope.max_daily_loss_lamports,
        )?;
        env_envelope_sol_lamports_i64(
            "MAYHEM_LIVE_MAX_DAILY_LOSS_SOL_CEILING",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_SOL",
            &mut envelope.max_daily_loss_lamports,
        )?;
        env_envelope_u32(
            "MAYHEM_LIVE_MAX_SLIPPAGE_CEILING_BPS",
            "MAYHEM_LIVE_MAX_SLIPPAGE_BPS",
            &mut envelope.max_slippage_bps,
        )?;
        env_envelope_i64(
            "MAYHEM_LIVE_MAX_HOLD_CEILING_SECONDS",
            "MAYHEM_LIVE_MAX_HOLD_SECONDS",
            &mut envelope.max_hold_seconds,
        )?;
        env_envelope_i64(
            "MAYHEM_LIVE_MAX_UNPRICED_EXIT_GRACE_MS",
            "MAYHEM_LIVE_UNPRICED_EXIT_GRACE_MS",
            &mut envelope.max_unpriced_exit_grace_ms,
        )?;
        env_envelope_u64(
            "MAYHEM_LIVE_MAX_EXIT_CHECK_INTERVAL_MS",
            "MAYHEM_LIVE_EXIT_CHECK_INTERVAL_MS",
            &mut envelope.max_exit_check_interval_ms,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_BACKFILL_DISABLED",
            &mut envelope.require_backfill_disabled,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_REAL_CURVE_FILLS",
            &mut envelope.require_real_curve_fills,
        )?;
        envelope.validate()?;
        Ok(envelope)
    }

    fn validate(&self) -> Result<()> {
        if self.max_entry_lamports == 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_ENTRY_LAMPORTS must be positive");
        }
        if self.max_open_positions == 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_OPEN_POSITIONS_CEILING must be positive");
        }
        if self.max_total_open_lamports == 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_TOTAL_OPEN_LAMPORTS_CEILING must be positive");
        }
        if self.max_daily_loss_lamports <= 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_DAILY_LOSS_CEILING_LAMPORTS must be positive");
        }
        if self.max_slippage_bps >= 10_000 {
            anyhow::bail!("CTARNITH_LIVE_MAX_SLIPPAGE_CEILING_BPS must be below 10000");
        }
        if self.max_hold_seconds <= 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_HOLD_CEILING_SECONDS must be positive");
        }
        if self.max_unpriced_exit_grace_ms < 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_UNPRICED_EXIT_GRACE_MS cannot be negative");
        }
        if self.max_exit_check_interval_ms == 0 {
            anyhow::bail!("CTARNITH_LIVE_MAX_EXIT_CHECK_INTERVAL_MS must be positive");
        }
        Ok(())
    }
}

impl Default for LiveRiskEnvelope {
    fn default() -> Self {
        Self {
            max_entry_lamports: DEFAULT_LIVE_MAX_ENTRY_LAMPORTS,
            max_open_positions: DEFAULT_LIVE_MAX_OPEN_POSITIONS,
            max_total_open_lamports: DEFAULT_LIVE_MAX_TOTAL_OPEN_LAMPORTS,
            max_daily_loss_lamports: DEFAULT_LIVE_MAX_DAILY_LOSS_LAMPORTS,
            max_slippage_bps: DEFAULT_LIVE_MAX_SLIPPAGE_BPS,
            max_hold_seconds: DEFAULT_LIVE_MAX_HOLD_SECONDS,
            max_unpriced_exit_grace_ms: DEFAULT_LIVE_MAX_UNPRICED_EXIT_GRACE_MS,
            max_exit_check_interval_ms: DEFAULT_LIVE_MAX_EXIT_CHECK_INTERVAL_MS,
            require_backfill_disabled: true,
            require_real_curve_fills: true,
        }
    }
}

/// Operational tuning for the live Pump.fun executor.
///
/// These knobs were historically set only through `MAYHEM_LIVE_*`
/// environment variables. They now live in the `[live]` section of
/// `config.toml` so paper and live share a single config file. Each
/// field still accepts the matching `MAYHEM_LIVE_*` env var as an
/// optional override at runtime, so existing deployment scripts keep
/// working — the env var wins when set.
///
/// Types here are deliberately plain (`String`, `Option<String>`) so
/// this struct compiles without the `live-executor` feature. The
/// executor parses `settlement_commitment` and `jito_tip_account`
/// into their Solana SDK types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LiveConfig {
    /// Compute unit limit per trade transaction.
    pub compute_unit_limit: u32,
    /// Priority fee, in micro-lamports per compute unit.
    pub compute_unit_price_microlamports: u64,
    /// Max RPC-side send retries per broadcast.
    pub send_max_retries: usize,
    /// Per-RPC send timeout in milliseconds.
    pub send_timeout_ms: u64,
    /// General RPC request timeout in milliseconds.
    pub rpc_timeout_ms: u64,
    /// Buy confirmation timeout in milliseconds.
    pub confirmation_timeout_ms: u64,
    /// Sell confirmation timeout in milliseconds.
    pub sell_confirmation_timeout_ms: u64,
    /// Confirmation poll interval in milliseconds.
    pub confirmation_poll_ms: u64,
    /// Simulate the signed transaction before broadcasting.
    pub pre_broadcast_simulation: bool,
    /// Settlement commitment: `processed`, `confirmed`, or `finalized`.
    pub settlement_commitment: String,
    /// Sell slippage in bps. `None` falls back to `max_slippage_bps`.
    pub sell_slippage_bps: Option<u32>,
    /// Refuse to trade when the wallet holds more than this many lamports.
    pub max_balance_lamports: u64,
    /// Optional Jito block-engine RPC URL for the panic-sell tip path.
    pub jito_block_engine_url: Option<String>,
    /// Jito tip account pubkey (base58). Required when the URL is set.
    pub jito_tip_account: Option<String>,
    /// Jito tip amount in lamports.
    pub jito_tip_lamports: u64,
    /// Per-leg timeout for the Jupiter sell fallback, in milliseconds.
    pub jupiter_timeout_ms: u64,
}

impl Default for LiveConfig {
    fn default() -> Self {
        Self {
            compute_unit_limit: 400_000,
            compute_unit_price_microlamports: 1,
            send_max_retries: 2,
            send_timeout_ms: 750,
            rpc_timeout_ms: 900,
            confirmation_timeout_ms: 5_000,
            sell_confirmation_timeout_ms: 5_000,
            confirmation_poll_ms: 200,
            pre_broadcast_simulation: true,
            settlement_commitment: "processed".to_string(),
            sell_slippage_bps: None,
            max_balance_lamports: 50_000_000,
            jito_block_engine_url: None,
            jito_tip_account: None,
            jito_tip_lamports: 10_000,
            jupiter_timeout_ms: 10_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CopyTradeSizing {
    /// Use `base_buy_lamports` for every copied buy.
    #[default]
    Fixed,
    /// Use the source wallet's observed buy size, capped by
    /// `copy_trade_max_buy_lamports`.
    Mirror,
    /// Use observed buy size multiplied by `copy_trade_scale_bps`, capped by
    /// `copy_trade_max_buy_lamports`.
    Scaled,
}

impl CopyTradeSizing {
    pub fn as_str(self) -> &'static str {
        match self {
            CopyTradeSizing::Fixed => "fixed",
            CopyTradeSizing::Mirror => "mirror",
            CopyTradeSizing::Scaled => "scaled",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            CopyTradeSizing::Fixed => "Fixed",
            CopyTradeSizing::Mirror => "Mirror",
            CopyTradeSizing::Scaled => "Scaled",
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            CopyTradeSizing::Fixed => CopyTradeSizing::Mirror,
            CopyTradeSizing::Mirror => CopyTradeSizing::Scaled,
            CopyTradeSizing::Scaled => CopyTradeSizing::Fixed,
        }
    }

    pub fn from_config_value(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "fixed" => Ok(CopyTradeSizing::Fixed),
            "mirror" => Ok(CopyTradeSizing::Mirror),
            "scaled" | "scale" => Ok(CopyTradeSizing::Scaled),
            other => {
                anyhow::bail!("copy_trade_sizing must be fixed, mirror, or scaled, got {other:?}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CopyTradeBuyPolicy {
    /// Copy only the first qualifying source buy for a mint.
    #[default]
    FirstOnly,
    /// Keep copying later source buys until copy/risk caps stop entries.
    Accumulate,
}

impl CopyTradeBuyPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            CopyTradeBuyPolicy::FirstOnly => "first_only",
            CopyTradeBuyPolicy::Accumulate => "accumulate",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            CopyTradeBuyPolicy::FirstOnly => "First only",
            CopyTradeBuyPolicy::Accumulate => "Accumulate",
        }
    }

    pub fn cycle(self) -> Self {
        match self {
            CopyTradeBuyPolicy::FirstOnly => CopyTradeBuyPolicy::Accumulate,
            CopyTradeBuyPolicy::Accumulate => CopyTradeBuyPolicy::FirstOnly,
        }
    }

    pub fn from_config_value(value: &str) -> Result<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "first" | "first_only" | "first-only" | "once" => Ok(CopyTradeBuyPolicy::FirstOnly),
            "accumulate" | "keep_buying" | "keep-buying" | "dca" => {
                Ok(CopyTradeBuyPolicy::Accumulate)
            }
            other => anyhow::bail!(
                "copy_trade_buy_policy must be first_only or accumulate, got {other:?}"
            ),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub mode: Mode,
    pub helius_api_key: String,
    pub wallet_keypair_path: String,
    /// Optional base58-encoded 64-byte secret key. When set, takes
    /// precedence over `wallet_keypair_path`. Use
    /// `CTARNITH_WALLET_KEYPAIR_BASE58` to set it from the environment
    /// without writing a key to disk.
    #[serde(default)]
    pub wallet_keypair_base58: Option<String>,

    pub pumpfun_program: String,
    pub pumpswap_program: String,
    pub mayhem_program: String,
    pub mayhem_agent_wallet: String,
    pub token_2022_program: String,
    pub axiom_route_program: String,
    pub axiom_jito_marker: String,
    #[serde(alias = "pair_scope")]
    pub market: Market,
    pub require_mayhem_evidence: bool,
    pub allow_indirect_mayhem_candidates: bool,
    pub require_route_confirmation: bool,
    pub require_reference_wallet_signal: bool,
    pub follow_observed_sell_signals: bool,
    pub exit_on_observed_agent_buy: bool,
    pub mayhem_mint_allowlist_path: String,
    pub mayhem_metadata_url_template: String,
    pub mayhem_metadata_timeout_ms: u64,
    pub mayhem_evidence_min_confidence: f64,
    pub pulse_mints_path: String,
    pub require_discovery_signal: bool,
    pub allow_onchain_mayhem_discovery: bool,
    pub require_curve_mayhem_flag: bool,
    pub require_fresh_mint_creation: bool,
    pub max_stream_event_age_ms: i64,
    pub entry_deadline_ms: i64,
    pub max_create_event_slot_lag: u64,
    pub buy_slippage_retry_attempts: u32,
    pub buy_slippage_retry_deadline_ms: i64,
    pub buy_slippage_retry_step_bps: u32,
    pub buy_slippage_retry_max_bps: u32,
    pub max_entry_curve_slot_ahead: u64,
    pub min_observed_buy_lamports: u64,
    pub max_observed_buy_lamports: Option<u64>,
    pub max_observed_buys_before_entry: Option<u64>,
    pub max_observed_sells_before_entry: Option<u64>,

    #[serde(alias = "buy_lamports")]
    pub base_buy_lamports: u64,
    pub max_open_positions: usize,
    pub max_buys_per_mint: u32,
    #[serde(alias = "max_position_lamports")]
    pub max_total_lamports_per_mint: u64,
    pub max_total_open_lamports: u64,
    #[serde(alias = "daily_loss_limit_lamports")]
    pub max_daily_loss_lamports: i64,
    pub max_failed_txs_per_minute: u32,
    pub max_failed_fee_burn_lamports_per_hour: u64,
    pub max_slippage_bps: u32,
    pub paper_slippage_bps: u32,
    pub paper_fee_lamports_floor: u64,

    pub take_profit_bps: i64,
    pub take_profit_sell_bps: u32,
    pub stop_loss_bps: i64,
    pub burst_entry_seconds: i64,
    pub max_hold_seconds: i64,
    pub cooldown_seconds_per_mint: i64,
    pub market_quote_max_age_ms: i64,
    pub unpriced_exit_grace_ms: i64,
    pub ambiguous_entry_expiry_ms: i64,
    pub ambiguous_inventory_recheck_ms: u64,
    pub enable_curve_exit_quotes: bool,
    pub exit_check_interval_ms: u64,
    pub enable_take_profit_exit: bool,
    pub enable_stop_loss_exit: bool,
    pub curve_observation_seconds: i64,

    pub enable_live_trading: bool,
    pub live_single_lifecycle: bool,
    pub require_manual_live_unlock: bool,
    pub hot_wallet_marker_required: bool,
    pub main_wallet_markers: Vec<String>,

    pub subscribe_commitment: String,
    pub subscribe_programs: bool,
    pub enable_transaction_subscribe: bool,
    pub enable_logs_fallback: bool,
    pub fetch_full_transaction: bool,
    pub use_observed_entry_fill: bool,
    pub backfill_limit: usize,
    pub watched_wallets: Vec<String>,
    pub target_wallet: Option<String>,
    pub copy_trade_enabled: bool,
    pub copy_trade_wallet: String,
    pub copy_trade_sizing: CopyTradeSizing,
    pub copy_trade_scale_bps: u32,
    pub copy_trade_max_buy_lamports: u64,
    pub copy_trade_buy_policy: CopyTradeBuyPolicy,
    pub copy_trade_max_buys_per_mint: u32,
    pub copy_trade_min_source_buy_lamports: u64,
    pub copy_trade_max_hold_seconds: i64,
    pub copy_trade_take_profit_bps: i64,
    pub copy_trade_take_profit_sell_bps: u32,
    pub copy_trade_stop_loss_bps: i64,
    pub copy_trade_follow_sells: bool,
    pub copy_trade_allow_pumpswap: bool,
    pub bot_keep_alive: bool,

    pub journal_dir: String,
    pub sqlite_path: String,
    pub paper_report_path: String,
    pub horizon_report_path: String,

    #[serde(default)]
    pub live: LiveConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            mode: Mode::Paper,
            helius_api_key: String::new(),
            wallet_keypair_path: String::new(),
            wallet_keypair_base58: None,
            pumpfun_program: PUMPFUN_BONDING_CURVE_PROGRAM.to_string(),
            pumpswap_program: PUMPSWAP_PROGRAM.to_string(),
            mayhem_program: MAYHEM_PROGRAM.to_string(),
            mayhem_agent_wallet: MAYHEM_AGENT_WALLET.to_string(),
            token_2022_program: TOKEN_2022_PROGRAM.to_string(),
            axiom_route_program: AXIOM_ROUTE_PROGRAM.to_string(),
            axiom_jito_marker: AXIOM_JITO_MARKER.to_string(),
            market: Market::MayhemOnly,
            require_mayhem_evidence: true,
            allow_indirect_mayhem_candidates: false,
            require_route_confirmation: true,
            require_reference_wallet_signal: false,
            follow_observed_sell_signals: true,
            exit_on_observed_agent_buy: false,
            mayhem_mint_allowlist_path: String::new(),
            mayhem_metadata_url_template: String::new(),
            mayhem_metadata_timeout_ms: 750,
            mayhem_evidence_min_confidence: 0.9,
            pulse_mints_path: String::new(),
            require_discovery_signal: false,
            allow_onchain_mayhem_discovery: true,
            require_curve_mayhem_flag: false,
            require_fresh_mint_creation: false,
            max_stream_event_age_ms: 500,
            entry_deadline_ms: 550,
            max_create_event_slot_lag: 2,
            buy_slippage_retry_attempts: 0,
            buy_slippage_retry_deadline_ms: 3_000,
            buy_slippage_retry_step_bps: 500,
            buy_slippage_retry_max_bps: 1_500,
            max_entry_curve_slot_ahead: 8,
            min_observed_buy_lamports: 0,
            max_observed_buy_lamports: None,
            max_observed_buys_before_entry: None,
            max_observed_sells_before_entry: None,
            base_buy_lamports: 13_025_001,
            max_open_positions: 3,
            max_buys_per_mint: 5,
            max_total_lamports_per_mint: 100_000_000,
            max_total_open_lamports: 300_000_000,
            max_daily_loss_lamports: 500_000_000,
            max_failed_txs_per_minute: 30,
            max_failed_fee_burn_lamports_per_hour: 20_000_000,
            max_slippage_bps: 1_500,
            paper_slippage_bps: 300,
            paper_fee_lamports_floor: 5_000,
            take_profit_bps: 8_000,
            take_profit_sell_bps: 10_000,
            stop_loss_bps: 3_500,
            burst_entry_seconds: 60,
            max_hold_seconds: 180,
            cooldown_seconds_per_mint: 60,
            market_quote_max_age_ms: 2_000,
            unpriced_exit_grace_ms: 3_000,
            ambiguous_entry_expiry_ms: 120_000,
            ambiguous_inventory_recheck_ms: 1_000,
            enable_curve_exit_quotes: true,
            exit_check_interval_ms: 100,
            enable_take_profit_exit: true,
            enable_stop_loss_exit: true,
            curve_observation_seconds: 180,
            enable_live_trading: false,
            live_single_lifecycle: false,
            require_manual_live_unlock: true,
            hot_wallet_marker_required: true,
            main_wallet_markers: vec![
                "main".to_string(),
                "cold".to_string(),
                "treasury".to_string(),
            ],
            subscribe_commitment: "processed".to_string(),
            subscribe_programs: true,
            enable_transaction_subscribe: true,
            enable_logs_fallback: true,
            fetch_full_transaction: true,
            use_observed_entry_fill: false,
            backfill_limit: 0,
            watched_wallets: Vec::new(),
            target_wallet: None,
            copy_trade_enabled: false,
            copy_trade_wallet: String::new(),
            copy_trade_sizing: CopyTradeSizing::Fixed,
            copy_trade_scale_bps: 10_000,
            copy_trade_max_buy_lamports: 13_025_001,
            copy_trade_buy_policy: CopyTradeBuyPolicy::FirstOnly,
            copy_trade_max_buys_per_mint: 1,
            copy_trade_min_source_buy_lamports: 0,
            copy_trade_max_hold_seconds: 180,
            copy_trade_take_profit_bps: 8_000,
            copy_trade_take_profit_sell_bps: 10_000,
            copy_trade_stop_loss_bps: 3_500,
            copy_trade_follow_sells: true,
            copy_trade_allow_pumpswap: false,
            bot_keep_alive: true,
            journal_dir: "journals".to_string(),
            sqlite_path: "journals/catarnith.sqlite".to_string(),
            paper_report_path: String::new(),
            horizon_report_path: String::new(),
            live: LiveConfig::default(),
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        Self::load_inner(path, true)
    }

    pub fn load_raw(path: &Path) -> Result<Self> {
        Self::load_inner(path, false)
    }

    fn load_inner(path: &Path, apply_env_overrides: bool) -> Result<Self> {
        // Source the project `.env` so a bare `catarnith`
        // invocation gets the same local env as a shell-sourced run.
        // Already-exported shell variables win.
        apply_dot_env();
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config at {}", path.display()))?;
        let mut cfg: Config = toml::from_str(&raw)
            .with_context(|| format!("failed to parse TOML at {}", path.display()))?;
        apply_sol_toml_aliases(&raw, &mut cfg)
            .with_context(|| format!("failed to apply SOL aliases from {}", path.display()))?;
        if apply_env_overrides {
            let mut helius_key = String::new();
            if let Ok(api_key) = env::var("HELIUS_API_KEY") {
                if !api_key.trim().is_empty() {
                    helius_key = api_key.trim().to_string();
                }
            } else if let Ok(api_key_path) = env::var("HELIUS_API_KEY_FILE") {
                let api_key = fs::read_to_string(&api_key_path).with_context(|| {
                    format!("failed to read HELIUS_API_KEY_FILE={api_key_path}")
                })?;
                if !api_key.trim().is_empty() {
                    helius_key = api_key.trim().to_string();
                }
            }
            // Last-ditch auto-discovery: if neither env var is
            // set and the config file's helius_api_key field is
            // empty, look for a `helius.txt` next to the config
            // we just loaded, then in CWD, then walking up
            // parents. This makes a bare `catarnith` invocation from the
            // workspace root actually find the key without
            // requiring the operator to source .env first.
            if helius_key.is_empty() && cfg.helius_api_key.trim().is_empty() {
                if let Some(found) = discover_helius_key_file(path) {
                    if let Ok(api_key) = fs::read_to_string(&found) {
                        if !api_key.trim().is_empty() {
                            helius_key = api_key.trim().to_string();
                        }
                    }
                }
            }
            if !helius_key.is_empty() {
                cfg.helius_api_key = helius_key;
            }
            if let Ok(wallet_path) =
                env_var("CTARNITH_WALLET_KEYPAIR_PATH", "MAYHEM_WALLET_KEYPAIR_PATH")
            {
                if !wallet_path.trim().is_empty() {
                    cfg.wallet_keypair_path = wallet_path.trim().to_string();
                }
            }
            if let Ok(wallet_b58) = env_var(
                "CTARNITH_WALLET_KEYPAIR_BASE58",
                "MAYHEM_WALLET_KEYPAIR_BASE58",
            ) {
                if !wallet_b58.trim().is_empty() {
                    cfg.wallet_keypair_base58 = Some(wallet_b58.trim().to_string());
                }
            }
            if let Some(market) = market_env_override()? {
                cfg.market = market;
            }
            // Last-ditch: if neither env var set the keypair
            // source and the config's field is empty, look for
            // the canonical canary wallet
            // (~/.config/solana/catarnith-live-canary.json). The
            // wrapper script defaults to this location; this
            // makes a bare `catarnith` find the same file the
            // wrapper would.
            if cfg.wallet_keypair_path.trim().is_empty()
                && cfg
                    .wallet_keypair_base58
                    .as_ref()
                    .map(|s| s.trim().is_empty())
                    .unwrap_or(true)
            {
                if let Some(canonical) = default_canary_wallet_path() {
                    if canonical.is_file() {
                        cfg.wallet_keypair_path = canonical.to_string_lossy().to_string();
                    }
                }
            }
            cfg.apply_live_env_overrides()?;
        }
        cfg.normalize();
        Ok(cfg)
    }

    pub fn apply_runtime_mode(&mut self, mode: Mode) -> Result<()> {
        self.mode = mode;
        self.apply_live_env_overrides()?;
        self.normalize();
        Ok(())
    }

    fn normalize(&mut self) {
        if self
            .target_wallet
            .as_ref()
            .is_some_and(|wallet| wallet.trim().is_empty())
        {
            self.target_wallet = None;
        }
        self.watched_wallets
            .retain(|wallet| !wallet.trim().is_empty());
    }

    fn apply_live_env_overrides(&mut self) -> Result<()> {
        self.apply_copy_trade_env_overrides()?;
        if self.mode != Mode::Live {
            return Ok(());
        }

        env_override_string("MAYHEM_LIVE_PUMPFUN_PROGRAM", &mut self.pumpfun_program);
        env_override_string("MAYHEM_LIVE_PUMPSWAP_PROGRAM", &mut self.pumpswap_program);
        env_override_string("MAYHEM_LIVE_MAYHEM_PROGRAM", &mut self.mayhem_program);
        env_override_string("MAYHEM_AGENT_WALLET", &mut self.mayhem_agent_wallet);
        env_override_string(
            "MAYHEM_LIVE_MAYHEM_AGENT_WALLET",
            &mut self.mayhem_agent_wallet,
        );
        env_override_string(
            "MAYHEM_LIVE_TOKEN_2022_PROGRAM",
            &mut self.token_2022_program,
        );
        env_override_string(
            "MAYHEM_LIVE_AXIOM_ROUTE_PROGRAM",
            &mut self.axiom_route_program,
        );
        env_override_string("MAYHEM_LIVE_AXIOM_JITO_MARKER", &mut self.axiom_jito_marker);
        if let Some(market) = market_env_override()? {
            self.market = market;
        }
        if let Some(market) = live_market_env_override()? {
            self.market = market;
        }
        env_override_string(
            "MAYHEM_LIVE_MAYHEM_MINT_ALLOWLIST_PATH",
            &mut self.mayhem_mint_allowlist_path,
        );
        env_override_string(
            "MAYHEM_LIVE_MAYHEM_METADATA_URL_TEMPLATE",
            &mut self.mayhem_metadata_url_template,
        );
        env_override_u64(
            "MAYHEM_LIVE_MAYHEM_METADATA_TIMEOUT_MS",
            &mut self.mayhem_metadata_timeout_ms,
        )?;
        env_override_f64(
            "MAYHEM_LIVE_MAYHEM_EVIDENCE_MIN_CONFIDENCE",
            &mut self.mayhem_evidence_min_confidence,
        )?;
        env_override_string("MAYHEM_LIVE_PULSE_MINTS_PATH", &mut self.pulse_mints_path);
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_MAYHEM_EVIDENCE",
            &mut self.require_mayhem_evidence,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ALLOW_INDIRECT_MAYHEM_CANDIDATES",
            &mut self.allow_indirect_mayhem_candidates,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_ROUTE_CONFIRMATION",
            &mut self.require_route_confirmation,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_REFERENCE_WALLET_SIGNAL",
            &mut self.require_reference_wallet_signal,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_FOLLOW_OBSERVED_SELL_SIGNALS",
            &mut self.follow_observed_sell_signals,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_EXIT_ON_OBSERVED_AGENT_BUY",
            &mut self.exit_on_observed_agent_buy,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_DISCOVERY_SIGNAL",
            &mut self.require_discovery_signal,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ALLOW_ONCHAIN_MAYHEM_DISCOVERY",
            &mut self.allow_onchain_mayhem_discovery,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_CURVE_MAYHEM_FLAG",
            &mut self.require_curve_mayhem_flag,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_FRESH_MINT_CREATION",
            &mut self.require_fresh_mint_creation,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_MAX_STREAM_EVENT_AGE_MS",
            &mut self.max_stream_event_age_ms,
        )?;
        env_override_i64("MAYHEM_LIVE_ENTRY_DEADLINE_MS", &mut self.entry_deadline_ms)?;
        env_override_u64(
            "MAYHEM_LIVE_MAX_CREATE_EVENT_SLOT_LAG",
            &mut self.max_create_event_slot_lag,
        )?;
        env_override_u32(
            "MAYHEM_LIVE_BUY_SLIPPAGE_RETRY_ATTEMPTS",
            &mut self.buy_slippage_retry_attempts,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_BUY_SLIPPAGE_RETRY_DEADLINE_MS",
            &mut self.buy_slippage_retry_deadline_ms,
        )?;
        env_override_u32(
            "MAYHEM_LIVE_BUY_SLIPPAGE_RETRY_STEP_BPS",
            &mut self.buy_slippage_retry_step_bps,
        )?;
        env_override_u32(
            "MAYHEM_LIVE_BUY_SLIPPAGE_RETRY_MAX_BPS",
            &mut self.buy_slippage_retry_max_bps,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_MAX_ENTRY_CURVE_SLOT_AHEAD",
            &mut self.max_entry_curve_slot_ahead,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_MIN_OBSERVED_BUY_LAMPORTS",
            &mut self.min_observed_buy_lamports,
        )?;
        env_override_sol_lamports(
            "MAYHEM_LIVE_MIN_OBSERVED_BUY_SOL",
            &mut self.min_observed_buy_lamports,
        )?;
        env_override_option_u64(
            "MAYHEM_LIVE_MAX_OBSERVED_BUY_LAMPORTS",
            &mut self.max_observed_buy_lamports,
        )?;
        env_override_option_sol_lamports(
            "MAYHEM_LIVE_MAX_OBSERVED_BUY_SOL",
            &mut self.max_observed_buy_lamports,
        )?;
        env_override_option_u64(
            "MAYHEM_LIVE_MAX_OBSERVED_BUYS_BEFORE_ENTRY",
            &mut self.max_observed_buys_before_entry,
        )?;
        env_override_option_u64(
            "MAYHEM_LIVE_MAX_OBSERVED_SELLS_BEFORE_ENTRY",
            &mut self.max_observed_sells_before_entry,
        )?;

        env_override_u64("MAYHEM_LIVE_BASE_BUY_LAMPORTS", &mut self.base_buy_lamports)?;
        env_override_sol_lamports("MAYHEM_LIVE_BASE_BUY_SOL", &mut self.base_buy_lamports)?;
        env_override_usize(
            "MAYHEM_LIVE_MAX_OPEN_POSITIONS",
            &mut self.max_open_positions,
        )?;
        env_override_u32("MAYHEM_LIVE_MAX_BUYS_PER_MINT", &mut self.max_buys_per_mint)?;
        env_override_u64(
            "MAYHEM_LIVE_MAX_TOTAL_LAMPORTS_PER_MINT",
            &mut self.max_total_lamports_per_mint,
        )?;
        env_override_sol_lamports(
            "MAYHEM_LIVE_MAX_TOTAL_SOL_PER_MINT",
            &mut self.max_total_lamports_per_mint,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_LAMPORTS",
            &mut self.max_total_open_lamports,
        )?;
        env_override_sol_lamports(
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL",
            &mut self.max_total_open_lamports,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_MAX_DAILY_LOSS_LAMPORTS",
            &mut self.max_daily_loss_lamports,
        )?;
        env_override_sol_lamports_i64(
            "MAYHEM_LIVE_MAX_DAILY_LOSS_SOL",
            &mut self.max_daily_loss_lamports,
        )?;
        env_override_u32(
            "MAYHEM_LIVE_MAX_FAILED_TXS_PER_MINUTE",
            &mut self.max_failed_txs_per_minute,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_MAX_FAILED_FEE_BURN_LAMPORTS_PER_HOUR",
            &mut self.max_failed_fee_burn_lamports_per_hour,
        )?;
        env_override_u32("MAYHEM_LIVE_MAX_SLIPPAGE_BPS", &mut self.max_slippage_bps)?;

        env_override_i64("MAYHEM_LIVE_TAKE_PROFIT_BPS", &mut self.take_profit_bps)?;
        env_override_u32(
            "MAYHEM_LIVE_TAKE_PROFIT_SELL_BPS",
            &mut self.take_profit_sell_bps,
        )?;
        env_override_i64("MAYHEM_LIVE_STOP_LOSS_BPS", &mut self.stop_loss_bps)?;
        env_override_i64(
            "MAYHEM_LIVE_BURST_ENTRY_SECONDS",
            &mut self.burst_entry_seconds,
        )?;
        env_override_i64("MAYHEM_LIVE_MAX_HOLD_SECONDS", &mut self.max_hold_seconds)?;
        env_override_i64(
            "MAYHEM_LIVE_COOLDOWN_SECONDS_PER_MINT",
            &mut self.cooldown_seconds_per_mint,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_MARKET_QUOTE_MAX_AGE_MS",
            &mut self.market_quote_max_age_ms,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_UNPRICED_EXIT_GRACE_MS",
            &mut self.unpriced_exit_grace_ms,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_AMBIGUOUS_ENTRY_EXPIRY_MS",
            &mut self.ambiguous_entry_expiry_ms,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_AMBIGUOUS_INVENTORY_RECHECK_MS",
            &mut self.ambiguous_inventory_recheck_ms,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ENABLE_CURVE_EXIT_QUOTES",
            &mut self.enable_curve_exit_quotes,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_EXIT_CHECK_INTERVAL_MS",
            &mut self.exit_check_interval_ms,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ENABLE_TAKE_PROFIT_EXIT",
            &mut self.enable_take_profit_exit,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ENABLE_STOP_LOSS_EXIT",
            &mut self.enable_stop_loss_exit,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_CURVE_OBSERVATION_SECONDS",
            &mut self.curve_observation_seconds,
        )?;

        env_override_bool(
            "MAYHEM_LIVE_ENABLE_LIVE_TRADING",
            &mut self.enable_live_trading,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_SINGLE_LIFECYCLE",
            &mut self.live_single_lifecycle,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_REQUIRE_MANUAL_LIVE_UNLOCK",
            &mut self.require_manual_live_unlock,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_HOT_WALLET_MARKER_REQUIRED",
            &mut self.hot_wallet_marker_required,
        )?;
        env_override_vec_string(
            "MAYHEM_LIVE_MAIN_WALLET_MARKERS",
            &mut self.main_wallet_markers,
        )?;
        env_override_string(
            "MAYHEM_LIVE_SUBSCRIBE_COMMITMENT",
            &mut self.subscribe_commitment,
        );
        env_override_bool(
            "MAYHEM_LIVE_SUBSCRIBE_PROGRAMS",
            &mut self.subscribe_programs,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ENABLE_TRANSACTION_SUBSCRIBE",
            &mut self.enable_transaction_subscribe,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_ENABLE_LOGS_FALLBACK",
            &mut self.enable_logs_fallback,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_FETCH_FULL_TRANSACTION",
            &mut self.fetch_full_transaction,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_USE_OBSERVED_ENTRY_FILL",
            &mut self.use_observed_entry_fill,
        )?;
        env_override_usize("MAYHEM_LIVE_BACKFILL_LIMIT", &mut self.backfill_limit)?;
        env_override_vec_string("MAYHEM_LIVE_WATCHED_WALLETS", &mut self.watched_wallets)?;
        env_override_option_string("MAYHEM_LIVE_TARGET_WALLET", &mut self.target_wallet)?;
        env_override_string("MAYHEM_LIVE_JOURNAL_DIR", &mut self.journal_dir);
        env_override_string("MAYHEM_LIVE_SQLITE_PATH", &mut self.sqlite_path);
        env_override_string("MAYHEM_LIVE_PAPER_REPORT_PATH", &mut self.paper_report_path);
        env_override_string(
            "MAYHEM_LIVE_HORIZON_REPORT_PATH",
            &mut self.horizon_report_path,
        );
        Ok(())
    }

    fn apply_copy_trade_env_overrides(&mut self) -> Result<()> {
        env_override_bool(
            "MAYHEM_LIVE_COPY_TRADE_ENABLED",
            &mut self.copy_trade_enabled,
        )?;
        env_override_string("MAYHEM_LIVE_COPY_TRADE_WALLET", &mut self.copy_trade_wallet);
        if let Some(value) = env_lookup("MAYHEM_LIVE_COPY_TRADE_SIZING") {
            self.copy_trade_sizing = CopyTradeSizing::from_config_value(&value)?;
        }
        env_override_u32(
            "MAYHEM_LIVE_COPY_TRADE_SCALE_BPS",
            &mut self.copy_trade_scale_bps,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_COPY_TRADE_MAX_BUY_LAMPORTS",
            &mut self.copy_trade_max_buy_lamports,
        )?;
        env_override_sol_lamports(
            "MAYHEM_LIVE_COPY_TRADE_MAX_BUY_SOL",
            &mut self.copy_trade_max_buy_lamports,
        )?;
        if let Some(value) = env_lookup("MAYHEM_LIVE_COPY_TRADE_BUY_POLICY") {
            self.copy_trade_buy_policy = CopyTradeBuyPolicy::from_config_value(&value)?;
        }
        env_override_u32(
            "MAYHEM_LIVE_COPY_TRADE_MAX_BUYS_PER_MINT",
            &mut self.copy_trade_max_buys_per_mint,
        )?;
        env_override_u64(
            "MAYHEM_LIVE_COPY_TRADE_MIN_SOURCE_BUY_LAMPORTS",
            &mut self.copy_trade_min_source_buy_lamports,
        )?;
        env_override_sol_lamports(
            "MAYHEM_LIVE_COPY_TRADE_MIN_SOURCE_BUY_SOL",
            &mut self.copy_trade_min_source_buy_lamports,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_COPY_TRADE_MAX_HOLD_SECONDS",
            &mut self.copy_trade_max_hold_seconds,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_COPY_TRADE_TAKE_PROFIT_BPS",
            &mut self.copy_trade_take_profit_bps,
        )?;
        env_override_u32(
            "MAYHEM_LIVE_COPY_TRADE_TAKE_PROFIT_SELL_BPS",
            &mut self.copy_trade_take_profit_sell_bps,
        )?;
        env_override_i64(
            "MAYHEM_LIVE_COPY_TRADE_STOP_LOSS_BPS",
            &mut self.copy_trade_stop_loss_bps,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_COPY_TRADE_FOLLOW_SELLS",
            &mut self.copy_trade_follow_sells,
        )?;
        env_override_bool(
            "MAYHEM_LIVE_COPY_TRADE_ALLOW_PUMPSWAP",
            &mut self.copy_trade_allow_pumpswap,
        )?;
        env_override_bool("MAYHEM_LIVE_BOT_KEEP_ALIVE", &mut self.bot_keep_alive)?;
        Ok(())
    }

    pub fn validate_live_risk_envelope(&self, label: &str) -> Result<()> {
        let envelope = LiveRiskEnvelope::from_env()?;
        // All ceiling comparisons are advisory: the operator's explicit
        // config values always take precedence. Stale shell env vars
        // from a previous `source .env` must never block a deliberate
        // config choice. Log the discrepancy so the operator can see
        // it in the log panel.
        if self.base_buy_lamports > envelope.max_entry_lamports {
            tracing::warn!(
                "{label} base_buy_sol={:.9} exceeds CTARNITH_LIVE_MAX_ENTRY_SOL={:.9} (non-blocking)",
                self.base_buy_lamports as f64 / 1_000_000_000.0,
                envelope.max_entry_lamports as f64 / 1_000_000_000.0
            );
        }
        if self.max_open_positions > envelope.max_open_positions {
            tracing::warn!(
                "{label} max_open_positions={} exceeds MAYHEM_LIVE_MAX_OPEN_POSITIONS_CEILING={} (non-blocking)",
                self.max_open_positions,
                envelope.max_open_positions
            );
        }
        if self.max_total_open_lamports > envelope.max_total_open_lamports {
            tracing::warn!(
                "{label} max_total_open_sol={:.9} exceeds CTARNITH_LIVE_MAX_TOTAL_OPEN_SOL_CEILING={:.9} (non-blocking)",
                self.max_total_open_lamports as f64 / 1_000_000_000.0,
                envelope.max_total_open_lamports as f64 / 1_000_000_000.0
            );
        }
        if self.max_daily_loss_lamports > envelope.max_daily_loss_lamports {
            tracing::warn!(
                "{label} max_daily_loss_sol={:.9} exceeds CTARNITH_LIVE_MAX_DAILY_LOSS_SOL_CEILING={:.9} (non-blocking)",
                self.max_daily_loss_lamports as f64 / 1_000_000_000.0,
                envelope.max_daily_loss_lamports as f64 / 1_000_000_000.0
            );
        }
        if self.max_slippage_bps > envelope.max_slippage_bps {
            tracing::warn!(
                "{label} max_slippage_bps={} exceeds MAYHEM_LIVE_MAX_SLIPPAGE_CEILING_BPS={} (non-blocking)",
                self.max_slippage_bps,
                envelope.max_slippage_bps
            );
        }
        if self.buy_slippage_retry_max_bps > envelope.max_slippage_bps {
            tracing::warn!(
                "{label} buy_slippage_retry_max_bps={} exceeds MAYHEM_LIVE_MAX_SLIPPAGE_CEILING_BPS={} (non-blocking)",
                self.buy_slippage_retry_max_bps,
                envelope.max_slippage_bps
            );
        }
        if self.max_hold_seconds > envelope.max_hold_seconds {
            tracing::warn!(
                "{label} max_hold_seconds={} exceeds MAYHEM_LIVE_MAX_HOLD_CEILING_SECONDS={} (non-blocking)",
                self.max_hold_seconds,
                envelope.max_hold_seconds
            );
        }
        if self.unpriced_exit_grace_ms > envelope.max_unpriced_exit_grace_ms {
            tracing::warn!(
                "{label} unpriced_exit_grace_ms={} exceeds MAYHEM_LIVE_MAX_UNPRICED_EXIT_GRACE_MS={} (non-blocking)",
                self.unpriced_exit_grace_ms,
                envelope.max_unpriced_exit_grace_ms
            );
        }
        if self.exit_check_interval_ms > envelope.max_exit_check_interval_ms {
            tracing::warn!(
                "{label} exit_check_interval_ms={} exceeds MAYHEM_LIVE_MAX_EXIT_CHECK_INTERVAL_MS={} (non-blocking)",
                self.exit_check_interval_ms,
                envelope.max_exit_check_interval_ms
            );
        }
        // Boolean gates (not affected by stale shell env vars):
        if envelope.require_backfill_disabled && self.backfill_limit != 0 {
            anyhow::bail!(
                "{label} backfill_limit must remain 0 unless MAYHEM_LIVE_REQUIRE_BACKFILL_DISABLED=false"
            );
        }
        if envelope.require_real_curve_fills && self.use_observed_entry_fill {
            anyhow::bail!(
                "{label} must use real Pump curve quotes unless MAYHEM_LIVE_REQUIRE_REAL_CURVE_FILLS=false"
            );
        }
        Ok(())
    }

    pub fn validate_for_bot(&self) -> Result<()> {
        if self.helius_api_key.trim().is_empty() || self.helius_api_key.contains("PUT_YOUR") {
            anyhow::bail!("set helius_api_key in config.toml before running the live scanner");
        }

        if self.mode == Mode::Live {
            if !self.enable_live_trading {
                anyhow::bail!("mode='live' is locked: set enable_live_trading=true only after paper validation");
            }
            if self.require_manual_live_unlock {
                anyhow::bail!("manual live unlock is still required; this build intentionally blocks live sends");
            }
            let has_base58 = self
                .wallet_keypair_base58
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
            if self.wallet_keypair_path.trim().is_empty() && !has_base58 {
                anyhow::bail!(
                    "live mode requires wallet_keypair_path or wallet_keypair_base58 for a dedicated hot wallet"
                );
            }
            if !has_base58 {
                for marker in &self.main_wallet_markers {
                    if !marker.is_empty()
                        && self
                            .wallet_keypair_path
                            .to_lowercase()
                            .contains(&marker.to_lowercase())
                    {
                        anyhow::bail!("wallet_keypair_path looks like a main/cold wallet path");
                    }
                }
            }
            if self.max_total_lamports_per_mint == 0
                || self.max_total_open_lamports == 0
                || self.max_daily_loss_lamports <= 0
            {
                anyhow::bail!("live mode refuses unlimited or missing risk caps");
            }
            if self.max_total_lamports_per_mint < self.base_buy_lamports {
                anyhow::bail!("live mode requires max_total_sol_per_mint >= base_buy_sol");
            }
            if self.max_total_open_lamports < self.base_buy_lamports {
                anyhow::bail!("live mode requires max_total_open_sol >= base_buy_sol");
            }
            if self.live_single_lifecycle {
                if self.max_open_positions != 1 {
                    anyhow::bail!("live_single_lifecycle requires max_open_positions=1");
                }
                if self.max_buys_per_mint != 1 {
                    anyhow::bail!("live_single_lifecycle requires max_buys_per_mint=1");
                }
                if self.max_total_open_lamports > self.max_total_lamports_per_mint {
                    anyhow::bail!(
                        "live_single_lifecycle requires max_total_open_sol <= max_total_sol_per_mint"
                    );
                }
                if self.enable_take_profit_exit && self.take_profit_sell_bps != 10_000 {
                    anyhow::bail!("live_single_lifecycle requires take_profit_sell_bps=10000");
                }
            }
        }

        if self.require_mayhem_evidence
            && self.mayhem_mint_allowlist_path.trim().is_empty()
            && self.mayhem_metadata_url_template.trim().is_empty()
            && self.mayhem_program.trim().is_empty()
            && self.mayhem_agent_wallet.trim().is_empty()
        {
            anyhow::bail!(
                "Mayhem evidence is required, but no Mayhem program, agent wallet, or metadata endpoint is configured"
            );
        }

        if !(0.0..=1.0).contains(&self.mayhem_evidence_min_confidence) {
            anyhow::bail!("mayhem_evidence_min_confidence must be between 0.0 and 1.0");
        }

        if self.require_route_confirmation && self.axiom_route_program.trim().is_empty() {
            anyhow::bail!("route confirmation is required, but axiom_route_program is empty");
        }
        if self.paper_slippage_bps > self.max_slippage_bps {
            anyhow::bail!("paper_slippage_bps cannot exceed max_slippage_bps");
        }
        if self.paper_slippage_bps >= 10_000 {
            anyhow::bail!("paper_slippage_bps must be below 10000");
        }
        if self.entry_deadline_ms <= 0 {
            anyhow::bail!("entry_deadline_ms must be positive");
        }
        if self.buy_slippage_retry_deadline_ms <= 0 {
            anyhow::bail!("buy_slippage_retry_deadline_ms must be positive");
        }
        if self.buy_slippage_retry_attempts > 0
            && self.buy_slippage_retry_deadline_ms < self.entry_deadline_ms
        {
            anyhow::bail!(
                "buy_slippage_retry_deadline_ms cannot be shorter than entry_deadline_ms"
            );
        }
        if self.buy_slippage_retry_step_bps == 0 {
            anyhow::bail!("buy_slippage_retry_step_bps must be positive");
        }
        if self.buy_slippage_retry_max_bps < self.max_slippage_bps {
            anyhow::bail!(
                "buy_slippage_retry_max_bps={} cannot be lower than max_slippage_bps={}",
                self.buy_slippage_retry_max_bps,
                self.max_slippage_bps
            );
        }
        if self.buy_slippage_retry_max_bps >= 10_000 {
            anyhow::bail!("buy_slippage_retry_max_bps must be below 10000");
        }
        if self.max_stream_event_age_ms <= 0 {
            anyhow::bail!("max_stream_event_age_ms must be positive");
        }
        if self.max_create_event_slot_lag > 64 {
            anyhow::bail!("max_create_event_slot_lag must be <= 64");
        }
        if self.require_fresh_mint_creation && !self.allow_onchain_mayhem_discovery {
            anyhow::bail!(
                "require_fresh_mint_creation requires allow_onchain_mayhem_discovery=true"
            );
        }
        if !self.enable_transaction_subscribe && !self.enable_logs_fallback {
            anyhow::bail!(
                "at least one live ingest path must be enabled: transactionSubscribe or logsSubscribe"
            );
        }
        if self.use_observed_entry_fill && !self.fetch_full_transaction {
            anyhow::bail!("use_observed_entry_fill requires fetch_full_transaction=true");
        }
        if self.copy_trade_enabled {
            let wallet = self.copy_trade_wallet.trim();
            if wallet.is_empty() {
                anyhow::bail!("copy_trade_enabled requires copy_trade_wallet");
            }
            wallet
                .parse::<Pubkey>()
                .with_context(|| "copy_trade_wallet is not a valid Solana pubkey")?;
            if !self.fetch_full_transaction {
                anyhow::bail!("copy_trade_enabled requires fetch_full_transaction=true");
            }
            if self.copy_trade_scale_bps == 0 {
                anyhow::bail!("copy_trade_scale_bps must be positive");
            }
            if self.copy_trade_max_buy_lamports == 0 {
                anyhow::bail!("copy_trade_max_buy_sol must be positive");
            }
            if self.copy_trade_max_buys_per_mint == 0 {
                anyhow::bail!("copy_trade_max_buys_per_mint must be positive");
            }
            if self.copy_trade_max_hold_seconds <= 0 {
                anyhow::bail!("copy_trade_max_hold_seconds must be positive");
            }
            if self.copy_trade_take_profit_bps < 0 {
                anyhow::bail!("copy_trade_take_profit_bps cannot be negative");
            }
            if self.copy_trade_take_profit_sell_bps == 0
                || self.copy_trade_take_profit_sell_bps > 10_000
            {
                anyhow::bail!("copy_trade_take_profit_sell_bps must be between 1 and 10000");
            }
            if self.copy_trade_stop_loss_bps < 0 {
                anyhow::bail!("copy_trade_stop_loss_bps cannot be negative");
            }
        }
        if self.mode == Mode::Live && self.use_observed_entry_fill {
            anyhow::bail!("use_observed_entry_fill is a paper-only execution model");
        }
        if self
            .max_observed_buy_lamports
            .is_some_and(|max| self.min_observed_buy_lamports > max)
        {
            anyhow::bail!("min_observed_buy_sol cannot exceed max_observed_buy_sol");
        }
        if self.max_hold_seconds <= 0 {
            anyhow::bail!("max_hold_seconds must be positive");
        }
        if self.market_quote_max_age_ms <= 0 {
            anyhow::bail!("market_quote_max_age_ms must be positive");
        }
        if self.unpriced_exit_grace_ms < 0 {
            anyhow::bail!("unpriced_exit_grace_ms cannot be negative");
        }
        if self.ambiguous_entry_expiry_ms <= 0 {
            anyhow::bail!("ambiguous_entry_expiry_ms must be positive");
        }
        if self.ambiguous_inventory_recheck_ms == 0 {
            anyhow::bail!("ambiguous_inventory_recheck_ms must be positive");
        }
        if self.exit_check_interval_ms == 0 {
            anyhow::bail!("exit_check_interval_ms must be positive");
        }
        if self.take_profit_sell_bps == 0 || self.take_profit_sell_bps > 10_000 {
            anyhow::bail!("take_profit_sell_bps must be between 1 and 10000");
        }
        if self.curve_observation_seconds < self.max_hold_seconds {
            anyhow::bail!("curve_observation_seconds cannot be shorter than max_hold_seconds");
        }
        if self.require_discovery_signal
            && self.pulse_mints_path.trim().is_empty()
            && !self.allow_onchain_mayhem_discovery
        {
            anyhow::bail!(
                "discovery signal is required, but neither pulse_mints_path nor on-chain Mayhem discovery is enabled"
            );
        }

        Ok(())
    }

    pub fn watched_wallets(&self) -> Vec<String> {
        let mut wallets = self.watched_wallets.clone();
        if let Some(wallet) = &self.target_wallet {
            if !wallet.trim().is_empty() && !wallets.iter().any(|w| w == wallet) {
                wallets.push(wallet.clone());
            }
        }
        if let Some(wallet) = self.copy_trade_wallet() {
            if !wallets.iter().any(|w| w == wallet) {
                wallets.push(wallet.to_string());
            }
        }
        wallets
    }

    pub fn copy_trade_wallet(&self) -> Option<&str> {
        let wallet = self.copy_trade_wallet.trim();
        (self.copy_trade_enabled && !wallet.is_empty()).then_some(wallet)
    }

    pub fn redacted(&self) -> serde_json::Value {
        let mut value = serde_json::to_value(self).unwrap_or_else(|_| serde_json::json!({}));
        if let Some(obj) = value.as_object_mut() {
            if obj.contains_key("helius_api_key") {
                obj.insert(
                    "helius_api_key".to_string(),
                    serde_json::json!("<redacted>"),
                );
            }
            if obj.contains_key("wallet_keypair_path") && !self.wallet_keypair_path.is_empty() {
                obj.insert(
                    "wallet_keypair_path".to_string(),
                    serde_json::json!("<redacted-path>"),
                );
            }
        }
        value
    }

    pub fn ws_url(&self) -> String {
        format!(
            "wss://mainnet.helius-rpc.com/?api-key={}",
            urlencoding::encode(&self.helius_api_key)
        )
    }

    pub fn rpc_url(&self) -> String {
        format!(
            "https://mainnet.helius-rpc.com/?api-key={}",
            urlencoding::encode(&self.helius_api_key)
        )
    }
}

/// Search for a `helius.txt` key file near the config we just
/// loaded. Order:
///   1. The directory containing the config file.
///   2. The current working directory.
///   3. Each ancestor of the current working directory.
///
/// Returns the first match. Used by `load_inner` so a bare
/// `catarnith` invocation (no `HELIUS_API_KEY_FILE` env, no key
/// in config) still finds the key the wrapper script places at
/// `<repo-root>/helius.txt`.
fn discover_helius_key_file(config_path: &Path) -> Option<std::path::PathBuf> {
    if let Some(parent) = config_path.parent() {
        let candidate = parent.join("helius.txt");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    if let Ok(cwd) = env::current_dir() {
        let candidate = cwd.join("helius.txt");
        if candidate.is_file() {
            return Some(candidate);
        }
        let mut cur: Option<&Path> = Some(cwd.as_path());
        while let Some(dir) = cur {
            let candidate = dir.join("helius.txt");
            if candidate.is_file() {
                return Some(candidate);
            }
            cur = dir.parent();
        }
    }
    None
}

/// Canonical canary-wallet location the wrapper script
/// defaults to. Used as a last-ditch fallback when neither
/// `CTARNITH_WALLET_KEYPAIR_PATH` nor
/// `CTARNITH_WALLET_KEYPAIR_BASE58` is set and the config
/// field is empty.
fn default_canary_wallet_path() -> Option<std::path::PathBuf> {
    if let Some(home) = env::var_os("HOME") {
        return Some(
            std::path::PathBuf::from(home).join(".config/solana/catarnith-live-canary.json"),
        );
    }
    None
}

/// Canonical env-var prefix. All runtime env overrides are read as
/// `CTARNITH_*` first and fall back to the legacy `MAYHEM_*` name so existing
/// `.env` files and deployment scripts keep working.
pub const ENV_PREFIX: &str = "CTARNITH_";
/// Legacy env-var prefix kept as a read-only fallback.
pub const LEGACY_ENV_PREFIX: &str = "MAYHEM_";

/// Map a legacy `MAYHEM_*` env name to its canonical `CTARNITH_*` form.
/// Names that do not start with the legacy prefix are returned unchanged.
fn canonical_env_name(name: &str) -> Option<String> {
    name.strip_prefix(LEGACY_ENV_PREFIX)
        .map(|rest| format!("{ENV_PREFIX}{rest}"))
}

fn display_env_name(name: &str) -> String {
    canonical_env_name(name).unwrap_or_else(|| name.to_string())
}

fn apply_sol_toml_aliases(raw: &str, cfg: &mut Config) -> Result<()> {
    let value: toml::Value = raw.parse().context("parse TOML for SOL aliases")?;
    let Some(table) = value.as_table() else {
        return Ok(());
    };

    apply_top_level_sol_alias(table, "base_buy_sol", &mut cfg.base_buy_lamports)?;
    apply_top_level_sol_alias(
        table,
        "max_total_sol_per_mint",
        &mut cfg.max_total_lamports_per_mint,
    )?;
    apply_top_level_sol_alias(
        table,
        "max_total_open_sol",
        &mut cfg.max_total_open_lamports,
    )?;
    apply_top_level_sol_alias_i64(
        table,
        "max_daily_loss_sol",
        &mut cfg.max_daily_loss_lamports,
    )?;
    apply_top_level_sol_alias(
        table,
        "copy_trade_max_buy_sol",
        &mut cfg.copy_trade_max_buy_lamports,
    )?;
    apply_top_level_sol_alias(
        table,
        "copy_trade_min_source_buy_sol",
        &mut cfg.copy_trade_min_source_buy_lamports,
    )?;
    apply_top_level_sol_alias(
        table,
        "min_observed_buy_sol",
        &mut cfg.min_observed_buy_lamports,
    )?;
    apply_top_level_option_sol_alias(
        table,
        "max_observed_buy_sol",
        &mut cfg.max_observed_buy_lamports,
    )?;

    if let Some(live) = table.get("live").and_then(toml::Value::as_table) {
        apply_top_level_sol_alias(live, "max_balance_sol", &mut cfg.live.max_balance_lamports)?;
        apply_top_level_sol_alias(live, "jito_tip_sol", &mut cfg.live.jito_tip_lamports)?;
    }

    Ok(())
}

fn apply_top_level_sol_alias(table: &toml::Table, key: &str, target: &mut u64) -> Result<()> {
    if let Some(value) = table.get(key) {
        *target = sol_value_to_lamports(value, key)?;
    }
    Ok(())
}

fn apply_top_level_option_sol_alias(
    table: &toml::Table,
    key: &str,
    target: &mut Option<u64>,
) -> Result<()> {
    if let Some(value) = table.get(key) {
        *target = Some(sol_value_to_lamports(value, key)?);
    }
    Ok(())
}

fn apply_top_level_sol_alias_i64(table: &toml::Table, key: &str, target: &mut i64) -> Result<()> {
    if let Some(value) = table.get(key) {
        *target = i64::try_from(sol_value_to_lamports(value, key)?)
            .with_context(|| format!("{key} exceeds i64 lamports"))?;
    }
    Ok(())
}

fn sol_value_to_lamports(value: &toml::Value, name: &str) -> Result<u64> {
    let sol = match value {
        toml::Value::Float(value) => *value,
        toml::Value::Integer(value) => *value as f64,
        toml::Value::String(value) => value
            .trim()
            .parse::<f64>()
            .with_context(|| format!("{name} must be a SOL number"))?,
        _ => anyhow::bail!("{name} must be a SOL number"),
    };
    sol_f64_to_lamports(sol, name)
}

fn sol_f64_to_lamports(sol: f64, name: &str) -> Result<u64> {
    if !sol.is_finite() || sol < 0.0 {
        anyhow::bail!("{name} must be a non-negative SOL number");
    }
    if sol == 0.0 {
        return Ok(0);
    }
    let lamports = (sol * 1_000_000_000.0).round();
    if lamports < 1.0 || lamports > u64::MAX as f64 {
        anyhow::bail!("{name} is outside supported lamport range");
    }
    Ok(lamports as u64)
}

/// Read an env var by its canonical name, falling back to a legacy name.
/// Returns the trimmed value, or `VarError::NotPresent` when neither is set
/// (or both are set but empty/whitespace).
pub fn env_var(canonical: &str, legacy: &str) -> std::result::Result<String, env::VarError> {
    for name in [canonical, legacy] {
        match env::var(name) {
            Ok(value) if !value.trim().is_empty() => return Ok(value),
            _ => continue,
        }
    }
    Err(env::VarError::NotPresent)
}

/// Look up an env var given its legacy `MAYHEM_*` name, automatically
/// preferring the canonical `CTARNITH_*` alias. Returns the trimmed value, or
/// `None` when neither name is set (or both are empty/whitespace). Use this
/// from the binaries so their env reads share the same fallback rules as the
/// config loader.
pub fn env_lookup(legacy_name: &str) -> Option<String> {
    env_present_value(legacy_name).ok().flatten()
}

fn env_present_value(name: &str) -> Result<Option<String>> {
    // Read the canonical CTARNITH_* name first, then fall back to the legacy
    // MAYHEM_* name the call sites still pass in. Callers throughout this
    // module pass legacy names, so deriving the canonical alias here routes
    // every override through the new prefix in one place.
    let names = match canonical_env_name(name) {
        Some(canonical) => vec![canonical, name.to_string()],
        None => vec![name.to_string()],
    };
    for candidate in &names {
        match env::var(candidate) {
            Ok(value) => {
                let value = value.trim();
                if value.is_empty() {
                    continue;
                }
                return Ok(Some(value.to_string()));
            }
            Err(env::VarError::NotPresent) => continue,
            Err(env::VarError::NotUnicode(_)) => {
                anyhow::bail!("{candidate} must be valid UTF-8");
            }
        }
    }
    Ok(None)
}

fn parse_env<T>(name: &str) -> Result<Option<T>>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    env_present_value(name)?
        .map(|value| {
            let display_name = display_env_name(name);
            value
                .parse::<T>()
                .map_err(|err| anyhow::anyhow!("failed to parse {display_name}={value:?}: {err}"))
        })
        .transpose()
}

fn market_env_override() -> Result<Option<Market>> {
    for (canonical, legacy) in [
        ("CTARNITH_MARKET", "MAYHEM_MARKET"),
        ("CTARNITH_PAIR_SCOPE", "MAYHEM_PAIR_SCOPE"),
    ] {
        if let Ok(value) = env_var(canonical, legacy) {
            return Market::from_config_value(value.trim()).map(Some);
        }
    }
    Ok(None)
}

fn live_market_env_override() -> Result<Option<Market>> {
    for legacy in ["MAYHEM_LIVE_MARKET", "MAYHEM_LIVE_PAIR_SCOPE"] {
        if let Some(value) = env_lookup(legacy) {
            return Market::from_config_value(value.trim()).map(Some);
        }
    }
    Ok(None)
}

fn env_override_string(name: &str, target: &mut String) {
    if let Ok(Some(value)) = env_present_value(name) {
        *target = value;
    }
}

fn env_override_option_string(name: &str, target: &mut Option<String>) -> Result<()> {
    if let Some(value) = env_present_value(name)? {
        if matches!(
            value.to_ascii_lowercase().as_str(),
            "" | "none" | "null" | "unset"
        ) {
            *target = None;
        } else {
            *target = Some(value);
        }
    }
    Ok(())
}

fn env_override_vec_string(name: &str, target: &mut Vec<String>) -> Result<()> {
    if let Some(value) = env_present_value(name)? {
        if matches!(
            value.to_ascii_lowercase().as_str(),
            "none" | "null" | "unset"
        ) {
            target.clear();
        } else {
            *target = value
                .split(',')
                .map(str::trim)
                .filter(|item| !item.is_empty())
                .map(ToString::to_string)
                .collect();
        }
    }
    Ok(())
}

fn env_override_bool(name: &str, target: &mut bool) -> Result<()> {
    if let Some(value) = env_present_value(name)? {
        *target = match value.to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => {
                let display_name = display_env_name(name);
                anyhow::bail!(
                    "{display_name} must be a boolean value: true/false, yes/no, on/off, or 1/0"
                )
            }
        };
    }
    Ok(())
}

fn env_override_i64(name: &str, target: &mut i64) -> Result<()> {
    if let Some(value) = parse_env::<i64>(name)? {
        *target = value;
    }
    Ok(())
}

fn env_override_u64(name: &str, target: &mut u64) -> Result<()> {
    if let Some(value) = parse_env::<u64>(name)? {
        *target = value;
    }
    Ok(())
}

fn env_override_sol_lamports(name: &str, target: &mut u64) -> Result<()> {
    if let Some(value) = parse_env::<f64>(name)? {
        *target = sol_f64_to_lamports(value, &display_env_name(name))?;
    }
    Ok(())
}

fn env_override_sol_lamports_i64(name: &str, target: &mut i64) -> Result<()> {
    if let Some(value) = parse_env::<f64>(name)? {
        *target = i64::try_from(sol_f64_to_lamports(value, &display_env_name(name))?)
            .with_context(|| format!("{} exceeds i64 lamports", display_env_name(name)))?;
    }
    Ok(())
}

fn env_override_option_sol_lamports(name: &str, target: &mut Option<u64>) -> Result<()> {
    if let Some(value) = parse_env::<f64>(name)? {
        *target = Some(sol_f64_to_lamports(value, &display_env_name(name))?);
    }
    Ok(())
}

fn env_override_u32(name: &str, target: &mut u32) -> Result<()> {
    if let Some(value) = parse_env::<u32>(name)? {
        *target = value;
    }
    Ok(())
}

fn env_override_usize(name: &str, target: &mut usize) -> Result<()> {
    if let Some(value) = parse_env::<usize>(name)? {
        *target = value;
    }
    Ok(())
}

fn env_envelope_i64(envelope_name: &str, strategy_name: &str, target: &mut i64) -> Result<()> {
    if let Some(value) = parse_env::<i64>(envelope_name)? {
        *target = value;
    }
    if let Some(value) = parse_env::<i64>(strategy_name)? {
        *target = (*target).max(value);
    }
    Ok(())
}

fn env_envelope_u64(envelope_name: &str, strategy_name: &str, target: &mut u64) -> Result<()> {
    if let Some(value) = parse_env::<u64>(envelope_name)? {
        *target = value;
    }
    if let Some(value) = parse_env::<u64>(strategy_name)? {
        *target = (*target).max(value);
    }
    Ok(())
}

fn env_envelope_sol_lamports(
    envelope_name: &str,
    strategy_name: &str,
    target: &mut u64,
) -> Result<()> {
    if let Some(value) = parse_env::<f64>(envelope_name)? {
        *target = sol_f64_to_lamports(value, &display_env_name(envelope_name))?;
    }
    if let Some(value) = parse_env::<f64>(strategy_name)? {
        let strategy = sol_f64_to_lamports(value, &display_env_name(strategy_name))?;
        *target = (*target).max(strategy);
    }
    Ok(())
}

fn env_envelope_sol_lamports_i64(
    envelope_name: &str,
    strategy_name: &str,
    target: &mut i64,
) -> Result<()> {
    if let Some(value) = parse_env::<f64>(envelope_name)? {
        *target = i64::try_from(sol_f64_to_lamports(
            value,
            &display_env_name(envelope_name),
        )?)
        .with_context(|| format!("{} exceeds i64 lamports", display_env_name(envelope_name)))?;
    }
    if let Some(value) = parse_env::<f64>(strategy_name)? {
        let strategy = i64::try_from(sol_f64_to_lamports(
            value,
            &display_env_name(strategy_name),
        )?)
        .with_context(|| format!("{} exceeds i64 lamports", display_env_name(strategy_name)))?;
        *target = (*target).max(strategy);
    }
    Ok(())
}

fn env_envelope_u32(envelope_name: &str, strategy_name: &str, target: &mut u32) -> Result<()> {
    if let Some(value) = parse_env::<u32>(envelope_name)? {
        *target = value;
    }
    if let Some(value) = parse_env::<u32>(strategy_name)? {
        *target = (*target).max(value);
    }
    Ok(())
}

fn env_envelope_usize(envelope_name: &str, strategy_name: &str, target: &mut usize) -> Result<()> {
    if let Some(value) = parse_env::<usize>(envelope_name)? {
        *target = value;
    }
    if let Some(value) = parse_env::<usize>(strategy_name)? {
        *target = (*target).max(value);
    }
    Ok(())
}

fn env_override_f64(name: &str, target: &mut f64) -> Result<()> {
    if let Some(value) = parse_env::<f64>(name)? {
        *target = value;
    }
    Ok(())
}

fn env_override_option_u64(name: &str, target: &mut Option<u64>) -> Result<()> {
    if let Some(value) = env_present_value(name)? {
        if matches!(
            value.to_ascii_lowercase().as_str(),
            "none" | "null" | "unset"
        ) {
            *target = None;
        } else {
            *target = Some(
                value
                    .parse::<u64>()
                    .with_context(|| format!("failed to parse {name}={value:?}"))?,
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    struct EnvRestore {
        values: Vec<(&'static str, Option<String>)>,
    }

    impl EnvRestore {
        fn capture(keys: &[&'static str]) -> Self {
            Self {
                values: keys.iter().map(|key| (*key, env::var(key).ok())).collect(),
            }
        }
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            for (key, value) in &self.values {
                match value {
                    Some(value) => env::set_var(key, value),
                    None => env::remove_var(key),
                }
            }
        }
    }

    #[test]
    fn live_envelope_uses_strategy_env_as_effective_cap() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock poisoned");
        let keys = [
            "MAYHEM_LIVE_MAX_DAILY_LOSS_CEILING_LAMPORTS",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_LAMPORTS",
        ];
        let _restore = EnvRestore::capture(&keys);

        env::set_var("MAYHEM_LIVE_MAX_DAILY_LOSS_CEILING_LAMPORTS", "100000000");
        env::set_var("MAYHEM_LIVE_MAX_DAILY_LOSS_LAMPORTS", "1000000000");

        let envelope = LiveRiskEnvelope::from_env().expect("live envelope from env");
        assert_eq!(envelope.max_daily_loss_lamports, 1_000_000_000);
    }

    #[test]
    fn live_envelope_supports_sol_sized_env_caps() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock poisoned");
        let keys = [
            "MAYHEM_LIVE_MAX_ENTRY_LAMPORTS",
            "MAYHEM_LIVE_BASE_BUY_LAMPORTS",
            "MAYHEM_LIVE_MAX_ENTRY_SOL",
            "MAYHEM_LIVE_BASE_BUY_SOL",
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_LAMPORTS_CEILING",
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_LAMPORTS",
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL_CEILING",
            "MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_CEILING_LAMPORTS",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_LAMPORTS",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_SOL_CEILING",
            "MAYHEM_LIVE_MAX_DAILY_LOSS_SOL",
        ];
        let _restore = EnvRestore::capture(&keys);
        for key in keys {
            env::remove_var(key);
        }

        env::set_var("MAYHEM_LIVE_MAX_ENTRY_SOL", "0.02");
        env::set_var("MAYHEM_LIVE_BASE_BUY_SOL", "0.03");
        env::set_var("MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL_CEILING", "0.2");
        env::set_var("MAYHEM_LIVE_MAX_TOTAL_OPEN_SOL", "0.25");
        env::set_var("MAYHEM_LIVE_MAX_DAILY_LOSS_SOL_CEILING", "0.4");
        env::set_var("MAYHEM_LIVE_MAX_DAILY_LOSS_SOL", "0.45");

        let envelope = LiveRiskEnvelope::from_env().expect("live envelope from SOL env");
        assert_eq!(envelope.max_entry_lamports, 30_000_000);
        assert_eq!(envelope.max_total_open_lamports, 250_000_000);
        assert_eq!(envelope.max_daily_loss_lamports, 450_000_000);
    }

    #[test]
    fn discover_helius_key_file_finds_sibling() {
        let dir = std::env::temp_dir().join("catarnith_helius_sibling_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("helius.txt"), "test-key-from-sibling").unwrap();
        let config_path = dir.join("config.toml");
        std::fs::write(&config_path, "").unwrap();

        let found = discover_helius_key_file(&config_path)
            .expect("expected to find helius.txt next to config");
        assert!(found.ends_with("helius.txt"));
        let contents = std::fs::read_to_string(&found).unwrap();
        assert_eq!(contents, "test-key-from-sibling");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_helius_key_file_walks_up_parents() {
        let dir = std::env::temp_dir().join("catarnith_helius_walk_test");
        let nested = dir.join("a").join("b").join("c");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::write(dir.join("helius.txt"), "test-key-from-parent").unwrap();
        let config_path = nested.join("config.toml");
        std::fs::write(&config_path, "").unwrap();

        // Walk up from the directory of config_path; it should
        // hit the `helius.txt` placed at `dir/`.
        let original_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(&nested).unwrap();
        let found = discover_helius_key_file(&config_path)
            .expect("expected to find helius.txt by walking up parents");
        std::env::set_current_dir(&original_cwd).unwrap();
        assert!(found.ends_with("helius.txt"));
        let contents = std::fs::read_to_string(&found).unwrap();
        assert_eq!(contents, "test-key-from-parent");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn discover_helius_key_file_returns_none_when_absent() {
        let dir = std::env::temp_dir().join("catarnith_helius_missing_test");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let config_path = dir.join("config.toml");
        std::fs::write(&config_path, "").unwrap();

        let original_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        let found = discover_helius_key_file(&config_path);
        std::env::set_current_dir(&original_cwd).unwrap();
        assert!(found.is_none());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_dot_env_basic() {
        let text = "\
# A comment
export FOO=bar
BAZ=qux
export QUX=\"double quoted\"
export SINGLE='single quoted'
empty=
";
        let parsed = parse_dot_env(text);
        let m: std::collections::HashMap<_, _> = parsed.into_iter().collect();
        assert_eq!(m.get("FOO").map(|s| s.as_str()), Some("bar"));
        assert_eq!(m.get("BAZ").map(|s| s.as_str()), Some("qux"));
        assert_eq!(m.get("QUX").map(|s| s.as_str()), Some("double quoted"));
        assert_eq!(m.get("SINGLE").map(|s| s.as_str()), Some("single quoted"));
        assert_eq!(m.get("empty").map(|s| s.as_str()), Some(""));
    }

    #[test]
    fn parse_dot_env_expands_home() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock poisoned");
        let _restore = EnvRestore::capture(&["HOME"]);
        env::set_var("HOME", "/Users/test");
        let text =
            "export CTARNITH_WALLET_KEYPAIR_PATH=$HOME/.config/solana/catarnith-live-canary.json\n";
        let parsed = parse_dot_env(text);
        let m: std::collections::HashMap<_, _> = parsed.into_iter().collect();
        assert_eq!(
            m.get("CTARNITH_WALLET_KEYPAIR_PATH").map(|s| s.as_str()),
            Some("/Users/test/.config/solana/catarnith-live-canary.json")
        );
    }

    #[test]
    fn parse_dot_env_ignores_malformed_lines() {
        let text = "\
not_a_valid_line
=missing_key
export =missing_key_too
KEY WITH SPACES=value
";
        let parsed = parse_dot_env(text);
        assert!(
            parsed.is_empty(),
            "expected 0 parsed entries, got {parsed:?}"
        );
    }

    #[test]
    fn discover_dot_env_finds_workspace_env() {
        // The workspace .env should be found when running from the
        // workspace root should find it.
        let original_cwd = std::env::current_dir().unwrap();
        let workspace_root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        if workspace_root.join(".env").is_file() {
            std::env::set_current_dir(&workspace_root).unwrap();
            let found = discover_dot_env().expect("expected to find .env in workspace root");
            std::env::set_current_dir(&original_cwd).unwrap();
            assert!(found.ends_with(".env"));
        }
    }

    #[test]
    fn default_canary_wallet_path_uses_home() {
        let _guard = ENV_LOCK
            .get_or_init(|| Mutex::new(()))
            .lock()
            .expect("env test lock poisoned");
        let _restore = EnvRestore::capture(&["HOME"]);
        env::set_var("HOME", "/Users/example");
        let p = default_canary_wallet_path().expect("HOME is set, should resolve");
        assert_eq!(
            p,
            std::path::PathBuf::from("/Users/example/.config/solana/catarnith-live-canary.json")
        );
    }
}
