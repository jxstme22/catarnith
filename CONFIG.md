# Catarnith Configuration

Catarnith uses one local profile by default: `config.toml`.

```bash
cp config.example.toml config.toml
cp .env.example .env
```

`config.toml` holds strategy, direct-run defaults, and risk settings. `.env`
holds local secrets and machine-specific overrides. Both files are ignored by
git.

No alternate runtime profiles are shipped anymore. Paper, live, and auto-bot
mode all load `config.toml` unless you explicitly pass `--config <PATH>` or set
`CTARNITH_LIVE_CONFIG`. Inside the TUI, the mode picker overrides `mode` for
that run: `[2] Live Trade` forces live execution and `[3] Paper Trade` forces
paper-only execution.

## Precedence

Runtime loading follows this order:

1. Start with `Config::default()`.
2. Merge the selected TOML file, normally `config.toml`.
3. Apply `.env` and process environment overrides.

Use `CTARNITH_*` names for new setup. Legacy `MAYHEM_*` names are still read as
fallbacks when the matching `CTARNITH_*` variable is not set.

Examples:

- `CTARNITH_LIVE_BASE_BUY_SOL` overrides `base_buy_sol`.
- `CTARNITH_LIVE_MAX_SLIPPAGE_BPS` overrides `max_slippage_bps`.
- `CTARNITH_FALLBACK_RPC_URL` optionally provides a distinct paid fallback RPC.
- `CTARNITH_MARKET` overrides `market` (`mayhem_only`, `non_mayhem_only`, `all_pumpfun`).
- `CTARNITH_WALLET_KEYPAIR_BASE58` wins over `wallet_keypair_path`.

The in-app Settings editor writes both `config.toml` and the matching `.env`
keys so saved values are not accidentally shadowed by stale overrides. Settings
edits wallet/secrets, market, buy size, buy slippage, max hold, and advanced
live-trade controls. Auto Bot Setup (`[1]`) owns bot mode, market, stream
timing, copy-trade setup, risk caps, and bot-specific advanced controls.

## Main Keys

| Key | Meaning |
|---|---|
| `mode` | `"paper"` or `"live"` default for direct non-picker runs. The TUI picker overrides this for Live/Paper trade mode. |
| `helius_api_key` | Helius key, or set `HELIUS_API_KEY` in `.env`. |
| `wallet_keypair_path` | Dedicated live hot-wallet JSON path. |
| `market` | `"mayhem_only"`, `"non_mayhem_only"`, or `"all_pumpfun"`. Edited from Settings and Auto Bot Setup. Legacy `pair_scope` still loads. This is an entry gate, including copy-trade buys. |
| `target_wallet` | Optional reference wallet. Leave unset unless intentionally using a reference signal/delta owner. |
| `watched_wallets` | Optional additional wallets to watch. |
| `base_buy_sol` | Buy size in SOL. Legacy `base_buy_lamports` still loads. |
| `max_open_positions` | Concurrent position cap. |
| `max_buys_per_mint` | Total buy-attempt cap per mint. |
| `max_total_sol_per_mint` | Per-mint exposure cap in SOL. |
| `max_total_open_sol` | Total open exposure cap in SOL. |
| `max_daily_loss_sol` | Daily loss stop for new entries in SOL. |
| `max_slippage_bps` | Buy slippage ceiling. |
| `take_profit_bps` | Take-profit trigger. |
| `take_profit_sell_bps` | Portion sold on take-profit, in bps. |
| `stop_loss_bps` | Stop-loss trigger. |
| `max_hold_seconds` | Forced exit timer. |
| `enable_live_trading` | Must be `true` before live broadcast is allowed. |
| `require_manual_live_unlock` | Must be `false` before live broadcast is allowed. |
| `backfill_limit` | Startup history depth. Keep `0` for live. |
| `copy_trade_enabled` | Enables copy trade in Auto Bot mode. |
| `copy_trade_wallet` | Source wallet to follow. Added to watched wallets automatically. |
| `copy_trade_sizing` | `"fixed"`, `"mirror"`, or `"scaled"`. |
| `copy_trade_scale_bps` | Scale used when sizing is `"scaled"`; `10000` means 1.0x. |
| `copy_trade_max_buy_sol` | Hard cap for copied buy size in SOL. |
| `copy_trade_buy_policy` | `"first_only"` copies only the first qualifying source buy per mint; `"accumulate"` keeps copying until caps stop it. |
| `copy_trade_max_buys_per_mint` | Copy-trade-specific buy limit per mint. |
| `copy_trade_min_source_buy_sol` | Ignore source buys below this size; `0` disables the filter. |
| `copy_trade_max_hold_seconds` | Forced exit timer for copy-entered positions. |
| `copy_trade_take_profit_bps` | Copy-entered position take-profit trigger; `0` disables copy take-profit. |
| `copy_trade_take_profit_sell_bps` | Portion sold on copy take-profit, in bps. |
| `copy_trade_stop_loss_bps` | Copy-entered position stop-loss trigger; `0` disables copy stop-loss. |
| `copy_trade_follow_sells` | Sell when the copied wallet sells a mint Catarnith holds. |
| `bot_keep_alive` | Restart the child bot if the process exits unexpectedly inside the TUI. |
| `journal_dir` | Runtime JSONL journal directory. Gitignored; default template uses `journals/bot`. |
| `sqlite_path` | Runtime SQLite position restore path. Gitignored; keep it if live positions may need recovery. |

Market behavior:

- `mayhem_only`: enters only when Mayhem evidence is verified or allowed by the
  configured strategy. The single-trade scanner waits for a positive Pump.fun
  curve Mayhem flag before entering.
- `non_mayhem_only`: fresh-launch only. It rejects old buy chatter, direct
  Mayhem evidence, indirect Mayhem candidates, and copied Mayhem buys. The
  single-trade scanner enters only when the Pump.fun curve reports
  `is_mayhem_mode = false`; unknown curve flags are skipped.
- `all_pumpfun`: allows both Mayhem and non-Mayhem Pump.fun bonding-curve
  candidates that pass the rest of the filters.

## Copy Trade Flow

Copy trade is an Auto Bot feature. When enabled, Catarnith:

1. Adds `copy_trade_wallet` to the stream subscriptions.
2. Fetches the full transaction for wallet-source log events when needed.
3. Decodes the copied wallet's SOL/token deltas only for wallet-source events
   or transactions where that wallet is the signer.
4. Filters copied buys through the copy strategy: source minimum, first-only or
   accumulate mode, and the copy max-buys-per-mint cap.
5. Applies the configured market gate to copied buys. `non_mayhem_only`
   rejects direct, indirect, or verified Mayhem signals; `mayhem_only` requires
   Mayhem evidence.
6. Converts accepted copied buys into normal `Buy` decisions using the selected
   sizing mode.
7. Converts copied sells into normal `Sell` decisions when Catarnith has that
   mint open and `copy_trade_follow_sells=true`.
8. Tags copy-entered positions so their max-hold, take-profit, and stop-loss
   rules can use copy-specific settings.
9. Sends every accepted decision through the same risk engine, pending-order
   checks, paper executor, live executor, and journals as the rest of the bot.

Copy trade does not bypass market selection or risk caps. In live mode, copied
PumpSwap AMM entries are still blocked because live PumpSwap execution is not
implemented in this project yet. Transactions that merely mention the copied
wallet as an account key are ignored unless the copied wallet is the stream
source or signer.

## Live Table

Live-only operational tuning lives under `[live]`. Paper mode ignores these
keys, but env overrides still work.

| `[live]` key | Env override | Meaning |
|---|---|---|
| `compute_unit_limit` | `CTARNITH_LIVE_COMPUTE_UNIT_LIMIT` | Compute units per trade transaction. |
| `compute_unit_price_microlamports` | `CTARNITH_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS` | Priority fee. Editable from Settings advanced. |
| `send_max_retries` | `CTARNITH_LIVE_SEND_MAX_RETRIES` | RPC send retries. |
| `send_timeout_ms` | `CTARNITH_LIVE_SEND_TIMEOUT_MS` | Per-RPC send timeout. |
| `rpc_timeout_ms` | `CTARNITH_LIVE_RPC_TIMEOUT_MS` | General RPC timeout. |
| `confirmation_timeout_ms` | `CTARNITH_LIVE_CONFIRMATION_TIMEOUT_MS` | Buy confirmation timeout. |
| `sell_confirmation_timeout_ms` | `CTARNITH_LIVE_SELL_CONFIRMATION_TIMEOUT_MS` | Sell confirmation timeout. |
| `confirmation_poll_ms` | `CTARNITH_LIVE_CONFIRMATION_POLL_MS` | Confirmation polling interval. |
| `pre_broadcast_simulation` | `CTARNITH_LIVE_PRE_BROADCAST_SIMULATION` | Simulate before broadcast. |
| `settlement_commitment` | `CTARNITH_LIVE_SETTLEMENT_COMMITMENT` | `processed`, `confirmed`, or `finalized`. |
| `sell_slippage_bps` | `CTARNITH_LIVE_SELL_SLIPPAGE_BPS` | Sell slippage; omit to reuse `max_slippage_bps`. |
| `max_balance_sol` | `CTARNITH_LIVE_MAX_BALANCE_SOL` | Refuse to trade above this wallet balance. |
| `jito_block_engine_url` | `CTARNITH_LIVE_JITO_BLOCK_ENGINE_URL` | Optional Jito block-engine broadcast path. Editable from Settings advanced. |
| `jito_tip_account` | `CTARNITH_LIVE_JITO_TIP_ACCOUNT` | Jito tip account. |
| `jito_tip_sol` | `CTARNITH_LIVE_JITO_TIP_SOL` | Jito tip amount in SOL. Editable from Settings advanced. Legacy lamport keys still load. |
| `jupiter_timeout_ms` | `CTARNITH_LIVE_JUPITER_TIMEOUT_MS` | Jupiter sell-fallback timeout. |

## Arming Live

To allow live broadcast, set all of the following deliberately:

1. Select `[2] Live Trade` in the TUI picker, or set `mode = "live"` for direct
   `catarnith scan`/bot runs.
2. `enable_live_trading = true`
3. `require_manual_live_unlock = false`
4. `wallet_keypair_path` to a dedicated hot-wallet file outside the repo, or
   `CTARNITH_WALLET_KEYPAIR_BASE58` in `.env`
5. `[live].max_balance_sol` to the maximum wallet balance Catarnith may
   trade with

Protective checks remain active even when those fields are set: secret files
must be owner-only, wallet paths with main/cold/treasury markers are rejected,
an optional fallback RPC cannot be the public mainnet endpoint or the same as
the primary RPC, and risk caps must be large enough for the configured buy size.

## Credit Controls

Broadcast paths can use primary and fallback RPCs for speed. Read-side polling
is conservative by default and falls back only when needed.

Useful low-credit settings:

- `CTARNITH_LIVE_CONFIRMATION_POLL_MS=200`
- `CTARNITH_LIVE_PARALLEL_FALLBACK_READS=0`
- `CTARNITH_LIVE_SKIP_POST_TRADE_BALANCES=1`
- `CTARNITH_LIVE_WAIT_FOR_BUY_CONFIRMATION=false`
- `backfill_limit = 0`
