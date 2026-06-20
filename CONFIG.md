# Catarnith Configuration

Catarnith uses one local profile by default: `config.toml`.

```bash
cp config.example.toml config.toml
cp .env.example .env
```

`config.toml` holds strategy, paper/live mode, and risk settings. `.env` holds
local secrets and machine-specific overrides. Both files are ignored by git.

No alternate runtime profiles are shipped anymore. Paper, live, and auto-bot
mode all load `config.toml` unless you explicitly pass `--config <PATH>` or set
`CTARNITH_LIVE_CONFIG`.

## Precedence

Runtime loading follows this order:

1. Start with `Config::default()`.
2. Merge the selected TOML file, normally `config.toml`.
3. Apply `.env` and process environment overrides.

Use `CTARNITH_*` names for new setup. Legacy `MAYHEM_*` names are still read as
fallbacks when the matching `CTARNITH_*` variable is not set.

Examples:

- `CTARNITH_LIVE_BASE_BUY_LAMPORTS` overrides `base_buy_lamports`.
- `CTARNITH_LIVE_MAX_SLIPPAGE_BPS` overrides `max_slippage_bps`.
- `CTARNITH_FALLBACK_RPC_URL` provides the paid fallback RPC.
- `CTARNITH_WALLET_KEYPAIR_BASE58` wins over `wallet_keypair_path`.

The in-app Settings editor writes both `config.toml` and the matching `.env`
keys so saved values are not accidentally shadowed by stale overrides.

## Main Keys

| Key | Meaning |
|---|---|
| `mode` | `"paper"` or `"live"`. Paper is the safe default. |
| `helius_api_key` | Helius key, or set `HELIUS_API_KEY` in `.env`. |
| `wallet_keypair_path` | Dedicated live hot-wallet JSON path. |
| `pair_scope` | `"mayhem_only"` or `"all_pumpfun"`. |
| `base_buy_lamports` | Buy size in lamports. |
| `max_open_positions` | Concurrent position cap. |
| `max_total_lamports_per_mint` | Per-mint exposure cap. |
| `max_total_open_lamports` | Total open exposure cap. |
| `max_daily_loss_lamports` | Daily loss stop for new entries. |
| `max_slippage_bps` | Buy slippage ceiling. |
| `take_profit_bps` | Take-profit trigger. |
| `take_profit_sell_bps` | Portion sold on take-profit, in bps. |
| `stop_loss_bps` | Stop-loss trigger. |
| `max_hold_seconds` | Forced exit timer. |
| `enable_live_trading` | Must be `true` before live broadcast is allowed. |
| `require_manual_live_unlock` | Must be `false` before live broadcast is allowed. |
| `backfill_limit` | Startup history depth. Keep `0` for live. |
| `journal_dir` | Runtime journal directory. |
| `sqlite_path` | Runtime SQLite journal path. |

## Live Table

Live-only operational tuning lives under `[live]`. Paper mode ignores these
keys, but env overrides still work.

| `[live]` key | Env override | Meaning |
|---|---|---|
| `compute_unit_limit` | `CTARNITH_LIVE_COMPUTE_UNIT_LIMIT` | Compute units per trade transaction. |
| `compute_unit_price_microlamports` | `CTARNITH_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS` | Priority fee. |
| `send_max_retries` | `CTARNITH_LIVE_SEND_MAX_RETRIES` | RPC send retries. |
| `send_timeout_ms` | `CTARNITH_LIVE_SEND_TIMEOUT_MS` | Per-RPC send timeout. |
| `rpc_timeout_ms` | `CTARNITH_LIVE_RPC_TIMEOUT_MS` | General RPC timeout. |
| `confirmation_timeout_ms` | `CTARNITH_LIVE_CONFIRMATION_TIMEOUT_MS` | Buy confirmation timeout. |
| `sell_confirmation_timeout_ms` | `CTARNITH_LIVE_SELL_CONFIRMATION_TIMEOUT_MS` | Sell confirmation timeout. |
| `confirmation_poll_ms` | `CTARNITH_LIVE_CONFIRMATION_POLL_MS` | Confirmation polling interval. |
| `pre_broadcast_simulation` | `CTARNITH_LIVE_PRE_BROADCAST_SIMULATION` | Simulate before broadcast. |
| `settlement_commitment` | `CTARNITH_LIVE_SETTLEMENT_COMMITMENT` | `processed`, `confirmed`, or `finalized`. |
| `sell_slippage_bps` | `CTARNITH_LIVE_SELL_SLIPPAGE_BPS` | Sell slippage; omit to reuse `max_slippage_bps`. |
| `max_balance_lamports` | `CTARNITH_LIVE_MAX_BALANCE_LAMPORTS` | Refuse to trade above this wallet balance. |
| `jito_block_engine_url` | `CTARNITH_LIVE_JITO_BLOCK_ENGINE_URL` | Optional Jito panic-sell path. |
| `jito_tip_account` | `CTARNITH_LIVE_JITO_TIP_ACCOUNT` | Jito tip account. |
| `jito_tip_lamports` | `CTARNITH_LIVE_JITO_TIP_LAMPORTS` | Jito tip amount. |
| `jupiter_timeout_ms` | `CTARNITH_LIVE_JUPITER_TIMEOUT_MS` | Jupiter sell-fallback timeout. |

## Arming Live

To allow live broadcast, set all of the following deliberately:

1. `mode = "live"`
2. `enable_live_trading = true`
3. `require_manual_live_unlock = false`
4. `wallet_keypair_path` to a dedicated hot-wallet file outside the repo, or
   `CTARNITH_WALLET_KEYPAIR_BASE58` in `.env`
5. `CTARNITH_FALLBACK_RPC_URL` to a distinct paid RPC
6. `[live].max_balance_lamports` to the maximum wallet balance Catarnith may
   trade with

Protective checks remain active even when those fields are set: secret files
must be owner-only, wallet paths with main/cold/treasury markers are rejected,
the fallback RPC cannot be the public mainnet endpoint, and risk caps must be
large enough for the configured buy size.

## Credit Controls

Broadcast paths can use primary and fallback RPCs for speed. Read-side polling
is conservative by default and falls back only when needed.

Useful low-credit settings:

- `CTARNITH_LIVE_CONFIRMATION_POLL_MS=200`
- `CTARNITH_LIVE_PARALLEL_FALLBACK_READS=0`
- `CTARNITH_LIVE_SKIP_POST_TRADE_BALANCES=1`
- `CTARNITH_LIVE_WAIT_FOR_BUY_CONFIRMATION=false`
- `backfill_limit = 0`
