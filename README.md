# Catarnith

Catarnith is a terminal-first Solana Pump.fun trading app. It gives you one
interactive TUI for paper trading, gated live trading, panic-sell handling, and
an autonomous Auto Bot with copy-trade support.

The project is intentionally paper-first. Live execution exists, but it is
behind explicit gates so a fresh checkout cannot accidentally broadcast orders.

## What Catarnith Does

- Runs a terminal UI with a clear mode picker.
- Trades Pump.fun bonding-curve mints in paper mode or live mode.
- Keeps all normal runtime setup in one local `config.toml`.
- Writes local journals for events, decisions, orders, executions, positions,
  and reports.
- Provides an Auto Bot mode for autonomous scanning and execution.
- Supports copy trade: follow a source wallet's Pump.fun buys and sells through
  Catarnith's own risk engine.
- Provides panic-sell paths from the TUI and from the CLI.

Catarnith focuses only on trading existing Pump.fun markets.

## Install

Requirements:

- Rust stable toolchain
- Helius API key
- For live trading only: a dedicated hot wallet
- Optional but recommended for live trading: a distinct paid fallback RPC

From source:

```bash
git clone https://github.com/jxstme22/catarnith.git
cd catarnith
cargo install --path . --locked
catarnith
```

For local development without installing:

```bash
cargo run --bin catarnith
```

If you only install the TUI binary, use:

```bash
cargo install --path . --locked --bin catarnith
```

For the smoothest CLI forwarding, install all binaries with `cargo install
--path . --locked`; that installs `catarnith`, `bot`, and `live_execute`.

## First Setup

Create local config files:

```bash
cp config.example.toml config.toml
cp .env.example .env
```

Then run:

```bash
catarnith
```

On first launch, open `S` Settings and fill the important local values:

| Setting | Why it matters |
|---|---|
| Helius key | Primary RPC and websocket access. |
| Wallet key | Required only for live execution. Use a dedicated hot wallet. |
| Market | `mayhem_only`, `non_mayhem_only`, or `all_pumpfun`. |
| Fallback RPC | Optional distinct paid RPC for extra live reliability. |
| Buy size | Base buy size used by manual trade mode and fixed copy sizing. |
| Slippage | Buy slippage ceiling. |
| Advanced live gates | Must be deliberately armed before live orders can broadcast. |

Settings writes both `config.toml` and `.env` so stale environment overrides do
not silently fight the values you saved in the TUI.

## Mode Picker

Running `catarnith` opens this picker:

```text
[1] Auto Bot      autonomous scanner/trader loop
[2] Live Trade    single live trade flow
[3] Paper Trade   paper trading, no real orders
[S] Settings      wallet, keys, market, buy size, live setup
```

Use `[3] Paper Trade` first. It uses the same scan/evaluate/hold/sell UI without
broadcasting real orders.

Use `[2] Live Trade` only after paper validation and after live gates are armed.
The picker overrides the `mode` value in `config.toml` for that run, so selecting
Paper stays paper-only and selecting Live runs live validation.

## TUI Controls

General controls:

| Key | Action |
|---|---|
| `1`, `2`, `3`, `S` | Pick Auto Bot, Live, Paper, or Settings from the mode picker. |
| `Enter` | Start, confirm, buy/sell when prompted, or save a setup screen. |
| `Esc` | Cancel current screen or stop the running bot. |
| `Q` | Quit outside text-entry screens. |
| `T` | Cycle theme. |
| `L` | Open the larger log overlay. |
| `Up` / `Down` | Scroll logs outside settings screens. |
| `PgUp` / `PgDn` | Scroll logs faster. |
| `Home` / `End` | Jump to oldest logs or return to tail. |
| `Tab` / `Shift+Tab` | Move between settings fields. |
| `Left` / `Right` | Toggle or cycle selected settings choices. |

The Auto Bot log panel is cleaned for readability. Expected lifecycle noise is
reduced, while real execution and transport failures remain visible.

## Auto Bot

Auto Bot is selected with `[1]`. Before it launches, Catarnith opens Auto Bot
Setup. That screen owns bot-specific settings such as:

- paper/live mode for direct bot runs
- market preference (`mayhem_only`, `non_mayhem_only`, or `all_pumpfun`)
- buy size and slippage
- stream freshness and buy deadline
- copy trade setup
- advanced bot controls, including max positions, buys per mint, per-mint
  exposure, total open exposure, and daily loss
- bot keep-alive behavior

## Market Selection

Catarnith treats the selected market as an entry gate, not a display filter:

| Market | Entry behavior |
|---|---|
| `mayhem_only` | Enters only when Mayhem evidence is verified or otherwise allowed by the configured strategy. In the single-trade scanner, Catarnith waits for a positive Pump.fun curve Mayhem flag. |
| `non_mayhem_only` | Fresh Pump.fun create/create-v2 entries only. It rejects direct and indirect Mayhem signals, and the single-trade scanner requires `is_mayhem_mode = false` from the curve. If that flag is unavailable, the candidate is skipped instead of guessed. |
| `all_pumpfun` | Allows both Mayhem and non-Mayhem Pump.fun bonding-curve candidates that pass the rest of the filters. |

Copy-trade buys obey the same market preference. For example,
`non_mayhem_only` will not copy a source-wallet buy if the mint has any Mayhem
signal.

When `bot_keep_alive = true`, the TUI restarts the bot child process if streams
or the process exit unexpectedly. Pressing `Esc` or `Q` is still an intentional
stop.

You can also run the bot directly:

```bash
catarnith bot --config config.toml
```

Or from source:

```bash
cargo run --bin bot -- --config config.toml
```

## Copy Trade

Copy trade is an Auto Bot feature. It is not a separate executor. Accepted copy
decisions still pass through Catarnith's normal risk engine, pending-order
checks, paper executor, live executor, journals, and position manager.

Main copy-trade settings:

| Key | Meaning |
|---|---|
| `copy_trade_enabled` | Turns copy trade on or off. |
| `copy_trade_wallet` | Source wallet to follow. |
| `copy_trade_sizing` | `fixed`, `mirror`, or `scaled`. |
| `copy_trade_scale_bps` | Scale factor for `scaled`; `10000` means 1.0x. |
| `copy_trade_max_buy_sol` | Hard cap for copied buy size. Legacy lamport key still loads. |
| `copy_trade_buy_policy` | `first_only` or `accumulate`. |
| `copy_trade_max_buys_per_mint` | Copy-specific buy limit per mint. |
| `copy_trade_min_source_buy_sol` | Ignore source buys below this size. |
| `copy_trade_follow_sells` | Sell when the source wallet sells a held mint. |
| `copy_trade_max_hold_seconds` | Forced exit timer for copy-entered positions. |
| `copy_trade_take_profit_bps` | Copy-specific take-profit trigger. |
| `copy_trade_take_profit_sell_bps` | Portion sold at copy take-profit. |
| `copy_trade_stop_loss_bps` | Copy-specific stop-loss trigger. |

Default copy behavior is conservative:

```toml
copy_trade_enabled = false
copy_trade_buy_policy = "first_only"
copy_trade_max_buys_per_mint = 1
copy_trade_follow_sells = true
```

Use `first_only` if you want one copied entry per mint. Use `accumulate` if you
want Catarnith to keep copying later source-wallet buys until the copy cap and
normal risk caps stop it.

Copy-trade buys also pass the configured market gate. `non_mayhem_only` rejects
any direct, indirect, or verified Mayhem signal; `mayhem_only` requires Mayhem
evidence; `all_pumpfun` allows either side of the Pump.fun market.

Copy trade attribution is strict. A transaction must come from the copied wallet
stream or have the copied wallet as signer. Transactions that merely mention the
wallet as an account key are ignored to avoid false copy entries.

Live PumpSwap copy execution is intentionally blocked for now. Live copy entries
use the supported Pump.fun bonding-curve path.

## Paper vs Live Safety

Paper mode:

- never broadcasts orders
- simulates fills and exits locally
- writes journals and paper reports
- is the recommended path before live runs

Live mode refuses to broadcast unless all of these are true:

- Live mode is selected in the picker, or `mode = "live"` is set for direct runs
- `enable_live_trading = true`
- `require_manual_live_unlock = false`
- a dedicated hot wallet is configured
- the wallet path does not look like a main/cold/treasury wallet
- optional fallback RPC, when set, is distinct from the primary RPC
- risk caps are present and large enough for the buy size
- `[live].max_balance_sol` caps the wallet balance

Selecting Live in the TUI does not bypass these gates.
During sell, `submitted` means Catarnith broadcasted a transaction but has not
confirmed final inventory yet. The TUI keeps the position visible until the sell
is confirmed or reconciled, so a slow confirmation does not look like a clean
exit too early.

## Important Config Files

| File | Purpose |
|---|---|
| `config.example.toml` | Safe template for local setup. |
| `config.toml` | Your real local runtime config. Gitignored. |
| `.env.example` | Template for local secrets and machine-specific overrides. |
| `.env` | Your real local secrets and overrides. Gitignored. |
| `CONFIG.md` | Full config reference and live-arming checklist. |

Use `CTARNITH_*` environment variables for new setup. Legacy `MAYHEM_*` names
are still read as fallbacks so older local scripts do not immediately break.

## Common Commands

```bash
# Open the mode-picker TUI
catarnith

# Skip the picker and enter trade mode
catarnith --config config.toml scan

# Run the autonomous bot directly
catarnith bot --config config.toml

# Panic-sell a held mint through the live helper path
catarnith panic-sell <MINT> --config config.toml

# Development equivalents
cargo run --bin catarnith
cargo run --bin catarnith -- --config config.toml scan
cargo run --bin bot -- --config config.toml
cargo run --bin live_execute -- --config config.toml --side sell --mint <MINT>
```

The lower-level live helper accepts:

```text
live_execute --config <path> --side <buy|sell> --mint <pubkey> [--out <path>] [--panic]
```

## Architecture

Runtime flow:

```text
                 +-------------------+
                 |   catarnith TUI   |
                 | mode/settings/logs|
                 +---------+---------+
                           |
          +----------------+----------------+
          |                                 |
          v                                 v
  +---------------+                 +---------------+
  | single trade  |                 |   Auto Bot    |
  | paper/live UI |                 | autonomous    |
  +-------+-------+                 +-------+-------+
          |                                 |
          v                                 v
  +---------------+       stream     +---------------+
  | ingest/decode | <--------------> | Helius WS/RPC |
  +-------+-------+                  +---------------+
          |
          v
  +---------------+     +------------+     +---------------+
  | classification| --> | strategy   | --> | risk engine   |
  | discovery     |     | copy trade |     | caps/gates    |
  +---------------+     +------------+     +-------+-------+
                                                    |
                                                    v
                                      +-------------+-------------+
                                      | paper executor / live exec |
                                      +-------------+-------------+
                                                    |
                                                    v
                                      +-------------+-------------+
                                      | journals / positions / UI  |
                                      +---------------------------+
```

Important source areas:

| Path | What lives there |
|---|---|
| `src/config.rs` | TOML loading, env aliases, validation, live gates. |
| `src/ingest/` | Websocket and stream handling. |
| `src/decoder/` | Transaction/log decoding and wallet deltas. |
| `src/classifier/` | Pump.fun, PumpSwap, Mayhem, route classification. |
| `src/discovery/` | Candidate and discovery signal registry. |
| `src/strategy/` | Normal strategy decisions. |
| `src/risk/` | Exposure, position, loss, and slippage caps. |
| `src/position/` | Position state and copy-trade entry tagging. |
| `src/executor/` | Paper execution and order conversion. |
| `src/live.rs` | Live Pump.fun execution. |
| `src/bin/catarnith/` | TUI screens, rendering, settings, bot wrapper. |
| `src/bin/bot.rs` | Autonomous bot loop and copy-trade decisions. |
| `src/bin/live_execute.rs` | One-shot live helper and panic-sell CLI. |

Runtime journals default to `journals/bot/`. They are local runtime state, not
source files: JSONL evidence, execution reports, and the SQLite position restore
file. They are gitignored. You can clear old paper/research runs, but do not
delete live journals while you still need open-position recovery or execution
evidence.

## Verification

Useful checks before shipping changes:

```bash
cargo fmt
cargo test
cargo clippy --all-targets -- -D warnings
git diff --check
```

Current code is expected to pass those checks.

## Troubleshooting

`catarnith` opens Settings immediately:

- Local setup is incomplete. Fill Helius/config values and save.

Live mode refuses to start:

- Check `enable_live_trading`, `require_manual_live_unlock`, wallet source,
  optional fallback RPC, balance cap, and risk caps. See `CONFIG.md`.

Sell says submitted or failed but you are unsure on-chain:

- Check the printed signature in your explorer before retrying. Catarnith keeps
  the position open when confirmation is pending so it does not mark a sell as
  final before inventory is reconciled.

Copy trade does not buy:

- Confirm `copy_trade_enabled = true`.
- Confirm `copy_trade_wallet` is a valid Solana pubkey.
- Confirm `fetch_full_transaction = true`.
- Check `copy_trade_buy_policy`, `copy_trade_max_buys_per_mint`, and
  `copy_trade_min_source_buy_sol`.

Non-Mayhem mode trades old/dead tokens:

- It should not now. `non_mayhem_only` requires a fresh Pump.fun create entry
  and an explicit curve flag of `is_mayhem_mode = false`; unknown flags are
  skipped.
- Copy-trade buys also obey this market gate, so a copied Mayhem buy should be
  ignored.
- Check the logs for `copy_trade_*` reason codes.

Logs are hard to read:

- Press `L` for the large log overlay.
- Use `PgUp`, `PgDn`, `Home`, and `End` to move through the log buffer.

## Notes

Catarnith is trading software. Paper validation and tiny-size live validation
come before real capital. Nothing in this repository is financial advice.

For the complete configuration reference, read [CONFIG.md](CONFIG.md).
