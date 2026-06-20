# Catarnith

Catarnith is a terminal-first Pump.fun trading app for Solana. It has one
interactive TUI, one autonomous scanner, and one live execution helper:

- `catarnith` - mode picker, settings editor, paper trade, live trade, and
  panic-sell UI.
- `bot` - autonomous paper/live scanner using the same config file.
- `live_execute` - one-shot live buy/sell helper used by the TUI panic-sell
  path and by operators who want a direct CLI.

The app is paper-first by default. Live trading only broadcasts when the config
is deliberately armed with `mode = "live"`, `enable_live_trading = true`, and
`require_manual_live_unlock = false`, plus the live wallet and RPC safeguards.

## Quickstart

Requires a recent stable Rust toolchain and a Helius API key.

```bash
cp config.example.toml config.toml
cp .env.example .env

cargo build --release --locked --bin catarnith
./target/release/catarnith
```

Or install the terminal binary onto your PATH:

```bash
cargo install --path . --locked --bin catarnith
catarnith
```

Use `--locked`: the project pins Solana and Pump client dependencies in
`Cargo.lock`.

## Modes

```text
[1] Auto Bot            autonomous scanner/trader loop
[2] Live Trade          single live trade flow
[3] Paper Trade         paper trading, no real orders
[S] Settings            wallet, keys, buy size, risk knobs
```

On a first run with no local config or `.env`, Catarnith opens Settings first.
Saving writes `config.toml` and `.env`, then returns to the mode picker.

## Configuration

Catarnith uses one local profile by default: `config.toml`.

- Copy `config.example.toml` to `config.toml`.
- Copy `.env.example` to `.env`.
- Keep both local files out of git; they are ignored.
- `.env` overrides matching TOML values at runtime.

Both paper and live mode load `config.toml` unless you explicitly pass
`--config <PATH>` or set `CTARNITH_LIVE_CONFIG`. The old
`MAYHEM_*` environment names are still accepted as read-only fallbacks, but new
local setup should use `CTARNITH_*`.

See [CONFIG.md](CONFIG.md) for the full config and live-arming reference.

## Common Commands

```bash
# Interactive terminal
cargo run --bin catarnith

# Skip picker and enter the trade screen
cargo run --bin catarnith -- scan

# Autonomous scanner using config.toml
cargo run --bin bot -- --config config.toml

# One-shot live execution helper
cargo run --bin live_execute -- \
  --config config.toml --side sell --mint <MINT>

# Test everything normally built for the TUI/live path
cargo test
```

## Safety Model

Paper mode never submits orders. It records simulated fills and PnL in local
journals only.

Live mode refuses to start unless:

- `mode = "live"`
- `enable_live_trading = true`
- `require_manual_live_unlock = false`
- a dedicated hot-wallet key is configured outside the repository
- the live wallet path does not look like a main/cold/treasury wallet
- a distinct paid fallback RPC is configured for broadcast paths that need it
- live risk caps are present and large enough for the configured buy size
- `[live].max_balance_lamports` caps the wallet balance

Selecting Live in the TUI does not bypass these gates.

## Project Shape

The core library is split into source-focused modules:

- `src/config.rs` - TOML and `.env` loading, env aliases, validation gates.
- `src/ingest.rs` and `src/decoder/` - Solana stream and transaction decoding.
- `src/discovery/`, `src/mayhem.rs`, `src/classifier/` - candidate and Mayhem
  evidence filters.
- `src/strategy.rs`, `src/risk.rs`, `src/position.rs` - decision, risk, and
  position state.
- `src/executor.rs` and `src/live.rs` - paper executor and live Pump.fun
  executor.
- `src/bin/catarnith/` - interactive TUI.

Runtime output goes to `journals/` by default.
