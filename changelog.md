# Changelog

All notable changes since the last committed version are documented here.

## Unreleased - since `b489c9f` (`fixed live`)

### Added

- Added market selection as a first-class entry gate with `mayhem_only`,
  `non_mayhem_only`, and `all_pumpfun`.
- Added strict `non_mayhem_only` behavior:
  - requires fresh Pump.fun create/create-v2 style entries,
  - rejects direct and indirect Mayhem signals,
  - requires the single-trade scanner to see curve `is_mayhem_mode = false`,
  - skips candidates when the curve flag is unavailable instead of guessing.
- Added copy-trade support in Auto Bot:
  - source wallet subscription and signer/source attribution,
  - fixed, mirror, and scaled sizing,
  - `first_only` and `accumulate` buy policies,
  - copied buy max size and source-buy minimum filters,
  - follow-sells behavior,
  - copy-specific max-hold, take-profit, and stop-loss settings,
  - normal risk engine, pending-order, executor, journal, and position-manager integration.
- Added copy-trade market gating so copied buys respect `market`.
- Added Auto Bot setup controls for market, copy trade, risk caps, stream timing,
  full-transaction fetch, curve exit quotes, confirmation polling, fallback reads,
  and bot keep-alive.
- Added scrollable and cleaner TUI logs, including a larger log overlay and
  summarized stale-stream/stale-create messages.
- Added SOL-facing config and env aliases for user money settings:
  - `base_buy_sol`
  - `max_total_sol_per_mint`
  - `max_total_open_sol`
  - `max_daily_loss_sol`
  - `copy_trade_max_buy_sol`
  - `copy_trade_min_source_buy_sol`
  - `min_observed_buy_sol`
  - `max_observed_buy_sol`
  - `[live].max_balance_sol`
  - `[live].jito_tip_sol`
- Added SOL env aliases for live envelope caps:
  - `CTARNITH_LIVE_MAX_ENTRY_SOL`
  - `CTARNITH_LIVE_MAX_TOTAL_OPEN_SOL_CEILING`
  - `CTARNITH_LIVE_MAX_DAILY_LOSS_SOL_CEILING`
- Added live executor support for both SPL Token and Token-2022 Pump.fun mints.
- Added live sell reconciliation against wallet token balance across RPCs.
- Added `LiveReconciled` handling for cases where inventory is already gone
  even if the sell report was pending, failed, or ambiguous.

### Changed

- Replaced the old `pair_scope`-style market concept with `market`, while still
  reading legacy `pair_scope` values for compatibility.
- Changed Settings and Auto Bot Setup to save user-facing money values in SOL
  instead of raw lamports.
- Changed `.env` saving/migration to prefer `CTARNITH_*` SOL keys while still
  reading legacy `MAYHEM_*` and lamport keys.
- Changed live fallback RPC from a hard requirement to an optional reliability
  path, while still rejecting public/same-provider fallback mistakes when set.
- Changed live risk envelope ceiling comparisons to advisory warnings for sizing
  caps, so stale shell env vars do not silently block an explicit config choice.
- Changed Mayhem curve-flag enforcement so it only applies to `mayhem_only`.
- Changed live execution errors and user-facing validation messages to use SOL
  wording for trade sizes and caps.
- Changed runtime output defaults to use `journals/bot/catarnith.sqlite`.
- Changed docs and examples to describe one default local profile,
  `config.toml`, for paper, live, and Auto Bot.

### Fixed

- Fixed `non_mayhem_only` allowing Mayhem entries through direct scanner,
  strategy, or copy-trade paths.
- Fixed direct scanner treating unknown Mayhem curve flags as non-Mayhem.
- Fixed copy trade bypassing market selection.
- Fixed indirect Mayhem candidates passing non-Mayhem mode when indirect Mayhem
  candidates were disabled.
- Fixed live execution refusing non-Mayhem/all-Pump.fun entries only because
  they were not Mayhem/Token-2022.
- Fixed paper mode surfacing paper quote failures as confusing executor errors
  instead of continuing candidate screening.
- Fixed live sell status drift where the UI could say "still holding" after the
  wallet inventory was already zero.
- Fixed panic-sell submitted paths reporting fake zero proceeds on pending
  confirmation.
- Fixed token-balance checks that only used one RPC path and missed reconciled
  inventory state.
- Fixed lifecycle crashes after failed force-sell by restoring the holding state
  and requiring the operator to retry/verify instead of returning to picker.
- Fixed noisy repeated stale event logs by rate-limiting and summarizing them.
- Fixed empty Mayhem program strings matching every log line.
- Fixed new-user setup friction from hidden or stale env/config overrides by
  exposing more settings in the TUI and writing matching `.env` values.

### Documentation

- Rewrote `README.md` for clearer project overview, setup, TUI controls,
  Auto Bot, copy trade, market selection, safety gates, architecture, commands,
  runtime journals, verification, and troubleshooting.
- Updated `CONFIG.md` with current config precedence, SOL config keys,
  market rules, copy-trade flow, live table, arming checklist, and credit
  controls.
- Updated `config.example.toml` to use current names, SOL values, copy-trade
  defaults, strict market comments, and `catarnith.sqlite`.
- Updated `.env.example` to use `CTARNITH_*`, SOL env values, copy-trade env
  settings, optional fallback RPC wording, and Jito tip in SOL.
- Added comprehensive landing-page docs in English and Indonesian.

### Tests

- Added/updated config-profile tests for:
  - current `market` values and legacy `pair_scope`,
  - SOL-sized config fields,
  - SOL-sized live envelope env caps,
  - blank optional wallet normalization,
  - live validation wording.
- Added/updated strategy tests for:
  - non-Mayhem fresh-create requirements,
  - direct Mayhem rejection in non-Mayhem mode,
  - indirect Mayhem rejection in non-Mayhem mode.
- Added copy-trade tests for:
  - fixed/mirror/scaled sizing and caps,
  - source sell follow behavior,
  - first-only and accumulate policies,
  - signer/source attribution,
  - market filter enforcement.
- Added TUI/state tests for:
  - mode picker rendering,
  - settings rendering,
  - log cleanup and scroll-buffer behavior,
  - market-scope decision rules,
  - fatal bot restart diagnostics.

### Verification Performed During This Change Set

- `cargo test --all-features`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo test --test config_profiles`
- `git diff --check`
- Local reinstall with `cargo install --path . --locked --force`
