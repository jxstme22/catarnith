//! Mayhem Trading Terminal — state, theme, and shared types.
//!
//! The terminal is a ratatui front-end over a `LivePumpExecutor`. It does
//! not own the executor's lifecycle; the binary in `bin/mayhem_terminal.rs`
//! constructs the executor and pushes events into a `TerminalState` via
//! `TerminalEvent`. The render task reads `TerminalState` and draws four
//! panes: Scanner, Position, Stream Health, Telemetry.
//!
//! Gamification is intentionally lightweight: ASCII rockets on entry
//! detection, color gradients on PnL, a streak counter, and a big
//! "PANIC SELL" banner with a confirmation window so a stray Enter key
//! doesn't dump the bag.

use crate::types::ExecutionStatus;
use ratatui::style::Color;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub mod ascii;
pub mod render;

/// One row in the scanner pane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateRow {
    pub mint: String,
    pub symbol: String,
    pub first_seen_ms: i64,
    pub age_ms: i64,
    pub mcap_sol: f64,
    pub creator_is_agent: bool,
    pub state: CandidateState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CandidateState {
    Watching,
    Locked,
    Ignored,
    Expired,
}

/// One row in the position pane.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionRow {
    pub mint: String,
    pub symbol: String,
    pub entry_lamports: u64,
    pub entry_signature: Option<String>,
    pub entry_ms: i64,
    pub token_amount_raw: u128,
    pub held_ms: i64,
    pub mcap_sol: f64,
    pub unrealized_sol: f64,
    pub unrealized_bps: i64,
    pub mcap_history: VecDeque<(i64, f64)>,
    pub last_panic_signature: Option<String>,
    pub last_panic_status: Option<ExecutionStatus>,
}

impl PositionRow {
    pub fn new(mint: String, symbol: String, entry_lamports: u64, entry_ms: i64) -> Self {
        Self {
            mint,
            symbol,
            entry_lamports,
            entry_signature: None,
            entry_ms,
            token_amount_raw: 0,
            held_ms: 0,
            mcap_sol: 0.0,
            unrealized_sol: 0.0,
            unrealized_bps: 0,
            mcap_history: VecDeque::with_capacity(64),
            last_panic_signature: None,
            last_panic_status: None,
        }
    }
}

/// Stream health metrics.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StreamHealth {
    pub ws_lag_ms: i64,
    pub rpc_lag_ms: i64,
    pub blockhash_age_ms: i64,
    pub last_event_ms: i64,
    pub connected: bool,
}

/// Aggregate telemetry (mostly counters).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Telemetry {
    pub scans_seen: u64,
    pub entries_taken: u64,
    pub entries_won: u64,
    pub entries_lost: u64,
    pub realized_sol: f64,
    pub streak: i32,
    pub best_streak: i32,
    pub last_fill_ms: i64,
}

/// Top-level state shared between the render task and the event pump.
#[derive(Debug, Clone, Default)]
pub struct TerminalState {
    pub now_ms: i64,
    pub candidates: Vec<CandidateRow>,
    pub position: Option<PositionRow>,
    pub stream: StreamHealth,
    pub telemetry: Telemetry,
    /// Set by the input thread when the user holds Enter. The sell task
    /// reads it and clears it.
    pub pending_panic: bool,
    /// Set by the input thread when the user wants to cancel a pending
    /// panic within the confirmation window.
    pub cancel_panic: bool,
    /// Last banner shown (entry, panic-armed, panic-submitted, etc.).
    pub banner: Option<Banner>,
    /// Frame counter for animations.
    pub tick: u64,
    /// Theme name (matches a `Color` palette in `render`).
    pub theme: Theme,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum Theme {
    #[default]
    Neon,
    Amber,
    Mono,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Banner {
    pub kind: BannerKind,
    pub text: String,
    /// `shown_at` is a wall-clock anchor used by the render task to
    /// auto-dismiss the banner. We use millis-since-epoch so the
    /// struct stays `Serialize`/`Deserialize` (in case the terminal
    /// ever needs to replay the last banner across restarts).
    pub shown_at_ms: i64,
    pub expires_in_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BannerKind {
    Entry,
    PanicArmed,
    PanicSubmitted,
    PanicFailed,
    Streak,
    Warning,
}

/// Events pushed from the executor / event-pump task into the render task.
#[derive(Debug, Clone)]
pub enum TerminalEvent {
    Candidate(CandidateRow),
    CandidateStateChange {
        mint: String,
        state: CandidateState,
    },
    PositionOpened(PositionRow),
    PositionMcap {
        mint: String,
        mcap_sol: f64,
        age_ms: i64,
        ts_ms: i64,
    },
    PositionClosed {
        mint: String,
        realized_sol: f64,
        won: bool,
    },
    PanicSubmitted {
        mint: String,
        signature: String,
        status: ExecutionStatus,
    },
    StreamHealth(StreamHealth),
    Telemetry(Telemetry),
    Banner(Banner),
    /// Explicit "dismiss the active banner" — emitted when the user
    /// presses Esc or when a panic-armed confirmation window expires
    /// without firing.
    BannerCleared,
    Tick,
    Quit,
}

/// Color palette for the dark theme.
pub fn neon_palette(theme: Theme) -> Palette {
    match theme {
        // Default: clean dark, no neon. True black background, light
        // gray foreground, accent in a soft cyan. This is what the
        // TUI ships as "Neon" but is really a neutral dark theme.
        Theme::Neon => Palette {
            bg: Color::Black,
            fg: Color::Rgb(220, 220, 220),
            accent: Color::Rgb(120, 170, 200),
            warn: Color::Rgb(220, 180, 80),
            danger: Color::Rgb(220, 90, 90),
            success: Color::Rgb(120, 200, 130),
            muted: Color::Rgb(110, 110, 110),
            banner: Color::Rgb(220, 220, 220),
        },
        // Amber-on-black CRT vibe.
        Theme::Amber => Palette {
            bg: Color::Black,
            fg: Color::Rgb(255, 176, 0),
            accent: Color::Rgb(255, 200, 80),
            warn: Color::Rgb(255, 120, 0),
            danger: Color::Rgb(255, 60, 0),
            success: Color::Rgb(180, 255, 80),
            muted: Color::Rgb(120, 80, 0),
            banner: Color::Rgb(255, 220, 120),
        },
        // Pure monochrome.
        Theme::Mono => Palette {
            bg: Color::Black,
            fg: Color::White,
            accent: Color::Gray,
            warn: Color::Gray,
            danger: Color::White,
            success: Color::White,
            muted: Color::DarkGray,
            banner: Color::White,
        },
    }
}

#[derive(Debug, Clone, Copy)]
pub struct Palette {
    pub bg: Color,
    pub fg: Color,
    pub accent: Color,
    pub warn: Color,
    pub danger: Color,
    pub success: Color,
    pub muted: Color,
    pub banner: Color,
}

impl Palette {
    /// Color for a PnL value in bps. Negative = red, positive = green,
    /// with a yellow band around zero.
    pub fn pnl_color(&self, bps: i64) -> Color {
        if bps > 5_000 {
            self.success
        } else if bps > 500 {
            Color::Rgb(120, 220, 120)
        } else if bps > 0 {
            self.warn
        } else if bps > -500 {
            self.muted
        } else if bps > -5_000 {
            Color::Rgb(220, 120, 120)
        } else {
            self.danger
        }
    }
}
