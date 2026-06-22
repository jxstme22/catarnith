//! Ratatui frame rendering for the Catarnith terminal.
//!
//! Layout (3 panes, fixed ratios):
//!
//! +------------------+------------------+
//! |  HEADER (status) |  HEADER (status) |
//! +------------------+------------------+
//! |   SCANNER        |   POSITION       |
//! |   (top-left)     |   (top-right)    |
//! +------------------+------------------+
//! |   TELEMETRY (full width)            |
//! +--------------------------------------+
//! | FOOTER (hotkey legend)              |
//! +--------------------------------------+
//!
//! Stream health was previously a 4th pane; it now lives as a
//! one-liner inside the header so the body of the screen is
//! dedicated to the things that actually drive decisions.

use crate::tui::ascii::{
    rocket_frame, sparkline, spinner_frame, streak_badge, CATARNITH_LOGO, PANIC_BANNER,
};
use crate::tui::{Palette, TerminalState, Theme};
use ratatui::{
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, List, ListItem, Paragraph, Wrap},
    Frame,
};

/// Render the full terminal frame. The caller owns the `Frame`.
pub fn render(frame: &mut Frame, state: &TerminalState) {
    let palette = neon_palette(state.theme);

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(8),    // body
            Constraint::Length(3), // telemetry strip
            Constraint::Length(3), // footer
        ])
        .split(frame.area());

    render_header(frame, outer[0], state, &palette);
    let body = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(45), Constraint::Percentage(55)])
        .split(outer[1]);
    render_scanner(frame, body[0], state, &palette);
    render_position(frame, body[1], state, &palette);
    render_telemetry(frame, outer[2], state, &palette);
    render_footer(frame, outer[3], state, &palette);

    if let Some(banner) = &state.banner {
        render_banner(frame, frame.area(), banner, &palette);
    }
}

fn render_header(frame: &mut Frame, area: Rect, state: &TerminalState, palette: &Palette) {
    let rocket = rocket_frame(state.tick);
    let spinner = spinner_frame(state.tick);
    let theme_label = match state.theme {
        Theme::Neon => "DARK",
        Theme::Amber => "AMBER",
        Theme::Mono => "MONO",
    };
    let readonly = matches!(
        crate::config::env_lookup("MAYHEM_TUI_READONLY").as_deref(),
        Some("1") | Some("true") | Some("yes")
    );
    let mut spans: Vec<Span> = vec![Span::styled(
        format!("CATARNITH  [{}]", theme_label),
        Style::default()
            .fg(palette.banner)
            .add_modifier(Modifier::BOLD),
    )];
    if readonly {
        spans.push(Span::styled(
            " [READONLY]",
            Style::default()
                .fg(palette.warn)
                .add_modifier(Modifier::BOLD),
        ));
    }
    spans.push(Span::raw("  "));
    spans.push(Span::styled(rocket, Style::default().fg(palette.accent)));
    spans.push(Span::raw("  "));
    spans.push(Span::styled(
        format!("{} SCANNING", spinner),
        Style::default().fg(palette.fg),
    ));
    let s = &state.stream;
    let ws = if s.connected { "WS ok" } else { "WS down" };
    let rpc_lag = format!("rpc {}ms", s.rpc_lag_ms);
    let ws_color = if s.ws_lag_ms < 250 {
        palette.success
    } else if s.ws_lag_ms < 1000 {
        palette.warn
    } else {
        palette.danger
    };
    spans.push(Span::raw("   "));
    spans.push(Span::styled(ws, Style::default().fg(ws_color)));
    spans.push(Span::raw("  "));
    spans.push(Span::styled(rpc_lag, Style::default().fg(palette.muted)));
    let header = Paragraph::new(Line::from(spans)).block(Block::default().borders(Borders::BOTTOM));
    frame.render_widget(header, area);
}

fn render_scanner(frame: &mut Frame, area: Rect, state: &TerminalState, palette: &Palette) {
    let items: Vec<ListItem> = state
        .candidates
        .iter()
        .take(20)
        .map(|c| {
            let state_label = match c.state {
                crate::tui::CandidateState::Watching => " watch",
                crate::tui::CandidateState::Locked => " lock ",
                crate::tui::CandidateState::Ignored => " skip ",
                crate::tui::CandidateState::Expired => " dead ",
            };
            let age_s = (c.age_ms.max(0) / 1000) as u64;
            let color = if c.creator_is_agent {
                palette.accent
            } else {
                palette.fg
            };
            ListItem::new(Line::from(vec![
                Span::styled(
                    format!("{:<6}", state_label),
                    Style::default().fg(palette.warn),
                ),
                Span::styled(
                    format!("{:<10}", c.symbol),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                ),
                Span::styled(
                    format!("{:<6}s ", age_s),
                    Style::default().fg(palette.muted),
                ),
                Span::styled(
                    format!("mcap {:>8.2} SOL", c.mcap_sol),
                    Style::default().fg(palette.fg),
                ),
                Span::styled(
                    format!("  {}", mint_short(&c.mint)),
                    Style::default().fg(palette.muted),
                ),
            ]))
        })
        .collect();
    let block = Block::default()
        .title(Span::styled(
            " SCANNER ",
            Style::default().fg(palette.accent),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(palette.accent));
    let list = List::new(items).block(block);
    frame.render_widget(list, area);
}

fn render_position(frame: &mut Frame, area: Rect, state: &TerminalState, palette: &Palette) {
    let block = Block::default()
        .title(Span::styled(
            " POSITION ",
            Style::default()
                .fg(palette.warn)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(palette.warn));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    let Some(pos) = &state.position else {
        let empty = Paragraph::new(Span::styled(
            "  no open position. waiting for fresh mint…",
            Style::default().fg(palette.muted),
        ))
        .wrap(Wrap { trim: true });
        frame.render_widget(empty, inner);
        return;
    };

    let spark = sparkline(&pos.mcap_history, inner.width.saturating_sub(2) as usize);
    let pnl_color = palette.pnl_color(pos.unrealized_bps);
    let line1 = Line::from(vec![
        Span::raw("  "),
        Span::styled(
            format!("{:<10}", pos.symbol),
            Style::default()
                .fg(palette.banner)
                .add_modifier(Modifier::BOLD),
        ),
        Span::styled(
            format!(" held {:>4}s", pos.held_ms / 1000),
            Style::default().fg(palette.muted),
        ),
        Span::raw("  "),
        Span::styled(
            format!("mcap {:>8.2} SOL", pos.mcap_sol),
            Style::default().fg(palette.fg),
        ),
    ]);
    let line2 = Line::from(vec![
        Span::raw("  "),
        Span::styled("PnL ", Style::default().fg(palette.muted)),
        Span::styled(
            format!("{:+8.4} SOL", pos.unrealized_sol),
            Style::default().fg(pnl_color).add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        Span::styled(
            format!("({:+6} bps)", pos.unrealized_bps),
            Style::default().fg(pnl_color),
        ),
    ]);
    let line3 = Line::from(vec![
        Span::raw("  "),
        Span::styled(spark, Style::default().fg(palette.accent)),
    ]);
    let panic_hint = if state.pending_panic {
        Line::from(vec![
            Span::raw("  "),
            Span::styled(
                "[ENTER] ARMED  hold Enter to panic-sell  [Esc] cancel",
                Style::default()
                    .fg(palette.danger)
                    .add_modifier(Modifier::BOLD),
            ),
        ])
    } else {
        // Readonly mode: the user is running this terminal
        // alongside an active `bot`, so the wallet lock is held
        // elsewhere. Replace the panic-sell hint with a hint to
        // use the shell command instead.
        let readonly = matches!(
            crate::config::env_lookup("MAYHEM_TUI_READONLY").as_deref(),
            Some("1") | Some("true") | Some("yes")
        );
        if readonly {
            Line::from(vec![
                Span::raw("  "),
                Span::styled(
                    "[ENTER] panic disabled (readonly) - use catarnith panic-sell <MINT>",
                    Style::default().fg(palette.muted),
                ),
            ])
        } else {
            Line::from(vec![
                Span::raw("  "),
                Span::styled("[ENTER] panic sell", Style::default().fg(palette.warn)),
            ])
        }
    };
    let last_panic = match (&pos.last_panic_signature, &pos.last_panic_status) {
        (Some(sig), Some(status)) => Line::from(vec![
            Span::raw("  "),
            Span::styled("last panic: ", Style::default().fg(palette.muted)),
            Span::styled(format!("{:?}  ", status), Style::default().fg(palette.warn)),
            Span::styled(sig_short(sig), Style::default().fg(palette.muted)),
        ]),
        _ => Line::from(Span::raw("")),
    };
    let body = Paragraph::new(vec![line1, line2, line3, panic_hint, last_panic])
        .wrap(Wrap { trim: false });
    frame.render_widget(body, inner);
}

fn render_telemetry(frame: &mut Frame, area: Rect, state: &TerminalState, palette: &Palette) {
    let t = &state.telemetry;
    let winrate = if t.entries_taken > 0 {
        (t.entries_won as f64 / t.entries_taken as f64) * 100.0
    } else {
        0.0
    };
    let streak_text = streak_badge(t.streak, t.best_streak);
    let lines = vec![
        Line::from(vec![
            Span::styled(" scanned ", Style::default().fg(palette.muted)),
            Span::styled(
                format!("{:>5}", t.scans_seen),
                Style::default().fg(palette.fg),
            ),
            Span::styled("  entered ", Style::default().fg(palette.muted)),
            Span::styled(
                format!("{:>4}", t.entries_taken),
                Style::default().fg(palette.fg),
            ),
            Span::styled("  win ", Style::default().fg(palette.muted)),
            Span::styled(
                format!("{:>5.1}%", winrate),
                Style::default().fg(palette.success),
            ),
        ]),
        Line::from(vec![
            Span::styled(" PnL ", Style::default().fg(palette.muted)),
            Span::styled(
                format!("{:+8.4} SOL", t.realized_sol),
                Style::default()
                    .fg(palette.pnl_color((t.realized_sol * 10_000.0) as i64))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  "),
            Span::styled(streak_text, Style::default().fg(palette.banner)),
        ]),
    ];
    let block = Block::default()
        .title(Span::styled(" TELEMETRY ", Style::default().fg(palette.fg)))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(palette.muted));
    let p = Paragraph::new(lines).block(block);
    frame.render_widget(p, area);
}

fn render_footer(frame: &mut Frame, area: Rect, _state: &TerminalState, palette: &Palette) {
    let readonly = matches!(
        crate::config::env_lookup("MAYHEM_TUI_READONLY").as_deref(),
        Some("1") | Some("true") | Some("yes")
    );
    let line = if readonly {
        Line::from(vec![
            Span::styled(" [ENTER] ", Style::default().fg(palette.muted)),
            Span::raw("panic disabled  "),
            Span::styled("[T] ", Style::default().fg(palette.warn)),
            Span::raw("theme  "),
            Span::styled("[ESC] ", Style::default().fg(palette.warn)),
            Span::raw("cancel  "),
            Span::styled("[Q] ", Style::default().fg(palette.warn)),
            Span::raw("quit  "),
            Span::styled("[READONLY]", Style::default().fg(palette.muted)),
        ])
    } else {
        Line::from(vec![
            Span::styled(" [ENTER] ", Style::default().fg(palette.danger)),
            Span::raw("panic sell  "),
            Span::styled("[T] ", Style::default().fg(palette.warn)),
            Span::raw("theme  "),
            Span::styled("[ESC] ", Style::default().fg(palette.warn)),
            Span::raw("cancel  "),
            Span::styled("[Q] ", Style::default().fg(palette.warn)),
            Span::raw("quit"),
        ])
    };
    let p = Paragraph::new(line).block(
        Block::default()
            .borders(Borders::TOP)
            .border_style(Style::default().fg(palette.muted)),
    );
    frame.render_widget(p, area);
}

fn render_banner(frame: &mut Frame, area: Rect, banner: &crate::tui::Banner, palette: &Palette) {
    let color = match banner.kind {
        crate::tui::BannerKind::Entry => palette.success,
        crate::tui::BannerKind::PanicArmed => palette.danger,
        crate::tui::BannerKind::PanicSubmitted => palette.warn,
        crate::tui::BannerKind::PanicFailed => palette.danger,
        crate::tui::BannerKind::Streak => palette.banner,
        crate::tui::BannerKind::Warning => palette.warn,
    };
    let text = if matches!(banner.kind, crate::tui::BannerKind::PanicArmed) {
        PANIC_BANNER.to_string()
    } else if matches!(banner.kind, crate::tui::BannerKind::Entry) {
        let mut out = String::new();
        for line in CATARNITH_LOGO {
            out.push_str(line);
            out.push('\n');
        }
        out.push_str("\n  >> ENTRY LOCKED  <<\n");
        out
    } else {
        banner.text.clone()
    };
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(color).add_modifier(Modifier::BOLD))
        .style(Style::default().bg(Color::Black));
    let popup_w = (area.width as f64 * 0.7) as u16;
    let popup_h = 10u16;
    let popup_x = area.x + (area.width.saturating_sub(popup_w)) / 2;
    let popup_y = area.y + (area.height.saturating_sub(popup_h)) / 2;
    let popup = Rect {
        x: popup_x,
        y: popup_y,
        width: popup_w,
        height: popup_h,
    };
    let p = Paragraph::new(text)
        .style(Style::default().fg(color).add_modifier(Modifier::BOLD))
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(p, popup);
}

fn mint_short(mint: &str) -> String {
    if mint.len() <= 10 {
        mint.to_string()
    } else {
        format!("{}…{}", &mint[..4], &mint[mint.len() - 4..])
    }
}

fn sig_short(sig: &str) -> String {
    if sig.len() <= 12 {
        sig.to_string()
    } else {
        format!("{}…{}", &sig[..6], &sig[sig.len() - 4..])
    }
}

fn neon_palette(theme: Theme) -> Palette {
    crate::tui::neon_palette(theme)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tui::ascii::sparkline;
    use std::collections::VecDeque;

    #[test]
    fn sparkline_handles_empty_history() {
        let empty: VecDeque<(i64, f64)> = VecDeque::new();
        assert_eq!(sparkline(&empty, 8), "        ");
    }

    #[test]
    fn sparkline_renders_in_requested_width() {
        let mut history: VecDeque<(i64, f64)> = VecDeque::new();
        for i in 0..10 {
            history.push_back((i, i as f64));
        }
        let s = sparkline(&history, 10);
        assert_eq!(s.chars().count(), 10);
    }

    #[test]
    fn sparkline_truncates_long_history() {
        let mut history: VecDeque<(i64, f64)> = VecDeque::new();
        for i in 0..100 {
            history.push_back((i, i as f64));
        }
        let s = sparkline(&history, 20);
        assert_eq!(s.chars().count(), 20);
    }

    #[test]
    fn pnl_palette_picks_extreme_colors() {
        let palette = neon_palette(Theme::Neon);
        let high = palette.pnl_color(8_000);
        let low = palette.pnl_color(-8_000);
        assert_ne!(high, palette.muted);
        assert_ne!(low, palette.muted);
    }

    #[test]
    fn render_does_not_panic_with_empty_state() {
        let backend = ratatui::backend::TestBackend::new(120, 30);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let state = TerminalState::default();
        terminal
            .draw(|frame| render(frame, &state))
            .expect("empty-state render should not panic");
    }

    #[test]
    fn render_does_not_panic_with_open_position_and_panic_armed() {
        let backend = ratatui::backend::TestBackend::new(120, 30);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = TerminalState::default();
        let mut pos = crate::tui::PositionRow::new(
            "Mint1111111111111111111111111111111111".to_string(),
            "CATARNITH".to_string(),
            13_025_001,
            1_700_000_000_000,
        );
        for i in 0..40 {
            pos.mcap_history.push_back((i, 25.0 + (i as f64) * 0.1));
        }
        pos.mcap_sol = 28.0;
        pos.unrealized_bps = 2_500;
        pos.unrealized_sol = 0.0042;
        pos.held_ms = 4_000;
        state.position = Some(pos);
        state.pending_panic = true;
        state.banner = Some(crate::tui::Banner {
            kind: crate::tui::BannerKind::PanicArmed,
            text: String::new(),
            shown_at_ms: 0,
            expires_in_ms: 500,
        });
        terminal
            .draw(|frame| render(frame, &state))
            .expect("panic-armed render should not panic");
    }
}
