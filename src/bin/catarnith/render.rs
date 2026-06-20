//! `catarnith` TUI rendering.
//!
//! Handheld console layout:
//!   * true-black background
//!   * bright ASCII art wallpaper visible through transparent panels
//!   * trade screen: MCAP (4) / Position (1) / Logs (1)
//!   * all panel text centered both vertically and horizontally
//!   * fixed requested terminal size: 93x54

use super::{BotSettingsField, Phase, ScanState, SettingsField, Theme};
use catarnith::tui::ascii::{sparkline, spinner_frame};
use ratatui::{
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph, Wrap},
    Frame,
};

/// Solid black background used inside panels so text is readable
/// while the bright ASCII wallpaper remains visible outside them.
const PANEL_BG: Color = Color::Black;
/// Dim foreground color for the wallpaper glyphs that show through inside panels.
const PANEL_WALLPAPER_FG: Color = Color::Rgb(50, 50, 50);

/// Render one frame.
pub fn render(frame: &mut Frame, state: &ScanState) {
    let palette = palette_for(state.theme);
    let area = frame.area();

    frame.render_widget(
        Block::default().style(Style::default().bg(Color::Black)),
        area,
    );

    let outer = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // header
            Constraint::Min(12),   // play area
            Constraint::Length(3), // footer
        ])
        .split(area);

    render_header(frame, outer[0], state, &palette);
    render_background(frame, outer[1], &palette);

    match state.phase {
        Phase::ModePicker => render_mode_picker(frame, outer[1], state, &palette),
        Phase::Welcome => render_welcome(frame, outer[1], state, &palette),
        Phase::Settings => render_settings(frame, outer[1], state, &palette),
        Phase::BotSettings => render_bot_settings(frame, outer[1], state, &palette),
        Phase::Scanning
        | Phase::Evaluating
        | Phase::Holding
        | Phase::Selling
        | Phase::TradeResult => {
            render_trade_screen(frame, outer[1], state, &palette);
        }
        Phase::BotRunning | Phase::BotStopped => {
            render_bot_screen(frame, outer[1], state, &palette);
        }
    }

    render_footer(frame, outer[2], state, &palette);

    if state.show_logs {
        render_log_overlay(frame, area, state, &palette);
    }
}

fn render_background(frame: &mut Frame, area: Rect, palette: &Palette) {
    let art = super::ascii_bg::lines();
    let art_h = art.len();
    let art_w = super::ascii_bg::width();

    let (y_off, visible_h) = if art_h > area.height as usize {
        let off = (art_h - area.height as usize) / 2;
        (off, area.height as usize)
    } else {
        (0, art_h)
    };
    let top_pad = if art_h < area.height as usize {
        (area.height as usize - art_h) / 2
    } else {
        0
    };

    let (x_off, visible_w) = if art_w > area.width as usize {
        let off = (art_w - area.width as usize) / 2;
        (off, area.width as usize)
    } else {
        (0, art_w)
    };

    let style = Style::default().fg(palette.bg_art).bg(Color::Black);
    for (i, raw) in art.iter().skip(y_off).take(visible_h).enumerate() {
        let visible: String = raw.chars().skip(x_off).take(visible_w).collect();
        let line_area = Rect {
            x: area.x,
            y: area.y + top_pad as u16 + i as u16,
            width: area.width,
            height: 1,
        };
        let p =
            Paragraph::new(Line::from(Span::styled(visible, style))).alignment(Alignment::Center);
        frame.render_widget(p, line_area);
    }
}

/// Render a dim, clipped copy of the ASCII wallpaper inside `target`
/// so that it visually continues the outer wallpaper but at a lower
/// intensity. Empty cells receive `bg` so they blend with the panel
/// background.
fn render_wallpaper_clip(frame: &mut Frame, target: Rect, play_area: Rect, fg: Color, bg: Color) {
    let art = super::ascii_bg::lines();
    let art_h = art.len();
    let art_w = super::ascii_bg::width();

    let (y_off, visible_h, top_pad) = if art_h > play_area.height as usize {
        (
            (art_h - play_area.height as usize) / 2,
            play_area.height as usize,
            0,
        )
    } else {
        (0, art_h, (play_area.height as usize - art_h) / 2)
    };
    let global_art_y = play_area.y as usize + top_pad;

    let (x_off, visible_w, left_pad) = if art_w > play_area.width as usize {
        (
            (art_w - play_area.width as usize) / 2,
            play_area.width as usize,
            0,
        )
    } else {
        (0, art_w, (play_area.width as usize - art_w) / 2)
    };
    let global_art_x = play_area.x as usize + left_pad;

    let mut symbol_buf = [0u8; 4];
    let buf = frame.buffer_mut();
    for ty in target.y..target.y.saturating_add(target.height) {
        for tx in target.x..target.x.saturating_add(target.width) {
            let row = ty as isize - global_art_y as isize;
            let col = tx as isize - global_art_x as isize;
            let symbol =
                if row >= 0 && row < visible_h as isize && col >= 0 && col < visible_w as isize {
                    let r = (y_off as isize + row) as usize;
                    let c = (x_off as isize + col) as usize;
                    if let Some(ch) = art.get(r).and_then(|line| line.chars().nth(c)) {
                        ch.encode_utf8(&mut symbol_buf)
                    } else {
                        " "
                    }
                } else {
                    " "
                };
            if let Some(cell) = buf.cell_mut((tx, ty)) {
                cell.set_symbol(symbol);
                cell.set_fg(fg);
                cell.set_bg(bg);
            }
        }
    }
}

/// Render the same ASCII wallpaper that `render_background` draws,
/// Transparent game-console panel. No background fill so the ASCII
/// wallpaper remains visible behind the text.
fn centered_box(
    frame: &mut Frame,
    outer: Rect,
    width: u16,
    height: u16,
    title: &str,
    border_color: Color,
    _dim_color: Color,
) -> Rect {
    let width = width.min(outer.width.saturating_sub(2)).max(20);
    let height = height.min(outer.height.saturating_sub(2)).max(6);
    let x = outer.x + (outer.width.saturating_sub(width)) / 2;
    let y = outer.y + (outer.height.saturating_sub(height)) / 2;
    let area = Rect {
        x,
        y,
        width,
        height,
    };
    let block = Block::default()
        .title(Span::styled(
            title,
            Style::default()
                .fg(border_color)
                .add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(PANEL_BG));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    // Fill the panel interior with a dim, clipped copy of the ASCII wallpaper
    // so it matches the outside wallpaper but stays readable behind text.
    render_wallpaper_clip(frame, inner, outer, PANEL_WALLPAPER_FG, PANEL_BG);
    inner
}

/// Render a paragraph inside `area` vertically and horizontally centered.
/// No background fill so the bright wallpaper shows through empty cells.
fn render_centered(frame: &mut Frame, area: Rect, lines: Vec<Line>, _fg: Color, bold: bool) {
    // Match the mode picker: only set a panel background on the paragraph.
    // Do NOT set a global foreground, so empty cells keep the dim wallpaper
    // glyphs instead of being recolored bright white.
    let mut style = Style::default().bg(PANEL_BG);
    if bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    let content_h = lines.len() as u16;
    let top_pad = area.height.saturating_sub(content_h) / 2;
    let para_area = Rect {
        x: area.x,
        y: area.y + top_pad,
        width: area.width,
        height: area.height.saturating_sub(top_pad),
    };
    let p = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .style(style);
    frame.render_widget(p, para_area);
}

fn render_header(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let spinner = spinner_frame(state.tick);
    let phase_label = state.phase.label();
    let blink = if state.tick % 2 == 0 { "●" } else { "○" };

    // Vertically center the content within the 3-row header.
    let v = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
        ])
        .split(area);
    let row = v[1];

    let parts = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(34),
            Constraint::Percentage(33),
        ])
        .split(row);

    let left = Paragraph::new(Line::from(vec![
        Span::styled(blink, Style::default().fg(palette.accent)),
        Span::raw("  "),
        Span::styled(phase_label, Style::default().fg(palette.fg)),
    ]))
    .alignment(Alignment::Left);

    let center = Paragraph::new(Line::from(Span::styled(
        "CATARNITH",
        Style::default()
            .fg(palette.banner)
            .add_modifier(Modifier::BOLD),
    )))
    .alignment(Alignment::Center);

    let right = Paragraph::new(Line::from(vec![
        Span::styled(
            format!("[{}]", state.theme.label()),
            Style::default().fg(palette.muted),
        ),
        Span::raw(" "),
        Span::styled(spinner, Style::default().fg(palette.accent)),
    ]))
    .alignment(Alignment::Right);

    let bottom = Block::default()
        .borders(Borders::BOTTOM)
        .border_style(Style::default().fg(palette.muted))
        .style(Style::default().bg(Color::Black));

    frame.render_widget(bottom, area);
    frame.render_widget(left, parts[0]);
    frame.render_widget(center, parts[1]);
    frame.render_widget(right, parts[2]);
}

fn render_footer(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let controls = match state.phase {
        Phase::ModePicker => "[1] Auto Bot  [2] Live  [3] Paper  [S] Settings",
        Phase::Welcome => "[any] start  [T] theme  [Q] quit  [L] logs",
        Phase::Holding if state.confirm_exit => {
            "[ESC] confirm leave (position stays open)  [any] stay"
        }
        Phase::Holding => "[ENTER] SELL  [ESC] menu  [T] theme  [Q] quit  [L] logs",
        Phase::Selling => "[ESC] menu  [Q] quit  [L] logs",
        Phase::TradeResult => "[ENTER] trade again  [ESC] menu  [T] theme  [Q] quit  [L] logs",
        Phase::Settings => "[Tab/↑↓] field  [←→] change  [Enter] save  [Esc] back  [Ctrl-C] quit",
        Phase::BotSettings => {
            "[Tab/↑↓] field  [←→] change  [Enter] save+start  [Esc] back  [Ctrl-C] quit"
        }
        Phase::BotRunning => "[ESC] stop bot  [Q] quit  [L] logs",
        Phase::BotStopped => "[ESC] back to menu",
        _ => "[ESC] menu  [T] theme  [Q] quit  [L] logs",
    };

    let wallet = if state.wallet_label.is_empty() {
        ""
    } else {
        &state.wallet_label
    };

    let parts = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(60), Constraint::Percentage(40)])
        .split(area);

    let left = Paragraph::new(Line::from(Span::styled(
        format!("  {controls}"),
        Style::default().fg(palette.warn),
    )))
    .alignment(Alignment::Left);

    let right = Paragraph::new(Line::from(Span::styled(
        format!("{wallet}  "),
        Style::default().fg(palette.muted),
    )))
    .alignment(Alignment::Right);

    let top = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(palette.muted))
        .style(Style::default().bg(Color::Black));

    frame.render_widget(top, area);
    frame.render_widget(left, parts[0]);
    frame.render_widget(right, parts[1]);
}

fn render_mode_picker(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let inner = centered_box(
        frame,
        area,
        58,
        18,
        " PICK A MODE ",
        palette.panel,
        palette.bg_art_dim,
    );

    let config_name =
        catarnith::config::env_lookup("MAYHEM_LIVE_CONFIG").unwrap_or_else(|| "config.toml".into());

    let mut lines: Vec<Line> = vec![
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::styled("▶ ", Style::default().fg(palette.accent)),
            Span::styled(
                "CATARNITH",
                Style::default()
                    .fg(palette.banner)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(" ◀", Style::default().fg(palette.accent)),
        ]),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "[1]  Auto Bot",
            Style::default()
                .fg(palette.success)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "     autonomous bot trading",
            Style::default().fg(palette.muted),
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "[2]  Live Trade",
            Style::default()
                .fg(palette.danger)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "     live trade (real SOL, real risk)",
            Style::default().fg(palette.muted),
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "[3]  Paper Trade",
            Style::default()
                .fg(palette.warn)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "     paper trade (simulated)",
            Style::default().fg(palette.muted),
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "[S]  Settings",
            Style::default()
                .fg(palette.accent)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::styled(
            "     wallet, keys, buy size, risk",
            Style::default().fg(palette.muted),
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            format!("cfg: {}", config_name),
            Style::default().fg(palette.muted),
        )),
    ];

    if let Some(err) = &state.last_error {
        lines.push(Line::from(Span::raw("")));
        let truncated = if err.len() > 60 {
            format!("{}…", &err[..60])
        } else {
            err.clone()
        };
        lines.push(Line::from(Span::styled(
            format!("⚠ {truncated}"),
            Style::default()
                .fg(palette.danger)
                .add_modifier(Modifier::BOLD),
        )));
    }

    lines.push(Line::from(Span::raw("")));
    lines.push(Line::from(Span::styled(
        "[S] settings  [T] theme  [Q] quit",
        Style::default().fg(palette.warn),
    )));

    let content_h = lines.len() as u16;
    let top_pad = inner.height.saturating_sub(content_h) / 2;
    let para_area = Rect {
        x: inner.x,
        y: inner.y + top_pad,
        width: inner.width,
        height: inner.height.saturating_sub(top_pad),
    };

    let para = Paragraph::new(lines)
        .alignment(Alignment::Center)
        .style(Style::default().bg(PANEL_BG));
    frame.render_widget(para, para_area);
}

fn render_welcome(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    // Minimal splash: wallpaper, CATARNITH, press any key.
    let inner = centered_box(frame, area, 50, 10, "", palette.panel, palette.bg_art_dim);

    let lines: Vec<Line> = vec![
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "CATARNITH",
            Style::default()
                .fg(palette.banner)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            if state.last_trade.is_some() {
                "press any key to trade again"
            } else {
                "press any key to start"
            },
            Style::default()
                .fg(palette.warn)
                .add_modifier(Modifier::BOLD),
        )),
    ];

    render_centered(frame, inner, lines, palette.fg, false);
}

fn render_settings(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let inner = centered_box(
        frame,
        area,
        64,
        24,
        " SETTINGS ",
        palette.panel,
        palette.bg_art_dim,
    );

    let st = &state.settings;
    let active = st.active_field;

    // Color helper: accent when the row is focused, muted otherwise.
    let row_color = |field: SettingsField| {
        if active == field {
            palette.accent
        } else {
            palette.muted
        }
    };
    let marker = |field: SettingsField, label: &str| -> String {
        if active == field {
            format!("> {label}")
        } else {
            format!("  {label}")
        }
    };
    // Selector fields (Theme/Mode) wrap their value in chevrons when
    // focused so it reads as a left/right toggle.
    let selector = |field: SettingsField, value: &str| -> String {
        if active == field {
            format!("‹ {value} ›")
        } else {
            value.to_string()
        }
    };

    // Mask secrets the same way the wallet field is masked.
    let masked = |s: &str| -> String {
        if s.is_empty() {
            "(empty — keeps current)".to_string()
        } else {
            "*".repeat(s.len())
        }
    };
    let plain = |s: &str| -> String {
        if s.is_empty() {
            "(empty — keeps current)".to_string()
        } else {
            s.to_string()
        }
    };

    let wallet_display = if st.wallet_b58.is_empty() {
        "(leave empty to keep existing key)".to_string()
    } else {
        "*".repeat(st.wallet_b58.len())
    };

    let label_style = |field: SettingsField| {
        Style::default()
            .fg(row_color(field))
            .add_modifier(Modifier::BOLD)
    };

    let mut lines: Vec<Line> = vec![
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::Wallet, "WALLET KEY   "),
                label_style(SettingsField::Wallet),
            ),
            Span::styled(wallet_display, Style::default().fg(palette.fg)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::BuySize, "BUY SIZE     "),
                label_style(SettingsField::BuySize),
            ),
            Span::styled(st.buy_size_sol.clone(), Style::default().fg(palette.fg)),
            Span::styled(" SOL", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::HeliusKey, "HELIUS KEY   "),
                label_style(SettingsField::HeliusKey),
            ),
            Span::styled(masked(&st.helius_key), Style::default().fg(palette.fg)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::FallbackRpc, "FALLBACK RPC "),
                label_style(SettingsField::FallbackRpc),
            ),
            Span::styled(plain(&st.fallback_rpc), Style::default().fg(palette.fg)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::JupiterKey, "JUPITER KEY  "),
                label_style(SettingsField::JupiterKey),
            ),
            Span::styled(masked(&st.jupiter_key), Style::default().fg(palette.fg)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::SlippageBps, "SLIPPAGE     "),
                label_style(SettingsField::SlippageBps),
            ),
            Span::styled(st.slippage_bps.clone(), Style::default().fg(palette.fg)),
            Span::styled(" bps", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::MaxHoldSecs, "MAX HOLD     "),
                label_style(SettingsField::MaxHoldSecs),
            ),
            Span::styled(st.max_hold_secs.clone(), Style::default().fg(palette.fg)),
            Span::styled(" sec", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::Theme, "THEME        "),
                label_style(SettingsField::Theme),
            ),
            Span::styled(
                selector(SettingsField::Theme, st.theme.label()),
                Style::default().fg(palette.fg),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::Mode, "MODE         "),
                label_style(SettingsField::Mode),
            ),
            Span::styled(
                selector(
                    SettingsField::Mode,
                    match st.mode {
                        catarnith::types::Mode::Paper => "Paper",
                        catarnith::types::Mode::Live => "Live",
                    },
                ),
                Style::default().fg(palette.fg),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                marker(SettingsField::PairScope, "PAIR SCOPE   "),
                label_style(SettingsField::PairScope),
            ),
            Span::styled(
                selector(SettingsField::PairScope, st.pair_scope.label()),
                Style::default().fg(palette.fg),
            ),
        ]),
    ];

    // Advanced risk section: a toggle row plus four risk fields that
    // only render when expanded. ←/→ on the toggle flips show_advanced.
    let toggle_label = if st.show_advanced {
        "▾ ADVANCED RISK"
    } else {
        "▸ ADVANCED RISK"
    };
    lines.push(Line::from(Span::raw("")));
    lines.push(Line::from(vec![
        Span::styled(
            marker(SettingsField::AdvancedToggle, toggle_label),
            label_style(SettingsField::AdvancedToggle),
        ),
        Span::styled(
            selector(
                SettingsField::AdvancedToggle,
                if st.show_advanced {
                    "expanded"
                } else {
                    "collapsed"
                },
            ),
            Style::default().fg(palette.muted),
        ),
    ]));
    if st.show_advanced {
        lines.push(Line::from(vec![
            Span::styled(
                marker(SettingsField::TakeProfitBps, "  TAKE PROFIT "),
                label_style(SettingsField::TakeProfitBps),
            ),
            Span::styled(st.take_profit_bps.clone(), Style::default().fg(palette.fg)),
            Span::styled(" bps", Style::default().fg(palette.muted)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(SettingsField::StopLossBps, "  STOP LOSS   "),
                label_style(SettingsField::StopLossBps),
            ),
            Span::styled(st.stop_loss_bps.clone(), Style::default().fg(palette.fg)),
            Span::styled(" bps", Style::default().fg(palette.muted)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(SettingsField::MaxOpenPositions, "  MAX OPEN    "),
                label_style(SettingsField::MaxOpenPositions),
            ),
            Span::styled(
                st.max_open_positions.clone(),
                Style::default().fg(palette.fg),
            ),
            Span::styled(" positions", Style::default().fg(palette.muted)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(SettingsField::DailyLossSol, "  DAILY LOSS  "),
                label_style(SettingsField::DailyLossSol),
            ),
            Span::styled(st.daily_loss_sol.clone(), Style::default().fg(palette.fg)),
            Span::styled(" SOL", Style::default().fg(palette.muted)),
        ]));
    }

    lines.push(Line::from(Span::raw("")));
    lines.push(Line::from(Span::styled(
        "secrets masked · ←/→ changes selectors & expands advanced",
        Style::default().fg(palette.muted),
    )));

    if st.saved {
        lines.push(Line::from(Span::styled(
            "✓ saved",
            Style::default()
                .fg(palette.success)
                .add_modifier(Modifier::BOLD),
        )));
    } else if let Some(err) = st.error.as_ref() {
        lines.push(Line::from(Span::styled(
            format!("✗ {err}"),
            Style::default()
                .fg(palette.danger)
                .add_modifier(Modifier::BOLD),
        )));
    }

    render_centered(frame, inner, lines, palette.fg, false);
}

fn render_bot_settings(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let inner = centered_box(
        frame,
        area,
        74,
        26,
        " AUTO BOT SETUP ",
        palette.panel,
        palette.bg_art_dim,
    );

    let st = &state.bot_settings;
    let active = st.active_field;
    let row_color = |field: BotSettingsField| {
        if active == field {
            palette.accent
        } else {
            palette.muted
        }
    };
    let marker = |field: BotSettingsField, label: &str| -> String {
        if active == field {
            format!("> {label}")
        } else {
            format!("  {label}")
        }
    };
    let selector = |field: BotSettingsField, value: &str| -> String {
        if active == field {
            format!("‹ {value} ›")
        } else {
            value.to_string()
        }
    };
    let bool_label = |value: bool| if value { "on" } else { "off" };
    let label_style = |field: BotSettingsField| {
        Style::default()
            .fg(row_color(field))
            .add_modifier(Modifier::BOLD)
    };

    let config_display = truncate_line(&st.config_path, inner.width.saturating_sub(10) as usize);
    let mut lines: Vec<Line> = vec![
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::styled("  PROFILE     ", Style::default().fg(palette.muted)),
            Span::styled(config_display, Style::default().fg(palette.fg)),
        ]),
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::Mode, "MODE        "),
                label_style(BotSettingsField::Mode),
            ),
            Span::styled(
                selector(
                    BotSettingsField::Mode,
                    match st.mode {
                        catarnith::types::Mode::Paper => "Paper",
                        catarnith::types::Mode::Live => "Live",
                    },
                ),
                Style::default().fg(palette.fg),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::PairScope, "PAIR SCOPE  "),
                label_style(BotSettingsField::PairScope),
            ),
            Span::styled(
                selector(BotSettingsField::PairScope, st.pair_scope.label()),
                Style::default().fg(palette.fg),
            ),
        ]),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::BuySize, "BUY SIZE    "),
                label_style(BotSettingsField::BuySize),
            ),
            Span::styled(st.buy_size_sol.clone(), Style::default().fg(palette.fg)),
            Span::styled(" SOL", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::SlippageBps, "SLIPPAGE    "),
                label_style(BotSettingsField::SlippageBps),
            ),
            Span::styled(st.slippage_bps.clone(), Style::default().fg(palette.fg)),
            Span::styled(" bps", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::MaxHoldSecs, "MAX HOLD    "),
                label_style(BotSettingsField::MaxHoldSecs),
            ),
            Span::styled(st.max_hold_secs.clone(), Style::default().fg(palette.fg)),
            Span::styled(" sec", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::StreamAgeMs, "STREAM AGE  "),
                label_style(BotSettingsField::StreamAgeMs),
            ),
            Span::styled(
                st.max_stream_event_age_ms.clone(),
                Style::default().fg(palette.fg),
            ),
            Span::styled(" ms", Style::default().fg(palette.muted)),
        ]),
        Line::from(vec![
            Span::styled(
                marker(BotSettingsField::EntryDeadlineMs, "BUY DEADLINE"),
                label_style(BotSettingsField::EntryDeadlineMs),
            ),
            Span::raw(" "),
            Span::styled(
                st.entry_deadline_ms.clone(),
                Style::default().fg(palette.fg),
            ),
            Span::styled(" ms", Style::default().fg(palette.muted)),
        ]),
    ];

    let toggle_label = if st.show_advanced {
        "▾ ADVANCED BOT"
    } else {
        "▸ ADVANCED BOT"
    };
    lines.push(Line::from(Span::raw("")));
    lines.push(Line::from(vec![
        Span::styled(
            marker(BotSettingsField::AdvancedToggle, toggle_label),
            label_style(BotSettingsField::AdvancedToggle),
        ),
        Span::styled(
            selector(
                BotSettingsField::AdvancedToggle,
                if st.show_advanced {
                    "expanded"
                } else {
                    "collapsed"
                },
            ),
            Style::default().fg(palette.muted),
        ),
    ]));

    if st.show_advanced {
        lines.push(Line::from(vec![
            Span::styled(
                marker(BotSettingsField::CreateSlotLag, "  SLOT LAG   "),
                label_style(BotSettingsField::CreateSlotLag),
            ),
            Span::styled(
                st.max_create_event_slot_lag.clone(),
                Style::default().fg(palette.fg),
            ),
            Span::styled(" slots", Style::default().fg(palette.muted)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(BotSettingsField::BackfillLimit, "  BACKFILL   "),
                label_style(BotSettingsField::BackfillLimit),
            ),
            Span::styled(st.backfill_limit.clone(), Style::default().fg(palette.fg)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(BotSettingsField::FetchFullTransaction, "  FULL TX    "),
                label_style(BotSettingsField::FetchFullTransaction),
            ),
            Span::styled(
                selector(
                    BotSettingsField::FetchFullTransaction,
                    bool_label(st.fetch_full_transaction),
                ),
                Style::default().fg(palette.fg),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(BotSettingsField::CurveExitQuotes, "  CURVE EXIT "),
                label_style(BotSettingsField::CurveExitQuotes),
            ),
            Span::styled(
                selector(
                    BotSettingsField::CurveExitQuotes,
                    bool_label(st.enable_curve_exit_quotes),
                ),
                Style::default().fg(palette.fg),
            ),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(BotSettingsField::ConfirmationPollMs, "  CONFIRM    "),
                label_style(BotSettingsField::ConfirmationPollMs),
            ),
            Span::styled(
                st.confirmation_poll_ms.clone(),
                Style::default().fg(palette.fg),
            ),
            Span::styled(" ms", Style::default().fg(palette.muted)),
        ]));
        lines.push(Line::from(vec![
            Span::styled(
                marker(BotSettingsField::ParallelFallbackReads, "  RPC READS  "),
                label_style(BotSettingsField::ParallelFallbackReads),
            ),
            Span::styled(
                selector(
                    BotSettingsField::ParallelFallbackReads,
                    if st.parallel_fallback_reads {
                        "parallel"
                    } else {
                        "primary-first"
                    },
                ),
                Style::default().fg(palette.fg),
            ),
        ]));
    }

    lines.push(Line::from(Span::raw("")));
    lines.push(Line::from(Span::styled(
        "Enter saves, validates, then launches bot",
        Style::default().fg(palette.warn),
    )));

    if st.saved {
        lines.push(Line::from(Span::styled(
            "saved",
            Style::default()
                .fg(palette.success)
                .add_modifier(Modifier::BOLD),
        )));
    } else if let Some(err) = st.error.as_ref() {
        lines.push(Line::from(Span::styled(
            format!(
                "error: {}",
                truncate_line(err, inner.width.saturating_sub(4) as usize)
            ),
            Style::default()
                .fg(palette.danger)
                .add_modifier(Modifier::BOLD),
        )));
    }

    render_centered(frame, inner, lines, palette.fg, false);
}

fn render_bot_screen(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let title = if state.phase == Phase::BotRunning {
        " BOT RUNNING "
    } else {
        " BOT STOPPED "
    };
    let width = area.width.saturating_sub(4).max(24);
    let height = area.height.saturating_sub(4).max(8);
    let inner = centered_box(
        frame,
        area,
        width,
        height,
        title,
        palette.panel,
        palette.bg_art_dim,
    );

    let capacity = inner.height.max(1) as usize;
    let max_width = inner.width.saturating_sub(2).max(1) as usize;

    // Show the newest logs at the bottom so the panel auto-scrolls.
    let logs: Vec<String> = state
        .logs
        .iter()
        .rev()
        .take(capacity)
        .rev()
        .map(|line| truncate_line(line, max_width))
        .collect();

    let lines: Vec<Line> = logs
        .iter()
        .map(|line| {
            let lower = line.to_lowercase();
            let color =
                if lower.contains("sell") || lower.contains("panic") || lower.contains("error") {
                    palette.danger
                } else if lower.contains("buy") {
                    palette.success
                } else if lower.starts_with("heartbeat") {
                    palette.muted
                } else {
                    palette.fg
                };
            Line::from(Span::styled(format!(" {line}"), Style::default().fg(color)))
        })
        .collect();

    let log_h = lines.len() as u16;
    let filler_h = inner.height.saturating_sub(log_h);
    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(filler_h), Constraint::Length(log_h)])
        .split(inner);

    let p = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .style(Style::default().bg(PANEL_BG));
    frame.render_widget(p, layout[1]);
}

fn truncate_line(line: &str, max_chars: usize) -> String {
    let char_count = line.chars().count();
    if char_count <= max_chars {
        line.to_string()
    } else {
        let mut out: String = line.chars().take(max_chars.saturating_sub(1)).collect();
        out.push('…');
        out
    }
}

/// Trade screen: MCAP / Position / Logs, ratio 4:1:1.
fn render_trade_screen(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let screen = centered_box(
        frame,
        area,
        area.width.saturating_sub(4).clamp(52, 92),
        area.height.saturating_sub(2).clamp(14, 42),
        " TRADE ",
        palette.panel,
        palette.bg_art_dim,
    );
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Ratio(4, 6),
            Constraint::Ratio(1, 6),
            Constraint::Ratio(1, 6),
        ])
        .split(screen);

    render_mcap_panel(frame, rows[0], state, palette, area);
    render_position_panel(frame, rows[1], state, palette, area);
    render_log_panel(frame, rows[2], state, palette, area);
}

fn render_mcap_panel(
    frame: &mut Frame,
    area: Rect,
    state: &ScanState,
    palette: &Palette,
    play_area: Rect,
) {
    if render_screening_in_mcap(state) {
        render_screening_mcap_panel(frame, area, state, palette, play_area);
        return;
    }

    let inner = clear_block(
        frame,
        area,
        " MCAP ",
        palette.panel,
        play_area,
        palette.bg_art_dim,
    );

    let title = if !state.symbol.is_empty() {
        format!("{}  mcap", state.symbol)
    } else {
        "mcap".to_string()
    };
    let spark = sparkline(&state.mcap_history, inner.width.saturating_sub(4) as usize);

    let mut lines: Vec<Line> = vec![Line::from(Span::styled(
        title,
        Style::default().fg(palette.muted),
    ))];
    lines.push(Line::from(Span::raw("")));
    lines.extend(mcap_ascii_lines(state.mcap_usd, palette));
    lines.push(Line::from(Span::raw("")));
    lines.push(Line::from(Span::styled(
        format!("{:.4} SOL", state.mcap_sol),
        Style::default().fg(palette.muted),
    )));
    if inner.height > 13 {
        lines.push(Line::from(Span::raw("")));
        lines.push(Line::from(Span::styled(
            spark,
            Style::default().fg(palette.accent),
        )));
    }

    render_centered(frame, inner, lines, palette.fg, false);
}

fn render_screening_in_mcap(state: &ScanState) -> bool {
    matches!(state.phase, Phase::Scanning | Phase::Evaluating) && state.token_amount_raw == 0
}

fn render_screening_mcap_panel(
    frame: &mut Frame,
    area: Rect,
    state: &ScanState,
    palette: &Palette,
    play_area: Rect,
) {
    let inner = clear_block(
        frame,
        area,
        " SCREENING ",
        palette.panel,
        play_area,
        palette.bg_art_dim,
    );

    let spinner = spinner_frame(state.tick);
    let title = if state.phase == Phase::Evaluating && !state.symbol.is_empty() {
        format!("{spinner}  evaluating {}", state.symbol)
    } else {
        format!("{spinner}  screening CATARNITH")
    };
    let status = if state.status_line.is_empty() {
        "waiting for fresh Mayhem curves".to_string()
    } else {
        state.status_line.clone()
    };

    let mut lines: Vec<Line> = vec![
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            title,
            Style::default()
                .fg(palette.warn)
                .add_modifier(Modifier::BOLD),
        )),
        Line::from(Span::raw("")),
        Line::from(Span::styled(status, Style::default().fg(palette.accent))),
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::styled(
                format!("scanned {}", state.scanned),
                Style::default().fg(palette.fg),
            ),
            Span::styled(
                format!("  |  skipped {}", state.trades_skipped),
                Style::default().fg(palette.muted),
            ),
            Span::styled(
                format!("  |  rpc {}", state.rpc_errors),
                Style::default().fg(palette.muted),
            ),
        ]),
    ];

    if state.phase == Phase::Evaluating && !state.mint.is_empty() {
        lines.push(Line::from(Span::raw("")));
        lines.push(Line::from(Span::styled(
            format!("candidate {}", short_mint(&state.mint)),
            Style::default().fg(palette.muted),
        )));
    }

    render_centered(frame, inner, lines, palette.fg, false);
}

fn render_position_panel(
    frame: &mut Frame,
    area: Rect,
    state: &ScanState,
    palette: &Palette,
    play_area: Rect,
) {
    let inner = clear_block(
        frame,
        area,
        " POSITION ",
        palette.panel,
        play_area,
        palette.bg_art_dim,
    );

    let tokens = state.token_amount_raw as f64 / 1_000_000.0;
    let pnl_usd = state.position_usd - state.entry_usd;
    let pnl_pct = if state.entry_usd > 0.0 {
        (pnl_usd / state.entry_usd) * 100.0
    } else {
        0.0
    };

    let pnl_color = if state.theme == Theme::Mono {
        if pnl_usd > 0.0 {
            Color::Rgb(60, 255, 140)
        } else if pnl_usd < 0.0 {
            Color::Rgb(255, 60, 90)
        } else {
            Color::Gray
        }
    } else if pnl_usd > 0.0 {
        palette.success
    } else if pnl_usd < 0.0 {
        palette.danger
    } else {
        palette.muted
    };

    let mut lines: Vec<Line> = vec![];
    if matches!(state.phase, Phase::Scanning | Phase::Evaluating) && state.token_amount_raw == 0 {
        let label = if state.phase == Phase::Evaluating {
            "entry pending"
        } else {
            "no open position"
        };
        lines.push(Line::from(Span::styled(
            label,
            Style::default()
                .fg(palette.muted)
                .add_modifier(Modifier::BOLD),
        )));
        lines.push(Line::from(Span::styled(
            format!("wallet {}", empty_or(&state.wallet_label, "ready")),
            Style::default().fg(palette.muted),
        )));
    } else {
        lines.push(Line::from(vec![
            Span::styled(
                format!("{tokens:.2} tokens"),
                Style::default().fg(palette.fg).add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                format!("  |  value {}", format_money(state.position_usd)),
                Style::default().fg(palette.fg),
            ),
        ]));
        lines.push(Line::from(Span::styled(
            format!(
                "entry {}  |  PnL {:+.2} USD ({:+.1}%)",
                format_money(state.entry_usd),
                pnl_usd,
                pnl_pct
            ),
            Style::default().fg(pnl_color),
        )));
        if state.phase == Phase::Holding {
            if state.confirm_exit {
                lines.push(Line::from(Span::styled(
                    "⚠ position open — [ESC] leave  [any] stay",
                    Style::default()
                        .fg(palette.warn)
                        .add_modifier(Modifier::BOLD),
                )));
            } else {
                lines.push(Line::from(Span::styled(
                    "[ENTER] SELL NOW",
                    Style::default()
                        .fg(palette.danger)
                        .add_modifier(Modifier::BOLD),
                )));
            }
        }
    }

    render_centered(frame, inner, lines, palette.fg, false);
}

fn short_mint(mint: &str) -> String {
    mint.chars().take(8).collect()
}

fn empty_or<'a>(value: &'a str, fallback: &'a str) -> &'a str {
    if value.is_empty() {
        fallback
    } else {
        value
    }
}

fn render_log_panel(
    frame: &mut Frame,
    area: Rect,
    state: &ScanState,
    palette: &Palette,
    play_area: Rect,
) {
    let inner = clear_block(
        frame,
        area,
        " LOGS ",
        palette.panel,
        play_area,
        palette.bg_art_dim,
    );

    let capacity = inner.height.saturating_sub(2).max(1) as usize;
    let lines: Vec<Line> = state
        .logs
        .iter()
        .rev()
        .take(capacity)
        .rev()
        .map(|line| {
            Line::from(Span::styled(
                format!(" {line}"),
                Style::default().fg(palette.fg),
            ))
        })
        .collect();

    render_centered(frame, inner, lines, palette.fg, false);
}

fn render_log_overlay(frame: &mut Frame, area: Rect, state: &ScanState, palette: &Palette) {
    let popup = centered_box(
        frame,
        area,
        76,
        18,
        " LOGS ",
        palette.panel,
        palette.bg_art_dim,
    );
    let lines: Vec<Line> = state
        .logs
        .iter()
        .map(|line| {
            Line::from(Span::styled(
                format!("  {line}"),
                Style::default().fg(palette.fg),
            ))
        })
        .collect();
    let p = Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .style(Style::default().bg(PANEL_BG));
    frame.render_widget(p, popup);
}

fn clear_block(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    border_color: Color,
    play_area: Rect,
    _dim_color: Color,
) -> Rect {
    let block = Block::default()
        .title(if title.is_empty() {
            Span::raw("")
        } else {
            Span::styled(
                title,
                Style::default()
                    .fg(border_color)
                    .add_modifier(Modifier::BOLD),
            )
        })
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .style(Style::default().bg(PANEL_BG));
    let inner = block.inner(area);
    frame.render_widget(block, area);
    // Dim wallpaper inside the sub-panel so it matches the outer wallpaper.
    render_wallpaper_clip(frame, inner, play_area, PANEL_WALLPAPER_FG, PANEL_BG);
    inner
}

// ------------------------------------------------------------------
// 7-character pure-ASCII MCAP display
// ------------------------------------------------------------------

/// Format a USD market-cap into exactly 7 characters, compact with
/// K/M/B suffixes when needed. Examples: "$ 12.5K", "$1.234M",
/// "$123.45", "$  0.12".
fn format_mcap_seven(amount: f64) -> String {
    let body = compact_mcap_body(amount);
    format!("${:>6}", body)
}

fn compact_mcap_body(amount: f64) -> String {
    let units = [(1e9_f64, 'B'), (1e6_f64, 'M'), (1e3_f64, 'K')];
    let mut div = 1.0_f64;
    let mut suffix = ' ';
    for (d, s) in units {
        if amount >= d * 0.9995 {
            div = d;
            suffix = s;
            break;
        }
    }
    loop {
        let n = amount / div;
        let num = if n >= 100.0 {
            format!("{:.1}", n)
        } else if n >= 10.0 {
            format!("{:.2}", n)
        } else {
            format!("{:.3}", n)
        };
        let mut out = num;
        if suffix != ' ' {
            out.push(suffix);
        }
        if out.len() <= 6 || suffix == ' ' {
            return out;
        }
        // Overflow (e.g. 999.95K -> 1000.0K): bump to the next unit.
        div *= 1000.0;
        suffix = match suffix {
            'K' => 'M',
            'M' => 'B',
            _ => ' ',
        };
    }
}

/// 5×7 pure-ASCII *outline* font. Each row is exactly 5 characters.
/// Interiors are left empty so the dim wallpaper shows through.
fn ascii_digit(ch: char) -> [&'static str; 7] {
    match ch {
        '0' => [
            "#### ", "#   #", "#   #", "#   #", "#   #", "#   #", "#### ",
        ],
        '1' => [
            "  #  ", " ##  ", "  #  ", "  #  ", "  #  ", "  #  ", " ### ",
        ],
        '2' => [
            "#### ", "    #", "#### ", "#    ", "#    ", "#    ", "#####",
        ],
        '3' => [
            "#### ", "    #", "#### ", "    #", "    #", "    #", "#### ",
        ],
        '4' => [
            "#   #", "#   #", "#   #", "#####", "    #", "    #", "    #",
        ],
        '5' => [
            "#### ", "#    ", "#### ", "    #", "    #", "    #", "#### ",
        ],
        '6' => [
            "#### ", "#    ", "#### ", "#   #", "#   #", "#   #", "#### ",
        ],
        '7' => [
            "#### ", "    #", "    #", "    #", "    #", "    #", "    #",
        ],
        '8' => [
            " ### ", "#   #", " ### ", "#   #", "#   #", "#   #", " ### ",
        ],
        '9' => [
            "#### ", "#   #", "#### ", "    #", "    #", "    #", "#### ",
        ],
        '.' => [
            "     ", "     ", "     ", "     ", "     ", "  #  ", "     ",
        ],
        '$' => [
            "  #  ", " ####", "# #  ", " ### ", "  # #", "#### ", "  #  ",
        ],
        'K' => [
            "#   #", "#  # ", "###  ", "#  # ", "#   #", "#   #", "#   #",
        ],
        'M' => [
            "#   #", "## ##", "# # #", "#   #", "#   #", "#   #", "#   #",
        ],
        'B' => [
            "#### ", "#   #", "#### ", "#   #", "#   #", "#   #", "#### ",
        ],
        _ => [
            "     ", "     ", "     ", "     ", "     ", "     ", "     ",
        ],
    }
}

/// Render the 7-character MCAP string as 7 rows of pure-ASCII art.
fn mcap_ascii_lines(amount: f64, palette: &Palette) -> Vec<Line<'static>> {
    let label = format_mcap_seven(amount);
    let style = Style::default()
        .fg(palette.banner)
        .add_modifier(Modifier::BOLD);
    let mut rows: Vec<Line<'static>> = Vec::with_capacity(7);
    for r in 0..7 {
        let mut spans: Vec<Span<'static>> = Vec::with_capacity(label.len() * 2);
        let chars: Vec<char> = label.chars().collect();
        for (i, ch) in chars.iter().enumerate() {
            spans.push(Span::styled(ascii_digit(*ch)[r], style));
            if i + 1 < chars.len() {
                spans.push(Span::raw(" "));
            }
        }
        rows.push(Line::from(spans));
    }
    rows
}

struct Palette {
    fg: Color,
    accent: Color,
    warn: Color,
    danger: Color,
    success: Color,
    muted: Color,
    banner: Color,
    panel: Color,
    /// Bright wallpaper color used outside panels.
    bg_art: Color,
    /// Dim wallpaper color re-rendered inside panels so the art is
    /// visible but text stays readable.
    bg_art_dim: Color,
}

fn format_money(amount: f64) -> String {
    if amount.abs() >= 1000.0 {
        format_usd(amount)
    } else {
        let sign = if amount < 0.0 { "-" } else { "" };
        format!("{sign}${:.2} USD", amount.abs())
    }
}

fn format_usd(amount: f64) -> String {
    let rounded = amount.round() as i64;
    let abs = rounded.unsigned_abs();
    let sign = if rounded < 0 { "-" } else { "" };
    let s = abs.to_string();
    let mut out = String::new();
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    let grouped: String = out.chars().rev().collect();
    format!("{sign}${grouped} USD")
}

fn palette_for(theme: Theme) -> Palette {
    match theme {
        Theme::Dark => Palette {
            fg: Color::Rgb(220, 220, 220),
            accent: Color::Rgb(0, 220, 220),
            warn: Color::Rgb(255, 200, 80),
            danger: Color::Rgb(255, 60, 90),
            success: Color::Rgb(60, 255, 140),
            muted: Color::Rgb(90, 90, 90),
            banner: Color::Rgb(220, 220, 220),
            panel: Color::Rgb(0, 180, 180),
            bg_art: Color::White,
            bg_art_dim: Color::Rgb(50, 50, 50),
        },
        Theme::Amber => Palette {
            fg: Color::Rgb(255, 176, 0),
            accent: Color::Rgb(255, 220, 80),
            warn: Color::Rgb(255, 140, 0),
            danger: Color::Rgb(255, 60, 0),
            success: Color::Rgb(200, 255, 80),
            muted: Color::Rgb(120, 80, 0),
            banner: Color::Rgb(255, 220, 120),
            panel: Color::Rgb(255, 160, 0),
            bg_art: Color::White,
            bg_art_dim: Color::Rgb(60, 40, 0),
        },
        Theme::Mono => Palette {
            fg: Color::White,
            accent: Color::Gray,
            warn: Color::Gray,
            danger: Color::White,
            success: Color::White,
            muted: Color::DarkGray,
            banner: Color::White,
            panel: Color::White,
            bg_art: Color::White,
            bg_art_dim: Color::Rgb(50, 50, 50),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catarnith::tui::ascii::sparkline;
    use std::collections::VecDeque;

    fn render_text(state: &ScanState) -> String {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| render(frame, state))
            .expect("render should not panic");
        let buf = terminal.backend().buffer();
        let mut text = String::new();
        for row in 0..buf.area.height {
            for col in 0..buf.area.width {
                text.push_str(buf.cell((col, row)).unwrap().symbol());
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn format_mcap_seven_is_exactly_seven_chars() {
        assert_eq!(format_mcap_seven(0.0).len(), 7);
        assert_eq!(format_mcap_seven(12.34).len(), 7);
        assert_eq!(format_mcap_seven(999.99).len(), 7);
        assert_eq!(format_mcap_seven(1_234.0).len(), 7);
        assert_eq!(format_mcap_seven(12_345.0).len(), 7);
        assert_eq!(format_mcap_seven(123_456.0).len(), 7);
        assert_eq!(format_mcap_seven(1_234_567.0).len(), 7);
        assert_eq!(format_mcap_seven(12_345_678.0).len(), 7);
        assert_eq!(format_mcap_seven(123_456_789.0).len(), 7);
        assert_eq!(format_mcap_seven(1_234_567_890.0).len(), 7);
        assert_eq!(format_mcap_seven(999_950_000.0).len(), 7);
        assert!(format_mcap_seven(123_456.0).starts_with('$'));
    }

    #[test]
    fn ascii_digit_font_has_exactly_five_columns() {
        for ch in [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '.', '$', 'K', 'M', 'B', ' ',
        ] {
            let art = ascii_digit(ch);
            assert_eq!(art.len(), 7, "{ch} should have 7 rows");
            for row in art {
                assert_eq!(row.chars().count(), 5, "{ch} row should be 5 columns");
            }
        }
    }

    #[test]
    fn renders_welcome_state_without_panicking() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let state = ScanState::new();
        terminal
            .draw(|frame| render(frame, &state))
            .expect("welcome render");
    }

    #[test]
    fn renders_holding_state_with_mcap_history() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = ScanState::new();
        state.phase = Phase::Holding;
        state.symbol = "CATARNITH".into();
        state.mcap_sol = 28.42;
        state.token_amount_raw = 1_000_000;
        state.entry_ms = chrono::Utc::now().timestamp_millis() - 4_000;
        for i in 0..40 {
            state.mcap_history.push_back((i, 25.0 + (i as f64) * 0.1));
        }
        terminal
            .draw(|frame| render(frame, &state))
            .expect("holding render");
    }

    #[test]
    fn renders_selling_state() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = ScanState::new();
        state.phase = Phase::Selling;
        state.symbol = "CATARNITH".into();
        state.mcap_sol = 32.0;
        state.push_log("SELL FIRED");
        terminal
            .draw(|frame| render(frame, &state))
            .expect("selling render");
    }

    #[test]
    fn renders_trade_result_state() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = ScanState::new();
        state.phase = Phase::TradeResult;
        state.symbol = "CATARNITH".into();
        state.last_trade = Some(crate::LastTrade {
            mint: "4uMzdeJCxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx".into(),
            entry_sol: 0.0049,
            exit_sol: 0.0072,
            realized_sol: 0.0023,
            held_ms: 1_200,
            won: true,
        });
        state.push_log("SOLD 4uMzdeJC | exit $0.02 | pnl +$0.01");
        terminal
            .draw(|frame| render(frame, &state))
            .expect("trade result render");
    }

    #[test]
    fn sparkline_smoke() {
        let mut h = VecDeque::new();
        for i in 0..10 {
            h.push_back((i, i as f64));
        }
        let s = sparkline(&h, 10);
        assert_eq!(s.chars().count(), 10);
    }

    #[test]
    fn format_usd_basic() {
        assert_eq!(format_usd(80_000.0), "$80,000 USD");
        assert_eq!(format_usd(0.0), "$0 USD");
        assert_eq!(format_usd(1234.567), "$1,235 USD");
        assert_eq!(format_usd(2_500.0), "$2,500 USD");
        assert_eq!(format_usd(80_000_000.0), "$80,000,000 USD");
    }

    #[test]
    fn format_usd_negative() {
        assert_eq!(format_usd(-1500.0), "-$1,500 USD");
    }

    #[test]
    fn format_money_small() {
        assert_eq!(format_money(80_000.0), "$80,000 USD");
        assert_eq!(format_money(0.97), "$0.97 USD");
        assert_eq!(format_money(-0.47), "-$0.47 USD");
    }

    #[test]
    fn renders_scanning_state_with_girl() {
        let mut state = ScanState::new();
        state.phase = Phase::Scanning;
        state.scanned = 14;
        state.status_line = "screening tokens…".into();
        let text = render_text(&state);
        assert!(text.contains("TRADE"), "scanning should use trade shell");
        assert!(
            text.contains("SCREENING"),
            "screening should render in the top trade panel"
        );
        assert!(text.contains("screening CATARNITH"));
        assert!(text.contains("scanned 14"));
        assert!(text.contains("no open position"));
        assert!(
            !text.contains("0.00 tokens"),
            "empty entries must not look like filled positions"
        );
    }

    #[test]
    fn renders_evaluating_inside_screening_mcap_until_fill() {
        let mut state = ScanState::new();
        state.phase = Phase::Evaluating;
        state.symbol = "MAYHEM".into();
        state.mint = "ABCDEFGH1111111111111111111111111111111111".into();
        state.status_line = "buying candidate…".into();
        let text = render_text(&state);
        assert!(text.contains("SCREENING"));
        assert!(text.contains("evaluating MAYHEM"));
        assert!(text.contains("buying candidate"));
        assert!(text.contains("entry pending"));
        assert!(
            !text.contains("0.00 tokens"),
            "pending buys must not render as held positions"
        );
    }

    #[test]
    fn renders_holding_replaces_screening_with_mcap() {
        let mut state = ScanState::new();
        state.phase = Phase::Holding;
        state.symbol = "MAYHEM".into();
        state.mcap_sol = 28.42;
        state.mcap_usd = 2_100.0;
        state.token_amount_raw = 1_000_000;
        state.entry_usd = 0.97;
        state.position_usd = 1.45;
        let text = render_text(&state);
        assert!(text.contains("MCAP"));
        assert!(text.contains("SELL NOW"));
        assert!(!text.contains("SCREENING"));
    }

    #[test]
    fn renders_welcome_state_with_girl() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let state = ScanState::new();
        terminal
            .draw(|frame| render(frame, &state))
            .expect("welcome render with girl should not panic");
    }

    #[test]
    fn renders_welcome_state_on_tiny_terminal() {
        let backend = ratatui::backend::TestBackend::new(60, 20);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let state = ScanState::new();
        terminal
            .draw(|frame| render(frame, &state))
            .expect("tiny welcome render should not panic");
    }

    #[test]
    fn renders_mode_picker() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let state = ScanState::new();
        terminal
            .draw(|frame| render(frame, &state))
            .expect("mode picker render should not panic");
    }

    #[test]
    fn renders_settings_with_all_fields() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = ScanState::new();
        state.phase = Phase::Settings;
        state.settings.helius_key = "secretkey".into();
        state.settings.jupiter_key = "jupkey".into();
        state.settings.fallback_rpc = "https://rpc.example".into();
        state.settings.slippage_bps = "1500".into();
        state.settings.max_hold_secs = "1".into();
        state.settings.active_field = SettingsField::SlippageBps;
        terminal
            .draw(|frame| render(frame, &state))
            .expect("settings render should not panic");
        let buf = terminal.backend().buffer();
        let mut text = String::new();
        for row in 0..buf.area.height {
            for col in 0..buf.area.width {
                text.push_str(buf.cell((col, row)).unwrap().symbol());
            }
        }
        // Secrets are masked, never shown verbatim.
        assert!(!text.contains("secretkey"), "helius key must be masked");
        assert!(!text.contains("jupkey"), "jupiter key must be masked");
        assert!(text.contains("SLIPPAGE"), "slippage row should render");
        assert!(text.contains("MAX HOLD"), "max hold row should render");
    }

    #[test]
    fn renders_bot_settings_with_core_fields() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = ScanState::new();
        state.phase = Phase::BotSettings;
        state.bot_settings.config_path = "config.toml".into();
        state.bot_settings.buy_size_sol = "0.0130".into();
        state.bot_settings.slippage_bps = "1500".into();
        state.bot_settings.max_hold_secs = "4".into();
        state.bot_settings.max_stream_event_age_ms = "500".into();
        state.bot_settings.entry_deadline_ms = "550".into();
        state.bot_settings.active_field = BotSettingsField::PairScope;
        terminal
            .draw(|frame| render(frame, &state))
            .expect("bot settings render should not panic");
        let buf = terminal.backend().buffer();
        let mut text = String::new();
        for row in 0..buf.area.height {
            for col in 0..buf.area.width {
                text.push_str(buf.cell((col, row)).unwrap().symbol());
            }
        }
        assert!(text.contains("AUTO BOT SETUP"));
        assert!(text.contains("PAIR SCOPE"));
        assert!(text.contains("BUY DEADLINE"));
        assert!(text.contains("save+start"));
    }

    #[test]
    fn renders_holding_state_with_sell_prompt() {
        let backend = ratatui::backend::TestBackend::new(93, 54);
        let mut terminal = ratatui::Terminal::new(backend).unwrap();
        let mut state = ScanState::new();
        state.phase = Phase::Holding;
        state.symbol = "CATARNITH".into();
        state.mcap_sol = 28.42;
        state.token_amount_raw = 1_000_000;
        state.entry_usd = 0.97;
        state.position_usd = 1.45;
        state.entry_ms = chrono::Utc::now().timestamp_millis() - 1_000;
        terminal
            .draw(|frame| render(frame, &state))
            .expect("holding render with sell prompt should not panic");
        let buf = terminal.backend().buffer();
        let mut text = String::new();
        for row in 0..buf.area.height {
            for col in 0..buf.area.width {
                text.push_str(buf.cell((col, row)).unwrap().symbol());
            }
            text.push('\n');
        }
        assert!(
            text.contains("SELL NOW"),
            "expected 'SELL NOW' in rendered buffer, got: {text}"
        );
        assert!(
            !text.contains("panic sell"),
            "stale 'panic sell' text should be gone, got: {text}"
        );
    }
}
