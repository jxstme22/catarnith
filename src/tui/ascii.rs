//! ASCII art used by the gamified terminal.

/// Rocket animation frames. Index by `tick % frames.len()`.
pub const ROCKET_FRAMES: &[&str] = &[
    "      |\n",
    "      .|\n",
    "     =>|\n",
    "    ==>>\n",
    "   *B U Y*\n",
];

/// Sticky "STREAK" badge shown when the user has a winning streak.
pub fn streak_badge(streak: i32, best: i32) -> String {
    if streak <= 0 {
        return String::new();
    }
    let crown = if streak >= 10 {
        "[\u{2694}\u{2694}]"
    } else if streak >= 5 {
        "[\u{2694}]"
    } else {
        "[+]"
    };
    format!("{crown} STREAK x{streak}  best x{best}")
}

/// Big "PANIC SELL" banner used in the confirmation window.
pub const PANIC_BANNER: &str = r"
  ____   _    _   _ _____ ___  __  __
 |  _ \ / \  | \ | | ____/ _ \|  \/  |
 | |_) / _ \ |  \| |  _|| | | | |\/| |
 |  __/ ___ \| |\  | |__| |_| | |  | |
 |_| /_/   \_\_| \_|_____\___/|_|  |_|

   >>>  HOLD ENTER TO CONFIRM  <<<
         (Esc to cancel)
";

/// "CATARNITH" wordmark drawn on top of the dashboard.
pub const CATARNITH_LOGO: &[&str] = &[
    r"  ____    _  _____  _    ____  _   _ ___ _____ _   _",
    r" / ___|  / \|_   _|/ \  |  _ \| \ | |_ _|_   _| | | |",
    r"| |     / _ \ | | / _ \ | |_) |  \| || |  | | | |_| |",
    r"| |___ / ___ \| |/ ___ \|  _ <| |\  || |  | | |  _  |",
    r" \____/_/   \_\_/_/   \_\_| \_\_| \_|___| |_| |_| |_|",
];

/// Spinner frames for "scanning…" indicators.
pub const SPINNER: &[&str] = &["|", "/", "-", "\\"];

pub fn spinner_frame(tick: u64) -> &'static str {
    SPINNER[(tick as usize) % SPINNER.len()]
}

pub fn rocket_frame(tick: u64) -> &'static str {
    ROCKET_FRAMES[(tick as usize) % ROCKET_FRAMES.len()]
}

/// Compact sparkline using Unicode block characters. Reads the last
/// `samples` points from `history` and renders them left-to-right.
pub fn sparkline(history: &std::collections::VecDeque<(i64, f64)>, width: usize) -> String {
    if history.is_empty() || width == 0 {
        return " ".repeat(width);
    }
    let mut values: Vec<f64> = history.iter().map(|(_, v)| *v).collect();
    if values.len() > width {
        values = values.split_off(values.len() - width);
    }
    let lo = values.iter().cloned().fold(f64::INFINITY, f64::min);
    let hi = values.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let span = (hi - lo).max(f64::MIN_POSITIVE);
    let blocks = [
        '\u{2581}', '\u{2582}', '\u{2583}', '\u{2584}', '\u{2585}', '\u{2586}', '\u{2587}',
        '\u{2588}',
    ];
    let mut out = String::with_capacity(width);
    for v in values {
        let norm = ((v - lo) / span).clamp(0.0, 0.999_999);
        let idx = (norm * blocks.len() as f64) as usize;
        out.push(blocks[idx]);
    }
    while out.chars().count() < width {
        out.insert(0, '\u{2581}');
    }
    out
}
