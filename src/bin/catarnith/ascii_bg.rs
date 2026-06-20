//! Full-screen ASCII wallpaper used as the background layer for
//! every screen in the `catarnith` TUI.
//!
//! The art is loaded from `bg.txt` so it can be edited without
//! touching Rust escaping rules. It is rendered in a very dim color
//! behind the centered game-console panels.

/// The raw wallpaper loaded at compile time.
pub const RAW: &str = include_str!("bg.txt");

/// Return the art as a vector of lines. The file is tiny, so this
/// is cheap enough to call every frame.
pub fn lines() -> Vec<&'static str> {
    RAW.lines().collect()
}

/// Display width of the widest line (counting Unicode scalar values;
/// the wallpaper uses single-cell braille patterns).
pub fn width() -> usize {
    RAW.lines().map(|l| l.chars().count()).max().unwrap_or(0)
}
