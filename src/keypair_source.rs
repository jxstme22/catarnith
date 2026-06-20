use crate::config::Config;
use anyhow::{bail, Context, Result};
use solana_sdk::{
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use std::{panic, path::Path};

/// Where the live canary keypair came from for the current process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeypairSource {
    /// Loaded from a Solana CLI JSON keypair file on disk.
    File,
    /// Decoded from a base58-encoded 64-byte secret key (Phantom/Solflare
    /// "private key" export, `solana-keygen pubkey ASK` output, etc.).
    Base58,
}

/// A live canary keypair plus how it was obtained.
pub struct ResolvedKeypair {
    pub keypair: Keypair,
    pub pubkey: Pubkey,
    pub source: KeypairSource,
    /// Path to the on-disk keypair, if the source is `File`.
    pub path: Option<String>,
}

const BASE58_ENV: &str = "CTARNITH_WALLET_KEYPAIR_BASE58";
const LEGACY_BASE58_ENV: &str = "MAYHEM_WALLET_KEYPAIR_BASE58";

/// Resolve the canary keypair for live execution.
///
/// Precedence:
/// 1. `CTARNITH_WALLET_KEYPAIR_BASE58` env var (or `cfg.wallet_keypair_base58`):
///    a 64-byte base58-encoded secret key. Never touches disk.
/// 2. `cfg.wallet_keypair_path`: a Solana CLI JSON keypair file. The file
///    must exist and be readable by the current process.
///
/// Returns the source so callers can skip path-only validations (e.g.
/// "must be outside the repository") when the key came from a base58 string.
pub fn resolve(cfg: &Config) -> Result<ResolvedKeypair> {
    let from_env = crate::config::env_var(BASE58_ENV, LEGACY_BASE58_ENV).ok();
    let from_cfg = cfg
        .wallet_keypair_base58
        .as_ref()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if let Some(encoded) = from_env.or(from_cfg) {
        let keypair = decode_base58_keypair(&encoded)
            .with_context(|| format!("{BASE58_ENV} is not a valid 64-byte base58 secret key"))?;
        let pubkey = keypair.pubkey();
        return Ok(ResolvedKeypair {
            keypair,
            pubkey,
            source: KeypairSource::Base58,
            path: None,
        });
    }

    let path = cfg.wallet_keypair_path.trim();
    if path.is_empty() {
        bail!(
            "live executor requires {BASE58_ENV} (base58 secret key) or CTARNITH_WALLET_KEYPAIR_PATH (keypair file)"
        );
    }
    let path_buf = Path::new(path);
    let keypair = solana_sdk::signature::read_keypair_file(path_buf).map_err(|err| {
        anyhow::anyhow!("failed to read keypair file {}: {err}", path_buf.display())
    })?;
    let pubkey = keypair.pubkey();
    Ok(ResolvedKeypair {
        keypair,
        pubkey,
        source: KeypairSource::File,
        path: Some(path.to_string()),
    })
}

/// Decode a 64-byte base58 secret key into a `Keypair` with validation.
///
/// `solana_sdk::signature::Keypair::from_base58_string` panics on bad input
/// via `unwrap`; this wrapper performs the same decode but returns a
/// structured `anyhow::Error` instead so the executor can refuse bad input
/// cleanly.
pub fn decode_base58_keypair(encoded: &str) -> Result<Keypair> {
    let trimmed = encoded.trim();
    if trimmed.is_empty() {
        bail!("base58 secret key is empty");
    }
    // `Keypair::from_base58_string` panics on malformed input. Catch the
    // panic so the executor exits with an `anyhow` error rather than
    // aborting the process.
    let panicked = panic::catch_unwind(|| Keypair::from_base58_string(trimmed))
        .map_err(|_| anyhow::anyhow!("input is not a valid 64-byte base58 secret key"));
    panicked
}

/// True when the live config has any usable keypair source configured.
pub fn is_configured(cfg: &Config) -> bool {
    let has_base58 = crate::config::env_var(BASE58_ENV, LEGACY_BASE58_ENV).is_ok()
        || cfg
            .wallet_keypair_base58
            .as_ref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
    has_base58 || !cfg.wallet_keypair_path.trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_inputs_are_rejected() {
        assert!(decode_base58_keypair("").is_err());
        assert!(decode_base58_keypair("   ").is_err());
    }

    #[test]
    fn round_trip_through_to_base58() {
        // Generate a fresh keypair, encode it, then decode the encoded
        // string and confirm it round-trips back to the same public key.
        let original = Keypair::new();
        let encoded = original.to_base58_string();
        let decoded = decode_base58_keypair(&encoded).expect("decode");
        assert_eq!(decoded.pubkey(), original.pubkey());
    }

    #[test]
    fn too_short_input_is_rejected() {
        // Valid base58 string that does not decode to a 64-byte keypair.
        let short = "3vVf7kXLV4bQ8";
        assert!(decode_base58_keypair(short).is_err());
    }

    #[test]
    fn real_phantom_length_round_trips() {
        // A real Phantom/Solflare export is an 88-character base58 string
        // encoding 64 bytes. Confirm we accept that length as well.
        let original = Keypair::new();
        let encoded = original.to_base58_string();
        assert!(
            encoded.len() >= 64,
            "expected base58 export to be at least 64 chars, got {}",
            encoded.len()
        );
        let decoded = decode_base58_keypair(&encoded).expect("decode phantom export");
        assert_eq!(decoded.pubkey(), original.pubkey());
    }
}
