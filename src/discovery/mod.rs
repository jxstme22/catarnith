use crate::{
    survival::PulseMint,
    types::{now_ms, CandidateMint, CandidateSource, TokenClassification},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoverySignal {
    pub mint: String,
    pub seen_ts_ms: i64,
    pub source: String,
    pub verified_mayhem: bool,
}

impl From<PulseMint> for DiscoverySignal {
    fn from(pulse: PulseMint) -> Self {
        Self {
            mint: pulse.mint,
            seen_ts_ms: pulse.seen_ts_ms.unwrap_or_else(now_ms),
            source: pulse.source,
            verified_mayhem: true,
        }
    }
}

#[derive(Debug, Default)]
pub struct DiscoveryRegistry {
    candidates: HashMap<String, DiscoverySignal>,
}

impl DiscoveryRegistry {
    pub fn register(&mut self, signal: DiscoverySignal) -> bool {
        match self.candidates.get(&signal.mint) {
            Some(existing)
                if existing.source != "pump_create_mayhem"
                    && signal.source == "pump_create_mayhem" =>
            {
                self.candidates.insert(signal.mint.clone(), signal);
                true
            }
            Some(existing)
                if existing.source == "pump_create_mayhem"
                    && signal.source != "pump_create_mayhem" =>
            {
                false
            }
            Some(existing) if existing.seen_ts_ms <= signal.seen_ts_ms => false,
            _ => {
                self.candidates.insert(signal.mint.clone(), signal);
                true
            }
        }
    }

    pub fn get(&self, mint: &str) -> Option<&DiscoverySignal> {
        self.candidates.get(mint)
    }

    pub fn len(&self) -> usize {
        self.candidates.len()
    }

    pub fn is_empty(&self) -> bool {
        self.candidates.is_empty()
    }

    pub fn is_entry_fresh(&self, mint: &str, timestamp_ms: i64, deadline_ms: i64) -> bool {
        self.get(mint).is_some_and(|signal| {
            let age_ms = timestamp_ms.saturating_sub(signal.seen_ts_ms);
            age_ms >= 0 && age_ms <= deadline_ms
        })
    }
}

pub fn candidate_from_classification(
    classification: &TokenClassification,
    slot: u64,
    timestamp_ms: i64,
    source: CandidateSource,
) -> Option<CandidateMint> {
    Some(CandidateMint {
        mint: classification.mint.clone()?,
        first_seen_slot: slot,
        first_seen_ts_ms: timestamp_ms,
        source,
        is_mayhem_direct: classification.is_mayhem_direct,
        is_mayhem_candidate: classification.is_mayhem_candidate,
        has_verified_mayhem_evidence: classification.has_verified_mayhem_evidence,
        is_axiom_route: classification.is_axiom_route,
        is_axiom_jito_route: classification.is_axiom_jito_route,
        has_confirmed_execution_route: classification.has_confirmed_execution_route,
        score: classification.score,
        reasons: classification.reasons.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn real_create_signal_replaces_older_non_create_discovery() {
        let mut registry = DiscoveryRegistry::default();
        assert!(registry.register(DiscoverySignal {
            mint: "mint".to_string(),
            seen_ts_ms: 100,
            source: "onchain_mayhem".to_string(),
            verified_mayhem: true,
        }));
        assert!(registry.register(DiscoverySignal {
            mint: "mint".to_string(),
            seen_ts_ms: 200,
            source: "pump_create_mayhem".to_string(),
            verified_mayhem: true,
        }));
        assert_eq!(
            registry.get("mint").map(|signal| signal.source.as_str()),
            Some("pump_create_mayhem")
        );
    }
}
