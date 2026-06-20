use crate::{
    classifier::ClassifierConfig,
    config::Config,
    types::{now_ms, DecodedTx, TokenClassification},
};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

#[derive(Debug, Clone)]
pub struct MayhemEvidenceConfig {
    pub mayhem_program: String,
    pub mayhem_agent_wallet: String,
    pub require_mayhem_evidence: bool,
    pub allow_indirect_mayhem_candidates: bool,
    pub mint_allowlist_path: Option<PathBuf>,
    pub metadata_url_template: Option<String>,
    pub metadata_timeout_ms: u64,
    pub min_confidence: f64,
}

impl From<&Config> for MayhemEvidenceConfig {
    fn from(config: &Config) -> Self {
        let metadata_url_template = if config.mayhem_metadata_url_template.trim().is_empty() {
            None
        } else {
            Some(config.mayhem_metadata_url_template.clone())
        };
        let mint_allowlist_path = if config.mayhem_mint_allowlist_path.trim().is_empty() {
            None
        } else {
            Some(PathBuf::from(config.mayhem_mint_allowlist_path.clone()))
        };

        Self {
            mayhem_program: config.mayhem_program.clone(),
            mayhem_agent_wallet: config.mayhem_agent_wallet.clone(),
            require_mayhem_evidence: config.require_mayhem_evidence,
            allow_indirect_mayhem_candidates: config.allow_indirect_mayhem_candidates,
            mint_allowlist_path,
            metadata_url_template,
            metadata_timeout_ms: config.mayhem_metadata_timeout_ms,
            min_confidence: config.mayhem_evidence_min_confidence,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MayhemEvidence {
    pub mint: String,
    pub is_mayhem: bool,
    pub confidence: f64,
    pub sources: Vec<String>,
    pub reasons: Vec<String>,
    pub checked_at_ms: i64,
}

impl MayhemEvidence {
    pub fn rejected(mint: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            mint: mint.into(),
            is_mayhem: false,
            confidence: 0.0,
            sources: Vec::new(),
            reasons: vec![reason.into()],
            checked_at_ms: now_ms(),
        }
    }

    pub fn passes(&self, config: &MayhemEvidenceConfig) -> bool {
        !config.require_mayhem_evidence
            || (self.is_mayhem && self.confidence >= config.min_confidence)
    }
}

#[derive(Debug, Clone)]
pub struct MayhemEvidenceClient {
    cfg: MayhemEvidenceConfig,
    mint_allowlist: HashSet<String>,
    http: reqwest::Client,
}

impl MayhemEvidenceClient {
    pub fn new(cfg: MayhemEvidenceConfig) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_millis(cfg.metadata_timeout_ms.max(100)))
            .build()
            .context("failed to build Mayhem metadata HTTP client")?;
        let mint_allowlist = if let Some(path) = &cfg.mint_allowlist_path {
            read_mint_allowlist(path)?
        } else {
            HashSet::new()
        };
        Ok(Self {
            cfg,
            mint_allowlist,
            http,
        })
    }

    pub async fn check_mint(
        &self,
        mint: &str,
        decoded: &DecodedTx,
        classification: &TokenClassification,
    ) -> MayhemEvidence {
        let mut evidence = evidence_from_onchain(
            mint,
            decoded,
            classification,
            &ClassifierConfig {
                pumpfun_program: String::new(),
                pumpswap_program: String::new(),
                mayhem_program: self.cfg.mayhem_program.clone(),
                mayhem_agent_wallet: self.cfg.mayhem_agent_wallet.clone(),
                token_2022_program: String::new(),
                axiom_route_program: String::new(),
                axiom_jito_marker: String::new(),
                reference_wallet: None,
            },
        );

        if evidence.passes(&self.cfg) {
            return evidence;
        }

        if self.mint_allowlist.contains(mint) {
            evidence.is_mayhem = true;
            evidence.confidence = 1.0;
            evidence.sources.push(
                self.cfg
                    .mint_allowlist_path
                    .as_ref()
                    .map(|path| format!("mint_allowlist:{}", path.display()))
                    .unwrap_or_else(|| "mint_allowlist".to_string()),
            );
            evidence.reasons.push("mint_allowlist_match".to_string());
            return evidence;
        }

        if self.cfg.allow_indirect_mayhem_candidates && classification.is_mayhem_candidate {
            evidence
                .reasons
                .push("indirect_mayhem_candidate_allowed_by_config".to_string());
        }

        if let Some(template) = &self.cfg.metadata_url_template {
            match self.check_metadata(template, mint).await {
                Ok(metadata_evidence) => merge_evidence(evidence, metadata_evidence),
                Err(err) => {
                    evidence
                        .reasons
                        .push(format!("metadata_lookup_error={err:#}"));
                    evidence
                }
            }
        } else {
            evidence
        }
    }

    async fn check_metadata(&self, template: &str, mint: &str) -> Result<MayhemEvidence> {
        let url = template.replace("{mint}", mint);
        let value = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("failed to fetch Mayhem metadata from {url}"))?
            .error_for_status()
            .with_context(|| format!("Mayhem metadata endpoint returned non-success for {url}"))?
            .json::<Value>()
            .await
            .with_context(|| format!("failed to parse Mayhem metadata JSON from {url}"))?;

        let mut evidence = parse_mayhem_metadata_json(mint, &value);
        evidence.sources.push(format!("metadata:{url}"));
        Ok(evidence)
    }
}

pub fn apply_mayhem_evidence(
    mut classification: TokenClassification,
    evidence: &MayhemEvidence,
) -> TokenClassification {
    if evidence.is_mayhem {
        classification.is_mayhem_candidate = true;
        classification.has_verified_mayhem_evidence = true;
        classification.score += evidence.confidence;
        classification
            .reasons
            .push("mayhem_evidence_verified".to_string());
        for source in &evidence.sources {
            classification
                .reasons
                .push(format!("mayhem_evidence_source={source}"));
        }
    } else if !evidence.reasons.is_empty() {
        classification.reasons.push(format!(
            "mayhem_evidence_unverified={}",
            evidence.reasons.join("|")
        ));
    }
    classification
}

pub fn evidence_from_onchain(
    mint: &str,
    decoded: &DecodedTx,
    classification: &TokenClassification,
    cfg: &ClassifierConfig,
) -> MayhemEvidence {
    let mut evidence = MayhemEvidence {
        mint: mint.to_string(),
        is_mayhem: false,
        confidence: 0.0,
        sources: Vec::new(),
        reasons: Vec::new(),
        checked_at_ms: now_ms(),
    };

    let mayhem_program_present = decoded
        .program_ids
        .iter()
        .any(|program| program == &cfg.mayhem_program);
    let mayhem_agent_present = decoded
        .program_ids
        .iter()
        .chain(decoded.account_keys.iter())
        .any(|account| account == &cfg.mayhem_agent_wallet);

    if classification.is_mayhem_direct || mayhem_program_present || mayhem_agent_present {
        evidence.is_mayhem = true;
        evidence.confidence = 1.0;
        evidence.sources.push("onchain".to_string());
        if mayhem_program_present {
            evidence.reasons.push("mayhem_program_present".to_string());
        }
        if mayhem_agent_present {
            evidence
                .reasons
                .push("mayhem_agent_wallet_present".to_string());
        }
        if classification.is_mayhem_direct && evidence.reasons.is_empty() {
            evidence
                .reasons
                .push("classifier_direct_mayhem".to_string());
        }
    } else {
        evidence
            .reasons
            .push("no_direct_mayhem_onchain_evidence".to_string());
    }

    evidence
}

pub fn parse_mayhem_metadata_json(mint: &str, value: &Value) -> MayhemEvidence {
    let mut evidence = MayhemEvidence {
        mint: mint.to_string(),
        is_mayhem: false,
        confidence: 0.0,
        sources: Vec::new(),
        reasons: Vec::new(),
        checked_at_ms: now_ms(),
    };

    let truthy_paths = [
        &["isMayhem"][..],
        &["mayhem"][..],
        &["data", "isMayhem"][..],
        &["data", "mayhem"][..],
        &["token", "isMayhem"][..],
        &["token", "mayhem"][..],
    ];
    for path in truthy_paths {
        if json_bool_at(value, path) == Some(true) {
            evidence.is_mayhem = true;
            evidence.confidence = 0.95;
            evidence
                .reasons
                .push(format!("metadata_flag_true={}", path.join(".")));
        }
    }

    for path in [
        &["mode"][..],
        &["data", "mode"][..],
        &["token", "mode"][..],
        &["launchMode"][..],
        &["data", "launchMode"][..],
        &["token", "launchMode"][..],
    ] {
        if json_string_at(value, path).is_some_and(|mode| mode.eq_ignore_ascii_case("mayhem")) {
            evidence.is_mayhem = true;
            evidence.confidence = evidence.confidence.max(0.95);
            evidence
                .reasons
                .push(format!("metadata_mode_mayhem={}", path.join(".")));
        }
    }

    if !evidence.is_mayhem {
        evidence
            .reasons
            .push("metadata_did_not_confirm_mayhem".to_string());
    }

    evidence
}

fn merge_evidence(mut base: MayhemEvidence, metadata: MayhemEvidence) -> MayhemEvidence {
    base.is_mayhem |= metadata.is_mayhem;
    base.confidence = base.confidence.max(metadata.confidence);
    base.sources.extend(metadata.sources);
    base.reasons.extend(metadata.reasons);
    base.checked_at_ms = now_ms();
    base
}

fn read_mint_allowlist(path: &Path) -> Result<HashSet<String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("failed to read Mayhem mint allowlist {}", path.display()))?;
    let mut mints = HashSet::new();
    for line in raw.lines() {
        let mint = line.split('#').next().unwrap_or_default().trim();
        if mint.is_empty() {
            continue;
        }
        mints.insert(mint.to_string());
    }
    Ok(mints)
}

fn json_bool_at(value: &Value, path: &[&str]) -> Option<bool> {
    value_at(value, path).and_then(Value::as_bool)
}

fn json_string_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    value_at(value, path).and_then(Value::as_str)
}

fn value_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}
