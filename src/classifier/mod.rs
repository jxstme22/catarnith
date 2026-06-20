use crate::{
    config::{
        AXIOM_JITO_MARKER, AXIOM_ROUTE_PROGRAM, MAYHEM_AGENT_WALLET, MAYHEM_PROGRAM,
        PUMPFUN_BONDING_CURVE_PROGRAM, PUMPSWAP_PROGRAM, TOKEN_2022_PROGRAM,
    },
    decoder::has_pump_create_signal,
    types::{CandidateSource, DecodedTx, TokenClassification, TradeSide},
};

#[derive(Debug, Clone)]
pub struct ClassifierConfig {
    pub pumpfun_program: String,
    pub pumpswap_program: String,
    pub mayhem_program: String,
    pub mayhem_agent_wallet: String,
    pub token_2022_program: String,
    pub axiom_route_program: String,
    pub axiom_jito_marker: String,
    pub reference_wallet: Option<String>,
}

impl Default for ClassifierConfig {
    fn default() -> Self {
        Self {
            pumpfun_program: PUMPFUN_BONDING_CURVE_PROGRAM.to_string(),
            pumpswap_program: PUMPSWAP_PROGRAM.to_string(),
            mayhem_program: MAYHEM_PROGRAM.to_string(),
            mayhem_agent_wallet: MAYHEM_AGENT_WALLET.to_string(),
            token_2022_program: TOKEN_2022_PROGRAM.to_string(),
            axiom_route_program: AXIOM_ROUTE_PROGRAM.to_string(),
            axiom_jito_marker: AXIOM_JITO_MARKER.to_string(),
            reference_wallet: None,
        }
    }
}

pub fn classify_token(decoded: &DecodedTx, cfg: &ClassifierConfig) -> TokenClassification {
    let mut reasons = Vec::new();
    let has_program = |program: &str| decoded.program_ids.iter().any(|id| id == program);
    let has_account = |account: &str| {
        decoded.account_keys.iter().any(|key| key == account)
            || decoded.program_ids.iter().any(|key| key == account)
    };

    let is_pumpfun_bonding_curve = has_program(&cfg.pumpfun_program);
    if is_pumpfun_bonding_curve {
        reasons.push("pumpfun_program_present".to_string());
    }

    let is_pumpswap = has_program(&cfg.pumpswap_program);
    if is_pumpswap {
        reasons.push("pumpswap_program_present".to_string());
    }

    let is_axiom_route =
        has_program(&cfg.axiom_route_program) || has_account(&cfg.axiom_route_program);
    if is_axiom_route {
        reasons.push("axiom_route_program_present".to_string());
    }

    let is_axiom_jito_route = has_account(&cfg.axiom_jito_marker);
    if is_axiom_jito_route {
        reasons.push("axiom_jito_marker_present".to_string());
    }

    let has_confirmed_execution_route = is_axiom_route && (is_pumpfun_bonding_curve || is_pumpswap);
    if has_confirmed_execution_route {
        reasons.push("axiom_pump_execution_route".to_string());
    }

    let is_mayhem_direct =
        has_program(&cfg.mayhem_program) || has_account(&cfg.mayhem_agent_wallet);
    if has_program(&cfg.mayhem_program) {
        reasons.push("mayhem_program_present".to_string());
    }
    if has_account(&cfg.mayhem_agent_wallet) {
        reasons.push("mayhem_agent_wallet_present".to_string());
    }

    let is_token_2022 = has_program(&cfg.token_2022_program);
    if is_token_2022 {
        reasons.push("token_2022_program_present".to_string());
    }

    let is_fresh_launch = has_pump_create_signal(decoded);
    if is_fresh_launch {
        reasons.push("fresh_launch_instruction_seen".to_string());
    }

    let is_reference_wallet_seen = cfg
        .reference_wallet
        .as_ref()
        .map(|wallet| {
            decoded.signer.as_ref() == Some(wallet)
                || decoded.account_keys.iter().any(|key| key == wallet)
        })
        .unwrap_or(false);
    if is_reference_wallet_seen {
        reasons.push("reference_wallet_seen".to_string());
    }

    let mut score = 0.0;
    if is_mayhem_direct {
        score += 1.0;
    }
    if is_pumpfun_bonding_curve {
        score += 0.35;
    }
    if is_pumpswap {
        score += 0.25;
    }
    if has_confirmed_execution_route {
        score += 0.25;
    }
    if is_token_2022 {
        score += 0.15;
    }
    if decoded.side == TradeSide::Buy {
        score += 0.10;
    }
    if is_fresh_launch {
        score += 0.15;
    }
    if decoded
        .mint
        .as_deref()
        .is_some_and(|mint| mint.ends_with("pump"))
    {
        score += 0.10;
        reasons.push("pump_suffix_mint".to_string());
    }

    let is_mayhem_candidate = is_mayhem_direct
        || (is_pumpfun_bonding_curve
            && is_token_2022
            && decoded
                .mint
                .as_deref()
                .is_some_and(|mint| mint.ends_with("pump")));
    if is_mayhem_candidate && !is_mayhem_direct {
        reasons.push("mayhem_candidate_indirect_pumpfun_token2022".to_string());
    }

    TokenClassification {
        mint: decoded.mint.clone(),
        is_pumpfun_bonding_curve,
        is_pumpswap,
        is_mayhem_direct,
        is_mayhem_candidate,
        has_verified_mayhem_evidence: is_mayhem_direct,
        is_axiom_route,
        is_axiom_jito_route,
        has_confirmed_execution_route,
        is_token_2022,
        is_fresh_launch,
        is_reference_wallet_seen,
        score,
        reasons,
    }
}

pub fn candidate_source(classification: &TokenClassification) -> CandidateSource {
    if classification.is_mayhem_direct {
        CandidateSource::MayhemDirect
    } else if classification.is_pumpfun_bonding_curve {
        CandidateSource::PumpfunBondingCurve
    } else if classification.is_pumpswap {
        CandidateSource::PumpSwap
    } else if classification.is_reference_wallet_seen {
        CandidateSource::ReferenceWallet
    } else {
        CandidateSource::Unknown
    }
}
