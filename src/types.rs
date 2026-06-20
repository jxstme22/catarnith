use chrono::{DateTime, TimeZone, Utc};
use serde::{Deserialize, Serialize};

pub const LAMPORTS_PER_SOL: i64 = 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Paper,
    Live,
}

impl Mode {
    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Paper => "paper",
            Mode::Live => "live",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TradeSide {
    Create,
    Buy,
    Sell,
    Swap,
    Failed,
    Unknown,
}

impl TradeSide {
    pub fn is_buy(self) -> bool {
        matches!(self, TradeSide::Buy)
    }

    pub fn is_sell(self) -> bool {
        matches!(self, TradeSide::Sell)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Ignore,
    Buy,
    Sell,
    Hold,
    KillSwitch,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DecodedTx {
    pub signature: String,
    pub slot: u64,
    pub timestamp_ms: Option<i64>,
    pub ok: bool,
    pub side: TradeSide,
    pub instruction_names: Vec<String>,
    pub program_ids: Vec<String>,
    pub account_keys: Vec<String>,
    pub mint: Option<String>,
    pub signer: Option<String>,
    pub sol_delta_lamports: Option<i64>,
    pub token_delta_raw: Option<i128>,
    pub fee_lamports: Option<u64>,
    pub logs: Vec<String>,
    pub err: Option<String>,
}

impl DecodedTx {
    pub fn timestamp(&self) -> Option<DateTime<Utc>> {
        self.timestamp_ms
            .and_then(|ms| Utc.timestamp_millis_opt(ms).single())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateMint {
    pub mint: String,
    pub first_seen_slot: u64,
    pub first_seen_ts_ms: i64,
    pub source: CandidateSource,
    pub is_mayhem_direct: bool,
    pub is_mayhem_candidate: bool,
    pub has_verified_mayhem_evidence: bool,
    pub is_axiom_route: bool,
    pub is_axiom_jito_route: bool,
    pub has_confirmed_execution_route: bool,
    pub score: f64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateSource {
    PumpfunBondingCurve,
    PumpSwap,
    MayhemDirect,
    ReferenceWallet,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenClassification {
    pub mint: Option<String>,
    pub is_pumpfun_bonding_curve: bool,
    pub is_pumpswap: bool,
    pub is_mayhem_direct: bool,
    pub is_mayhem_candidate: bool,
    pub has_verified_mayhem_evidence: bool,
    pub is_axiom_route: bool,
    pub is_axiom_jito_route: bool,
    pub has_confirmed_execution_route: bool,
    pub is_token_2022: bool,
    pub is_fresh_launch: bool,
    pub is_reference_wallet_seen: bool,
    pub score: f64,
    pub reasons: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Decision {
    pub id: String,
    pub timestamp_ms: i64,
    pub source_signature: Option<String>,
    pub mint: Option<String>,
    pub action: Action,
    pub mode: Mode,
    pub reason_codes: Vec<String>,
    pub requested_lamports: Option<u64>,
    pub risk_approved: bool,
    pub risk_veto_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuyOrder {
    pub id: String,
    pub timestamp_ms: i64,
    pub mint: String,
    pub lamports: u64,
    pub source_decision_id: String,
    pub source_signature: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SellOrder {
    pub id: String,
    pub timestamp_ms: i64,
    pub mint: String,
    pub source_decision_id: String,
    pub source_signature: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    PaperFilled,
    PaperRejected,
    Simulated,
    LiveDisabled,
    LiveSubmitted,
    LiveConfirmed,
    LiveReconciled,
    LiveFailed,
    Errored,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionReport {
    pub order_id: String,
    pub signature: Option<String>,
    #[serde(default)]
    pub quote_slot: Option<u64>,
    pub status: ExecutionStatus,
    pub requested_lamports: u64,
    pub filled_lamports: Option<u64>,
    pub filled_token_amount_raw: Option<u128>,
    pub fee_lamports: Option<u64>,
    pub error: Option<String>,
    pub latency_ms: Option<u64>,
}

pub fn lamports_to_sol(lamports: i64) -> f64 {
    lamports as f64 / LAMPORTS_PER_SOL as f64
}

pub fn now_ms() -> i64 {
    Utc::now().timestamp_millis()
}
