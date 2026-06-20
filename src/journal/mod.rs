use crate::position::Position;
use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use serde::Serialize;
use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[derive(Debug, Clone, Copy)]
pub enum JournalKind {
    RawEvent,
    DiscoverySignal,
    MarketQuote,
    CurveState,
    EntryFeatures,
    DecodedTransaction,
    CandidateMint,
    MayhemEvidence,
    Decision,
    Order,
    Execution,
    Position,
    LiveLifecycle,
    RiskVeto,
    MetricsSnapshot,
    StreamFreshness,
}

impl JournalKind {
    fn table(self) -> &'static str {
        match self {
            JournalKind::RawEvent => "raw_events",
            JournalKind::DiscoverySignal => "discovery_signals",
            JournalKind::MarketQuote => "market_quotes",
            JournalKind::CurveState => "curve_states",
            JournalKind::EntryFeatures => "entry_features",
            JournalKind::DecodedTransaction => "decoded_transactions",
            JournalKind::CandidateMint => "candidate_mints",
            JournalKind::MayhemEvidence => "mayhem_evidence",
            JournalKind::Decision => "decisions",
            JournalKind::Order => "orders",
            JournalKind::Execution => "executions",
            JournalKind::Position => "positions",
            JournalKind::LiveLifecycle => "live_lifecycles",
            JournalKind::RiskVeto => "risk_vetoes",
            JournalKind::MetricsSnapshot => "metrics_snapshots",
            JournalKind::StreamFreshness => "stream_freshness",
        }
    }

    fn file_name(self) -> &'static str {
        match self {
            JournalKind::RawEvent => "raw_events.jsonl",
            JournalKind::DiscoverySignal => "discovery_signals.jsonl",
            JournalKind::MarketQuote => "market_quotes.jsonl",
            JournalKind::CurveState => "curve_states.jsonl",
            JournalKind::EntryFeatures => "entry_features.jsonl",
            JournalKind::DecodedTransaction => "decoded_transactions.jsonl",
            JournalKind::CandidateMint => "candidate_mints.jsonl",
            JournalKind::MayhemEvidence => "mayhem_evidence.jsonl",
            JournalKind::Decision => "decisions.jsonl",
            JournalKind::Order => "orders.jsonl",
            JournalKind::Execution => "executions.jsonl",
            JournalKind::Position => "positions.jsonl",
            JournalKind::LiveLifecycle => "live_lifecycles.jsonl",
            JournalKind::RiskVeto => "risk_vetoes.jsonl",
            JournalKind::MetricsSnapshot => "metrics_snapshots.jsonl",
            JournalKind::StreamFreshness => "stream_freshness.jsonl",
        }
    }
}

pub struct Journal {
    dir: PathBuf,
    conn: Connection,
}

impl Journal {
    pub fn open(dir: impl AsRef<Path>, sqlite_path: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)
            .with_context(|| format!("failed to create journal dir {}", dir.display()))?;
        if let Some(parent) = sqlite_path.as_ref().parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).with_context(|| {
                    format!("failed to create sqlite parent dir {}", parent.display())
                })?;
            }
        }
        let conn = Connection::open(sqlite_path.as_ref())
            .with_context(|| format!("failed to open sqlite {}", sqlite_path.as_ref().display()))?;
        let journal = Self { dir, conn };
        journal.init()?;
        Ok(journal)
    }

    fn init(&self) -> Result<()> {
        for table in [
            "raw_events",
            "discovery_signals",
            "market_quotes",
            "curve_states",
            "entry_features",
            "decoded_transactions",
            "candidate_mints",
            "mayhem_evidence",
            "decisions",
            "orders",
            "executions",
            "positions",
            "live_lifecycles",
            "risk_vetoes",
            "metrics_snapshots",
            "stream_freshness",
        ] {
            self.conn.execute(
                &format!(
                    "CREATE TABLE IF NOT EXISTS {table} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        created_at_ms INTEGER NOT NULL,
                        payload TEXT NOT NULL
                    )"
                ),
                [],
            )?;
        }
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS journal_dedup (
                kind TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (kind, dedupe_key)
            )",
            [],
        )?;
        Ok(())
    }

    pub fn record<T: Serialize>(&self, kind: JournalKind, value: &T) -> Result<()> {
        let payload =
            serde_json::to_string(value).context("failed to serialize journal payload")?;
        let created_at_ms = chrono::Utc::now().timestamp_millis();

        let path = self.dir.join(kind.file_name());
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .with_context(|| format!("failed to open journal file {}", path.display()))?;
        writeln!(file, "{payload}")
            .with_context(|| format!("failed to write journal file {}", path.display()))?;

        self.conn.execute(
            &format!(
                "INSERT INTO {} (created_at_ms, payload) VALUES (?1, ?2)",
                kind.table()
            ),
            params![created_at_ms, payload],
        )?;
        Ok(())
    }

    pub fn record_once<T: Serialize>(
        &self,
        kind: JournalKind,
        dedupe_key: &str,
        value: &T,
    ) -> Result<bool> {
        let created_at_ms = chrono::Utc::now().timestamp_millis();
        let inserted = self.conn.execute(
            "INSERT OR IGNORE INTO journal_dedup (kind, dedupe_key, created_at_ms)
             VALUES (?1, ?2, ?3)",
            params![kind.table(), dedupe_key, created_at_ms],
        )?;
        if inserted == 0 {
            return Ok(false);
        }

        if let Err(err) = self.record(kind, value) {
            let _ = self.conn.execute(
                "DELETE FROM journal_dedup WHERE kind = ?1 AND dedupe_key = ?2",
                params![kind.table(), dedupe_key],
            );
            return Err(err);
        }
        Ok(true)
    }

    pub fn load_latest_positions(&self) -> Result<Vec<Position>> {
        let mut stmt = self
            .conn
            .prepare("SELECT payload FROM positions ORDER BY id ASC")?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        let mut latest = HashMap::<String, Position>::new();
        for row in rows {
            let payload = row?;
            let position: Position =
                serde_json::from_str(&payload).context("failed to decode journal position")?;
            latest.insert(position.mint.clone(), position);
        }
        Ok(latest.into_values().collect())
    }
}
