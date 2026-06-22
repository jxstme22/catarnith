use anyhow::{bail, Context, Result};
use catarnith::{
    config::{Config, Market},
    executor::Order,
    journal::{Journal, JournalKind},
    types::{now_ms, BuyOrder, ExecutionReport, ExecutionStatus, Mode, SellOrder},
};
use pump_rust_client::{
    constants,
    math::bonding_curve::{buy_token_amount_from_sol_amount, sell_sol_amount_from_token_amount},
    pda, AsyncPumpClient, ComputeBudget, PumpSdk,
};
use serde::Serialize;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{read_keypair_file, Signature, Signer},
};
use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::sleep;

const DEFAULT_COMPUTE_UNITS: u32 = 400_000;
const DEFAULT_PRIORITY_MICRO_LAMPORTS: u64 = 1;
const DEFAULT_RPC_TIMEOUT_MS: u64 = 900;
const DEFAULT_STARTUP_RPC_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_STARTUP_RPC_RETRIES: usize = 3;
const DEFAULT_STARTUP_RPC_RETRY_DELAY_MS: u64 = 150;
const DEFAULT_CONFIRMATION_TIMEOUT_MS: u64 = 5_000;
const DEFAULT_CONFIRMATION_POLL_MS: u64 = 200;
const DEFAULT_MAX_BALANCE: u64 = 50_000_000;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "snake_case")]
enum Side {
    Buy,
    Sell,
}

#[derive(Debug)]
struct Args {
    config: PathBuf,
    side: Side,
    mint: String,
    out: Option<PathBuf>,
    /// When true, use the panic-sell path: skip client-side
    /// simulation, skip preflight, and return the moment an RPC
    /// accepts the transaction (no confirmation poll).
    panic: bool,
}

#[derive(Debug, Serialize)]
struct LiveExecutionReport {
    ready_to_broadcast: bool,
    broadcast_attempted: bool,
    confirmed: bool,
    wallet_pubkey: String,
    mint: String,
    side: Side,
    instruction: String,
    input_amount: u64,
    quoted_output: u64,
    protected_minimum_output: u64,
    slippage_bps: u32,
    compute_unit_limit: u32,
    compute_unit_price_micro_lamports: u64,
    pre_sol_balance_lamports: u64,
    post_sol_balance_lamports: Option<u64>,
    pre_token_amount_raw: u64,
    post_token_amount_raw: Option<u64>,
    simulation_succeeded: bool,
    simulation_error: Option<String>,
    simulation_units_consumed: Option<u64>,
    signature: Option<String>,
    confirmation_slot: Option<u64>,
    confirmation_status: Option<String>,
    confirmation_error: Option<String>,
    elapsed_ms: u64,
    journal_dir: String,
    notes: Vec<String>,
}

struct BuiltTrade {
    transaction: solana_sdk::transaction::Transaction,
    instruction_name: &'static str,
    input_amount: u64,
    quoted_output: u64,
    protected_minimum_output: u64,
    slippage_bps: u32,
    token_account: Pubkey,
}

struct ConfirmedStatus {
    slot: u64,
    confirmation_status: String,
}

struct BaseReportInput<'a> {
    args: &'a Args,
    journal_dir: &'a str,
    user: Pubkey,
    built: BuiltTrade,
    pre_sol: u64,
    pre_token: u64,
    started: Instant,
    simulation_succeeded: bool,
    simulation_error: Option<String>,
    simulation_units_consumed: Option<u64>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let started = Instant::now();
    let args = Args::parse()?;
    let cfg = Config::load(&args.config)?;
    cfg.validate_for_bot()?;
    validate_live_profile(&cfg)?;
    validate_runtime_unlocks(&cfg)?;

    if args.panic {
        return run_panic_sell(&cfg, &args, started).await;
    }

    let keypair = match catarnith::config::env_lookup("MAYHEM_WALLET_KEYPAIR_BASE58") {
        Some(encoded) => catarnith::keypair_source::decode_base58_keypair(&encoded)
            .map_err(|err| anyhow::anyhow!("failed to decode base58 keypair: {err}"))?,
        None => read_keypair_file(&cfg.wallet_keypair_path)
            .map_err(|err| anyhow::anyhow!("failed to read dedicated canary keypair: {err}"))?,
    };
    let user = keypair.pubkey();
    let mint = Pubkey::from_str(&args.mint).context("invalid mint pubkey")?;

    let primary_url = cfg.rpc_url();
    let fallback_url = optional_distinct_fallback(&primary_url)?;
    let startup_timeout = Duration::from_millis(env_u64(
        "MAYHEM_LIVE_STARTUP_RPC_TIMEOUT_MS",
        DEFAULT_STARTUP_RPC_TIMEOUT_MS,
    )?);
    let startup_retries = env_usize(
        "MAYHEM_LIVE_STARTUP_RPC_RETRIES",
        DEFAULT_STARTUP_RPC_RETRIES,
    )?;
    let startup_retry_delay = Duration::from_millis(env_u64(
        "MAYHEM_LIVE_STARTUP_RPC_RETRY_DELAY_MS",
        DEFAULT_STARTUP_RPC_RETRY_DELAY_MS,
    )?);
    check_rpc_startup(
        "primary RPC",
        &primary_url,
        startup_timeout,
        startup_retries,
        startup_retry_delay,
    )
    .await?;
    if let Some(fallback_url) = &fallback_url {
        check_rpc_startup(
            "fallback RPC",
            fallback_url,
            startup_timeout,
            startup_retries,
            startup_retry_delay,
        )
        .await?;
    }

    let rpc_timeout = Duration::from_millis(env_u64(
        "MAYHEM_LIVE_RPC_TIMEOUT_MS",
        DEFAULT_RPC_TIMEOUT_MS,
    )?);
    let primary_rpc = Arc::new(RpcClient::new_with_timeout_and_commitment(
        primary_url.clone(),
        rpc_timeout,
        CommitmentConfig::confirmed(),
    ));
    let state_rpc = Arc::new(RpcClient::new_with_timeout_and_commitment(
        primary_url.clone(),
        rpc_timeout,
        CommitmentConfig::processed(),
    ));
    let fallback_rpc = RpcClient::new_with_timeout_and_commitment(
        fallback_url.unwrap_or_else(|| primary_url.clone()),
        rpc_timeout,
        CommitmentConfig::confirmed(),
    );

    let pre_sol = state_rpc
        .get_balance(&user)
        .await
        .context("fetch live SOL balance before send")?;
    let max_balance = env_sol_lamports(
        "MAYHEM_LIVE_MAX_BALANCE_SOL",
        env_u64("MAYHEM_LIVE_MAX_BALANCE_LAMPORTS", DEFAULT_MAX_BALANCE)?,
    )?;
    if pre_sol > max_balance {
        bail!(
            "live wallet balance exceeds CTARNITH_LIVE_MAX_BALANCE_SOL={:.4}",
            max_balance as f64 / 1_000_000_000.0
        );
    }

    let built = build_trade(&cfg, state_rpc.clone(), &keypair, user, mint, args.side).await?;
    let pre_token = token_balance_or_zero(state_rpc.as_ref(), &built.token_account).await?;
    let pre_broadcast_simulation = env_bool("MAYHEM_LIVE_PRE_BROADCAST_SIMULATION", true)?;
    let (simulation_succeeded, simulation_error, simulation_units_consumed) =
        if pre_broadcast_simulation {
            let simulation = state_rpc
                .simulate_transaction_with_config(
                    &built.transaction,
                    RpcSimulateTransactionConfig {
                        sig_verify: true,
                        replace_recent_blockhash: false,
                        commitment: Some(CommitmentConfig::processed()),
                        ..RpcSimulateTransactionConfig::default()
                    },
                )
                .await
                .context("simulate signed live transaction before broadcast")?
                .value;
            let error = simulation.err.map(|err| format!("{err:?}"));
            (error.is_none(), error, simulation.units_consumed)
        } else {
            (true, None, None)
        };
    if simulation_error.is_some() {
        let report = base_report(BaseReportInput {
            args: &args,
            journal_dir: &cfg.journal_dir,
            user,
            built,
            pre_sol,
            pre_token,
            started,
            simulation_succeeded,
            simulation_error,
            simulation_units_consumed,
        });
        write_report(&args, &report)?;
        bail!("signed transaction simulation failed; not broadcasting");
    }

    let signature = primary_rpc
        .send_transaction_with_config(
            &built.transaction,
            RpcSendTransactionConfig {
                skip_preflight: true,
                max_retries: Some(env_usize("MAYHEM_LIVE_SEND_MAX_RETRIES", 2)?),
                ..RpcSendTransactionConfig::default()
            },
        )
        .await
        .context("broadcast live transaction")?;

    let confirmation = wait_for_confirmation(
        primary_rpc.as_ref(),
        &fallback_rpc,
        &signature,
        env_commitment(
            "MAYHEM_LIVE_SETTLEMENT_COMMITMENT",
            CommitmentConfig::processed(),
        )?,
        env_u64(
            "MAYHEM_LIVE_CONFIRMATION_TIMEOUT_MS",
            DEFAULT_CONFIRMATION_TIMEOUT_MS,
        )?,
        env_u64(
            "MAYHEM_LIVE_CONFIRMATION_POLL_MS",
            DEFAULT_CONFIRMATION_POLL_MS,
        )?,
        env_bool("MAYHEM_LIVE_PARALLEL_FALLBACK_READS", false)?,
    )
    .await;

    let post_sol = state_rpc.get_balance(&user).await.ok();
    let post_token = token_balance_or_zero(state_rpc.as_ref(), &built.token_account)
        .await
        .ok();

    let (confirmed, slot, status, error) = match confirmation {
        Ok(status) => (
            true,
            Some(status.slot),
            Some(status.confirmation_status),
            None,
        ),
        Err(err) => (false, None, None, Some(err.to_string())),
    };

    let report = LiveExecutionReport {
        ready_to_broadcast: true,
        broadcast_attempted: true,
        confirmed,
        wallet_pubkey: user.to_string(),
        mint: args.mint.clone(),
        side: args.side,
        instruction: built.instruction_name.to_string(),
        input_amount: built.input_amount,
        quoted_output: built.quoted_output,
        protected_minimum_output: built.protected_minimum_output,
        slippage_bps: built.slippage_bps,
        compute_unit_limit: env_u32("MAYHEM_LIVE_COMPUTE_UNIT_LIMIT", DEFAULT_COMPUTE_UNITS)?,
        compute_unit_price_micro_lamports: env_u64(
            "MAYHEM_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS",
            DEFAULT_PRIORITY_MICRO_LAMPORTS,
        )?,
        pre_sol_balance_lamports: pre_sol,
        post_sol_balance_lamports: post_sol,
        pre_token_amount_raw: pre_token,
        post_token_amount_raw: post_token,
        simulation_succeeded,
        simulation_error: None,
        simulation_units_consumed,
        signature: Some(signature.to_string()),
        confirmation_slot: slot,
        confirmation_status: status,
        confirmation_error: error,
        elapsed_ms: started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64,
        journal_dir: cfg.journal_dir.clone(),
        notes: vec![
            "real mainnet transaction broadcast was attempted exactly once".to_string(),
            "do not rerun blindly after a settlement timeout; inspect the signature first"
                .to_string(),
        ],
    };
    record_live_journal(&cfg, &args, &report)?;
    write_report(&args, &report)?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    if !report.confirmed {
        bail!("live transaction was broadcast but did not settle before timeout");
    }
    Ok(())
}

async fn build_trade(
    cfg: &Config,
    rpc: Arc<RpcClient>,
    keypair: &solana_sdk::signature::Keypair,
    user: Pubkey,
    mint: Pubkey,
    side: Side,
) -> Result<BuiltTrade> {
    let client = AsyncPumpClient::new(rpc.clone());
    let sdk = PumpSdk::new();
    let (global_result, fee_config_result, bonding_curve_result) = tokio::join!(
        client.fetch_global(),
        client.fetch_fee_config(),
        client.fetch_bonding_curve(&mint),
    );
    let global = global_result.context("fetch Pump global")?;
    let fee_config = fee_config_result.context("fetch Pump fee config")?;
    let bonding_curve = bonding_curve_result.context("fetch Pump bonding curve")?;

    if bonding_curve.complete {
        bail!("bonding curve is complete; PumpSwap execution is not implemented");
    }
    if require_mayhem_curve_flag(cfg) && !bonding_curve.is_mayhem_mode {
        bail!("refusing live execution because the on-chain curve is not Mayhem mode");
    }
    if bonding_curve.quote_mint != Pubkey::default() {
        bail!("only native-SOL Pump.fun bonding curves are supported by live execution");
    }
    let mint_account = rpc.get_account(&mint).await.context("fetch mint account")?;
    let base_token_program = live_base_token_program(mint_account.owner)?;

    let supply = rpc
        .get_token_supply(&mint)
        .await
        .context("fetch mint supply")?
        .amount
        .parse::<u64>()
        .context("parse mint supply")?;
    let token_account = pda::associated_token(&user, &base_token_program, &mint).0;

    let (instructions, instruction_name, input, quote, protected, slippage_bps) = match side {
        Side::Buy => {
            let spend = cfg.base_buy_lamports;
            let slippage_bps = cfg.max_slippage_bps;
            let quote = buy_token_amount_from_sol_amount(
                &global,
                Some(&fee_config),
                &bonding_curve,
                supply,
                spend,
            )
            .context("quote exact-SOL buy")?;
            let protected = apply_slippage_floor(quote, slippage_bps)?;
            let instructions = sdk
                .buy_exact_quote_in_v2_instructions(
                    &global,
                    &bonding_curve,
                    mint,
                    base_token_program,
                    user,
                    spend,
                    protected,
                )
                .context("Pump SDK could not select fee recipients")?;
            (
                instructions,
                "buy_exact_quote_in_v2",
                spend,
                quote,
                protected,
                slippage_bps,
            )
        }
        Side::Sell => {
            let slippage_bps = env_u32("MAYHEM_LIVE_SELL_SLIPPAGE_BPS", cfg.max_slippage_bps)?;
            let amount = rpc
                .get_token_account_balance(&token_account)
                .await
                .context("fetch canary token balance")?
                .amount
                .parse::<u64>()
                .context("parse canary token balance")?;
            if amount == 0 {
                bail!("canary wallet has no token inventory for this mint");
            }
            let quote = sell_sol_amount_from_token_amount(
                &global,
                Some(&fee_config),
                &bonding_curve,
                supply,
                amount,
            )
            .context("quote full-inventory sell")?;
            let protected = apply_slippage_floor(quote, slippage_bps)?;
            let instructions = sdk
                .sell_v2_instructions(
                    &global,
                    &bonding_curve,
                    mint,
                    base_token_program,
                    user,
                    amount,
                    protected,
                )
                .context("Pump SDK could not select fee recipients")?;
            (
                instructions,
                "sell_v2",
                amount,
                quote,
                protected,
                slippage_bps,
            )
        }
    };

    let transaction = client
        .build_transaction(
            &instructions,
            &user,
            &[keypair],
            Some(ComputeBudget {
                units: Some(env_u32(
                    "MAYHEM_LIVE_COMPUTE_UNIT_LIMIT",
                    DEFAULT_COMPUTE_UNITS,
                )?),
                micro_lamports_per_unit: Some(env_u64(
                    "MAYHEM_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS",
                    DEFAULT_PRIORITY_MICRO_LAMPORTS,
                )?),
            }),
        )
        .await
        .context("build and sign live transaction")?;

    Ok(BuiltTrade {
        transaction,
        instruction_name,
        input_amount: input,
        quoted_output: quote,
        protected_minimum_output: protected,
        slippage_bps,
        token_account,
    })
}

async fn wait_for_confirmation(
    primary: &RpcClient,
    fallback: &RpcClient,
    signature: &Signature,
    commitment: CommitmentConfig,
    timeout_ms: u64,
    poll_ms: u64,
    parallel_fallback_reads: bool,
) -> Result<ConfirmedStatus> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(poll_ms).max(1));
    let poll = Duration::from_millis(poll_ms.max(20));
    let mut last_error = None::<String>;
    let mut primary_empty_polls = 0_u32;

    loop {
        if parallel_fallback_reads {
            for (label, rpc) in [("primary", primary), ("fallback", fallback)] {
                match rpc
                    .get_signature_statuses(std::slice::from_ref(signature))
                    .await
                {
                    Ok(response) => {
                        if let Some(status) = response.value.into_iter().next().flatten() {
                            if let Some(err) = status.err.as_ref() {
                                bail!("transaction failed on-chain via {label}: {err:?}");
                            }
                            if status.satisfies_commitment(commitment) {
                                return Ok(ConfirmedStatus {
                                    slot: status.slot,
                                    confirmation_status: format!(
                                        "{:?}",
                                        status.confirmation_status()
                                    ),
                                });
                            }
                        }
                    }
                    Err(err) => last_error = Some(format!("{label}: {err}")),
                }
            }
        } else {
            let mut primary_had_status = false;
            match primary
                .get_signature_statuses(std::slice::from_ref(signature))
                .await
            {
                Ok(response) => {
                    if let Some(status) = response.value.into_iter().next().flatten() {
                        primary_had_status = true;
                        if let Some(err) = status.err.as_ref() {
                            bail!("transaction failed on-chain via primary: {err:?}");
                        }
                        if status.satisfies_commitment(commitment) {
                            return Ok(ConfirmedStatus {
                                slot: status.slot,
                                confirmation_status: format!("{:?}", status.confirmation_status()),
                            });
                        }
                    }
                }
                Err(err) => last_error = Some(format!("primary: {err}")),
            }
            if primary_had_status {
                primary_empty_polls = 0;
            } else {
                primary_empty_polls = primary_empty_polls.saturating_add(1);
                let remaining = deadline.saturating_duration_since(Instant::now());
                let probe_fallback = primary_empty_polls.is_multiple_of(3)
                    || last_error.is_some()
                    || remaining
                        <= poll
                            .checked_mul(2)
                            .unwrap_or_else(|| Duration::from_millis(poll_ms.max(20)));
                if probe_fallback {
                    match fallback
                        .get_signature_statuses(std::slice::from_ref(signature))
                        .await
                    {
                        Ok(response) => {
                            if let Some(status) = response.value.into_iter().next().flatten() {
                                if let Some(err) = status.err.as_ref() {
                                    bail!("transaction failed on-chain via fallback: {err:?}");
                                }
                                if status.satisfies_commitment(commitment) {
                                    return Ok(ConfirmedStatus {
                                        slot: status.slot,
                                        confirmation_status: format!(
                                            "{:?}",
                                            status.confirmation_status()
                                        ),
                                    });
                                }
                            }
                        }
                        Err(err) => last_error = Some(format!("fallback: {err}")),
                    }
                }
            }
        }
        if Instant::now() >= deadline {
            bail!(
                "settlement timeout after {timeout_ms}ms at commitment {:?}{}",
                commitment.commitment,
                last_error
                    .map(|err| format!("; last status error: {err}"))
                    .unwrap_or_default()
            );
        }
        sleep(poll).await;
    }
}

async fn check_rpc_startup(
    label: &str,
    url: &str,
    timeout: Duration,
    retries: usize,
    retry_delay: Duration,
) -> Result<()> {
    let rpc = RpcClient::new_with_timeout_and_commitment(
        url.to_string(),
        timeout,
        CommitmentConfig::confirmed(),
    );
    let attempts = retries.max(1);
    for attempt in 1..=attempts {
        if rpc.get_latest_blockhash().await.is_ok() {
            return Ok(());
        }
        if attempt < attempts {
            sleep(retry_delay).await;
        }
    }
    bail!(
        "{label} startup health check failed after {attempts} attempts with timeout_ms={}; endpoint redacted",
        timeout.as_millis()
    )
}

fn base_report(input: BaseReportInput<'_>) -> LiveExecutionReport {
    LiveExecutionReport {
        ready_to_broadcast: false,
        broadcast_attempted: false,
        confirmed: false,
        wallet_pubkey: input.user.to_string(),
        mint: input.args.mint.clone(),
        side: input.args.side,
        instruction: input.built.instruction_name.to_string(),
        input_amount: input.built.input_amount,
        quoted_output: input.built.quoted_output,
        protected_minimum_output: input.built.protected_minimum_output,
        slippage_bps: input.built.slippage_bps,
        compute_unit_limit: env_u32("MAYHEM_LIVE_COMPUTE_UNIT_LIMIT", DEFAULT_COMPUTE_UNITS)
            .unwrap_or(DEFAULT_COMPUTE_UNITS),
        compute_unit_price_micro_lamports: env_u64(
            "MAYHEM_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS",
            DEFAULT_PRIORITY_MICRO_LAMPORTS,
        )
        .unwrap_or(DEFAULT_PRIORITY_MICRO_LAMPORTS),
        pre_sol_balance_lamports: input.pre_sol,
        post_sol_balance_lamports: None,
        pre_token_amount_raw: input.pre_token,
        post_token_amount_raw: None,
        simulation_succeeded: input.simulation_succeeded,
        simulation_error: input.simulation_error,
        simulation_units_consumed: input.simulation_units_consumed,
        signature: None,
        confirmation_slot: None,
        confirmation_status: None,
        confirmation_error: None,
        elapsed_ms: input
            .started
            .elapsed()
            .as_millis()
            .min(u128::from(u64::MAX)) as u64,
        journal_dir: input.journal_dir.to_string(),
        notes: vec!["simulation failed; no transaction was broadcast".to_string()],
    }
}

fn record_live_journal(cfg: &Config, args: &Args, report: &LiveExecutionReport) -> Result<()> {
    let journal = Journal::open(&cfg.journal_dir, &cfg.sqlite_path)?;
    let order_id = format!(
        "live-{}-{}-{}",
        match args.side {
            Side::Buy => "buy",
            Side::Sell => "sell",
        },
        now_ms(),
        mint_prefix(&args.mint)
    );
    let order = match args.side {
        Side::Buy => Order::Buy(BuyOrder {
            id: order_id.clone(),
            timestamp_ms: now_ms(),
            mint: args.mint.clone(),
            lamports: report.input_amount,
            source_decision_id: "manual-live-canary".to_string(),
            source_signature: report.signature.clone(),
        }),
        Side::Sell => Order::Sell(SellOrder {
            id: order_id.clone(),
            timestamp_ms: now_ms(),
            mint: args.mint.clone(),
            source_decision_id: "manual-live-canary".to_string(),
            source_signature: report.signature.clone(),
        }),
    };
    let token_delta = report.post_token_amount_raw.map(|post| match args.side {
        Side::Buy => post.saturating_sub(report.pre_token_amount_raw) as u128,
        Side::Sell => report.pre_token_amount_raw.saturating_sub(post) as u128,
    });
    let sol_delta = report
        .post_sol_balance_lamports
        .map(|post| match args.side {
            Side::Buy => report.input_amount,
            Side::Sell => post.saturating_sub(report.pre_sol_balance_lamports),
        });
    let execution = ExecutionReport {
        order_id,
        signature: report.signature.clone(),
        quote_slot: report.confirmation_slot,
        status: if report.confirmed {
            ExecutionStatus::LiveConfirmed
        } else {
            ExecutionStatus::LiveFailed
        },
        requested_lamports: match args.side {
            Side::Buy => report.input_amount,
            Side::Sell => 0,
        },
        filled_lamports: sol_delta,
        filled_token_amount_raw: token_delta,
        fee_lamports: None,
        error: report.confirmation_error.clone(),
        latency_ms: Some(report.elapsed_ms),
    };
    journal.record(JournalKind::Order, &order)?;
    journal.record(JournalKind::Execution, &execution)?;
    journal.record(JournalKind::MetricsSnapshot, report)?;
    Ok(())
}

fn write_report(args: &Args, report: &LiveExecutionReport) -> Result<()> {
    let output = serde_json::to_string_pretty(report)?;
    if let Some(out) = &args.out {
        if let Some(parent) = out.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        fs::write(out, format!("{output}\n"))
            .with_context(|| format!("failed to write {}", out.display()))?;
    }
    Ok(())
}

async fn token_balance_or_zero(rpc: &RpcClient, token_account: &Pubkey) -> Result<u64> {
    match rpc.get_token_account_balance(token_account).await {
        Ok(balance) => balance.amount.parse::<u64>().context("parse token balance"),
        Err(_) => Ok(0),
    }
}

fn validate_live_profile(cfg: &Config) -> Result<()> {
    if cfg.mode != Mode::Live {
        bail!("live executor requires mode='live'");
    }
    cfg.validate_live_risk_envelope("one-shot live executor")?;
    if cfg.wallet_keypair_path.trim().is_empty()
        && cfg
            .wallet_keypair_base58
            .as_ref()
            .map(|s| s.trim().is_empty())
            .unwrap_or(true)
    {
        bail!(
            "live executor requires CTARNITH_WALLET_KEYPAIR_PATH or CTARNITH_WALLET_KEYPAIR_BASE58"
        );
    }
    if !cfg.wallet_keypair_path.trim().is_empty() {
        let wallet_path = PathBuf::from(&cfg.wallet_keypair_path);
        validate_secret_file(&wallet_path, "wallet keypair")?;
        validate_wallet_path(&wallet_path)?;
    } else if let Some(encoded) = cfg.wallet_keypair_base58.as_ref() {
        catarnith::keypair_source::decode_base58_keypair(encoded)
            .map_err(|err| anyhow::anyhow!("invalid wallet_keypair_base58: {err}"))?;
    }
    Ok(())
}

fn validate_runtime_unlocks(cfg: &Config) -> Result<()> {
    // Live arming is config-driven: enable_live_trading=true and
    // require_manual_live_unlock=false. The protective gates (wallet
    // outside the repo, chmod 600 secrets, optional distinct paid fallback RPC,
    // max-balance cap) are enforced separately and are not relaxed here.
    if !cfg.enable_live_trading {
        bail!("live trading is locked: set enable_live_trading=true after paper validation");
    }
    if cfg.require_manual_live_unlock {
        bail!("live executor requires config require_manual_live_unlock=false");
    }
    Ok(())
}

fn validate_wallet_path(path: &Path) -> Result<()> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("wallet keypair does not exist: {}", path.display()))?;
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("workspace root unavailable")?
        .canonicalize()?;
    if canonical.starts_with(repo) {
        bail!("live wallet keypair must be stored outside the repository");
    }
    let filename = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_lowercase();
    if !filename.contains("catarnith")
        || (!filename.contains("canary") && !filename.contains("hot"))
    {
        bail!("wallet filename must contain catarnith and canary or hot");
    }
    Ok(())
}

fn validate_secret_file(path: &Path, label: &str) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("failed to inspect {label}: {}", path.display()))?;
    let mode = metadata.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        bail!("{label} must be owner-only (chmod 600), found mode {mode:o}");
    }
    Ok(())
}

fn optional_distinct_fallback(primary: &str) -> Result<Option<String>> {
    let Some(fallback) = catarnith::config::env_lookup("MAYHEM_FALLBACK_RPC_URL") else {
        return Ok(None);
    };
    let fallback = fallback.trim();
    if fallback.is_empty() {
        return Ok(None);
    }
    if fallback.contains("api.mainnet-beta.solana.com") {
        bail!("fallback RPC must be a paid provider, not the public Solana endpoint");
    }
    if fallback == primary {
        bail!("fallback RPC must be distinct from the primary RPC when configured");
    }
    Ok(Some(fallback.to_string()))
}

fn apply_slippage_floor(value: u64, slippage_bps: u32) -> Result<u64> {
    if value == 0 {
        bail!("quote output is zero");
    }
    if slippage_bps >= 10_000 {
        bail!("slippage must be below 10000 bps");
    }
    let protected = (value as u128).saturating_mul((10_000 - slippage_bps) as u128) / 10_000;
    u64::try_from(protected.max(1)).context("protected output exceeds u64")
}

fn require_mayhem_curve_flag(cfg: &Config) -> bool {
    cfg.require_curve_mayhem_flag && cfg.market == Market::MayhemOnly
}

fn live_base_token_program(owner: Pubkey) -> Result<Pubkey> {
    if owner == constants::SPL_TOKEN_PROGRAM_ID || owner == constants::SPL_TOKEN_2022_PROGRAM_ID {
        Ok(owner)
    } else {
        bail!(
            "live execution supports SPL Token or Token-2022 Pump.fun mints only; mint owner={owner}"
        );
    }
}

fn env_u32(name: &str, default: u32) -> Result<u32> {
    match catarnith::config::env_lookup(name) {
        Some(value) => value
            .parse::<u32>()
            .with_context(|| format!("{name} must be an unsigned integer")),
        None => Ok(default),
    }
}

fn env_u64(name: &str, default: u64) -> Result<u64> {
    match catarnith::config::env_lookup(name) {
        Some(value) => value
            .parse::<u64>()
            .with_context(|| format!("{name} must be an unsigned integer")),
        None => Ok(default),
    }
}

fn env_sol_lamports(name: &str, default: u64) -> Result<u64> {
    match catarnith::config::env_lookup(name) {
        Some(value) => {
            let sol = value
                .parse::<f64>()
                .with_context(|| format!("{name} must be a SOL number"))?;
            if !sol.is_finite() || sol < 0.0 {
                bail!("{name} must be a non-negative SOL number");
            }
            Ok((sol * 1_000_000_000.0).round() as u64)
        }
        None => Ok(default),
    }
}

fn env_usize(name: &str, default: usize) -> Result<usize> {
    match catarnith::config::env_lookup(name) {
        Some(value) => value
            .parse::<usize>()
            .with_context(|| format!("{name} must be an unsigned integer")),
        None => Ok(default),
    }
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match catarnith::config::env_lookup(name) {
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => Ok(true),
            "0" | "false" | "no" | "off" => Ok(false),
            other => bail!("{name} must be a boolean; got {other}"),
        },
        None => Ok(default),
    }
}

fn env_commitment(name: &str, default: CommitmentConfig) -> Result<CommitmentConfig> {
    match catarnith::config::env_lookup(name) {
        Some(value) => match value.trim().to_ascii_lowercase().as_str() {
            "processed" => Ok(CommitmentConfig::processed()),
            "confirmed" => Ok(CommitmentConfig::confirmed()),
            "finalized" => Ok(CommitmentConfig::finalized()),
            other => bail!("{name} must be processed, confirmed, or finalized; got {other}"),
        },
        None => Ok(default),
    }
}

fn mint_prefix(mint: &str) -> String {
    mint.chars().take(8).collect()
}

async fn run_panic_sell(cfg: &Config, args: &Args, started: Instant) -> Result<()> {
    if !matches!(args.side, Side::Sell) {
        bail!("--panic requires --side sell");
    }
    let mint = Pubkey::from_str(&args.mint).context("invalid panic-sell mint pubkey")?;
    let executor = catarnith::live::LivePumpExecutor::new(cfg)
        .await
        .context("construct LivePumpExecutor for panic-sell")?;
    let token_account = pump_rust_client::pda::associated_token(
        &executor_pubkey(&executor),
        &pump_rust_client::constants::SPL_TOKEN_2022_PROGRAM_ID,
        &mint,
    )
    .0;
    let amount = match tokio::time::timeout(
        Duration::from_millis(env_u64("MAYHEM_LIVE_PANIC_BALANCE_TIMEOUT_MS", 500)?),
        executor_token_balance(&executor, &token_account),
    )
    .await
    {
        Ok(Ok(amount)) => amount,
        Ok(Err(err)) => bail!("panic-sell balance read failed: {err}"),
        Err(_) => bail!("panic-sell balance read timed out"),
    };
    if amount == 0 {
        bail!("panic-sell: wallet holds zero of mint {}", args.mint);
    }
    let report = executor
        .panic_sell(&args.mint, u128::from(amount))
        .await
        .context("panic-sell execution failed")?;
    let elapsed_ms = started.elapsed().as_millis() as u64;
    let panic_json = serde_json::json!({
        "origin": "cli_panic_sell",
        "mint": args.mint,
        "side": "sell",
        "status": report.status,
        "signature": report.signature,
        "submitted_token_amount_raw": amount.to_string(),
        "filled_token_amount_raw": report.filled_token_amount_raw.map(|v| v.to_string()),
        "fee_lamports": report.fee_lamports,
        "error": report.error,
        "elapsed_ms": elapsed_ms,
        "timestamp_ms": chrono::Utc::now().timestamp_millis(),
    });
    if let Some(out) = args.out.as_ref() {
        if let Some(parent) = out.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(out, format!("{panic_json}\n"))?;
    }
    println!("{}", serde_json::to_string_pretty(&panic_json)?);
    Ok(())
}

// Helper accessors that work on the public but feature-gated
// `LivePumpExecutor` without leaking its internals through this
// binary's `pub` surface.
fn executor_pubkey(_executor: &catarnith::live::LivePumpExecutor) -> Pubkey {
    // Re-derive from the canonical env so we don't need to expose a
    // private field on the executor. Falls back to system program if
    // the env is missing (which would be a programming error; the
    // executor would have failed to construct).
    let encoded = catarnith::config::env_lookup("MAYHEM_WALLET_KEYPAIR_BASE58");
    if let Some(encoded) = encoded {
        if let Ok(kp) = catarnith::keypair_source::decode_base58_keypair(&encoded) {
            return kp.pubkey();
        }
    }
    if let Some(path) = catarnith::config::env_lookup("MAYHEM_WALLET_KEYPAIR_PATH") {
        if let Ok(kp) = solana_sdk::signature::read_keypair_file(&path) {
            return kp.pubkey();
        }
    }
    Pubkey::default()
}

async fn read_token_balance_via(url: String, token_account: Pubkey, timeout: Duration) -> u64 {
    if url.is_empty() {
        return 0;
    }
    let client = solana_client::nonblocking::rpc_client::RpcClient::new_with_timeout_and_commitment(
        url,
        timeout,
        CommitmentConfig::confirmed(),
    );
    match client.get_token_account_balance(&token_account).await {
        Ok(balance) => balance.amount.parse::<u64>().unwrap_or(0),
        Err(_) => 0,
    }
}

async fn executor_token_balance(
    _executor: &catarnith::live::LivePumpExecutor,
    token_account: &Pubkey,
) -> Result<u64> {
    let primary_url = catarnith::config::env_lookup("MAYHEM_LIVE_PRIMARY_RPC_URL")
        .or_else(|| std::env::var("HELIUS_RPC_URL").ok())
        .unwrap_or_default();
    let fallback_url = catarnith::config::env_lookup("MAYHEM_LIVE_FALLBACK_RPC_URL")
        .or_else(|| catarnith::config::env_lookup("MAYHEM_FALLBACK_RPC_URL"))
        .unwrap_or_default();
    let timeout = Duration::from_millis(env_u64("MAYHEM_LIVE_RPC_TIMEOUT_MS", 900)?);
    let (primary, fallback) = tokio::join!(
        read_token_balance_via(primary_url, *token_account, timeout),
        read_token_balance_via(fallback_url, *token_account, timeout),
    );
    Ok(primary.max(fallback))
}

impl Args {
    fn parse() -> Result<Self> {
        let mut config = None;
        let mut side = None;
        let mut mint = None;
        let mut out = None;
        let mut panic = false;
        let mut args = env::args().skip(1);
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--config" => config = args.next().map(PathBuf::from),
                "--side" => {
                    side = Some(match args.next().as_deref() {
                        Some("buy") => Side::Buy,
                        Some("sell") => Side::Sell,
                        Some(other) => bail!("unsupported side: {other}"),
                        None => bail!("--side requires buy or sell"),
                    });
                }
                "--mint" => mint = args.next(),
                "--out" => out = args.next().map(PathBuf::from),
                "--panic" => panic = true,
                "-h" | "--help" => {
                    println!(
                        "Usage: live_execute --config <path> --side <buy|sell> --mint <pubkey> [--out <path>] [--panic]"
                    );
                    std::process::exit(0);
                }
                other => bail!("unknown argument: {other}"),
            }
        }
        Ok(Self {
            config: config.context("--config is required")?,
            side: side.context("--side is required")?,
            mint: mint.context("--mint is required")?,
            out,
            panic,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn slippage_floor_is_adverse_and_nonzero() {
        assert_eq!(apply_slippage_floor(1_000, 1_000).unwrap(), 900);
        assert_eq!(apply_slippage_floor(1, 1_000).unwrap(), 1);
        assert!(apply_slippage_floor(0, 100).is_err());
        assert!(apply_slippage_floor(1_000, 10_000).is_err());
    }

    #[test]
    fn mayhem_curve_flag_is_required_only_for_mayhem_market() {
        let mut cfg = Config {
            require_curve_mayhem_flag: true,
            market: Market::MayhemOnly,
            ..Config::default()
        };
        assert!(require_mayhem_curve_flag(&cfg));

        cfg.market = Market::NonMayhemOnly;
        assert!(!require_mayhem_curve_flag(&cfg));

        cfg.market = Market::AllPumpfun;
        assert!(!require_mayhem_curve_flag(&cfg));
    }

    #[test]
    fn live_base_token_program_accepts_spl_and_token_2022() {
        assert_eq!(
            live_base_token_program(constants::SPL_TOKEN_PROGRAM_ID).unwrap(),
            constants::SPL_TOKEN_PROGRAM_ID
        );
        assert_eq!(
            live_base_token_program(constants::SPL_TOKEN_2022_PROGRAM_ID).unwrap(),
            constants::SPL_TOKEN_2022_PROGRAM_ID
        );
        assert!(live_base_token_program(Pubkey::new_unique()).is_err());
    }

    #[test]
    fn fallback_rpc_is_optional_for_broadcast() {
        let _guard = env_lock().lock().expect("env test lock should not poison");
        env::remove_var("MAYHEM_FALLBACK_RPC_URL");
        env::remove_var("CTARNITH_FALLBACK_RPC_URL");
        env::set_var("CTARNITH_FALLBACK_RPC_URL", "");
        assert_eq!(
            optional_distinct_fallback("https://mainnet.helius-rpc.com").unwrap(),
            None
        );
        env::remove_var("MAYHEM_FALLBACK_RPC_URL");
        env::remove_var("CTARNITH_FALLBACK_RPC_URL");
    }

    #[test]
    fn rejects_public_or_same_fallback_for_broadcast() {
        let _guard = env_lock().lock().expect("env test lock should not poison");
        env::remove_var("CTARNITH_FALLBACK_RPC_URL");
        env::set_var(
            "CTARNITH_FALLBACK_RPC_URL",
            "https://api.mainnet-beta.solana.com",
        );
        assert!(optional_distinct_fallback("https://mainnet.helius-rpc.com").is_err());

        env::set_var("CTARNITH_FALLBACK_RPC_URL", "https://same.example");
        assert!(optional_distinct_fallback("https://same.example").is_err());

        env::set_var("CTARNITH_FALLBACK_RPC_URL", "https://paid.example");
        assert_eq!(
            optional_distinct_fallback("https://mainnet.helius-rpc.com").unwrap(),
            Some("https://paid.example".to_string())
        );
        env::remove_var("MAYHEM_FALLBACK_RPC_URL");
        env::remove_var("CTARNITH_FALLBACK_RPC_URL");
    }
}
