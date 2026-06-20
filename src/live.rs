use crate::{config::Config, executor::Order, types::ExecutionReport};
use anyhow::Result;
#[cfg(not(feature = "live-executor"))]
use solana_pubkey::Pubkey;

#[cfg(feature = "live-executor")]
mod imp {
    use super::*;
    use crate::{
        config::TOKEN_2022_PROGRAM,
        types::{BuyOrder, ExecutionStatus, Mode, SellOrder},
    };
    use anyhow::{bail, Context};
    use fs2::FileExt;
    use pump_rust_client::{
        constants,
        math::bonding_curve::{
            buy_token_amount_from_sol_amount, sell_sol_amount_from_token_amount,
        },
        pda,
        state::{BondingCurve, FeeConfig, Global},
        AsyncPumpClient, ComputeBudget, PumpClientError, PumpSdk,
    };
    use solana_client::{
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    };
    use solana_sdk::{commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature};
    use std::{
        env, fs,
        fs::{File, OpenOptions},
        future::Future,
        io::Write,
        os::unix::fs::PermissionsExt,
        path::{Path, PathBuf},
        str::FromStr,
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::{sync::Mutex, time::sleep};

    const DEFAULT_RPC_TIMEOUT_MS: u64 = 900;
    const DEFAULT_STARTUP_RPC_TIMEOUT_MS: u64 = 5_000;
    const DEFAULT_STARTUP_RPC_RETRIES: usize = 3;
    const DEFAULT_STARTUP_RPC_RETRY_DELAY_MS: u64 = 150;
    const DEFAULT_PUMP_CONFIG_CACHE_TTL_MS: u64 = 60_000;
    const ENTRY_STATE_BUILD_RESERVE_MS: i64 = 250;
    const ENTRY_DEADLINE_GRACE_MS: i64 = 500;
    const ENTRY_STATE_RETRY_MIN_MS: u64 = 20;
    const ENTRY_STATE_RETRY_MAX_MS: u64 = 100;

    pub struct LivePumpExecutor {
        cfg: Config,
        rpc: Arc<RpcClient>,
        state_rpc: Arc<RpcClient>,
        fallback_rpc: RpcClient,
        keypair: solana_sdk::signature::Keypair,
        user: Pubkey,
        _wallet_lock: File,
        compute_units: u32,
        priority_micro_lamports: u64,
        send_max_retries: usize,
        send_timeout_ms: u64,
        rpc_timeout: Duration,
        token_balance_timeout: Duration,
        sell_build_timeout: Duration,
        wait_for_buy_confirmation: bool,
        confirmation_timeout_ms: u64,
        sell_confirmation_timeout_ms: u64,
        confirmation_poll_ms: u64,
        settlement_commitment: CommitmentConfig,
        pre_broadcast_simulation: bool,
        skip_buy_pre_token_balance: bool,
        skip_post_trade_balances: bool,
        parallel_fallback_reads: bool,
        sell_slippage_bps: u32,
        pump_config_cache_ttl: Duration,
        pump_config_cache: Mutex<Option<CachedPumpConfig>>,
        /// Optional Jito block-engine RPC URL. When set, panic-sell
        /// appends a tip transfer and broadcasts to this endpoint in
        /// addition to the regular RPCs.
        jito_block_engine_url: Option<String>,
        /// Tip account pubkey for the configured Jito region. Required
        /// when `jito_block_engine_url` is set.
        jito_tip_account: Option<Pubkey>,
        /// Tip amount in lamports. Defaults to 10_000 (0.00001 SOL).
        jito_tip_lamports: u64,
        /// Send timeout used by panic-sell. Default 350ms.
        panic_send_timeout: Duration,
        /// Whether panic-sell waits for on-chain confirmation before
        /// returning. Default true. Set `MAYHEM_LIVE_PANIC_SELL_NO_WAIT=1`
        /// to restore the old fire-and-forget behavior.
        panic_sell_wait_for_confirmation: bool,
        /// Authenticated `api.jup.ag` key for the Jupiter sell fallback.
        /// `None` (env unset or empty) disables the fallback gracefully —
        /// a failed force-sell then logs "remains on-chain" as before.
        jupiter_api_key: Option<String>,
        /// Per-leg timeout for the Jupiter fallback (quote, swap build,
        /// broadcast, confirmation). Generous by design — this only fires
        /// after the fast local path already failed, so reliability beats
        /// latency. Default 10s.
        jupiter_timeout: Duration,
    }

    struct CachedPumpConfig {
        global: Global,
        fee_config: FeeConfig,
        fetched_at: Instant,
    }

    struct BuiltTrade {
        transaction: solana_sdk::transaction::Transaction,
        input_amount: u64,
        token_account: Pubkey,
        expected_buy_token_amount_raw: Option<u64>,
        expected_sell_lamports: Option<u64>,
        sell_amount_raw: Option<u64>,
    }

    /// Token amount for a concurrent Jupiter sell raced against the local
    /// pump v2 sell broadcast.
    struct JupiterSellLegParams {
        amount: u64,
    }

    struct ConfirmedStatus {
        slot: u64,
    }

    impl LivePumpExecutor {
        pub async fn new(cfg: &Config) -> Result<Self> {
            validate_live_profile(cfg)?;
            validate_runtime_unlocks(cfg)?;
            let resolved = crate::keypair_source::resolve(cfg)
                .map_err(|err| anyhow::anyhow!("failed to resolve live keypair: {err}"))?;
            let keypair = resolved.keypair;
            let user = resolved.pubkey;
            let wallet_lock = acquire_wallet_lock(&user)?;
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
                cfg.live.rpc_timeout_ms,
            )?);
            let rpc = Arc::new(RpcClient::new_with_timeout_and_commitment(
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
            let pump_config = fetch_pump_config_snapshot(
                &AsyncPumpClient::new(Arc::clone(&state_rpc)),
                startup_timeout,
                startup_retries,
                startup_retry_delay,
            )
            .await?;

            Ok(Self {
                cfg: cfg.clone(),
                rpc,
                state_rpc,
                fallback_rpc,
                keypair,
                user,
                _wallet_lock: wallet_lock,
                compute_units: env_u32(
                    "MAYHEM_LIVE_COMPUTE_UNIT_LIMIT",
                    cfg.live.compute_unit_limit,
                )?,
                priority_micro_lamports: env_u64(
                    "MAYHEM_LIVE_COMPUTE_UNIT_PRICE_MICROLAMPORTS",
                    cfg.live.compute_unit_price_microlamports,
                )?,
                send_max_retries: env_usize(
                    "MAYHEM_LIVE_SEND_MAX_RETRIES",
                    cfg.live.send_max_retries,
                )?,
                send_timeout_ms: env_u64("MAYHEM_LIVE_SEND_TIMEOUT_MS", cfg.live.send_timeout_ms)?,
                rpc_timeout,
                token_balance_timeout: Duration::from_millis(env_u64(
                    "MAYHEM_LIVE_TOKEN_BALANCE_TIMEOUT_MS",
                    rpc_timeout.as_millis().min(u128::from(u64::MAX)) as u64,
                )?),
                sell_build_timeout: Duration::from_millis(env_u64(
                    "MAYHEM_LIVE_SELL_BUILD_TIMEOUT_MS",
                    DEFAULT_RPC_TIMEOUT_MS,
                )?),
                wait_for_buy_confirmation: env_bool("MAYHEM_LIVE_WAIT_FOR_BUY_CONFIRMATION", true)?,
                confirmation_timeout_ms: env_u64(
                    "MAYHEM_LIVE_CONFIRMATION_TIMEOUT_MS",
                    cfg.live.confirmation_timeout_ms,
                )?,
                sell_confirmation_timeout_ms: env_u64(
                    "MAYHEM_LIVE_SELL_CONFIRMATION_TIMEOUT_MS",
                    cfg.live.sell_confirmation_timeout_ms,
                )?,
                confirmation_poll_ms: env_u64(
                    "MAYHEM_LIVE_CONFIRMATION_POLL_MS",
                    cfg.live.confirmation_poll_ms,
                )?,
                settlement_commitment: env_commitment(
                    "MAYHEM_LIVE_SETTLEMENT_COMMITMENT",
                    parse_commitment(&cfg.live.settlement_commitment)?,
                )?,
                pre_broadcast_simulation: env_bool(
                    "MAYHEM_LIVE_PRE_BROADCAST_SIMULATION",
                    cfg.live.pre_broadcast_simulation,
                )?,
                skip_buy_pre_token_balance: env_bool(
                    "MAYHEM_LIVE_SKIP_BUY_PRE_TOKEN_BALANCE",
                    true,
                )?,
                skip_post_trade_balances: env_bool("MAYHEM_LIVE_SKIP_POST_TRADE_BALANCES", false)?,
                parallel_fallback_reads: env_bool("MAYHEM_LIVE_PARALLEL_FALLBACK_READS", false)?,
                sell_slippage_bps: env_u32(
                    "MAYHEM_LIVE_SELL_SLIPPAGE_BPS",
                    cfg.live.sell_slippage_bps.unwrap_or(cfg.max_slippage_bps),
                )?,
                pump_config_cache_ttl: Duration::from_millis(env_u64(
                    "MAYHEM_LIVE_PUMP_CONFIG_CACHE_TTL_MS",
                    DEFAULT_PUMP_CONFIG_CACHE_TTL_MS,
                )?),
                pump_config_cache: Mutex::new(Some(CachedPumpConfig {
                    global: pump_config.0,
                    fee_config: pump_config.1,
                    fetched_at: Instant::now(),
                })),
                jito_block_engine_url: crate::config::env_lookup(
                    "MAYHEM_LIVE_JITO_BLOCK_ENGINE_URL",
                )
                .or_else(|| {
                    cfg.live
                        .jito_block_engine_url
                        .as_ref()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                }),
                jito_tip_account: crate::config::env_lookup("MAYHEM_LIVE_JITO_TIP_ACCOUNT")
                    .or_else(|| {
                        cfg.live
                            .jito_tip_account
                            .as_ref()
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                    })
                    .map(|s| Pubkey::from_str(&s))
                    .transpose()
                    .context("jito tip account is not a valid pubkey")?,
                jito_tip_lamports: env_u64(
                    "MAYHEM_LIVE_JITO_TIP_LAMPORTS",
                    cfg.live.jito_tip_lamports,
                )?,
                panic_send_timeout: Duration::from_millis(env_u64(
                    "MAYHEM_LIVE_PANIC_SEND_TIMEOUT_MS",
                    350,
                )?),
                panic_sell_wait_for_confirmation: env_bool(
                    "MAYHEM_LIVE_PANIC_SELL_WAIT_FOR_CONFIRMATION",
                    true,
                )?,
                jupiter_api_key: env::var("JUP_API_KEY")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty()),
                jupiter_timeout: Duration::from_millis(env_u64(
                    "MAYHEM_LIVE_JUPITER_TIMEOUT_MS",
                    cfg.live.jupiter_timeout_ms,
                )?),
            })
        }

        pub fn wallet(&self) -> Option<Pubkey> {
            Some(self.user)
        }

        pub async fn finalized_failure_for_signature(
            &self,
            signature: &str,
        ) -> Result<Option<String>> {
            let signature = Signature::from_str(signature).context("invalid signature")?;
            let request_timeout = self.rpc_timeout.max(Duration::from_millis(1));
            let mut results = Vec::new();
            if self.parallel_fallback_reads {
                let (primary_result, fallback_result) = tokio::join!(
                    rpc_call_bounded(
                        self.rpc
                            .get_signature_statuses(std::slice::from_ref(&signature)),
                        request_timeout,
                        "primary signature status",
                    ),
                    rpc_call_bounded(
                        self.fallback_rpc
                            .get_signature_statuses(std::slice::from_ref(&signature)),
                        request_timeout,
                        "fallback signature status",
                    ),
                );
                results.push(primary_result);
                results.push(fallback_result);
            } else {
                let primary_result = rpc_call_bounded(
                    self.rpc
                        .get_signature_statuses(std::slice::from_ref(&signature)),
                    request_timeout,
                    "primary signature status",
                )
                .await;
                let should_probe_fallback = primary_result
                    .as_ref()
                    .map(|response| response.value.iter().all(Option::is_none))
                    .unwrap_or(true);
                results.push(primary_result);
                if should_probe_fallback {
                    results.push(
                        rpc_call_bounded(
                            self.fallback_rpc
                                .get_signature_statuses(std::slice::from_ref(&signature)),
                            request_timeout,
                            "fallback signature status",
                        )
                        .await,
                    );
                }
            }

            let mut saw_success = false;
            let mut saw_status = false;
            for response in results.into_iter().flatten() {
                if let Some(status) = response.value.into_iter().next().flatten() {
                    saw_status = true;
                    if let Some(err) = status.err {
                        return Ok(Some(format!("{err:?}")));
                    }
                    saw_success = true;
                }
            }

            if saw_success || saw_status {
                return Ok(None);
            }
            Ok(None)
        }

        pub async fn execute(
            &self,
            order: &Order,
            sell_token_amount_raw: Option<u128>,
            buy_slippage_bps: Option<u32>,
        ) -> Result<ExecutionReport> {
            let started = Instant::now();
            self.execute_inner(order, sell_token_amount_raw, buy_slippage_bps, started)
                .await
        }

        async fn execute_inner(
            &self,
            order: &Order,
            sell_token_amount_raw: Option<u128>,
            buy_slippage_bps: Option<u32>,
            started: Instant,
        ) -> Result<ExecutionReport> {
            ensure_buy_entry_deadline(order, self.cfg.entry_deadline_ms, "before_balance_read")?;
            let pre_sol = if matches!(order, Order::Buy(_)) {
                let balance = self
                    .state_rpc
                    .get_balance(&self.user)
                    .await
                    .context("fetch live SOL balance before send")?;
                let max_balance = env_u64(
                    "MAYHEM_LIVE_MAX_BALANCE_LAMPORTS",
                    self.cfg.live.max_balance_lamports,
                )?;
                if balance > max_balance {
                    bail!(
                        "live wallet balance exceeds CTARNITH_LIVE_MAX_BALANCE_LAMPORTS={max_balance}"
                    );
                }
                balance
            } else {
                0
            };

            ensure_buy_entry_deadline(order, self.cfg.entry_deadline_ms, "before_trade_build")?;
            let mint = Pubkey::from_str(order.mint()).context("invalid order mint pubkey")?;
            let build_result = if matches!(order, Order::Sell(_)) {
                match tokio::time::timeout(
                    self.sell_build_timeout,
                    self.build_trade(order, mint, sell_token_amount_raw, None, None, false),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => bail!(
                        "sell_build_timeout_elapsed timeout_ms={}",
                        self.sell_build_timeout.as_millis()
                    ),
                }
            } else {
                self.build_trade(
                    order,
                    mint,
                    sell_token_amount_raw,
                    None,
                    buy_slippage_bps,
                    false,
                )
                .await
            };
            let built = match build_result {
                Ok(built) => built,
                Err(error)
                    if matches!(order, Order::Sell(_))
                        && error
                            .to_string()
                            .contains("live wallet has no token inventory for this mint") =>
                {
                    return Ok(self.report(
                        order,
                        None,
                        None,
                        ExecutionStatus::LiveReconciled,
                        None,
                        Some(0),
                        sell_token_amount_raw,
                        started,
                        Some(0),
                    ));
                }
                Err(error) => return Err(error),
            };
            let pre_token = if self.skip_post_trade_balances
                || (matches!(order, Order::Buy(_)) && self.skip_buy_pre_token_balance)
            {
                0
            } else {
                self.token_balance_across_rpcs(&built.token_account).await?
            };
            if self.pre_broadcast_simulation {
                ensure_buy_entry_deadline(order, self.cfg.entry_deadline_ms, "before_simulation")?;
                let simulation = self
                    .state_rpc
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
                if let Some(err) = simulation.err {
                    return Ok(self.report(
                        order,
                        None,
                        None,
                        ExecutionStatus::Errored,
                        Some(format!("pre_broadcast_simulation_failed:{err:?}")),
                        None,
                        None,
                        started,
                        None,
                    ));
                }
            }

            // Optionally race local pump v2 sells against a full Jupiter
            // sell. Buys are deliberately single-path: racing a Jupiter
            // buy here can spend ~2x the intended SOL if both legs land.
            let jupiter_leg = self.jupiter_sell_leg_params(order, &built);
            let local = self.broadcast_and_confirm_local(order, built, pre_sol, pre_token, started);
            match jupiter_leg {
                Some(params) => {
                    let key = match self.jupiter_api_key.as_deref() {
                        Some(key) => key,
                        None => return local.await,
                    };
                    let mint_str = order.mint().to_string();
                    let jupiter = async move {
                        crate::jupiter::jupiter_sell(
                            self.rpc.as_ref(),
                            &self.keypair,
                            self.user,
                            &mint_str,
                            params.amount,
                            key,
                            self.sell_slippage_bps,
                            self.jupiter_timeout,
                        )
                        .await
                    };
                    self.race_local_and_jupiter(local, jupiter).await
                }
                None => local.await,
            }
        }

        /// Optional concurrent Jupiter sell leg. Buys intentionally return
        /// `None` so entries never double-spend SOL; sells use the built
        /// sell amount and still race local `sell_v2` against Jupiter.
        fn jupiter_sell_leg_params(
            &self,
            order: &Order,
            built: &BuiltTrade,
        ) -> Option<JupiterSellLegParams> {
            self.jupiter_api_key.as_ref()?;
            match order {
                Order::Buy(_) => None,
                Order::Sell(_) => built
                    .sell_amount_raw
                    .map(|amount| JupiterSellLegParams { amount }),
            }
        }

        /// Run the local broadcast+confirm future against a Jupiter swap
        /// future. The first leg to produce a report wins and is returned
        /// immediately; if a leg errors we fall back to awaiting the other.
        /// Because both are real broadcasts already in flight, the losing
        /// leg may still confirm on-chain — we warn loudly so the operator
        /// can reconcile a possible double fill.
        async fn race_local_and_jupiter(
            &self,
            local: impl std::future::Future<Output = Result<ExecutionReport>>,
            jupiter: impl std::future::Future<Output = Result<ExecutionReport>>,
        ) -> Result<ExecutionReport> {
            tokio::pin!(local);
            tokio::pin!(jupiter);
            tokio::select! {
                biased;
                result = &mut local => match result {
                    Ok(report) => {
                        tracing::warn!(
                            "jupiter_race local leg won sig={:?}; jupiter leg may still land \
                             on-chain (possible double fill)",
                            report.signature
                        );
                        Ok(report)
                    }
                    Err(err) => {
                        tracing::warn!("jupiter_race local leg failed ({err}); awaiting jupiter leg");
                        (&mut jupiter).await
                    }
                },
                result = &mut jupiter => match result {
                    Ok(report) => {
                        tracing::warn!(
                            "jupiter_race jupiter leg won sig={:?}; local leg may still land \
                             on-chain (possible double fill)",
                            report.signature
                        );
                        Ok(report)
                    }
                    Err(err) => {
                        tracing::warn!("jupiter_race jupiter leg failed ({err}); awaiting local leg");
                        (&mut local).await
                    }
                },
            }
        }

        /// Broadcast the locally built pump v2 transaction across the
        /// primary/fallback RPCs and confirm it. Extracted verbatim from
        /// `execute_inner` so it can be raced against a Jupiter swap.
        async fn broadcast_and_confirm_local(
            &self,
            order: &Order,
            built: BuiltTrade,
            pre_sol: u64,
            pre_token: u64,
            started: Instant,
        ) -> Result<ExecutionReport> {
            ensure_buy_entry_deadline(order, self.cfg.entry_deadline_ms, "before_broadcast")?;
            let expected_signature = built
                .transaction
                .signatures
                .first()
                .copied()
                .context("signed live transaction has no signature")?;
            let send_config = RpcSendTransactionConfig {
                // The speed profile may skip local simulation. Avoid paying
                // for an additional RPC-side simulation before broadcast.
                skip_preflight: true,
                max_retries: Some(self.send_max_retries),
                ..RpcSendTransactionConfig::default()
            };
            let primary_timeout =
                send_timeout_for_order(order, self.cfg.entry_deadline_ms, self.send_timeout_ms)?;
            let fallback_timeout =
                send_timeout_for_order(order, self.cfg.entry_deadline_ms, self.send_timeout_ms)?;
            let fallback_config = RpcSendTransactionConfig {
                skip_preflight: true,
                max_retries: Some(self.send_max_retries),
                ..RpcSendTransactionConfig::default()
            };
            let (primary_result, fallback_result) = tokio::join!(
                send_transaction_bounded(
                    self.rpc.as_ref(),
                    &built.transaction,
                    send_config,
                    primary_timeout,
                    "primary",
                ),
                send_transaction_bounded(
                    &self.fallback_rpc,
                    &built.transaction,
                    fallback_config,
                    fallback_timeout,
                    "fallback",
                ),
            );
            let signature = match (primary_result, fallback_result) {
                (Ok(signature), _) | (_, Ok(signature)) => signature,
                (Err(primary_error), Err(fallback_error)) => {
                    return self
                        .reconcile_ambiguous_broadcast(
                            order,
                            expected_signature,
                            built,
                            started,
                            format!(
                                "broadcast failed on primary ({primary_error}) and fallback \
                                 ({fallback_error})"
                            ),
                        )
                        .await;
                }
            };

            if matches!(order, Order::Buy(_)) && !self.wait_for_buy_confirmation {
                return Ok(self.report(
                    order,
                    Some(signature.to_string()),
                    None,
                    ExecutionStatus::LiveSubmitted,
                    Some(
                        "confirmation_pending: broadcast accepted; sell loop will reconcile \
                         inventory"
                            .to_string(),
                    ),
                    Some(built.input_amount),
                    built.expected_buy_token_amount_raw.map(u128::from),
                    started,
                    Some(0),
                ));
            }

            let confirmation_timeout_ms = if matches!(order, Order::Sell(_)) {
                self.sell_confirmation_timeout_ms
            } else {
                self.confirmation_timeout_ms
            };
            let confirmation = wait_for_confirmation(
                self.rpc.as_ref(),
                &self.fallback_rpc,
                &signature,
                self.settlement_commitment,
                confirmation_timeout_ms,
                self.confirmation_poll_ms,
                self.rpc_timeout,
                self.parallel_fallback_reads,
            )
            .await;

            match confirmation {
                Ok(status) => {
                    let (post_sol, post_token) = if self.skip_post_trade_balances {
                        (None, None)
                    } else {
                        let (post_sol, post_token) = tokio::join!(
                            self.state_rpc.get_balance(&self.user),
                            self.token_balance_across_rpcs(&built.token_account),
                        );
                        (post_sol.ok(), post_token.ok())
                    };
                    Ok(self.confirmed_report(
                        order,
                        signature,
                        status.slot,
                        pre_sol,
                        post_sol,
                        pre_token,
                        post_token,
                        built,
                        started,
                    ))
                }
                Err(err) => {
                    let error = err.to_string();
                    if error.contains("transaction failed on-chain") {
                        return Ok(self.report(
                            order,
                            Some(signature.to_string()),
                            None,
                            ExecutionStatus::LiveFailed,
                            Some(error),
                            Some(0),
                            Some(0),
                            started,
                            None,
                        ));
                    }
                    self.reconcile_ambiguous_broadcast(order, signature, built, started, error)
                        .await
                }
            }
        }

        /// Skip simulation, skip preflight, broadcast immediately, and
        /// return the moment the RPC accepts the transaction.
        ///
        /// `sell_token_amount_raw` is the *only* source of truth for how
        /// many tokens to sell; the executor does not re-read the
        /// balance, so a stale value produces a smaller partial sell
        /// (the chain rejects more than the wallet holds).
        ///
        /// When `MAYHEM_LIVE_JITO_BLOCK_ENGINE_URL` is set the call also
        /// broadcasts to that endpoint and appends a Jito tip transfer
        /// as the last instruction.
        pub async fn panic_sell(
            &self,
            mint: &str,
            sell_token_amount_raw: u128,
        ) -> Result<ExecutionReport> {
            self.panic_sell_inner(mint, sell_token_amount_raw, None)
                .await
        }

        /// Same as `panic_sell`, but allows overriding the configured
        /// sell slippage. Used for the force-sell retry path in the
        /// catarnith TUI when normal slippage keeps getting rejected.
        pub async fn panic_sell_with_slippage(
            &self,
            mint: &str,
            sell_token_amount_raw: u128,
            slippage_bps: u32,
        ) -> Result<ExecutionReport> {
            self.panic_sell_inner(mint, sell_token_amount_raw, Some(slippage_bps))
                .await
        }

        /// Last-resort sell through Jupiter. Only call this after the
        /// local panic-sell and force-sell have both failed — the local
        /// path is faster and is always preferred. Bails when no
        /// `JUP_API_KEY` is configured so the caller can fall through to
        /// the existing "remains on-chain" log.
        pub async fn jupiter_sell(&self, mint: &str, amount_raw: u128) -> Result<ExecutionReport> {
            let key = self
                .jupiter_api_key
                .as_deref()
                .context("jupiter fallback disabled: JUP_API_KEY not set")?;
            let amount = u64::try_from(amount_raw).context("jupiter sell amount exceeds u64")?;
            crate::jupiter::jupiter_sell(
                self.rpc.as_ref(),
                &self.keypair,
                self.user,
                mint,
                amount,
                key,
                self.sell_slippage_bps,
                self.jupiter_timeout,
            )
            .await
        }

        async fn panic_sell_inner(
            &self,
            mint: &str,
            sell_token_amount_raw: u128,
            slippage_bps: Option<u32>,
        ) -> Result<ExecutionReport> {
            let started = Instant::now();
            let order = Order::Sell(crate::executor::order_from_decision_sell(
                mint,
                "panic-sell",
            ));
            let mint_pubkey = Pubkey::from_str(mint).context("invalid panic-sell mint pubkey")?;
            let amount =
                u64::try_from(sell_token_amount_raw).context("panic-sell amount exceeds u64")?;

            // Build the sell trade. We reuse `build_trade` so the same
            // curve/fee/blockhash logic applies, but we keep the
            // *requested* amount instead of letting build_trade
            // re-read the balance and possibly lower it.
            let mut built = self
                .build_trade(
                    &order,
                    mint_pubkey,
                    Some(sell_token_amount_raw),
                    slippage_bps,
                    None,
                    true,
                )
                .await?;

            // Optionally append a Jito tip as the last instruction and
            // re-sign. We cannot reach back to the original `Vec<Instruction>`
            // because `build_trade` only stored the compiled message, so
            // we re-fetch the global + curve + blockhash to rebuild the
            // sell instructions and tack the tip on the end. This is the
            // slow Jito path; the regular panic-sell path (no tip) keeps
            // the original transaction as `built.transaction` and is
            // what most calls will use.
            if let Some(tip_account) = self.jito_tip_account {
                match self
                    .rebuild_with_jito_tip(mint_pubkey, amount, tip_account)
                    .await
                {
                    Ok(jito_built) => built.transaction = jito_built,
                    Err(err) => {
                        tracing::warn!(
                            "panic-sell jito rebuild failed, falling back to no-tip: {err}"
                        );
                    }
                }
            }

            // Snapshot pre-sell SOL for proceeds estimation. A failure
            // here is non-fatal; we fall back to the curve quote.
            let pre_sol = tokio::time::timeout(self.rpc_timeout, self.rpc.get_balance(&self.user))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or(0);

            // Race the local Jito/RPC broadcast against a full Jupiter
            // sell when a key is configured. Both are real broadcasts;
            // first confirmation wins and the loser may still land
            // on-chain (logged in `race_local_and_jupiter`).
            let local = self.panic_sell_broadcast_local(
                order.clone(),
                built,
                sell_token_amount_raw,
                pre_sol,
                started,
            );
            match self.jupiter_api_key.as_deref() {
                Some(key) => {
                    let mint_str = mint.to_string();
                    let jupiter = async move {
                        crate::jupiter::jupiter_sell(
                            self.rpc.as_ref(),
                            &self.keypair,
                            self.user,
                            &mint_str,
                            amount,
                            key,
                            slippage_bps.unwrap_or(self.sell_slippage_bps),
                            self.jupiter_timeout,
                        )
                        .await
                    };
                    self.race_local_and_jupiter(local, jupiter).await
                }
                None => local.await,
            }
        }

        /// Broadcast a built panic-sell across the primary/fallback RPCs
        /// (and Jito when configured) and confirm it. Extracted from
        /// `panic_sell_inner` so it can be raced against a Jupiter sell.
        async fn panic_sell_broadcast_local(
            &self,
            order: Order,
            built: BuiltTrade,
            sell_token_amount_raw: u128,
            pre_sol: u64,
            started: Instant,
        ) -> Result<ExecutionReport> {
            // Send with skip_preflight and zero retries. By default we
            // wait for on-chain confirmation before returning; set
            // MAYHEM_LIVE_PANIC_SELL_NO_WAIT=1 for the old behavior.
            let send_config = RpcSendTransactionConfig {
                skip_preflight: true,
                max_retries: Some(0),
                preflight_commitment: None,
                ..RpcSendTransactionConfig::default()
            };
            // Try the regular RPCs and the Jito block-engine in
            // parallel. The first `Ok(sig)` short-circuits the join.
            // We clone the transaction for the Jito path so the
            // primary/fallback futures can borrow it independently.
            let primary = send_transaction_bounded(
                self.rpc.as_ref(),
                &built.transaction,
                send_config,
                self.panic_send_timeout,
                "panic-sell primary",
            );
            let fallback = send_transaction_bounded(
                &self.fallback_rpc,
                &built.transaction,
                send_config,
                self.panic_send_timeout,
                "panic-sell fallback",
            );
            let jito = self.jito_block_engine_url.as_ref().map(|url| {
                let client = RpcClient::new_with_timeout_and_commitment(
                    url.clone(),
                    self.panic_send_timeout,
                    self.settlement_commitment,
                );
                let tx = built.transaction.clone();
                let cfg = send_config;
                let timeout = self.panic_send_timeout;
                async move {
                    send_transaction_bounded(&client, &tx, cfg, timeout, "panic-sell jito").await
                }
            });

            let (primary_res, fallback_res, jito_res) = match jito {
                Some(j) => tokio::join!(primary, fallback, j),
                None => {
                    let (p, f) = tokio::join!(primary, fallback);
                    (p, f, Err(anyhow::anyhow!("jito_disabled")))
                }
            };

            let signature = match (primary_res, fallback_res, jito_res) {
                (Ok(sig), _, _) | (_, Ok(sig), _) | (_, _, Ok(sig)) => sig,
                (Err(e1), Err(e2), Err(e3)) if e3.to_string() == "jito_disabled" => {
                    bail!("panic-sell broadcast failed: primary={e1}, fallback={e2}")
                }
                (Err(e1), Err(e2), Err(e3)) => {
                    bail!("panic-sell broadcast failed: primary={e1}, fallback={e2}, jito={e3}")
                }
            };

            if !self.panic_sell_wait_for_confirmation {
                let report = self.report(
                    &order,
                    Some(signature.to_string()),
                    None,
                    ExecutionStatus::LiveSubmitted,
                    Some(
                        "panic_sell: skip_preflight, no client-side simulation, \
                         submitted to one or more RPCs"
                            .to_string(),
                    ),
                    Some(0),
                    Some(sell_token_amount_raw),
                    started,
                    Some(self.jito_tip_lamports),
                );
                return Ok(report);
            }

            // Wait for on-chain confirmation so the TUI can tell the
            // operator whether the position was actually liquidated.
            let confirmation = wait_for_confirmation(
                self.rpc.as_ref(),
                &self.fallback_rpc,
                &signature,
                self.settlement_commitment,
                self.sell_confirmation_timeout_ms,
                self.confirmation_poll_ms,
                self.rpc_timeout,
                self.parallel_fallback_reads,
            )
            .await;

            match confirmation {
                Ok(status) => {
                    let post_sol =
                        tokio::time::timeout(self.rpc_timeout, self.rpc.get_balance(&self.user))
                            .await
                            .ok()
                            .and_then(|r| r.ok());
                    let proceeds = if pre_sol > 0 {
                        post_sol
                            .map(|post| post.saturating_sub(pre_sol))
                            .filter(|delta| *delta > 0)
                    } else {
                        None
                    }
                    .or(built.expected_sell_lamports)
                    .unwrap_or(0);
                    Ok(self.report(
                        &order,
                        Some(signature.to_string()),
                        Some(status.slot),
                        ExecutionStatus::LiveConfirmed,
                        None,
                        Some(proceeds),
                        Some(sell_token_amount_raw),
                        started,
                        Some(self.jito_tip_lamports),
                    ))
                }
                Err(err) => Err(err.context(format!(
                    "panic-sell submitted {signature} but confirmation failed"
                ))),
            }
        }

        /// Read the live wallet's token balance for `mint` in
        /// raw base units. Used as a fallback when a buy
        /// confirmation didn't include `filled_token_amount_raw`
        /// — without this we'd sell 0 and leave the position
        /// stranded on-chain.
        ///
        /// Returns `Ok(0)` when the ATA does not exist (fresh
        /// wallet, never received) and propagates any other RPC
        /// error. The caller decides whether 0 is acceptable.
        pub async fn fetch_token_balance(&self, mint: &str) -> Result<u128> {
            let mint_pubkey = Pubkey::from_str(mint).context("invalid mint pubkey")?;
            let (ata, _) = pump_rust_client::pda::associated_token(
                &self.user,
                &pump_rust_client::constants::SPL_TOKEN_2022_PROGRAM_ID,
                &mint_pubkey,
            );
            match self.rpc.get_token_account_balance(&ata).await {
                Ok(balance) => balance
                    .amount
                    .parse::<u128>()
                    .context("parse token balance as u128"),
                Err(error) => {
                    let s = error.to_string();
                    if is_fresh_account_visibility_error(&s) {
                        Ok(0)
                    } else {
                        Err(error).context("fetch live token balance")
                    }
                }
            }
        }

        /// Re-fetch pump state and rebuild the sell with a Jito tip
        /// appended. Returns an error on any failure; the caller falls
        /// back to the non-tip transaction.
        async fn rebuild_with_jito_tip(
            &self,
            mint: Pubkey,
            amount: u64,
            tip_account: Pubkey,
        ) -> Result<solana_sdk::transaction::Transaction> {
            let client = AsyncPumpClient::new(self.state_rpc.clone());
            let sdk = PumpSdk::new();
            let (pump_config_result, bonding_curve_result, blockhash_result) = tokio::join!(
                self.fetch_pump_config(&client),
                client.fetch_bonding_curve(&mint),
                self.state_rpc.get_latest_blockhash(),
            );
            let (global, fee_config) = pump_config_result?;
            let bonding_curve = bonding_curve_result?;
            let blockhash = blockhash_result?;

            let quote = sell_sol_amount_from_token_amount(
                &global,
                Some(&fee_config),
                &bonding_curve,
                global.token_total_supply,
                amount,
            )
            .context("quote panic-sell with jito")?;
            let protected = apply_slippage_floor(quote, self.sell_slippage_bps)?;
            let mut instructions = sdk
                .sell_v2_instructions(
                    &global,
                    &bonding_curve,
                    mint,
                    constants::SPL_TOKEN_PROGRAM_ID,
                    self.user,
                    amount,
                    protected,
                )
                .context("Pump SDK could not build panic-sell with jito")?;
            instructions.push(solana_sdk::system_instruction::transfer(
                &self.user,
                &tip_account,
                self.jito_tip_lamports,
            ));
            let tx = client.build_transaction_with_blockhash(
                &instructions,
                &self.user,
                &[&self.keypair],
                blockhash,
                Some(ComputeBudget {
                    units: Some(self.compute_units),
                    micro_lamports_per_unit: Some(self.priority_micro_lamports),
                }),
            );
            Ok(tx)
        }

        async fn build_trade(
            &self,
            order: &Order,
            mint: Pubkey,
            requested_sell_amount_raw: Option<u128>,
            requested_sell_slippage_bps: Option<u32>,
            requested_buy_slippage_bps: Option<u32>,
            trust_sell_amount: bool,
        ) -> Result<BuiltTrade> {
            let client = AsyncPumpClient::new(self.state_rpc.clone());
            let sdk = PumpSdk::new();
            let token_account =
                pda::associated_token(&self.user, &constants::SPL_TOKEN_2022_PROGRAM_ID, &mint).0;

            if let Order::Sell(SellOrder { .. }) = order {
                let (
                    wallet_amount_result,
                    pump_config_result,
                    bonding_curve_result,
                    blockhash_result,
                ) = tokio::join!(
                    self.token_balance_across_rpcs(&token_account),
                    self.fetch_pump_config(&client),
                    self.fetch_bonding_curve_for_order(&client, order, &mint),
                    self.state_rpc.get_latest_blockhash(),
                );
                let wallet_amount = wallet_amount_result?;
                let amount = if trust_sell_amount {
                    requested_sell_amount_raw
                        .map(u64::try_from)
                        .transpose()
                        .context("sell amount exceeds u64")?
                        .unwrap_or(wallet_amount)
                } else {
                    requested_sell_amount_raw
                        .map(u64::try_from)
                        .transpose()
                        .context("sell amount exceeds u64")?
                        .unwrap_or(wallet_amount)
                        .min(wallet_amount)
                };
                if amount == 0 {
                    bail!("live wallet has no token inventory for this mint");
                }

                let (global, fee_config) = pump_config_result?;
                let bonding_curve = bonding_curve_result?;
                let recent_blockhash = blockhash_result.context("fetch recent blockhash")?;

                if bonding_curve.complete {
                    bail!("bonding curve is complete; PumpSwap live execution is not implemented");
                }
                if bonding_curve.quote_mint != Pubkey::default() {
                    bail!("only native-SOL Mayhem curves are supported by live execution");
                }

                let quote = sell_sol_amount_from_token_amount(
                    &global,
                    Some(&fee_config),
                    &bonding_curve,
                    global.token_total_supply,
                    amount,
                )
                .context("quote live sell")?;
                let sell_slippage_bps =
                    requested_sell_slippage_bps.unwrap_or(self.sell_slippage_bps);
                let protected = apply_slippage_floor(quote, sell_slippage_bps)?;
                let instructions = sdk
                    .sell_v2_instructions(
                        &global,
                        &bonding_curve,
                        mint,
                        constants::SPL_TOKEN_PROGRAM_ID,
                        self.user,
                        amount,
                        protected,
                    )
                    .context("Pump SDK could not select fee recipients")?;

                let transaction = client.build_transaction_with_blockhash(
                    &instructions,
                    &self.user,
                    &[&self.keypair],
                    recent_blockhash,
                    Some(ComputeBudget {
                        units: Some(self.compute_units),
                        micro_lamports_per_unit: Some(self.priority_micro_lamports),
                    }),
                );

                return Ok(BuiltTrade {
                    transaction,
                    input_amount: amount,
                    token_account,
                    expected_buy_token_amount_raw: None,
                    expected_sell_lamports: Some(quote),
                    sell_amount_raw: Some(amount),
                });
            }

            let state_fetch_started = Instant::now();
            let (
                pump_config_result,
                bonding_curve_result,
                mint_account_result,
                supply_result,
                blockhash_result,
            ) = tokio::join!(
                self.fetch_pump_config(&client),
                self.fetch_bonding_curve_for_order(&client, order, &mint),
                self.fetch_fresh_rpc_state(order, "mint_account", || {
                    self.state_rpc.get_account(&mint)
                }),
                self.fetch_fresh_rpc_state(order, "mint_supply", || {
                    self.state_rpc.get_token_supply(&mint)
                }),
                self.state_rpc.get_latest_blockhash(),
            );
            let (global, fee_config) = pump_config_result?;
            let bonding_curve = bonding_curve_result?;
            let recent_blockhash = blockhash_result.context("fetch recent blockhash")?;

            if bonding_curve.complete {
                bail!("bonding curve is complete; PumpSwap live execution is not implemented");
            }
            if matches!(order, Order::Buy(_))
                && self.cfg.require_curve_mayhem_flag
                && !bonding_curve.is_mayhem_mode
            {
                bail!("refusing live execution because the on-chain curve is not Mayhem mode");
            }
            if bonding_curve.quote_mint != Pubkey::default() {
                bail!("only native-SOL Mayhem curves are supported by live execution");
            }
            let mint_account = mint_account_result.context("fetch mint account")?;
            if mint_account.owner.to_string() != TOKEN_2022_PROGRAM {
                bail!("live execution currently supports Token-2022 Mayhem mints only");
            }

            let supply = supply_result
                .context("fetch mint supply")?
                .amount
                .parse::<u64>()
                .context("parse mint supply")?;

            let (
                instructions,
                input,
                expected_buy_token_amount_raw,
                expected_sell_lamports,
                sell_amount_raw,
            ) = match order {
                Order::Buy(BuyOrder { lamports, .. }) => {
                    let quote = buy_token_amount_from_sol_amount(
                        &global,
                        Some(&fee_config),
                        &bonding_curve,
                        supply,
                        *lamports,
                    )
                    .context("quote exact-SOL buy")?;
                    let buy_slippage_bps =
                        requested_buy_slippage_bps.unwrap_or(self.cfg.max_slippage_bps);
                    let protected = apply_slippage_floor(quote, buy_slippage_bps)?;
                    tracing::info!(
                        "buy_build_diag mint={mint} spend_lamports={} quote_tokens={quote} \
                         min_tokens_out={protected} slippage_bps={buy_slippage_bps} \
                         v_token_reserves={} v_quote_reserves={} real_token_reserves={} \
                         supply={supply} state_fetch_to_build_ms={}",
                        *lamports,
                        bonding_curve.virtual_token_reserves,
                        bonding_curve.virtual_quote_reserves,
                        bonding_curve.real_token_reserves,
                        state_fetch_started.elapsed().as_millis(),
                    );
                    let instructions = sdk
                        .buy_exact_quote_in_v2_instructions(
                            &global,
                            &bonding_curve,
                            mint,
                            constants::SPL_TOKEN_PROGRAM_ID,
                            self.user,
                            *lamports,
                            protected,
                        )
                        .context("Pump SDK could not select fee recipients")?;
                    (instructions, *lamports, Some(quote), None, None)
                }
                Order::Sell(_) => unreachable!("sell orders return through the fast sell path"),
            };

            let transaction = client.build_transaction_with_blockhash(
                &instructions,
                &self.user,
                &[&self.keypair],
                recent_blockhash,
                Some(ComputeBudget {
                    units: Some(self.compute_units),
                    micro_lamports_per_unit: Some(self.priority_micro_lamports),
                }),
            );

            Ok(BuiltTrade {
                transaction,
                input_amount: input,
                token_account,
                expected_buy_token_amount_raw,
                expected_sell_lamports,
                sell_amount_raw,
            })
        }

        async fn fetch_pump_config(&self, client: &AsyncPumpClient) -> Result<(Global, FeeConfig)> {
            {
                let cache = self.pump_config_cache.lock().await;
                if let Some(cached) = cache.as_ref() {
                    if cached.fetched_at.elapsed() < self.pump_config_cache_ttl {
                        return Ok((cached.global.clone(), cached.fee_config.clone()));
                    }
                }
            }

            let (global_result, fee_config_result) =
                tokio::join!(client.fetch_global(), client.fetch_fee_config());
            let global = global_result.context("fetch Pump global")?;
            let fee_config = fee_config_result.context("fetch Pump fee config")?;
            let mut cache = self.pump_config_cache.lock().await;
            if let Some(cached) = cache.as_ref() {
                if cached.fetched_at.elapsed() < self.pump_config_cache_ttl {
                    return Ok((cached.global.clone(), cached.fee_config.clone()));
                }
            }
            *cache = Some(CachedPumpConfig {
                global: global.clone(),
                fee_config: fee_config.clone(),
                fetched_at: Instant::now(),
            });
            Ok((global, fee_config))
        }

        async fn reconcile_ambiguous_broadcast(
            &self,
            order: &Order,
            signature: Signature,
            built: BuiltTrade,
            started: Instant,
            error: String,
        ) -> Result<ExecutionReport> {
            let wallet_amount = self.token_balance_across_rpcs(&built.token_account).await?;
            match order {
                Order::Buy(_) if wallet_amount > 0 => Ok(self.report(
                    order,
                    Some(signature.to_string()),
                    None,
                    ExecutionStatus::LiveConfirmed,
                    None,
                    Some(built.input_amount),
                    Some(u128::from(wallet_amount)),
                    started,
                    Some(0),
                )),
                Order::Buy(_) => Ok(self.report(
                    order,
                    Some(signature.to_string()),
                    None,
                    ExecutionStatus::LiveSubmitted,
                    Some(format!("confirmation_pending: {error}")),
                    Some(built.input_amount),
                    built.expected_buy_token_amount_raw.map(u128::from),
                    started,
                    Some(0),
                )),
                Order::Sell(_) if wallet_amount == 0 => Ok(self.report(
                    order,
                    Some(signature.to_string()),
                    None,
                    ExecutionStatus::LiveReconciled,
                    None,
                    built.expected_sell_lamports,
                    built.sell_amount_raw.map(u128::from),
                    started,
                    Some(0),
                )),
                Order::Sell(_) => Ok(self.report(
                    order,
                    Some(signature.to_string()),
                    None,
                    ExecutionStatus::LiveFailed,
                    Some(error),
                    None,
                    None,
                    started,
                    None,
                )),
            }
        }

        async fn token_balance_across_rpcs(&self, token_account: &Pubkey) -> Result<u64> {
            if !self.parallel_fallback_reads {
                match rpc_call_bounded(
                    token_balance_or_zero(self.state_rpc.as_ref(), token_account),
                    self.token_balance_timeout,
                    "primary token balance",
                )
                .await
                {
                    Ok(amount) if amount > 0 => return Ok(amount),
                    Ok(primary_amount) => {
                        let fallback = rpc_call_bounded(
                            token_balance_or_zero(&self.fallback_rpc, token_account),
                            self.token_balance_timeout,
                            "fallback token balance",
                        )
                        .await
                        .unwrap_or_default();
                        return Ok(primary_amount.max(fallback));
                    }
                    Err(primary) => {
                        return rpc_call_bounded(
                            token_balance_or_zero(&self.fallback_rpc, token_account),
                            self.token_balance_timeout,
                            "fallback token balance",
                        )
                        .await
                        .with_context(|| {
                            format!("primary token balance unavailable ({primary:#})")
                        });
                    }
                }
            }

            let (primary, fallback) = tokio::join!(
                rpc_call_bounded(
                    token_balance_or_zero(self.state_rpc.as_ref(), token_account),
                    self.token_balance_timeout,
                    "primary token balance",
                ),
                rpc_call_bounded(
                    token_balance_or_zero(&self.fallback_rpc, token_account),
                    self.token_balance_timeout,
                    "fallback token balance",
                ),
            );
            match (primary, fallback) {
                (Ok(primary), Ok(fallback)) => Ok(primary.max(fallback)),
                (Ok(amount), Err(_)) | (Err(_), Ok(amount)) => Ok(amount),
                (Err(primary), Err(fallback)) => Err(anyhow::anyhow!(
                    "token balance unavailable on primary ({primary:#}) and fallback \
                     ({fallback:#})"
                )),
            }
        }

        async fn fetch_bonding_curve_for_order(
            &self,
            client: &AsyncPumpClient,
            order: &Order,
            mint: &Pubkey,
        ) -> Result<BondingCurve> {
            let mut attempt = 0_u32;
            loop {
                match client.fetch_bonding_curve(mint).await {
                    Ok(curve) => return Ok(curve),
                    Err(error @ PumpClientError::AccountNotFound { .. })
                        if matches!(order, Order::Buy(_)) =>
                    {
                        let remaining_ms = buy_entry_state_remaining_ms(
                            order,
                            crate::types::now_ms(),
                            self.cfg.entry_deadline_ms,
                        )
                        .unwrap_or_default();
                        if remaining_ms <= 0 {
                            bail!(
                                "fresh_curve_visibility_deadline_elapsed attempts={} last_error={}",
                                attempt + 1,
                                error
                            );
                        }
                        let delay_ms = fresh_state_retry_delay_ms(attempt, remaining_ms as u64);
                        attempt = attempt.saturating_add(1);
                        sleep(Duration::from_millis(delay_ms)).await;
                    }
                    Err(error) => {
                        return Err(error).context("fetch Pump bonding curve");
                    }
                }
            }
        }

        async fn fetch_fresh_rpc_state<T, E, Op, Fut>(
            &self,
            order: &Order,
            label: &str,
            mut operation: Op,
        ) -> Result<T>
        where
            E: std::fmt::Display,
            Op: FnMut() -> Fut,
            Fut: Future<Output = std::result::Result<T, E>>,
        {
            let mut attempt = 0_u32;
            loop {
                match operation().await {
                    Ok(value) => return Ok(value),
                    Err(error)
                        if matches!(order, Order::Buy(_))
                            && is_fresh_account_visibility_error(&error.to_string()) =>
                    {
                        let remaining_ms = buy_entry_state_remaining_ms(
                            order,
                            crate::types::now_ms(),
                            self.cfg.entry_deadline_ms,
                        )
                        .unwrap_or_default();
                        if remaining_ms <= 0 {
                            bail!(
                                "fresh_{label}_visibility_deadline_elapsed attempts={} \
                                 last_error={error}",
                                attempt + 1
                            );
                        }
                        let delay_ms = fresh_state_retry_delay_ms(attempt, remaining_ms as u64);
                        attempt = attempt.saturating_add(1);
                        sleep(Duration::from_millis(delay_ms)).await;
                    }
                    Err(error) => return Err(anyhow::anyhow!("{error}")),
                }
            }
        }

        #[allow(clippy::too_many_arguments)]
        fn confirmed_report(
            &self,
            order: &Order,
            signature: Signature,
            slot: u64,
            pre_sol: u64,
            post_sol: Option<u64>,
            pre_token: u64,
            post_token: Option<u64>,
            built: BuiltTrade,
            started: Instant,
        ) -> ExecutionReport {
            let (filled_lamports, filled_tokens, fee_lamports) = match order {
                Order::Buy(_) => {
                    let fee = post_sol
                        .map(|post| {
                            pre_sol
                                .saturating_sub(post)
                                .saturating_sub(built.input_amount)
                        })
                        .unwrap_or_default();
                    let token_delta = post_token
                        .map(|post| post.saturating_sub(pre_token))
                        .filter(|delta| *delta > 0)
                        .or(built.expected_buy_token_amount_raw);
                    (
                        Some(built.input_amount),
                        token_delta.map(u128::from),
                        Some(fee),
                    )
                }
                Order::Sell(_) => {
                    let proceeds = post_sol
                        .map(|post| post.saturating_sub(pre_sol))
                        .filter(|delta| *delta > 0)
                        .or(built.expected_sell_lamports);
                    (
                        proceeds,
                        built.sell_amount_raw.map(u128::from).or_else(|| {
                            post_token.map(|post| pre_token.saturating_sub(post) as u128)
                        }),
                        Some(0),
                    )
                }
            };
            self.report(
                order,
                Some(signature.to_string()),
                Some(slot),
                ExecutionStatus::LiveConfirmed,
                None,
                filled_lamports,
                filled_tokens,
                started,
                fee_lamports,
            )
        }

        #[allow(clippy::too_many_arguments)]
        fn report(
            &self,
            order: &Order,
            signature: Option<String>,
            quote_slot: Option<u64>,
            status: ExecutionStatus,
            error: Option<String>,
            filled_lamports: Option<u64>,
            filled_token_amount_raw: Option<u128>,
            started: Instant,
            fee_lamports: Option<u64>,
        ) -> ExecutionReport {
            ExecutionReport {
                order_id: order.id().to_string(),
                signature,
                quote_slot,
                status,
                requested_lamports: match order {
                    Order::Buy(order) => order.lamports,
                    Order::Sell(_) => 0,
                },
                filled_lamports,
                filled_token_amount_raw,
                fee_lamports,
                error,
                latency_ms: Some(started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64),
            }
        }
    }

    async fn wait_for_confirmation(
        primary: &RpcClient,
        fallback: &RpcClient,
        signature: &Signature,
        commitment: CommitmentConfig,
        timeout_ms: u64,
        poll_ms: u64,
        rpc_timeout: Duration,
        parallel_fallback_reads: bool,
    ) -> Result<ConfirmedStatus> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(poll_ms).max(1));
        let poll = Duration::from_millis(poll_ms.max(20));
        let mut last_error = None::<String>;
        let mut primary_empty_polls = 0_u32;

        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                bail!(
                    "settlement timeout after {timeout_ms}ms at commitment {:?}{}",
                    commitment.commitment,
                    last_error
                        .map(|err| format!("; last status error: {err}"))
                        .unwrap_or_default()
                );
            }
            let request_timeout = rpc_timeout.min(remaining).max(Duration::from_millis(1));
            if parallel_fallback_reads {
                let (primary_result, fallback_result) = tokio::join!(
                    rpc_call_bounded(
                        primary.get_signature_statuses(std::slice::from_ref(signature)),
                        request_timeout,
                        "primary signature status",
                    ),
                    rpc_call_bounded(
                        fallback.get_signature_statuses(std::slice::from_ref(signature)),
                        request_timeout,
                        "fallback signature status",
                    ),
                );
                for (label, result) in [("primary", primary_result), ("fallback", fallback_result)]
                {
                    match result {
                        Ok(response) => {
                            if let Some(status) = response.value.into_iter().next().flatten() {
                                if let Some(err) = status.err.as_ref() {
                                    bail!("transaction failed on-chain via {label}: {err:?}");
                                }
                                if status.satisfies_commitment(commitment) {
                                    return Ok(ConfirmedStatus { slot: status.slot });
                                }
                            }
                        }
                        Err(err) => last_error = Some(format!("{label}: {err}")),
                    }
                }
            } else {
                let primary_result = rpc_call_bounded(
                    primary.get_signature_statuses(std::slice::from_ref(signature)),
                    request_timeout,
                    "primary signature status",
                )
                .await;
                let mut primary_had_status = false;
                match primary_result {
                    Ok(response) => {
                        if let Some(status) = response.value.into_iter().next().flatten() {
                            primary_had_status = true;
                            if let Some(err) = status.err.as_ref() {
                                bail!("transaction failed on-chain via primary: {err:?}");
                            }
                            if status.satisfies_commitment(commitment) {
                                return Ok(ConfirmedStatus { slot: status.slot });
                            }
                        }
                    }
                    Err(err) => last_error = Some(format!("primary: {err}")),
                }

                if primary_had_status {
                    primary_empty_polls = 0;
                } else {
                    primary_empty_polls = primary_empty_polls.saturating_add(1);
                    let probe_fallback = primary_empty_polls % 3 == 0
                        || last_error.is_some()
                        || remaining
                            <= poll
                                .checked_mul(2)
                                .unwrap_or_else(|| Duration::from_millis(poll_ms.max(20)));
                    if probe_fallback {
                        match rpc_call_bounded(
                            fallback.get_signature_statuses(std::slice::from_ref(signature)),
                            request_timeout,
                            "fallback signature status",
                        )
                        .await
                        {
                            Ok(response) => {
                                if let Some(status) = response.value.into_iter().next().flatten() {
                                    if let Some(err) = status.err.as_ref() {
                                        bail!("transaction failed on-chain via fallback: {err:?}");
                                    }
                                    if status.satisfies_commitment(commitment) {
                                        return Ok(ConfirmedStatus { slot: status.slot });
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

    async fn rpc_call_bounded<T, E, F>(
        future: F,
        timeout_duration: Duration,
        label: &str,
    ) -> Result<T>
    where
        E: std::fmt::Display,
        F: Future<Output = std::result::Result<T, E>>,
    {
        match tokio::time::timeout(timeout_duration, future).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(anyhow::anyhow!("{label} failed: {error}")),
            Err(_) => bail!("{label} timeout after {}ms", timeout_duration.as_millis()),
        }
    }

    async fn send_transaction_bounded(
        rpc: &RpcClient,
        transaction: &solana_sdk::transaction::Transaction,
        config: RpcSendTransactionConfig,
        timeout_duration: Duration,
        label: &str,
    ) -> Result<Signature> {
        match tokio::time::timeout(
            timeout_duration,
            rpc.send_transaction_with_config(transaction, config),
        )
        .await
        {
            Ok(result) => result.with_context(|| format!("{label} broadcast failed")),
            Err(_) => bail!(
                "{label} broadcast timeout after {}ms; transaction state is ambiguous",
                timeout_duration.as_millis()
            ),
        }
    }

    fn send_timeout_for_order(
        order: &Order,
        entry_deadline_ms: i64,
        configured_timeout_ms: u64,
    ) -> Result<Duration> {
        if !matches!(order, Order::Buy(_)) {
            return Ok(Duration::from_millis(configured_timeout_ms.max(1)));
        }
        let remaining_ms =
            buy_entry_broadcast_remaining_ms(order, crate::types::now_ms(), entry_deadline_ms)
                .unwrap_or_default();
        if remaining_ms <= 0 {
            bail!("entry deadline elapsed before broadcast retry");
        }
        Ok(Duration::from_millis(configured_timeout_ms.max(1)))
    }

    fn acquire_wallet_lock(user: &Pubkey) -> Result<File> {
        let lock_dir = env::var_os("MAYHEM_LIVE_WALLET_LOCK_DIR")
            .map(PathBuf::from)
            .unwrap_or_else(env::temp_dir);
        fs::create_dir_all(&lock_dir)
            .with_context(|| format!("create live wallet lock directory {}", lock_dir.display()))?;
        let lock_path = lock_dir.join(format!("mayhem-live-wallet-{user}.lock"));
        let mut lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)
            .with_context(|| format!("open live wallet lock {}", lock_path.display()))?;
        FileExt::try_lock_exclusive(&lock_file).with_context(|| {
            format!(
                "another live executor already holds wallet {}; stop every other bot or wallet \
                 automation before starting V1",
                user
            )
        })?;
        lock_file
            .set_len(0)
            .context("truncate live wallet lock metadata")?;
        writeln!(lock_file, "wallet={user}\npid={}", std::process::id())
            .context("write live wallet lock metadata")?;
        Ok(lock_file)
    }

    async fn fetch_pump_config_snapshot(
        client: &AsyncPumpClient,
        timeout: Duration,
        retries: usize,
        retry_delay: Duration,
    ) -> Result<(Global, FeeConfig)> {
        let attempts = retries.max(1);
        let mut last_error = None;
        for attempt in 1..=attempts {
            let result = tokio::time::timeout(timeout, async {
                let (global, fee_config) =
                    tokio::join!(client.fetch_global(), client.fetch_fee_config());
                Ok::<_, anyhow::Error>((
                    global.context("fetch Pump global")?,
                    fee_config.context("fetch Pump fee config")?,
                ))
            })
            .await;
            match result {
                Ok(Ok(config)) => return Ok(config),
                Ok(Err(error)) => last_error = Some(error.to_string()),
                Err(_) => {
                    last_error = Some(format!(
                        "Pump config startup timeout after {}ms",
                        timeout.as_millis()
                    ))
                }
            }
            if attempt < attempts {
                sleep(retry_delay).await;
            }
        }
        bail!(
            "Pump config startup fetch failed after {attempts} attempts: {}",
            last_error.unwrap_or_else(|| "unknown error".to_string())
        )
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

    async fn token_balance_or_zero(rpc: &RpcClient, token_account: &Pubkey) -> Result<u64> {
        match rpc.get_token_account_balance(token_account).await {
            Ok(balance) => balance.amount.parse::<u64>().context("parse token balance"),
            Err(error) if is_fresh_account_visibility_error(&error.to_string()) => Ok(0),
            Err(error) => Err(error).context("fetch live token balance"),
        }
    }

    fn order_timestamp_ms(order: &Order) -> i64 {
        match order {
            Order::Buy(order) => order.timestamp_ms,
            Order::Sell(order) => order.timestamp_ms,
        }
    }

    fn ensure_buy_entry_deadline(order: &Order, deadline_ms: i64, phase: &str) -> Result<()> {
        if !matches!(order, Order::Buy(_)) {
            return Ok(());
        }
        let now = crate::types::now_ms();
        let timestamp = order_timestamp_ms(order);
        if now < timestamp {
            bail!("entry_order_timestamp_is_in_the_future phase={phase}");
        }
        let age_ms = now.saturating_sub(timestamp);
        let effective_deadline_ms = buy_entry_effective_deadline_ms(deadline_ms);
        if age_ms > effective_deadline_ms {
            bail!(
                "entry_deadline_elapsed phase={phase} age_ms={age_ms} \
                 deadline_ms={deadline_ms} grace_ms={ENTRY_DEADLINE_GRACE_MS}"
            );
        }
        Ok(())
    }

    fn buy_entry_effective_deadline_ms(deadline_ms: i64) -> i64 {
        deadline_ms.saturating_add(ENTRY_DEADLINE_GRACE_MS)
    }

    fn buy_entry_state_remaining_ms(order: &Order, now_ms: i64, deadline_ms: i64) -> Option<i64> {
        let Order::Buy(order) = order else {
            return None;
        };
        let effective_deadline_ms = buy_entry_effective_deadline_ms(deadline_ms);
        Some(
            order
                .timestamp_ms
                .saturating_add(effective_deadline_ms)
                .saturating_sub(ENTRY_STATE_BUILD_RESERVE_MS)
                .saturating_sub(now_ms),
        )
    }

    fn buy_entry_broadcast_remaining_ms(
        order: &Order,
        now_ms: i64,
        deadline_ms: i64,
    ) -> Option<i64> {
        let Order::Buy(order) = order else {
            return None;
        };
        let effective_deadline_ms = buy_entry_effective_deadline_ms(deadline_ms);
        Some(
            order
                .timestamp_ms
                .saturating_add(effective_deadline_ms)
                .saturating_sub(now_ms),
        )
    }

    fn fresh_state_retry_delay_ms(attempt: u32, remaining_ms: u64) -> u64 {
        let stepped = ENTRY_STATE_RETRY_MIN_MS
            .saturating_mul(u64::from(attempt).saturating_add(1))
            .min(ENTRY_STATE_RETRY_MAX_MS);
        stepped.min(remaining_ms.max(1))
    }

    fn is_fresh_account_visibility_error(error: &str) -> bool {
        let error = error.to_ascii_lowercase();
        error.contains("accountnotfound")
            || error.contains("account not found")
            || error.contains("could not find account")
    }

    fn validate_live_profile(cfg: &Config) -> Result<()> {
        // Paper mode short-circuits the risk-envelope + wallet
        // checks. The paper executor is allowed to run without
        // a real keypair, without paid-RPC acks, and without
        // any unlock file. This makes the TUI a useful
        // read-only debugger for live config files without
        // requiring the operator to own a funded wallet.
        if cfg.mode == Mode::Paper {
            return Ok(());
        }
        cfg.validate_live_risk_envelope("autonomous live executor")?;
        let has_base58 = crate::config::env_var(
            "CTARNITH_WALLET_KEYPAIR_BASE58",
            "MAYHEM_WALLET_KEYPAIR_BASE58",
        )
        .is_ok()
            || cfg
                .wallet_keypair_base58
                .as_ref()
                .map(|s| !s.trim().is_empty())
                .unwrap_or(false);
        if cfg.wallet_keypair_path.trim().is_empty() && !has_base58 {
            bail!(
                "live executor requires CTARNITH_WALLET_KEYPAIR_PATH or CTARNITH_WALLET_KEYPAIR_BASE58"
            );
        }
        if !cfg.wallet_keypair_path.trim().is_empty() {
            let wallet_path = PathBuf::from(&cfg.wallet_keypair_path);
            validate_secret_file(&wallet_path, "wallet keypair")?;
            validate_wallet_path(&wallet_path)?;
        } else {
            // Base58 path: validate the encoded secret shape but skip
            // path-based checks (the key never touches disk).
            if let Some(encoded) = cfg.wallet_keypair_base58.as_ref() {
                crate::keypair_source::decode_base58_keypair(encoded)
                    .map_err(|err| anyhow::anyhow!("invalid wallet_keypair_base58: {err}"))?;
            }
        }
        Ok(())
    }

    fn validate_runtime_unlocks(cfg: &Config) -> Result<()> {
        // Paper mode never broadcasts, so the live arming checks are
        // unnecessary. The executor short-circuits to a non-broadcasting
        // path before this matters; the validation here is a safety net
        // for live mode.
        if cfg.mode == Mode::Paper {
            return Ok(());
        }
        // Live arming is config-driven: `enable_live_trading=true` and
        // `require_manual_live_unlock=false` must both be set in the
        // active config (or via their MAYHEM_LIVE_* env overrides). The
        // genuinely protective gates (wallet outside the repo, chmod 600
        // secrets, distinct paid fallback RPC, and the max-balance cap)
        // are enforced elsewhere and are not relaxed here.
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
        let Some(fallback) = crate::config::env_lookup("MAYHEM_FALLBACK_RPC_URL") else {
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
        if slippage_bps > 10_000 {
            bail!("slippage must not exceed 10000 bps");
        }
        let protected = (value as u128).saturating_mul((10_000 - slippage_bps) as u128) / 10_000;
        u64::try_from(protected.max(1)).context("protected output exceeds u64")
    }

    fn env_u32(name: &str, default: u32) -> Result<u32> {
        match crate::config::env_lookup(name) {
            Some(value) => value
                .parse::<u32>()
                .with_context(|| format!("{name} must be an unsigned integer")),
            None => Ok(default),
        }
    }

    fn env_bool(name: &str, default: bool) -> Result<bool> {
        match crate::config::env_lookup(name) {
            Some(value) => match value.trim().to_ascii_lowercase().as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                other => bail!("{name} must be a boolean; got {other}"),
            },
            None => Ok(default),
        }
    }

    fn env_u64(name: &str, default: u64) -> Result<u64> {
        match crate::config::env_lookup(name) {
            Some(value) => value
                .parse::<u64>()
                .with_context(|| format!("{name} must be an unsigned integer")),
            None => Ok(default),
        }
    }

    fn env_usize(name: &str, default: usize) -> Result<usize> {
        match crate::config::env_lookup(name) {
            Some(value) => value
                .parse::<usize>()
                .with_context(|| format!("{name} must be an unsigned integer")),
            None => Ok(default),
        }
    }

    fn env_commitment(name: &str, default: CommitmentConfig) -> Result<CommitmentConfig> {
        match crate::config::env_lookup(name) {
            Some(value) => parse_commitment(value.trim()),
            None => Ok(default),
        }
    }

    /// Parse a `processed` / `confirmed` / `finalized` string into a
    /// `CommitmentConfig`. Shared by the `[live]` config field and the
    /// `MAYHEM_LIVE_SETTLEMENT_COMMITMENT` env override.
    fn parse_commitment(value: &str) -> Result<CommitmentConfig> {
        match value.trim().to_ascii_lowercase().as_str() {
            "processed" => Ok(CommitmentConfig::processed()),
            "confirmed" => Ok(CommitmentConfig::confirmed()),
            "finalized" => Ok(CommitmentConfig::finalized()),
            other => {
                bail!(
                    "settlement commitment must be processed, confirmed, or finalized; got {other}"
                )
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn slippage_floor_is_adverse_and_nonzero() {
            assert_eq!(apply_slippage_floor(1_000, 1_000).unwrap(), 900);
            assert_eq!(apply_slippage_floor(1, 1_000).unwrap(), 1);
            assert!(apply_slippage_floor(0, 100).is_err());
            // 10000 bps = "accept any output price" (force-sell): floor clamps to 1.
            assert_eq!(apply_slippage_floor(1_000, 10_000).unwrap(), 1);
            assert!(apply_slippage_floor(1_000, 10_001).is_err());
        }

        #[test]
        fn fresh_state_retry_delay_is_bounded() {
            assert_eq!(fresh_state_retry_delay_ms(0, 500), 20);
            assert_eq!(fresh_state_retry_delay_ms(2, 500), 60);
            assert_eq!(fresh_state_retry_delay_ms(20, 500), 100);
            assert_eq!(fresh_state_retry_delay_ms(20, 35), 35);
        }

        #[test]
        fn buy_entry_state_reserves_time_for_build_and_simulation() {
            let order = Order::Buy(BuyOrder {
                id: "buy".to_string(),
                timestamp_ms: 1_000,
                mint: Pubkey::new_unique().to_string(),
                lamports: 1,
                source_decision_id: "decision".to_string(),
                source_signature: None,
            });
            assert_eq!(
                buy_entry_state_remaining_ms(&order, 1_500, 1_000),
                Some(750)
            );
            assert_eq!(buy_entry_state_remaining_ms(&order, 2_250, 1_000), Some(0));
            assert_eq!(
                buy_entry_broadcast_remaining_ms(&order, 1_750, 1_000),
                Some(750)
            );
            assert_eq!(
                buy_entry_broadcast_remaining_ms(&order, 2_250, 1_000),
                Some(250)
            );
        }

        #[test]
        fn buy_send_timeout_keeps_rpc_window_after_deadline_check() {
            let order = Order::Buy(BuyOrder {
                id: "buy".to_string(),
                timestamp_ms: crate::types::now_ms(),
                mint: Pubkey::new_unique().to_string(),
                lamports: 1,
                source_decision_id: "decision".to_string(),
                source_signature: None,
            });
            assert_eq!(
                send_timeout_for_order(&order, 500, 750).unwrap(),
                Duration::from_millis(750)
            );
        }

        #[test]
        fn wallet_lock_allows_only_one_live_executor_per_wallet() {
            let wallet = Pubkey::new_unique();
            let first = acquire_wallet_lock(&wallet).expect("first lock should succeed");
            let second = acquire_wallet_lock(&wallet);
            assert!(second.is_err());
            drop(first);
            acquire_wallet_lock(&wallet).expect("lock should release when executor drops");
        }

        #[test]
        fn newborn_account_visibility_errors_are_retryable() {
            assert!(is_fresh_account_visibility_error(
                "AccountNotFound: pubkey=abc"
            ));
            assert!(is_fresh_account_visibility_error(
                "Invalid param: could not find account"
            ));
            assert!(!is_fresh_account_visibility_error("429 Too Many Requests"));
        }

        #[tokio::test]
        async fn bounded_rpc_call_cancels_a_stalled_request() {
            let result = rpc_call_bounded(
                async {
                    sleep(Duration::from_millis(100)).await;
                    Ok::<_, anyhow::Error>(())
                },
                Duration::from_millis(5),
                "test RPC",
            )
            .await;

            assert!(result
                .unwrap_err()
                .to_string()
                .contains("test RPC timeout after 5ms"));
        }
    }
}

#[cfg(feature = "live-executor")]
pub use imp::LivePumpExecutor;

#[cfg(not(feature = "live-executor"))]
pub struct LivePumpExecutor;

#[cfg(not(feature = "live-executor"))]
impl LivePumpExecutor {
    pub async fn new(_cfg: &Config) -> Result<Self> {
        anyhow::bail!("live executor was not compiled; rebuild with --features live-executor")
    }

    pub fn wallet(&self) -> Option<Pubkey> {
        None
    }

    pub async fn panic_sell_with_slippage(
        &self,
        _mint: &str,
        _sell_token_amount_raw: u128,
        _slippage_bps: u32,
    ) -> Result<ExecutionReport> {
        anyhow::bail!("live executor was not compiled; rebuild with --features live-executor")
    }

    pub async fn jupiter_sell(&self, _mint: &str, _amount_raw: u128) -> Result<ExecutionReport> {
        anyhow::bail!("live executor was not compiled; rebuild with --features live-executor")
    }

    pub async fn execute(
        &self,
        _order: &Order,
        _sell_token_amount_raw: Option<u128>,
        _buy_slippage_bps: Option<u32>,
    ) -> Result<ExecutionReport> {
        anyhow::bail!("live executor was not compiled; rebuild with --features live-executor")
    }

    pub async fn finalized_failure_for_signature(
        &self,
        _signature: &str,
    ) -> Result<Option<String>> {
        anyhow::bail!("live executor was not compiled; rebuild with --features live-executor")
    }
}
