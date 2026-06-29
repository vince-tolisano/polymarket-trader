// exec: the order-placement layer. This is the ONLY module in the workspace
// that signs and posts real orders, so all of the SDK's authenticated-client
// surface lives here, isolated behind a command channel.
//
// The authenticated CLOB client (`Client<Authenticated<..>>`) has an internal
// generic state parameter that is awkward to name in a struct field. To avoid
// ever naming it, the client is owned by a single long-lived task; the
// strategy loop talks to it only through `ExecCmd` messages and oneshot
// replies. This also matches the rest of the codebase's broadcast/mpsc style.

use std::str::FromStr;

use anyhow::Result;
use polymarket_client_sdk_v2::auth::LocalSigner;
use polymarket_client_sdk_v2::clob::types::{OrderType, Side};
use polymarket_client_sdk_v2::clob::{Client, Config};
use polymarket_core::{Decimal, U256};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

/// How to reach the CLOB and which wallet signs orders.
pub struct ExecConfig {
    pub host: String,
    /// Hex private key (with or without `0x`). Only read when `dry_run` is
    /// false; in dry-run no wallet is touched.
    pub private_key: String,
    /// EIP-712 domain chain id. Polymarket runs on Polygon mainnet = 137.
    pub chain_id: u64,
    /// Optional proxy/funder address for Polymarket email/magic wallets. For a
    /// plain EOA wallet leave this `None`.
    pub funder: Option<String>,
    /// When true, never authenticate or post — `Place` just acks. Lets you run
    /// the full strategy against the live book without spending anything.
    pub dry_run: bool,
}

/// Result of attempting to post a single order.
pub struct PlaceOutcome {
    /// CLOB order id, used later to query fill / cancel the remainder.
    pub order_id: Option<String>,
    pub success: bool,
    /// Stringified `OrderStatusType` (or "dry-run" / "error").
    pub status: String,
    /// Shares taken immediately at post time (the marketable portion of the
    /// GTC limit). Authoritative fill is read again at settle via `size_matched`.
    pub immediate_taking: Decimal,
    pub error: Option<String>,
}

/// Commands the strategy loop sends to the executor task.
pub enum ExecCmd {
    /// Post a GTC limit BUY for `size` shares of `token_id` at `price`.
    Place {
        token_id: U256,
        price: Decimal,
        size: Decimal,
        reply: oneshot::Sender<PlaceOutcome>,
    },
    /// At window rollover: read how much of the order filled (`size_matched`),
    /// then cancel any unfilled remainder. Replies with the matched share count.
    Settle {
        order_id: String,
        reply: oneshot::Sender<Decimal>,
    },
}

/// Spawn the executor task. Returns the command sender, a oneshot that resolves
/// once authentication succeeds (carrying the wallet address) or fails, and the
/// task handle.
pub fn spawn_executor(
    cfg: ExecConfig,
) -> (
    mpsc::UnboundedSender<ExecCmd>,
    oneshot::Receiver<Result<String, String>>,
    JoinHandle<()>,
) {
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel::<ExecCmd>();
    let (ready_tx, ready_rx) = oneshot::channel::<Result<String, String>>();
    let handle = tokio::spawn(async move {
        run_executor(cfg, ready_tx, cmd_rx).await;
    });
    (cmd_tx, ready_rx, handle)
}

async fn run_executor(
    cfg: ExecConfig,
    ready_tx: oneshot::Sender<Result<String, String>>,
    mut cmd_rx: mpsc::UnboundedReceiver<ExecCmd>,
) {
    // Dry-run path: no wallet, no SDK auth. Acknowledge every command so the
    // strategy loop behaves identically minus the real spend.
    if cfg.dry_run {
        let _ = ready_tx.send(Ok("dry-run (no wallet)".to_string()));
        while let Some(cmd) = cmd_rx.recv().await {
            match cmd {
                ExecCmd::Place { reply, .. } => {
                    let _ = reply.send(PlaceOutcome {
                        order_id: None,
                        success: true,
                        status: "dry-run".to_string(),
                        immediate_taking: Decimal::ZERO,
                        error: None,
                    });
                }
                ExecCmd::Settle { reply, .. } => {
                    let _ = reply.send(Decimal::ZERO);
                }
            }
        }
        return;
    }

    let signer = match LocalSigner::from_str(&cfg.private_key) {
        Ok(s) => s.with_chain_id(Some(cfg.chain_id)),
        Err(e) => {
            let _ = ready_tx.send(Err(format!("parsing private key: {e}")));
            return;
        }
    };
    let address = format!("{}", signer.address());

    let base = match Client::new(cfg.host.as_str(), Config::default()) {
        Ok(c) => c,
        Err(e) => {
            let _ = ready_tx.send(Err(format!("creating clob client: {e}")));
            return;
        }
    };
    let mut auth_builder = base.authentication_builder(&signer);
    if let Some(funder) = &cfg.funder {
        match funder.parse() {
            Ok(addr) => auth_builder = auth_builder.funder(addr),
            Err(e) => {
                let _ = ready_tx.send(Err(format!("parsing --funder {funder}: {e}")));
                return;
            }
        }
    }
    let client = match auth_builder.authenticate().await {
        Ok(c) => c,
        Err(e) => {
            let _ = ready_tx.send(Err(format!("authenticating with CLOB: {e:#}")));
            return;
        }
    };

    let _ = ready_tx.send(Ok(address));

    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            ExecCmd::Place {
                token_id,
                price,
                size,
                reply,
            } => {
                // Resting GTC limit BUY at the captured ask: it crosses the
                // book and takes whatever liquidity is resting at/below the
                // ask now, leaving any remainder resting as a bid until it
                // fills or we cancel it at rollover.
                let res = client
                    .limit_order()
                    .token_id(token_id)
                    .side(Side::Buy)
                    .price(price)
                    .size(size)
                    .order_type(OrderType::GTC)
                    .build_sign_and_post(&signer)
                    .await;
                let outcome = match res {
                    Ok(r) => PlaceOutcome {
                        order_id: Some(r.order_id),
                        success: r.success,
                        status: format!("{:?}", r.status),
                        immediate_taking: r.taking_amount,
                        error: r.error_msg,
                    },
                    Err(e) => PlaceOutcome {
                        order_id: None,
                        success: false,
                        status: "error".to_string(),
                        immediate_taking: Decimal::ZERO,
                        error: Some(format!("{e:#}")),
                    },
                };
                let _ = reply.send(outcome);
            }
            ExecCmd::Settle { order_id, reply } => {
                let matched = match client.order(&order_id).await {
                    Ok(o) => o.size_matched,
                    // Order may already be gone (auto-cancelled on market close
                    // or fully settled) — treat as unknown fill.
                    Err(_) => Decimal::ZERO,
                };
                // Cancel any unfilled remainder so it doesn't linger into the
                // next window; ignore errors (already filled/closed/cancelled).
                let _ = client.cancel_order(&order_id).await;
                let _ = reply.send(matched);
            }
        }
    }
}
