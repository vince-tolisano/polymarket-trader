// Late-window scraping tracker. Strategy: in the last minute of a 5-min
// btc-updown-5m window, enter a side whose ask sits in (min_ask, max_ask]
// — default (0.95, 0.99] — and hold to settlement. The trade pays
// (1 − ask) on a win and loses the full ask on a loss, so the strategy
// needs a high win rate; the tracker exists to measure whether the
// realized rate clears the breakeven bar.
//
// For each window and side (YES, NO), records the first qualifying ask.
// At rollover resolves the window via Pyth BTC/USD at window_end_ts (with
// retry + timestamp walkback, since Hermes 404s on boundary-aligned recent
// timestamps) and writes one CSV row per triggered side with PnL per share.
// Stderr emits a live alert at each trigger.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use polymarket_core::{
    Decimal, FeedSource, MarketSnapshot, Polymarket, PolymarketEvent, PolymarketFeed,
    PolymarketPayload, RecordedEvent, U256,
};
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const WINDOW_SECS: u64 = 300;

#[derive(Clone, Copy)]
struct Entry {
    ask: Decimal,
    offset_s: u64,
}

#[derive(Default, Clone, Copy)]
struct SideState {
    ask: Option<Decimal>,
    bid: Option<Decimal>,
    entry: Option<Entry>,
}

struct PmState {
    yes_token: U256,
    no_token: Option<U256>,
    yes: SideState,
    no: SideState,
    condition_id: String,
    window_start_ts: u64,
    target_price: Option<Decimal>,
}

struct State {
    pm: PmState,
    out: BufWriter<File>,
    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    n_entries: u64,
    n_wins: u64,
    n_losses: u64,
    n_unresolved: u64,
    pnl: Decimal,
}

struct OldWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    yes_entry: Option<Entry>,
    no_entry: Option<Entry>,
}

struct ResolvedWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
    yes_entry: Option<Entry>,
    no_entry: Option<Entry>,
}

fn write_header(w: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        w,
        "window_start_ts,condition_id,side,entry_ask,entry_offset_s,\
         target_pyth,final_pyth,resolved_side,won,pnl"
    )?;
    w.flush()?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn write_row(
    w: &mut BufWriter<File>,
    window_start_ts: u64,
    condition_id: &str,
    side: &str,
    entry: Entry,
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
    resolved_side: Option<&str>,
    won: Option<bool>,
    pnl: Option<Decimal>,
) -> Result<()> {
    let target_s = target.map(|d| d.to_string()).unwrap_or_default();
    let final_s = final_pyth.map(|d| d.to_string()).unwrap_or_default();
    let resolved_s = resolved_side.unwrap_or("");
    let won_s = won.map(|b| if b { "1" } else { "0" }).unwrap_or("");
    let pnl_s = pnl.map(|d| d.to_string()).unwrap_or_default();
    writeln!(
        w,
        "{window_start_ts},{condition_id},{side},{},{},{target_s},{final_s},{resolved_s},{won_s},{pnl_s}",
        entry.ask, entry.offset_s,
    )?;
    w.flush()?;
    Ok(())
}

fn pick_yes_no(snapshot: &MarketSnapshot) -> (U256, Option<U256>) {
    let yes_idx = snapshot
        .outcomes
        .iter()
        .position(|o| {
            let lc = o.outcome.to_lowercase();
            lc == "yes" || lc == "up" || lc == "above"
        })
        .unwrap_or(0);
    let yes = snapshot.outcomes[yes_idx].token_id;
    let no = snapshot
        .outcomes
        .iter()
        .enumerate()
        .find(|(i, _)| *i != yes_idx)
        .map(|(_, o)| o.token_id);
    (yes, no)
}

fn current_window_start_ts() -> Result<u64> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    Ok(now - (now % WINDOW_SECS))
}

fn next_window_boundary() -> Result<Instant> {
    let now = SystemTime::now().duration_since(UNIX_EPOCH)?;
    let next_boundary = (now.as_secs() / WINDOW_SECS + 1) * WINDOW_SECS;
    let dur_until = Duration::from_secs(next_boundary) - now;
    Ok(Instant::now() + dur_until)
}

fn now_wall_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// First qualifying tick triggers a one-shot entry on the side. Later
/// ticks just update the latest ask used in the summary line; we don't
/// re-enter, since the real strategy would have already bought.
#[allow(clippy::too_many_arguments)]
fn try_trigger(
    side_label: &str,
    window_ts: u64,
    side: &mut SideState,
    ask: Decimal,
    offset_s: u64,
    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
) {
    side.ask = Some(ask);
    if side.entry.is_some() {
        return;
    }
    // Eligible window is [min_offset_s, max_offset_s): too late (default
    // last 10s) leaves no time to fill and risks the resolution print first.
    if offset_s < min_offset_s || offset_s >= max_offset_s {
        return;
    }
    if ask <= min_ask || ask > max_ask {
        return;
    }
    // Require the bid to also be high: filters out wide-spread thin books
    // where both YES and NO asks sit near 1.00 with bids near 0.
    let Some(bid) = side.bid else {
        return;
    };
    if bid < min_bid {
        return;
    }
    side.entry = Some(Entry { ask, offset_s });
    let remaining = WINDOW_SECS.saturating_sub(offset_s);
    eprintln!(
        ">>> ENTRY  window {window_ts}  {side_label} @ {ask} (bid {bid})  (+{offset_s}s in, {remaining}s remaining)"
    );
}

fn apply_pm(state: &mut State, e: &PolymarketEvent) {
    let yes = state.pm.yes_token;
    let no = state.pm.no_token;
    let offset_s = now_wall_secs().saturating_sub(state.pm.window_start_ts);
    let min_ask = state.min_ask;
    let max_ask = state.max_ask;
    let min_bid = state.min_bid;
    let min_off = state.min_offset_s;
    let max_off = state.max_offset_s;
    let window_ts = state.pm.window_start_ts;
    match &e.payload {
        PolymarketPayload::Book(b) => {
            // Update bid/ask together — the bid filter in try_trigger reads
            // the side's stored bid, so it must be current before we trigger.
            let side_state = if b.asset_id == yes {
                Some(("YES", &mut state.pm.yes))
            } else if Some(b.asset_id) == no {
                Some(("NO ", &mut state.pm.no))
            } else {
                None
            };
            if let Some((label, side)) = side_state {
                side.bid = b.bids.first().map(|l| l.price);
                if let Some(a) = b.asks.first().map(|l| l.price) {
                    try_trigger(label, window_ts, side, a, offset_s, min_ask, max_ask, min_bid, min_off, max_off);
                } else {
                    side.ask = None;
                }
            }
        }
        PolymarketPayload::PriceChange(p) => {
            for entry in &p.price_changes {
                let side_state = if entry.asset_id == yes {
                    Some(("YES", &mut state.pm.yes))
                } else if Some(entry.asset_id) == no {
                    Some(("NO ", &mut state.pm.no))
                } else {
                    None
                };
                if let Some((label, side)) = side_state {
                    if let Some(bb) = entry.best_bid {
                        side.bid = Some(bb);
                    }
                    if let Some(ba) = entry.best_ask {
                        try_trigger(label, window_ts, side, ba, offset_s, min_ask, max_ask, min_bid, min_off, max_off);
                    }
                }
            }
        }
        PolymarketPayload::LastTrade(_) => {}
    }
}

fn apply_event(state: &mut State, evt: RecordedEvent) {
    match evt {
        RecordedEvent::Polymarket(e) => apply_pm(state, &e),
        RecordedEvent::Cex(_) => {}
        RecordedEvent::FeedError {
            source, message, ..
        } => {
            let tag = match source {
                FeedSource::Polymarket => "polymarket",
                FeedSource::Coinbase => "coinbase",
                FeedSource::Kraken => "kraken",
                FeedSource::Bitstamp => "bitstamp",
            };
            eprintln!("[{tag}] {message}");
        }
    }
}

fn pnl_for(entry_ask: Decimal, win: bool) -> Decimal {
    if win {
        Decimal::ONE - entry_ask
    } else {
        -entry_ask
    }
}

/// Hand off the closing window to the async resolver. Returns None when
/// neither side triggered — nothing to log or resolve in that case.
fn snapshot_window(state: &State) -> Option<OldWindow> {
    if state.pm.yes.entry.is_none() && state.pm.no.entry.is_none() {
        return None;
    }
    Some(OldWindow {
        window_start_ts: state.pm.window_start_ts,
        condition_id: state.pm.condition_id.clone(),
        target: state.pm.target_price,
        yes_entry: state.pm.yes.entry,
        no_entry: state.pm.no.entry,
    })
}

/// Hermes 404s on boundary-aligned recent timestamps: its index lags real
/// time and Pyth updates don't always land on second boundaries. The BTC
/// price 1–30s before window end is functionally identical for binary
/// up/down resolution, so we walk the timestamp back on each retry rather
/// than just waiting longer at the same one.
async fn fetch_final_pyth_retry(pm: &Polymarket, window_end_ts: u64) -> Option<Decimal> {
    // (delay before attempt, ts_offset_from_window_end_seconds)
    let attempts = [(0_u64, 0_i64), (2, -1), (3, -3), (5, -10), (10, -30)];
    for (i, &(delay, offset)) in attempts.iter().enumerate() {
        if delay > 0 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        let ts = (window_end_ts as i64 + offset).max(0) as u64;
        match pm.pyth_btc_usd_at(ts).await {
            Ok(Some(v)) => return Some(v),
            Ok(None) => continue,
            Err(e) => {
                let last = i + 1 == attempts.len();
                let tag = if last { "give up" } else { "retry" };
                eprintln!("WARN: pyth ts {ts} (offset {offset}s) attempt {} ({tag}): {e:#}", i + 1);
            }
        }
    }
    None
}

/// Spawned at each rollover so the new window can start subscribing
/// immediately while Pyth catches up on the old window's end timestamp.
async fn resolve_window(
    old: OldWindow,
    pm: Arc<Polymarket>,
    tx: mpsc::UnboundedSender<ResolvedWindow>,
) {
    let window_end_ts = old.window_start_ts + WINDOW_SECS;
    let final_pyth = fetch_final_pyth_retry(&pm, window_end_ts).await;
    let _ = tx.send(ResolvedWindow {
        window_start_ts: old.window_start_ts,
        condition_id: old.condition_id,
        target: old.target,
        final_pyth,
        yes_entry: old.yes_entry,
        no_entry: old.no_entry,
    });
}

fn sink_resolved(state: &mut State, r: ResolvedWindow) {
    let resolved_side = match (r.target, r.final_pyth) {
        (Some(t), Some(f)) => Some(if f > t { "YES" } else { "NO" }),
        _ => None,
    };
    for (side, entry_opt) in [("YES", r.yes_entry), ("NO", r.no_entry)] {
        let Some(entry) = entry_opt else { continue };
        let (won, pnl) = match resolved_side {
            Some(rs) => {
                let win = rs == side;
                let p = pnl_for(entry.ask, win);
                state.n_entries += 1;
                if win {
                    state.n_wins += 1;
                } else {
                    state.n_losses += 1;
                }
                state.pnl += p;
                (Some(win), Some(p))
            }
            None => {
                state.n_unresolved += 1;
                (None, None)
            }
        };
        if let Err(e) = write_row(
            &mut state.out,
            r.window_start_ts,
            &r.condition_id,
            side,
            entry,
            r.target,
            r.final_pyth,
            resolved_side,
            won,
            pnl,
        ) {
            eprintln!("WARN: write row: {e:#}");
        }
    }
}

async fn roll_window(
    state: &mut State,
    tx: &broadcast::Sender<RecordedEvent>,
    pm: &Arc<Polymarket>,
    pm_feed: &mut Option<PolymarketFeed>,
) -> Result<()> {
    let condition_id = pm
        .current_btc_updown_5m_condition_id()
        .await
        .context("resolving new btc-updown-5m market")?;
    let snapshot = pm
        .fetch_snapshot(&condition_id)
        .await
        .context("fetching new market snapshot")?;
    if snapshot.outcomes.is_empty() {
        anyhow::bail!("new market has no outcome tokens");
    }
    let (yes_token, no_token) = pick_yes_no(&snapshot);
    let window_start_ts = current_window_start_ts()?;
    let target = pm.pyth_btc_usd_at(window_start_ts).await.ok().flatten();
    let token_ids: Vec<U256> = snapshot.outcomes.iter().map(|o| o.token_id).collect();

    *pm_feed = None;
    *pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);

    state.pm = PmState {
        yes_token,
        no_token,
        yes: SideState::default(),
        no: SideState::default(),
        condition_id: snapshot.condition_id,
        window_start_ts,
        target_price: target,
    };
    Ok(())
}

fn fmt_opt(d: Option<Decimal>) -> String {
    d.map(|v| format!("{v:.4}")).unwrap_or_else(|| "—".into())
}

fn print_summary(state: &State, t_start: Instant) {
    let elapsed = t_start.elapsed().as_secs_f64();
    let resolved = state.n_wins + state.n_losses;
    let (win_rate, avg_pnl) = if resolved > 0 {
        (
            state.n_wins as f64 * 100.0 / resolved as f64,
            state.pnl / Decimal::from(resolved),
        )
    } else {
        (0.0, Decimal::ZERO)
    };
    eprintln!(
        "[{:>7.1}s] entries={} wins={} losses={} unresolved={} | win_rate={:.1}% \
         pnl={} (avg {}) | YES ask={} NO ask={}",
        elapsed,
        state.n_entries,
        state.n_wins,
        state.n_losses,
        state.n_unresolved,
        win_rate,
        state.pnl,
        avg_pnl,
        fmt_opt(state.pm.yes.ask),
        fmt_opt(state.pm.no.ask),
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut args = std::env::args().skip(1);
    let mut out_path: Option<PathBuf> = None;
    let mut explicit_market: Option<String> = None;
    let mut min_ask = Decimal::new(95, 2);
    let mut max_ask = Decimal::new(99, 2);
    let mut min_bid = Decimal::new(50, 2);
    let mut min_offset_s: u64 = 240;
    let mut max_offset_s: u64 = WINDOW_SECS - 10;
    while let Some(a) = args.next() {
        match a.as_str() {
            "-o" | "--out" => {
                out_path = Some(PathBuf::from(
                    args.next().context("--out needs a path")?,
                ));
            }
            "--min-ask" => {
                let v = args.next().context("--min-ask needs a value")?;
                min_ask = v.parse().with_context(|| format!("parsing --min-ask {v}"))?;
            }
            "--max-ask" => {
                let v = args.next().context("--max-ask needs a value")?;
                max_ask = v.parse().with_context(|| format!("parsing --max-ask {v}"))?;
            }
            "--min-bid" => {
                let v = args.next().context("--min-bid needs a value")?;
                min_bid = v.parse().with_context(|| format!("parsing --min-bid {v}"))?;
            }
            "--min-offset" => {
                let v = args.next().context("--min-offset needs a value")?;
                min_offset_s = v
                    .parse()
                    .with_context(|| format!("parsing --min-offset {v}"))?;
            }
            "--max-offset" => {
                let v = args.next().context("--max-offset needs a value")?;
                max_offset_s = v
                    .parse()
                    .with_context(|| format!("parsing --max-offset {v}"))?;
            }
            other => {
                if explicit_market.is_none() {
                    explicit_market = Some(other.to_string());
                } else {
                    anyhow::bail!("unexpected arg: {other}");
                }
            }
        }
    }
    let auto_roll = explicit_market.is_none();

    let out_path = out_path.unwrap_or_else(|| {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        PathBuf::from(format!("itm-{ts}.csv"))
    });
    let mut out = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("creating {}", out_path.display()))?,
    );
    write_header(&mut out)?;
    eprintln!("writing to {}", out_path.display());
    eprintln!(
        "strategy: enter at ask in ({min_ask}, {max_ask}] AND bid ≥ {min_bid} at offset [{min_offset_s}, {max_offset_s})s ({}s–{}s remaining)",
        WINDOW_SECS.saturating_sub(min_offset_s),
        WINDOW_SECS.saturating_sub(max_offset_s),
    );

    let pm = Arc::new(Polymarket::new()?);
    let condition_id = match &explicit_market {
        Some(a) => pm.resolve_condition_id(a).await?,
        None => pm
            .current_btc_updown_5m_condition_id()
            .await
            .context("resolving current btc-updown-5m market")?,
    };
    let snapshot = pm.fetch_snapshot(&condition_id).await?;
    if snapshot.outcomes.is_empty() {
        return Err(anyhow!("market has no outcome tokens"));
    }
    let (yes_token, no_token) = pick_yes_no(&snapshot);
    let window_start_ts = current_window_start_ts()?;
    let target = pm.pyth_btc_usd_at(window_start_ts).await.ok().flatten();
    eprintln!(
        "market: {} ({})\n  target (pyth at window start): {}",
        snapshot.question,
        snapshot.condition_id,
        target.map(|d| d.to_string()).unwrap_or_else(|| "—".into()),
    );

    let token_ids: Vec<U256> = snapshot.outcomes.iter().map(|o| o.token_id).collect();
    let (tx, mut rx) = broadcast::channel::<RecordedEvent>(4096);
    let mut pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);

    let mut state = State {
        pm: PmState {
            yes_token,
            no_token,
            yes: SideState::default(),
            no: SideState::default(),
            condition_id: snapshot.condition_id.clone(),
            window_start_ts,
            target_price: target,
        },
        out,
        min_ask,
        max_ask,
        min_bid,
        min_offset_s,
        max_offset_s,
        n_entries: 0,
        n_wins: 0,
        n_losses: 0,
        n_unresolved: 0,
        pnl: Decimal::ZERO,
    };

    let t_start = Instant::now();
    let mut next_rollover: Option<Instant> = if auto_roll {
        Some(next_window_boundary()?)
    } else {
        None
    };
    let mut summary_tick = tokio::time::interval(Duration::from_secs(30));
    summary_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let (resolved_tx, mut resolved_rx) = mpsc::unbounded_channel::<ResolvedWindow>();
    let mut resolver_tasks: Vec<JoinHandle<()>> = Vec::new();

    let outcome: Result<()> = loop {
        let rollover_in = match next_rollover {
            Some(t) => t.saturating_duration_since(Instant::now()),
            None => Duration::from_secs(86400),
        };

        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(evt) => apply_event(&mut state, evt),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        eprintln!("WARN: broadcast lagged {n}");
                    }
                    Err(broadcast::error::RecvError::Closed) => break Ok(()),
                }
                loop {
                    match rx.try_recv() {
                        Ok(evt) => apply_event(&mut state, evt),
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Lagged(n)) => {
                            eprintln!("WARN: broadcast lagged {n}");
                        }
                        Err(broadcast::error::TryRecvError::Closed) => break,
                    }
                }
            }
            Some(r) = resolved_rx.recv() => {
                sink_resolved(&mut state, r);
            }
            _ = tokio::time::sleep(rollover_in), if auto_roll => {
                eprintln!(
                    "window {} closing: YES entry={} NO entry={}",
                    state.pm.window_start_ts,
                    state.pm.yes.entry.map(|e| format!("{}@{}s", e.ask, e.offset_s)).unwrap_or_else(|| "—".into()),
                    state.pm.no.entry.map(|e| format!("{}@{}s", e.ask, e.offset_s)).unwrap_or_else(|| "—".into()),
                );
                if let Some(old) = snapshot_window(&state) {
                    let pm_c = pm.clone();
                    let tx_c = resolved_tx.clone();
                    resolver_tasks.push(tokio::spawn(async move {
                        resolve_window(old, pm_c, tx_c).await;
                    }));
                }
                match roll_window(&mut state, &tx, &pm, &mut pm_feed).await {
                    Ok(()) => {
                        next_rollover = next_window_boundary().ok();
                        eprintln!("rollover: now tracking {}", state.pm.condition_id);
                    }
                    Err(e) => {
                        eprintln!("WARN: rollover failed: {e:#}");
                        next_rollover = Some(Instant::now() + Duration::from_secs(2));
                    }
                }
            }
            _ = summary_tick.tick() => {
                print_summary(&state, t_start);
            }
            _ = signal::ctrl_c() => {
                eprintln!("\nshutting down…");
                break Ok(());
            }
        }
    };

    // Drain any in-flight resolutions on shutdown — without this, the final
    // window(s) would be lost since their Pyth lookup is still in progress.
    if let Some(old) = snapshot_window(&state) {
        let pm_c = pm.clone();
        let tx_c = resolved_tx.clone();
        resolver_tasks.push(tokio::spawn(async move {
            resolve_window(old, pm_c, tx_c).await;
        }));
    }
    drop(resolved_tx);
    eprintln!("waiting on {} pending window resolution(s)…", resolver_tasks.len());
    for h in resolver_tasks {
        let _ = h.await;
    }
    while let Some(r) = resolved_rx.recv().await {
        sink_resolved(&mut state, r);
    }
    state.out.flush()?;
    print_summary(&state, t_start);
    outcome
}
