// ITM tracker: per 5-min btc-updown-5m window, records the first and
// best (lowest) ask seen in a configurable band on both YES and NO sides,
// resolves the window via Pyth BTC/USD at window_end_ts, and writes one
// CSV row per side per window with PnL per share.

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
use tokio::sync::broadcast;

const WINDOW_SECS: u64 = 300;

#[derive(Default, Clone, Copy)]
struct OutcomeBook {
    ask: Option<Decimal>,
}

#[derive(Default, Clone, Copy)]
struct WindowEntry {
    first_ask: Option<Decimal>,
    first_offset_s: Option<u64>,
    best_ask: Option<Decimal>,
    best_offset_s: Option<u64>,
}

struct PmState {
    yes_token: U256,
    no_token: Option<U256>,
    yes_book: OutcomeBook,
    no_book: OutcomeBook,
    condition_id: String,
    window_start_ts: u64,
    yes_entry: WindowEntry,
    no_entry: WindowEntry,
    target_price: Option<Decimal>,
}

struct State {
    pm: PmState,
    out: BufWriter<File>,
    min_ask: Decimal,
    max_ask: Decimal,
    n_entries: u64,
    n_wins: u64,
    n_losses: u64,
    n_unresolved: u64,
    pnl_first: Decimal,
    pnl_best: Decimal,
}

fn write_header(w: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        w,
        "window_start_ts,condition_id,side,first_ask,first_offset_s,best_ask,best_offset_s,\
         target_pyth,final_pyth,resolved_side,pnl_first,pnl_best"
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
    entry: &WindowEntry,
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
    resolved_side: Option<&str>,
    pnl_first: Option<Decimal>,
    pnl_best: Option<Decimal>,
) -> Result<()> {
    let first_ask = entry.first_ask.map(|d| d.to_string()).unwrap_or_default();
    let first_off = entry.first_offset_s.map(|s| s.to_string()).unwrap_or_default();
    let best_ask = entry.best_ask.map(|d| d.to_string()).unwrap_or_default();
    let best_off = entry.best_offset_s.map(|s| s.to_string()).unwrap_or_default();
    let target_s = target.map(|d| d.to_string()).unwrap_or_default();
    let final_s = final_pyth.map(|d| d.to_string()).unwrap_or_default();
    let resolved_s = resolved_side.unwrap_or("");
    let pnl_first_s = pnl_first.map(|d| d.to_string()).unwrap_or_default();
    let pnl_best_s = pnl_best.map(|d| d.to_string()).unwrap_or_default();
    writeln!(
        w,
        "{window_start_ts},{condition_id},{side},{first_ask},{first_off},{best_ask},{best_off},\
         {target_s},{final_s},{resolved_s},{pnl_first_s},{pnl_best_s}"
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

fn maybe_update_entry(
    entry: &mut WindowEntry,
    ask: Decimal,
    min_ask: Decimal,
    max_ask: Decimal,
    offset_s: u64,
) {
    if ask < min_ask || ask > max_ask {
        return;
    }
    if entry.first_ask.is_none() {
        entry.first_ask = Some(ask);
        entry.first_offset_s = Some(offset_s);
    }
    match entry.best_ask {
        None => {
            entry.best_ask = Some(ask);
            entry.best_offset_s = Some(offset_s);
        }
        Some(prev) if ask < prev => {
            entry.best_ask = Some(ask);
            entry.best_offset_s = Some(offset_s);
        }
        _ => {}
    }
}

fn apply_pm(state: &mut State, e: &PolymarketEvent) {
    let yes = state.pm.yes_token;
    let no = state.pm.no_token;
    let offset_s = now_wall_secs().saturating_sub(state.pm.window_start_ts);
    match &e.payload {
        PolymarketPayload::Book(b) => {
            if b.asset_id == yes {
                state.pm.yes_book.ask = b.asks.first().map(|l| l.price);
            } else if Some(b.asset_id) == no {
                state.pm.no_book.ask = b.asks.first().map(|l| l.price);
            }
        }
        PolymarketPayload::PriceChange(p) => {
            for entry in &p.price_changes {
                if entry.asset_id == yes {
                    if let Some(ba) = entry.best_ask {
                        state.pm.yes_book.ask = Some(ba);
                    }
                } else if Some(entry.asset_id) == no {
                    if let Some(ba) = entry.best_ask {
                        state.pm.no_book.ask = Some(ba);
                    }
                }
            }
        }
        PolymarketPayload::LastTrade(_) => {}
    }
    let (min_ask, max_ask) = (state.min_ask, state.max_ask);
    if let Some(ask) = state.pm.yes_book.ask {
        maybe_update_entry(&mut state.pm.yes_entry, ask, min_ask, max_ask, offset_s);
    }
    if let Some(ask) = state.pm.no_book.ask {
        maybe_update_entry(&mut state.pm.no_entry, ask, min_ask, max_ask, offset_s);
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

fn pnl_for_side(entry_ask: Decimal, win: bool) -> Decimal {
    if win {
        Decimal::ONE - entry_ask
    } else {
        -entry_ask
    }
}

async fn flush_window_for_rollover(state: &mut State, pm: &Arc<Polymarket>) {
    let window_end_ts = state.pm.window_start_ts + WINDOW_SECS;
    let final_pyth = match pm.pyth_btc_usd_at(window_end_ts).await {
        Ok(v) => v,
        Err(e) => {
            eprintln!("WARN: fetching final pyth for ts {window_end_ts}: {e:#}");
            None
        }
    };
    let target = state.pm.target_price;
    let resolved_side = match (target, final_pyth) {
        (Some(t), Some(f)) => Some(if f > t { "YES" } else { "NO" }),
        _ => None,
    };

    let condition_id = state.pm.condition_id.clone();
    let window_start_ts = state.pm.window_start_ts;
    let yes_entry = state.pm.yes_entry;
    let no_entry = state.pm.no_entry;

    for (side, entry) in [("YES", yes_entry), ("NO", no_entry)] {
        if entry.first_ask.is_none() {
            continue;
        }
        let (pnl_first, pnl_best) = match resolved_side {
            Some(r) => {
                let win = r == side;
                let pf = entry.first_ask.map(|a| pnl_for_side(a, win));
                let pb = entry.best_ask.map(|a| pnl_for_side(a, win));
                state.n_entries += 1;
                if win {
                    state.n_wins += 1;
                } else {
                    state.n_losses += 1;
                }
                if let Some(p) = pf {
                    state.pnl_first += p;
                }
                if let Some(p) = pb {
                    state.pnl_best += p;
                }
                (pf, pb)
            }
            None => {
                state.n_unresolved += 1;
                (None, None)
            }
        };
        if let Err(e) = write_row(
            &mut state.out,
            window_start_ts,
            &condition_id,
            side,
            &entry,
            target,
            final_pyth,
            resolved_side,
            pnl_first,
            pnl_best,
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
        yes_book: OutcomeBook::default(),
        no_book: OutcomeBook::default(),
        condition_id: snapshot.condition_id,
        window_start_ts,
        yes_entry: WindowEntry::default(),
        no_entry: WindowEntry::default(),
        target_price: target,
    };
    Ok(())
}

fn print_summary(state: &State, t_start: Instant) {
    let elapsed = t_start.elapsed().as_secs_f64();
    let resolved = state.n_wins + state.n_losses;
    let (win_rate, avg_first, avg_best) = if resolved > 0 {
        (
            state.n_wins as f64 * 100.0 / resolved as f64,
            state.pnl_first / Decimal::from(resolved),
            state.pnl_best / Decimal::from(resolved),
        )
    } else {
        (0.0, Decimal::ZERO, Decimal::ZERO)
    };
    eprintln!(
        "[{:>7.1}s] entries={} wins={} losses={} unresolved={} | win_rate={:.1}% \
         pnl_first={} (avg {}) pnl_best={} (avg {})",
        elapsed,
        state.n_entries,
        state.n_wins,
        state.n_losses,
        state.n_unresolved,
        win_rate,
        state.pnl_first,
        avg_first,
        state.pnl_best,
        avg_best,
    );
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut args = std::env::args().skip(1);
    let mut out_path: Option<PathBuf> = None;
    let mut explicit_market: Option<String> = None;
    let mut min_ask = Decimal::new(95, 2); // 0.95
    let mut max_ask = Decimal::new(99, 2); // 0.99
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
    eprintln!("entry band: ask in [{min_ask}, {max_ask}]");

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
            yes_book: OutcomeBook::default(),
            no_book: OutcomeBook::default(),
            condition_id: snapshot.condition_id.clone(),
            window_start_ts,
            yes_entry: WindowEntry::default(),
            no_entry: WindowEntry::default(),
            target_price: target,
        },
        out,
        min_ask,
        max_ask,
        n_entries: 0,
        n_wins: 0,
        n_losses: 0,
        n_unresolved: 0,
        pnl_first: Decimal::ZERO,
        pnl_best: Decimal::ZERO,
    };

    let t_start = Instant::now();
    let mut next_rollover: Option<Instant> = if auto_roll {
        Some(next_window_boundary()?)
    } else {
        None
    };
    let mut summary_tick = tokio::time::interval(Duration::from_secs(30));
    summary_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                    Err(broadcast::error::RecvError::Closed) => {
                        break Ok(());
                    }
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
            _ = tokio::time::sleep(rollover_in), if auto_roll => {
                flush_window_for_rollover(&mut state, &pm).await;
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

    flush_window_for_rollover(&mut state, &pm).await;
    state.out.flush()?;
    print_summary(&state, t_start);
    outcome
}
