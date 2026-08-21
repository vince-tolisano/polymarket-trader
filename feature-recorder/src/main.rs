// feature-recorder: samples one row per second per side during the late part
// of each 5-min btc-updown-5m window, capturing decision-time features that
// a model can use to predict whether entering at that moment would have won.
// Buffers rows in-memory per window until resolution lands, then writes
// labeled rows (won, pnl_if_entered, resolved_source) to CSV.
//
// RESOLUTION (KEEP IN SYNC with live-trader/dry-trader/live-itm): the on-chain
// CLOB winner is authoritative and Pyth target-vs-final is only the FALLBACK.
// This binary used to resolve from Pyth alone; on 2026-08-12..20 data that
// mislabeled ~8.4% of windows (a market at >=99% confidence one second before
// expiry agreed with the Pyth label only 91.6% of the time), which biased every
// win-rate/edge figure computed from the file. Do not "simplify" this back to
// Pyth-only.
//
// No strategy logic — the row is a hypothetical entry at that moment on that
// side. The model fits "given features at time t, P(side wins)".

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use polymarket_core::{
    BitstampFeed, CexPayload, CexVenue, CoinbaseFeed, Decimal, FeedSource, KrakenFeed,
    MarketSnapshot, Polymarket, PolymarketEvent, PolymarketFeed, PolymarketPayload,
    RecordedEvent, U256,
};
use rust_decimal::prelude::ToPrimitive;
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const WINDOW_SECS: u64 = 300;
const COINBASE_PRODUCT: &str = "BTC-USD";
const KRAKEN_SYMBOL: &str = "BTC/USD";
const BITSTAMP_PAIR: &str = "btcusd";
const VENUES: &[CexVenue] = &[CexVenue::Coinbase, CexVenue::Kraken, CexVenue::Bitstamp];
const MEDIAN_FRESHNESS: Duration = Duration::from_secs(5);
const ROLLING_LOOKBACK: Duration = Duration::from_secs(30);
const TARGET_FETCH_RETRIES: u32 = 5;
const TARGET_FETCH_BACKOFF: Duration = Duration::from_secs(2);

#[derive(Default, Clone, Copy)]
struct SideBook {
    bid: Option<Decimal>,
    bid_size: Option<Decimal>,
    ask: Option<Decimal>,
    ask_size: Option<Decimal>,
    last: Option<Decimal>,
}

#[derive(Default)]
struct VenueState {
    last: Option<Decimal>,
    last_at: Option<Instant>,
}

#[derive(Clone)]
struct FeatureRow {
    offset_s: u64,
    side: &'static str,
    yes_book: SideBook,
    no_book: SideBook,
    book_tightness: Option<Decimal>,
    btc_median: Option<Decimal>,
    btc_target: Option<Decimal>,
    btc_delta: Option<Decimal>,
    btc_slope_30s: Option<f64>,
    btc_vol_30s: Option<f64>,
    n_cex_trades_30s: usize,
}

/// Snapshot of a window's collected rows handed off to the async resolver
/// at rollover. Pyth lookup happens off the main loop.
struct PendingWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    /// Needed to map the on-chain winning token back to a side.
    yes_token: U256,
    no_token: Option<U256>,
    rows: Vec<FeatureRow>,
}

struct ResolvedWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
    /// On-chain winner side if PM resolved it; else None and we fall back to Pyth.
    pm_winner_side: Option<&'static str>,
    rows: Vec<FeatureRow>,
}

struct State {
    yes_token: U256,
    no_token: Option<U256>,
    yes: SideBook,
    no: SideBook,
    condition_id: String,
    window_start_ts: u64,
    target_price: Option<Decimal>,
    btc: HashMap<CexVenue, VenueState>,
    /// One sample per tick — bounded to ROLLING_LOOKBACK so slope/vol over
    /// the last 30s of BTC median is cheap to compute.
    btc_samples: VecDeque<(Instant, Decimal)>,
    /// Receipt times of CEX Trade events over the last ROLLING_LOOKBACK.
    /// Ticker-only updates (Kraken) are not counted — this is a "CEX print
    /// activity" proxy, not a generic event counter.
    btc_trades: VecDeque<Instant>,
    pending_rows: Vec<FeatureRow>,
}

impl State {
    fn btc_median(&self) -> Option<Decimal> {
        let now = Instant::now();
        let mut prices: Vec<Decimal> = VENUES
            .iter()
            .filter_map(|v| {
                let s = self.btc.get(v)?;
                let last_at = s.last_at?;
                if now.saturating_duration_since(last_at) > MEDIAN_FRESHNESS {
                    return None;
                }
                s.last
            })
            .collect();
        if prices.is_empty() {
            return None;
        }
        prices.sort();
        let mid = prices.len() / 2;
        Some(if prices.len() % 2 == 1 {
            prices[mid]
        } else {
            (prices[mid - 1] + prices[mid]) / Decimal::from(2)
        })
    }

    fn trim_rolling(&mut self, now: Instant) {
        let cutoff = now.checked_sub(ROLLING_LOOKBACK).unwrap_or(now);
        while let Some(&(t, _)) = self.btc_samples.front() {
            if t < cutoff {
                self.btc_samples.pop_front();
            } else {
                break;
            }
        }
        while let Some(&t) = self.btc_trades.front() {
            if t < cutoff {
                self.btc_trades.pop_front();
            } else {
                break;
            }
        }
    }

    fn slope_vol(&self) -> (Option<f64>, Option<f64>) {
        let prices: Vec<f64> = self
            .btc_samples
            .iter()
            .filter_map(|(_, p)| p.to_f64())
            .collect();
        if prices.len() < 2 {
            return (None, None);
        }
        let n = prices.len();
        let (t0, _) = self.btc_samples.front().unwrap();
        let (t1, _) = self.btc_samples.back().unwrap();
        let dt = t1.saturating_duration_since(*t0).as_secs_f64();
        let slope = if dt >= 0.5 {
            Some((prices[n - 1] - prices[0]) / dt)
        } else {
            None
        };
        let mean = prices.iter().sum::<f64>() / n as f64;
        let var = prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / n as f64;
        let vol = if n >= 3 { Some(var.sqrt()) } else { None };
        (slope, vol)
    }
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

fn apply_pm(state: &mut State, e: &PolymarketEvent) {
    let yes = state.yes_token;
    let no = state.no_token;
    match &e.payload {
        PolymarketPayload::Book(b) => {
            let side = if b.asset_id == yes {
                Some(&mut state.yes)
            } else if Some(b.asset_id) == no {
                Some(&mut state.no)
            } else {
                None
            };
            if let Some(s) = side {
                s.bid = b.bids.first().map(|l| l.price);
                s.bid_size = b.bids.first().map(|l| l.size);
                s.ask = b.asks.first().map(|l| l.price);
                s.ask_size = b.asks.first().map(|l| l.size);
            }
        }
        PolymarketPayload::PriceChange(p) => {
            for entry in &p.price_changes {
                let side = if entry.asset_id == yes {
                    Some(&mut state.yes)
                } else if Some(entry.asset_id) == no {
                    Some(&mut state.no)
                } else {
                    None
                };
                if let Some(s) = side {
                    if let Some(bb) = entry.best_bid {
                        s.bid = Some(bb);
                    }
                    if let Some(ba) = entry.best_ask {
                        s.ask = Some(ba);
                    }
                }
            }
        }
        PolymarketPayload::LastTrade(t) => {
            let side = if t.asset_id == yes {
                Some(&mut state.yes)
            } else if Some(t.asset_id) == no {
                Some(&mut state.no)
            } else {
                None
            };
            if let Some(s) = side {
                s.last = Some(t.price);
            }
        }
    }
}

fn apply_event(state: &mut State, evt: RecordedEvent) {
    match evt {
        RecordedEvent::Polymarket(e) => apply_pm(state, &e),
        RecordedEvent::Cex(e) => {
            let now = Instant::now();
            let v = state.btc.entry(e.venue).or_default();
            v.last_at = Some(now);
            match &e.payload {
                CexPayload::Ticker { last, .. } => {
                    v.last = Some(*last);
                }
                CexPayload::Trade { price, .. } => {
                    v.last = Some(*price);
                    state.btc_trades.push_back(now);
                }
            }
        }
        RecordedEvent::FeedError { source, message, .. } => {
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

/// Captures the current snapshot of every feature for both YES and NO,
/// and appends two rows (one per side) to the pending buffer. Called once
/// per second after offset crosses min_offset_s.
fn sample_features(state: &mut State, now: Instant, offset_s: u64) {
    if let Some(m) = state.btc_median() {
        state.btc_samples.push_back((now, m));
    }
    state.trim_rolling(now);

    let btc_median = state.btc_median();
    let btc_target = state.target_price;
    let btc_delta = match (btc_median, btc_target) {
        (Some(m), Some(t)) => Some(m - t),
        _ => None,
    };
    let (slope, vol) = state.slope_vol();
    let n_trades = state.btc_trades.len();
    let book_tightness = match (state.yes.bid, state.no.bid) {
        (Some(yb), Some(nb)) => Some(Decimal::ONE - (yb + nb)),
        _ => None,
    };
    let yes = state.yes;
    let no = state.no;

    for side in ["YES", "NO"] {
        state.pending_rows.push(FeatureRow {
            offset_s,
            side,
            yes_book: yes,
            no_book: no,
            book_tightness,
            btc_median,
            btc_target,
            btc_delta,
            btc_slope_30s: slope,
            btc_vol_30s: vol,
            n_cex_trades_30s: n_trades,
        });
    }
}

fn write_header(w: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        w,
        "window_start_ts,condition_id,offset_s,side,\
         yes_bid,yes_bid_size,yes_ask,yes_ask_size,yes_last,\
         no_bid,no_bid_size,no_ask,no_ask_size,no_last,\
         book_tightness,\
         btc_median,btc_target,btc_delta,btc_slope_30s,btc_vol_30s,n_cex_trades_30s,\
         final_pyth,resolved_side,resolved_source,won,pnl_if_entered"
    )?;
    w.flush()?;
    Ok(())
}

fn opt_dec(v: Option<Decimal>) -> String {
    v.map(|d| d.to_string()).unwrap_or_default()
}
fn opt_f64(v: Option<f64>) -> String {
    v.map(|d| format!("{d:.6}")).unwrap_or_default()
}

fn write_row(
    w: &mut BufWriter<File>,
    window_start_ts: u64,
    condition_id: &str,
    row: &FeatureRow,
    final_pyth: Option<Decimal>,
    resolved_side: Option<&str>,
    resolved_source: &str,
    won: Option<bool>,
    pnl: Option<Decimal>,
) -> Result<()> {
    let won_s = won.map(|b| if b { "1" } else { "0" }).unwrap_or("");
    writeln!(
        w,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        window_start_ts,
        condition_id,
        row.offset_s,
        row.side,
        opt_dec(row.yes_book.bid),
        opt_dec(row.yes_book.bid_size),
        opt_dec(row.yes_book.ask),
        opt_dec(row.yes_book.ask_size),
        opt_dec(row.yes_book.last),
        opt_dec(row.no_book.bid),
        opt_dec(row.no_book.bid_size),
        opt_dec(row.no_book.ask),
        opt_dec(row.no_book.ask_size),
        opt_dec(row.no_book.last),
        opt_dec(row.book_tightness),
        opt_dec(row.btc_median),
        opt_dec(row.btc_target),
        opt_dec(row.btc_delta),
        opt_f64(row.btc_slope_30s),
        opt_f64(row.btc_vol_30s),
        row.n_cex_trades_30s,
        opt_dec(final_pyth),
        resolved_side.unwrap_or(""),
        resolved_source,
        won_s,
        opt_dec(pnl),
    )?;
    Ok(())
}

fn pnl_for(entry_ask: Decimal, win: bool) -> Decimal {
    if win {
        Decimal::ONE - entry_ask
    } else {
        -entry_ask
    }
}

/// Walks the Pyth timestamp back on each retry because Hermes 404s on
/// boundary-aligned recent timestamps. BTC 1–30s before window end is
/// functionally identical for binary up/down resolution.
async fn fetch_final_pyth_retry(pm: &Polymarket, window_end_ts: u64) -> Option<Decimal> {
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

/// Poll the CLOB market until it closes and reports a winner; match the winner
/// token back to a side. Returns None if it never resolves within the budget,
/// in which case the caller falls back to Pyth. Patient ~20-minute budget for
/// the same reason as live-trader/dry-trader: PM publishes the winner minutes
/// after the window closes, and giving up early silently downgrades the label.
async fn poll_pm_winner(
    pm: &Polymarket,
    condition_id: &str,
    yes_token: U256,
    no_token: Option<U256>,
    cancel: Arc<AtomicBool>,
) -> Option<&'static str> {
    tokio::time::sleep(Duration::from_secs(5)).await;
    for _ in 0..240 {
        // On shutdown, stop the patient poll and let the caller fall back to
        // Pyth immediately rather than blocking exit for up to ~20 minutes.
        if cancel.load(Ordering::Relaxed) {
            return None;
        }
        if let Ok(Some(token_id)) = pm.market_winner(condition_id).await {
            if token_id == yes_token {
                return Some("YES");
            }
            if no_token == Some(token_id) {
                return Some("NO");
            }
            return None;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    None
}

async fn resolve_window(
    old: PendingWindow,
    pm: Arc<Polymarket>,
    tx: mpsc::UnboundedSender<ResolvedWindow>,
    cancel: Arc<AtomicBool>,
) {
    let window_end_ts = old.window_start_ts + WINDOW_SECS;
    let final_pyth = fetch_final_pyth_retry(&pm, window_end_ts).await;
    let pm_winner_side =
        poll_pm_winner(&pm, &old.condition_id, old.yes_token, old.no_token, cancel).await;
    let _ = tx.send(ResolvedWindow {
        window_start_ts: old.window_start_ts,
        condition_id: old.condition_id,
        target: old.target,
        final_pyth,
        pm_winner_side,
        rows: old.rows,
    });
}

type TargetMsg = Result<Decimal, String>;

fn spawn_target_fetcher(
    pm: Arc<Polymarket>,
    tx: mpsc::UnboundedSender<TargetMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        for attempt in 1..=TARGET_FETCH_RETRIES {
            if attempt > 1 {
                tokio::time::sleep(TARGET_FETCH_BACKOFF).await;
            }
            let reason = match pm.current_btc_updown_5m_target().await {
                Ok(Some(v)) => {
                    let _ = tx.send(Ok(v));
                    return;
                }
                Ok(None) => "pyth returned no update for window-start ts".to_string(),
                Err(e) => format!("{e:#}"),
            };
            let _ = tx.send(Err(format!(
                "target [{attempt}/{TARGET_FETCH_RETRIES}]: {reason}"
            )));
        }
    })
}

fn snapshot_pending(state: &mut State) -> Option<PendingWindow> {
    if state.pending_rows.is_empty() {
        return None;
    }
    let rows = std::mem::take(&mut state.pending_rows);
    Some(PendingWindow {
        window_start_ts: state.window_start_ts,
        condition_id: state.condition_id.clone(),
        target: state.target_price,
        yes_token: state.yes_token,
        no_token: state.no_token,
        rows,
    })
}

fn sink_resolved(
    out: &mut BufWriter<File>,
    n_rows_written: &mut u64,
    n_windows_labeled: &mut u64,
    n_windows_unresolved: &mut u64,
    r: ResolvedWindow,
) {
    // Prefer the authoritative on-chain winner; fall back to Pyth
    // target-vs-final. resolved_source is written to the CSV so the label
    // quality is auditable after the fact rather than assumed.
    let (resolved_side, resolved_source) = match r.pm_winner_side {
        Some(s) => (Some(s), "pm"),
        None => match (r.target, r.final_pyth) {
            (Some(t), Some(f)) => (Some(if f > t { "YES" } else { "NO" }), "pyth"),
            _ => (None, ""),
        },
    };
    if resolved_side.is_some() {
        *n_windows_labeled += 1;
    } else {
        *n_windows_unresolved += 1;
    }
    for row in &r.rows {
        let this_ask = match row.side {
            "YES" => row.yes_book.ask,
            "NO" => row.no_book.ask,
            _ => None,
        };
        let (won, pnl) = match (resolved_side, this_ask) {
            (Some(rs), Some(ask)) => {
                let w = rs == row.side;
                (Some(w), Some(pnl_for(ask, w)))
            }
            _ => (None, None),
        };
        if let Err(e) = write_row(
            out,
            r.window_start_ts,
            &r.condition_id,
            row,
            r.final_pyth,
            resolved_side,
            resolved_source,
            won,
            pnl,
        ) {
            eprintln!("WARN: write row: {e:#}");
            return;
        }
        *n_rows_written += 1;
    }
    if let Err(e) = out.flush() {
        eprintln!("WARN: flush: {e:#}");
    }
}

async fn roll_window(
    state: &mut State,
    tx: &broadcast::Sender<RecordedEvent>,
    pm: &Arc<Polymarket>,
    pm_feed: &mut Option<PolymarketFeed>,
    target_tx: &mpsc::UnboundedSender<TargetMsg>,
    target_fetcher: &mut Option<JoinHandle<()>>,
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

    state.yes_token = yes_token;
    state.no_token = no_token;
    state.yes = SideBook::default();
    state.no = SideBook::default();
    state.condition_id = snapshot.condition_id;
    state.window_start_ts = window_start_ts;
    state.target_price = target;
    // Keep rolling BTC buffers; they're independent of window.
    // pending_rows was taken at snapshot_pending; ensure it's empty.
    state.pending_rows.clear();

    if let Some(h) = target_fetcher.take() {
        h.abort();
    }
    if target.is_none() {
        *target_fetcher = Some(spawn_target_fetcher(Arc::clone(pm), target_tx.clone()));
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut args = std::env::args().skip(1);
    let mut out_path: Option<PathBuf> = None;
    let mut explicit_market: Option<String> = None;
    let mut min_offset_s: u64 = 180;
    while let Some(a) = args.next() {
        match a.as_str() {
            "-o" | "--out" => {
                out_path = Some(PathBuf::from(
                    args.next().context("--out needs a path")?,
                ));
            }
            "--min-offset" => {
                let v = args.next().context("--min-offset needs a value")?;
                min_offset_s = v
                    .parse()
                    .with_context(|| format!("parsing --min-offset {v}"))?;
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
        let ts = now_wall_secs();
        PathBuf::from(format!("features-{ts}.csv"))
    });
    let mut out = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("creating {}", out_path.display()))?,
    );
    write_header(&mut out)?;
    eprintln!("writing to {}", out_path.display());
    eprintln!(
        "sampling at 1s ticks while offset_s ≥ {min_offset_s} ({}s remaining at start)",
        WINDOW_SECS.saturating_sub(min_offset_s),
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
    let _cb_feed = CoinbaseFeed::start(COINBASE_PRODUCT, tx.clone());
    let _kr_feed = KrakenFeed::start(KRAKEN_SYMBOL, tx.clone());
    let _bs_feed = BitstampFeed::start(BITSTAMP_PAIR, tx.clone());

    let mut state = State {
        yes_token,
        no_token,
        yes: SideBook::default(),
        no: SideBook::default(),
        condition_id: snapshot.condition_id.clone(),
        window_start_ts,
        target_price: target,
        btc: HashMap::new(),
        btc_samples: VecDeque::new(),
        btc_trades: VecDeque::new(),
        pending_rows: Vec::new(),
    };

    let (target_tx, mut target_rx) = mpsc::unbounded_channel::<TargetMsg>();
    let mut target_fetcher: Option<JoinHandle<()>> = if auto_roll && target.is_none() {
        Some(spawn_target_fetcher(Arc::clone(&pm), target_tx.clone()))
    } else {
        None
    };

    let (resolved_tx, mut resolved_rx) = mpsc::unbounded_channel::<ResolvedWindow>();
    let mut resolver_tasks: Vec<JoinHandle<()>> = Vec::new();
    // Set on SIGINT so in-flight winner polls stop waiting on PM (up to ~20
    // minutes) and fall back to Pyth immediately, letting shutdown finish
    // inside the container's stop grace period.
    let cancel = Arc::new(AtomicBool::new(false));

    let mut next_rollover: Option<Instant> = if auto_roll {
        Some(next_window_boundary()?)
    } else {
        None
    };
    let mut sample_tick = tokio::time::interval(Duration::from_secs(1));
    sample_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut summary_tick = tokio::time::interval(Duration::from_secs(30));
    summary_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let t_start = Instant::now();
    let mut n_rows_written: u64 = 0;
    let mut n_windows_labeled: u64 = 0;
    let mut n_windows_unresolved: u64 = 0;

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
            _ = sample_tick.tick() => {
                let now = Instant::now();
                let offset_s = now_wall_secs().saturating_sub(state.window_start_ts);
                if offset_s < WINDOW_SECS {
                    // Always update rolling BTC buffers, even before eligible
                    // — so slope/vol at min_offset_s already reflects 30s of
                    // history.
                    if let Some(m) = state.btc_median() {
                        state.btc_samples.push_back((now, m));
                    }
                    state.trim_rolling(now);
                    if offset_s >= min_offset_s {
                        sample_features(&mut state, now, offset_s);
                    }
                }
            }
            _ = tokio::time::sleep(rollover_in), if auto_roll => {
                let n_rows = state.pending_rows.len();
                if let Some(pending) = snapshot_pending(&mut state) {
                    let pm_c = pm.clone();
                    let tx_c = resolved_tx.clone();
                    let cancel_c = cancel.clone();
                    resolver_tasks.push(tokio::spawn(async move {
                        resolve_window(pending, pm_c, tx_c, cancel_c).await;
                    }));
                }
                match roll_window(
                    &mut state, &tx, &pm, &mut pm_feed, &target_tx, &mut target_fetcher,
                ).await {
                    Ok(()) => {
                        eprintln!(
                            "rollover: handed off {n_rows} rows for window {}; now tracking {}",
                            state.window_start_ts.saturating_sub(WINDOW_SECS),
                            state.condition_id,
                        );
                        next_rollover = next_window_boundary().ok();
                    }
                    Err(e) => {
                        eprintln!("WARN: rollover failed: {e:#}");
                        next_rollover = Some(Instant::now() + Duration::from_secs(2));
                    }
                }
                while target_rx.try_recv().is_ok() {}
            }
            Some(msg) = target_rx.recv() => {
                match msg {
                    Ok(value) => {
                        state.target_price = Some(value);
                    }
                    Err(reason) => {
                        eprintln!("WARN: {reason}");
                    }
                }
            }
            Some(r) = resolved_rx.recv() => {
                sink_resolved(
                    &mut out,
                    &mut n_rows_written,
                    &mut n_windows_labeled,
                    &mut n_windows_unresolved,
                    r,
                );
            }
            _ = summary_tick.tick() => {
                let elapsed = t_start.elapsed().as_secs_f64();
                eprintln!(
                    "[{:>7.1}s] rows={n_rows_written} windows_labeled={n_windows_labeled} \
                     unresolved={n_windows_unresolved} pending_buf={} btc_samples={} \
                     btc_trades_30s={}",
                    elapsed,
                    state.pending_rows.len(),
                    state.btc_samples.len(),
                    state.btc_trades.len(),
                );
            }
            _ = signal::ctrl_c() => {
                eprintln!("\nshutting down…");
                // Stop in-flight winner polls so they fall back to Pyth immediately;
                // otherwise shutdown blocks for up to ~20 min and the container is
                // SIGKILLed past its grace period, losing the final window's rows.
                cancel.store(true, Ordering::Relaxed);
                break Ok(());
            }
        }
    };

    // Drain any in-flight resolutions on shutdown — without this, the final
    // window's rows would be lost since their Pyth lookup is in progress.
    if let Some(pending) = snapshot_pending(&mut state) {
        let pm_c = pm.clone();
        let tx_c = resolved_tx.clone();
        let cancel_c = cancel.clone();
        resolver_tasks.push(tokio::spawn(async move {
            resolve_window(pending, pm_c, tx_c, cancel_c).await;
        }));
    }
    drop(resolved_tx);
    eprintln!("waiting on {} pending window resolution(s)…", resolver_tasks.len());
    for h in resolver_tasks {
        let _ = h.await;
    }
    while let Some(r) = resolved_rx.recv().await {
        sink_resolved(
            &mut out,
            &mut n_rows_written,
            &mut n_windows_labeled,
            &mut n_windows_unresolved,
            r,
        );
    }
    out.flush()?;
    eprintln!(
        "final: rows={n_rows_written} windows_labeled={n_windows_labeled} \
         unresolved={n_windows_unresolved}"
    );
    outcome
}
