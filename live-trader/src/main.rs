// live-trader: executes real Polymarket orders on the SAME entry criteria as
// `live-itm`, headless. During the eligible late part of each 5-min
// btc-updown-5m window it watches both outcome books and, for the first side
// whose ask sits in [--min-ask, --max-ask] with bid >= --min-bid AND whose
// direction matches the live BTC median-vs-target signal, it posts a resting
// GTC limit BUY at that ask sized to --notional USDC. The window locks after
// the first side fires (no straddles). At rollover it reads each order's fill
// (`size_matched`), cancels any unfilled remainder, resolves the window via
// Pyth (target vs final) + the on-chain PM winner, and logs one CSV row per
// order with realized PnL on the filled size.
//
// Order placement is LIVE by default. Pass --dry-run to run the full strategy
// against the live book without posting (and without needing a wallet).
//
// Entry criteria are intentionally identical to live-itm/src/main.rs — see the
// `try_trigger` / direction / window-lock logic there. The only additions here
// are sizing, posting, fill tracking, and cancel-on-rollover.

mod exec;

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use polymarket_core::{
    BitstampFeed, CexPayload, CexVenue, CoinbaseFeed, Decimal, FeedSource, KrakenFeed,
    MarketSnapshot, Polymarket, PolymarketEvent, PolymarketFeed, PolymarketPayload,
    RecordedEvent, U256,
};
use tokio::signal;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use exec::{ExecCmd, ExecConfig, PlaceOutcome, spawn_executor};

const COINBASE_PRODUCT: &str = "BTC-USD";
const KRAKEN_SYMBOL: &str = "BTC/USD";
const BITSTAMP_PAIR: &str = "btcusd";
const WINDOW_SECS: u64 = 300;
const VENUES: &[CexVenue] = &[CexVenue::Coinbase, CexVenue::Kraken, CexVenue::Bitstamp];
const MEDIAN_FRESHNESS: Duration = Duration::from_secs(5);
const SWING_BUFFER_RETENTION: Duration = Duration::from_secs(60);
const POLYGON_CHAIN_ID: u64 = 137;
const DEFAULT_KEY_ENV: &str = "POLY_PRIVATE_KEY";
const DEFAULT_CLOB_HOST: &str = "https://clob.polymarket.com";

const TARGET_FETCH_RETRIES: u32 = 5;
const TARGET_FETCH_BACKOFF: Duration = Duration::from_secs(2);

/// One simulated-then-real entry on a side. `ask`/`offset_s`/`swing_at_entry`
/// are captured at trigger time (identical to live-itm); the rest are filled
/// in once the order is posted and again at settlement.
#[derive(Clone)]
struct Entry {
    ask: Decimal,
    offset_s: u64,
    swing_at_entry: Option<Decimal>,
    /// Multi-venue BTC median at the instant the entry triggered. Compared
    /// against `final_pyth` at settlement to log how far BTC moved post-entry.
    btc_at_entry: Option<Decimal>,
    /// Intended share size = round(notional / ask, 2). Set at submit.
    size: Decimal,
    /// Whether we've already sent the Place command for this entry.
    submitted: bool,
    order_id: Option<String>,
    /// Post status string and any error, for the CSV/log.
    status: String,
    error: Option<String>,
    /// Filled shares, read at settle via the order's `size_matched`.
    matched: Decimal,
}

impl Entry {
    fn new(
        ask: Decimal,
        offset_s: u64,
        swing_at_entry: Option<Decimal>,
        btc_at_entry: Option<Decimal>,
    ) -> Self {
        Entry {
            ask,
            offset_s,
            swing_at_entry,
            btc_at_entry,
            size: Decimal::ZERO,
            submitted: false,
            order_id: None,
            status: String::new(),
            error: None,
            matched: Decimal::ZERO,
        }
    }
}

struct SideState {
    token_id: U256,
    ask: Option<Decimal>,
    bid: Option<Decimal>,
    entry: Option<Entry>,
    /// Rolling (recv_time, ask) buffer for the swing-at-entry feature.
    ask_samples: VecDeque<(Instant, Decimal)>,
}

impl SideState {
    fn new(token_id: U256) -> Self {
        SideState {
            token_id,
            ask: None,
            bid: None,
            entry: None,
            ask_samples: VecDeque::new(),
        }
    }

    fn push_ask_sample(&mut self, now: Instant, ask: Decimal) {
        self.ask_samples.push_back((now, ask));
        if let Some(cutoff) = now.checked_sub(SWING_BUFFER_RETENTION) {
            while let Some(&(t, _)) = self.ask_samples.front() {
                if t < cutoff {
                    self.ask_samples.pop_front();
                } else {
                    break;
                }
            }
        }
    }

    fn ask_move_over(&self, lookback: Duration) -> Option<Decimal> {
        let now = Instant::now();
        let target_t = now.checked_sub(lookback)?;
        let baseline = self
            .ask_samples
            .iter()
            .take_while(|(t, _)| *t <= target_t)
            .last()
            .map(|(_, v)| *v)?;
        let current = self.ask?;
        Some(current - baseline)
    }
}

#[derive(Default)]
struct VenueState {
    last: Option<Decimal>,
    last_at: Option<Instant>,
}

#[derive(Clone, Copy)]
struct Target {
    value: Decimal,
}

struct PmState {
    condition_id: String,
    window_start_ts: u64,
    yes: SideState,
    no: SideState,
}

struct State {
    pm: PmState,
    btc: HashMap<CexVenue, VenueState>,
    btc_target: Option<Target>,

    out: BufWriter<File>,
    exec_tx: mpsc::UnboundedSender<ExecCmd>,

    notional: Decimal,
    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    swing_lookback: Duration,

    n_orders: u64,
    n_wins: u64,
    n_losses: u64,
    n_unresolved: u64,
    pnl: Decimal,
}

impl State {
    fn btc_median_last(&self) -> Option<Decimal> {
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
        let n = prices.len();
        let mid = n / 2;
        Some(if n % 2 == 1 {
            prices[mid]
        } else {
            (prices[mid - 1] + prices[mid]) / Decimal::from(2)
        })
    }

    /// Direction confirmation, identical to live-itm: median above target ->
    /// favor YES, below -> favor NO, unknown -> None (skip the trigger).
    fn direction(&self) -> Option<&'static str> {
        match (self.btc_target, self.btc_median_last()) {
            (Some(t), Some(m)) if m > t.value => Some("YES"),
            (Some(t), Some(m)) if m < t.value => Some("NO "),
            _ => None,
        }
    }
}

fn offset_now(window_start_ts: u64) -> u64 {
    now_wall_secs().saturating_sub(window_start_ts)
}

fn now_wall_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// First qualifying tick on a side captures the entry. Mirrors live-itm's
/// `try_trigger` exactly: eligible offset window, ask band (inclusive both
/// ends), bid floor, and direction match. The window-lock (only the first
/// side may fire) is enforced by the caller via `window_locked`.
#[allow(clippy::too_many_arguments)]
fn try_trigger(
    side: &mut SideState,
    side_label: &'static str,
    ask: Decimal,
    offset_s: u64,
    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    direction: Option<&'static str>,
    swing: Option<Decimal>,
    btc_median: Option<Decimal>,
) {
    side.ask = Some(ask);
    if side.entry.is_some() {
        return;
    }
    if offset_s < min_offset_s || offset_s >= max_offset_s {
        return;
    }
    if ask < min_ask || ask > max_ask {
        return;
    }
    let Some(bid) = side.bid else {
        return;
    };
    if bid < min_bid {
        return;
    }
    if direction != Some(side_label) {
        return;
    }
    side.entry = Some(Entry::new(ask, offset_s, swing, btc_median));
}

fn apply_pm(state: &mut State, e: &PolymarketEvent) {
    let now = Instant::now();
    let offset_s = offset_now(state.pm.window_start_ts);
    let min_ask = state.min_ask;
    let max_ask = state.max_ask;
    let min_bid = state.min_bid;
    let min_off = state.min_offset_s;
    let max_off = state.max_offset_s;
    let swing_lookback = state.swing_lookback;
    let direction = state.direction();
    let btc_median = state.btc_median_last();
    let yes_token = state.pm.yes.token_id;
    let no_token = state.pm.no.token_id;
    // Window lock: if either side already has an entry, only update book state
    // and stop evaluating triggers — prevents both legs of a straddle.
    let window_locked = state.pm.yes.entry.is_some() || state.pm.no.entry.is_some();

    let handle = |side: &mut SideState,
                  label: &'static str,
                  bid: Option<Decimal>,
                  ask: Option<Decimal>| {
        if let Some(b) = bid {
            side.bid = Some(b);
        }
        if let Some(a) = ask {
            side.push_ask_sample(now, a);
            let swing = side.ask_move_over(swing_lookback);
            if window_locked {
                side.ask = Some(a);
            } else {
                try_trigger(
                    side, label, a, offset_s, min_ask, max_ask, min_bid, min_off, max_off,
                    direction, swing, btc_median,
                );
            }
        }
    };

    match &e.payload {
        PolymarketPayload::Book(b) => {
            let bid = b.bids.first().map(|l| l.price);
            let ask = b.asks.first().map(|l| l.price);
            if b.asset_id == yes_token {
                handle(&mut state.pm.yes, "YES", bid, ask);
            } else if b.asset_id == no_token {
                handle(&mut state.pm.no, "NO ", bid, ask);
            }
        }
        PolymarketPayload::PriceChange(p) => {
            for entry in &p.price_changes {
                if entry.asset_id == yes_token {
                    handle(&mut state.pm.yes, "YES", entry.best_bid, entry.best_ask);
                } else if entry.asset_id == no_token {
                    handle(&mut state.pm.no, "NO ", entry.best_bid, entry.best_ask);
                }
            }
        }
        PolymarketPayload::LastTrade(_) => {}
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
                CexPayload::Ticker { last, .. } => v.last = Some(*last),
                CexPayload::Trade { price, .. } => v.last = Some(*price),
            }
            // Median fallback target (md): once every venue has printed and we
            // still have no px target, seed it from the median. live-itm does
            // the same so direction can resolve even if Pyth is slow.
            if state.btc_target.is_none()
                && VENUES
                    .iter()
                    .all(|venue| state.btc.get(venue).and_then(|s| s.last).is_some())
                && let Some(value) = state.btc_median_last()
            {
                state.btc_target = Some(Target { value });
            }
        }
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

/// After draining events, post any captured-but-unsubmitted entry. Sizing is
/// fixed USDC notional: shares = round(notional / ask, 2).
async fn submit_pending(state: &mut State) {
    for label in ["YES", "NO "] {
        let side = if label == "YES" {
            &mut state.pm.yes
        } else {
            &mut state.pm.no
        };
        let Some(entry) = side.entry.as_mut() else {
            continue;
        };
        if entry.submitted {
            continue;
        }
        entry.submitted = true; // mark first so a failed send isn't retried in a loop

        let size = (state.notional / entry.ask).round_dp(2);
        if size <= Decimal::ZERO {
            entry.status = "skipped: size<=0".to_string();
            eprintln!(">>> {label} entry @ {} but size {size} <= 0, not posting", entry.ask);
            continue;
        }
        entry.size = size;
        let token_id = side.token_id;
        let price = entry.ask;

        let (reply_tx, reply_rx) = oneshot::channel::<PlaceOutcome>();
        if state
            .exec_tx
            .send(ExecCmd::Place {
                token_id,
                price,
                size,
                reply: reply_tx,
            })
            .is_err()
        {
            entry.status = "executor gone".to_string();
            eprintln!("WARN: executor channel closed; cannot post {label} order");
            continue;
        }
        match reply_rx.await {
            Ok(outcome) => {
                let remaining = WINDOW_SECS.saturating_sub(entry.offset_s);
                entry.order_id = outcome.order_id.clone();
                entry.status = outcome.status.clone();
                entry.error = outcome.error.clone();
                state.n_orders += 1;
                eprintln!(
                    ">>> ORDER {label} BUY {size} @ {price} (notional ~{}) +{}s in, {remaining}s left | status={} id={} took={}{}",
                    state.notional,
                    entry.offset_s,
                    outcome.status,
                    outcome.order_id.as_deref().unwrap_or("—"),
                    outcome.immediate_taking,
                    outcome
                        .error
                        .as_ref()
                        .map(|e| format!(" err={e}"))
                        .unwrap_or_default(),
                );
            }
            Err(_) => {
                entry.status = "no reply".to_string();
                eprintln!("WARN: no reply from executor for {label} order");
            }
        }
    }
}

/// At rollover, ask the executor for each posted order's fill and cancel the
/// remainder. Mutates `matched` on the entries in place.
async fn settle_orders(state: &mut State) {
    for label in ["YES", "NO "] {
        let side = if label == "YES" {
            &mut state.pm.yes
        } else {
            &mut state.pm.no
        };
        let Some(entry) = side.entry.as_mut() else {
            continue;
        };
        let Some(order_id) = entry.order_id.clone() else {
            continue;
        };
        let (reply_tx, reply_rx) = oneshot::channel::<Decimal>();
        if state
            .exec_tx
            .send(ExecCmd::Settle {
                order_id,
                reply: reply_tx,
            })
            .is_err()
        {
            continue;
        }
        if let Ok(matched) = reply_rx.await {
            entry.matched = matched;
        }
    }
}

struct OldWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    yes_token: U256,
    no_token: U256,
    yes_entry: Option<Entry>,
    no_entry: Option<Entry>,
}

struct ResolvedWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
    /// On-chain winner side if PM resolved it; else None and we fall back to Pyth.
    pm_winner_side: Option<&'static str>,
    yes_entry: Option<Entry>,
    no_entry: Option<Entry>,
}

/// Build the closing window's resolver input. Returns None when neither side
/// posted an order — nothing to resolve or log.
fn snapshot_window(state: &State) -> Option<OldWindow> {
    if state.pm.yes.entry.is_none() && state.pm.no.entry.is_none() {
        return None;
    }
    Some(OldWindow {
        window_start_ts: state.pm.window_start_ts,
        condition_id: state.pm.condition_id.clone(),
        target: state.btc_target.map(|t| t.value),
        yes_token: state.pm.yes.token_id,
        no_token: state.pm.no.token_id,
        yes_entry: state.pm.yes.entry.clone(),
        no_entry: state.pm.no.entry.clone(),
    })
}

/// Hermes 404s on boundary-aligned recent timestamps; walk the ts back on each
/// retry. The BTC price 1–30s before window end is functionally identical for
/// binary up/down resolution. (Same approach as live-itm.)
async fn fetch_final_pyth_retry(pm: &Polymarket, window_end_ts: u64) -> Option<Decimal> {
    let attempts = [(0_u64, 0_i64), (2, -1), (3, -3), (5, -10), (10, -30)];
    for &(delay, offset) in attempts.iter() {
        if delay > 0 {
            tokio::time::sleep(Duration::from_secs(delay)).await;
        }
        let ts = (window_end_ts as i64 + offset).max(0) as u64;
        if let Ok(Some(v)) = pm.pyth_btc_usd_at(ts).await {
            return Some(v);
        }
    }
    None
}

/// Poll the CLOB market until it closes and reports a winner; match the winner
/// token back to a side. Returns None if it never resolves within the budget.
async fn poll_pm_winner(
    pm: &Polymarket,
    condition_id: &str,
    yes_token: U256,
    no_token: U256,
) -> Option<&'static str> {
    tokio::time::sleep(Duration::from_secs(5)).await;
    for _ in 0..60 {
        if let Ok(Some(token_id)) = pm.market_winner(condition_id).await {
            if token_id == yes_token {
                return Some("YES");
            }
            if token_id == no_token {
                return Some("NO ");
            }
            return None;
        }
        tokio::time::sleep(Duration::from_secs(5)).await;
    }
    None
}

async fn resolve_window(
    old: OldWindow,
    pm: Arc<Polymarket>,
    tx: mpsc::UnboundedSender<ResolvedWindow>,
) {
    let window_end_ts = old.window_start_ts + WINDOW_SECS;
    let final_pyth = fetch_final_pyth_retry(&pm, window_end_ts).await;
    let pm_winner_side =
        poll_pm_winner(&pm, &old.condition_id, old.yes_token, old.no_token).await;
    let _ = tx.send(ResolvedWindow {
        window_start_ts: old.window_start_ts,
        condition_id: old.condition_id,
        target: old.target,
        final_pyth,
        pm_winner_side,
        yes_entry: old.yes_entry,
        no_entry: old.no_entry,
    });
}

/// The window's 5-minute period index within the local day: 00:00 local -> 0,
/// 00:05 -> 1, ... 23:55 -> 287. `window_start_ts` is always 300s-aligned so
/// this is exact. None only if the timestamp can't be mapped to local time.
fn period_of_day(window_start_ts: u64) -> Option<u32> {
    use chrono::{Local, TimeZone, Timelike};
    Local
        .timestamp_opt(window_start_ts as i64, 0)
        .single()
        .map(|dt| dt.num_seconds_from_midnight() / 300)
}

fn pnl_per_share(entry_ask: Decimal, win: bool) -> Decimal {
    if win {
        Decimal::ONE - entry_ask
    } else {
        -entry_ask
    }
}

fn sink_resolved(state: &mut State, r: ResolvedWindow) {
    // Prefer the authoritative on-chain winner; fall back to Pyth target-vs-final.
    let (resolved_side, _source) = match r.pm_winner_side {
        Some(s) => (Some(s), "pm"),
        None => match (r.target, r.final_pyth) {
            (Some(t), Some(f)) => (Some(if f > t { "YES" } else { "NO " }), "pyth"),
            _ => (None, ""),
        },
    };

    for (side, entry_opt) in [("YES", r.yes_entry), ("NO ", r.no_entry)] {
        let Some(entry) = entry_opt else { continue };
        let (won, realized) = match resolved_side {
            Some(rs) => {
                let win = rs.trim() == side.trim();
                // Realized PnL is on the FILLED shares only.
                let realized = entry.matched * pnl_per_share(entry.ask, win);
                if entry.matched > Decimal::ZERO {
                    if win {
                        state.n_wins += 1;
                    } else {
                        state.n_losses += 1;
                    }
                    state.pnl += realized;
                }
                (Some(win), realized)
            }
            None => {
                state.n_unresolved += 1;
                (None, Decimal::ZERO)
            }
        };
        if let Err(e) = write_row(
            &mut state.out,
            r.window_start_ts,
            &r.condition_id,
            side.trim(),
            &entry,
            state.notional,
            r.target,
            r.final_pyth,
            resolved_side.map(|s| s.trim()),
            won,
            realized,
        ) {
            eprintln!("WARN: write row: {e:#}");
        }
    }
}

fn write_header(w: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        w,
        "window_start_ts,period_of_day,condition_id,side,order_id,intended_ask,size_shares,size_matched,\
         notional_target,entry_offset_s,swing_at_entry,btc_at_entry,post_status,post_error,\
         target_pyth,final_pyth,price_diff_from_entry,resolved_side,won,realized_pnl"
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
    entry: &Entry,
    notional: Decimal,
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
    resolved_side: Option<&str>,
    won: Option<bool>,
    realized: Decimal,
) -> Result<()> {
    let order_id = entry.order_id.as_deref().unwrap_or("");
    let swing_s = entry
        .swing_at_entry
        .map(|d| d.to_string())
        .unwrap_or_default();
    let btc_entry_s = entry
        .btc_at_entry
        .map(|d| d.to_string())
        .unwrap_or_default();
    // Signed BTC move from entry to settlement (final − entry); blank unless
    // both the entry-time median and the settlement Pyth price are known.
    let price_diff_s = match (entry.btc_at_entry, final_pyth) {
        (Some(entry_px), Some(final_px)) => (final_px - entry_px).to_string(),
        _ => String::new(),
    };
    // Keep status/error inside a single CSV field by swapping commas.
    let status_s = entry.status.replace(',', ";");
    let error_s = entry.error.as_deref().unwrap_or("").replace(',', ";");
    let target_s = target.map(|d| d.to_string()).unwrap_or_default();
    let final_s = final_pyth.map(|d| d.to_string()).unwrap_or_default();
    let resolved_s = resolved_side.unwrap_or("");
    let won_s = won.map(|b| if b { "1" } else { "0" }).unwrap_or("");
    let period_s = period_of_day(window_start_ts)
        .map(|p| p.to_string())
        .unwrap_or_default();
    writeln!(
        w,
        "{window_start_ts},{period_s},{condition_id},{side},{order_id},{},{},{},{notional},{},{swing_s},{btc_entry_s},{status_s},{error_s},{target_s},{final_s},{price_diff_s},{resolved_s},{won_s},{realized}",
        entry.ask, entry.size, entry.matched, entry.offset_s,
    )?;
    w.flush()?;
    Ok(())
}

fn pick_yes_no(snapshot: &MarketSnapshot) -> Result<(U256, U256)> {
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
        .map(|(_, o)| o.token_id)
        .ok_or_else(|| anyhow!("market has no second outcome token"))?;
    Ok((yes, no))
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
            match pm.current_btc_updown_5m_target().await {
                Ok(Some(v)) => {
                    let _ = tx.send(Ok(v));
                    return;
                }
                Ok(None) => {
                    let _ = tx.send(Err(format!(
                        "px target [{attempt}/{TARGET_FETCH_RETRIES}]: pyth had no update"
                    )));
                }
                Err(e) => {
                    let _ = tx.send(Err(format!(
                        "px target [{attempt}/{TARGET_FETCH_RETRIES}]: {e:#}"
                    )));
                }
            }
        }
    })
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
    let (yes_token, no_token) = pick_yes_no(&snapshot)?;
    let window_start_ts = current_window_start_ts()?;
    let token_ids: Vec<U256> = snapshot.outcomes.iter().map(|o| o.token_id).collect();

    *pm_feed = None;
    *pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);

    state.pm = PmState {
        condition_id: snapshot.condition_id,
        window_start_ts,
        yes: SideState::new(yes_token),
        no: SideState::new(no_token),
    };
    state.btc_target = None;

    if let Some(h) = target_fetcher.take() {
        h.abort();
    }
    *target_fetcher = Some(spawn_target_fetcher(Arc::clone(pm), target_tx.clone()));
    Ok(())
}

fn print_summary(state: &State) {
    let resolved = state.n_wins + state.n_losses;
    let win_rate = if resolved > 0 {
        state.n_wins as f64 * 100.0 / resolved as f64
    } else {
        0.0
    };
    eprintln!(
        "orders={} filled_wins={} filled_losses={} unresolved={} | win_rate={:.1}% realized_pnl={}",
        state.n_orders, state.n_wins, state.n_losses, state.n_unresolved, win_rate, state.pnl,
    );
}

struct Args {
    out_path: Option<PathBuf>,
    explicit_market: Option<String>,
    notional: Decimal,
    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    swing_lookback_s: u64,
    key_env: String,
    host: String,
    chain_id: u64,
    funder: Option<String>,
    dry_run: bool,
}

fn parse_args() -> Result<Args> {
    let mut a = Args {
        out_path: None,
        explicit_market: None,
        notional: Decimal::new(5, 0),
        min_ask: Decimal::new(95, 2),
        max_ask: Decimal::new(99, 2),
        min_bid: Decimal::new(50, 2),
        min_offset_s: 240,
        max_offset_s: WINDOW_SECS - 10,
        swing_lookback_s: 10,
        key_env: DEFAULT_KEY_ENV.to_string(),
        host: DEFAULT_CLOB_HOST.to_string(),
        chain_id: POLYGON_CHAIN_ID,
        funder: None,
        dry_run: false,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-o" | "--out" => {
                a.out_path = Some(PathBuf::from(args.next().context("--out needs a path")?))
            }
            "--notional" => {
                let v = args.next().context("--notional needs a value")?;
                a.notional = v.parse().with_context(|| format!("parsing --notional {v}"))?;
            }
            "--min-ask" => {
                let v = args.next().context("--min-ask needs a value")?;
                a.min_ask = v.parse().with_context(|| format!("parsing --min-ask {v}"))?;
            }
            "--max-ask" => {
                let v = args.next().context("--max-ask needs a value")?;
                a.max_ask = v.parse().with_context(|| format!("parsing --max-ask {v}"))?;
            }
            "--min-bid" => {
                let v = args.next().context("--min-bid needs a value")?;
                a.min_bid = v.parse().with_context(|| format!("parsing --min-bid {v}"))?;
            }
            "--min-offset" => {
                let v = args.next().context("--min-offset needs a value")?;
                a.min_offset_s = v.parse().with_context(|| format!("parsing --min-offset {v}"))?;
            }
            "--max-offset" => {
                let v = args.next().context("--max-offset needs a value")?;
                a.max_offset_s = v.parse().with_context(|| format!("parsing --max-offset {v}"))?;
            }
            "--swing-lookback" => {
                let v = args.next().context("--swing-lookback needs a value")?;
                a.swing_lookback_s =
                    v.parse().with_context(|| format!("parsing --swing-lookback {v}"))?;
            }
            "--key-env" => {
                a.key_env = args.next().context("--key-env needs a value")?;
            }
            "--host" => {
                a.host = args.next().context("--host needs a value")?;
            }
            "--chain-id" => {
                let v = args.next().context("--chain-id needs a value")?;
                a.chain_id = v.parse().with_context(|| format!("parsing --chain-id {v}"))?;
            }
            "--funder" => {
                a.funder = Some(args.next().context("--funder needs an address")?);
            }
            "--dry-run" => a.dry_run = true,
            other => {
                if a.explicit_market.is_none() {
                    a.explicit_market = Some(other.to_string());
                } else {
                    anyhow::bail!("unexpected arg: {other}");
                }
            }
        }
    }
    Ok(a)
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let args = parse_args()?;
    let auto_roll = args.explicit_market.is_none();

    // Read the private key from the environment (never the command line) unless
    // dry-running, in which case no wallet is needed.
    let private_key = if args.dry_run {
        String::new()
    } else {
        std::env::var(&args.key_env).with_context(|| {
            format!(
                "reading private key from ${} (set it, or pass --dry-run)",
                args.key_env
            )
        })?
    };

    let out_path = args.out_path.clone().unwrap_or_else(|| {
        let stamp = chrono::Local::now().format("%m-%d-%Y-%H.%M");
        PathBuf::from(format!("data/trade-{stamp}.csv"))
    });
    if let Some(parent) = out_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating dir {}", parent.display()))?;
    }
    let mut out = BufWriter::new(
        File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?,
    );
    write_header(&mut out)?;

    eprintln!(
        "live-trader {} | notional=${} ask in [{}, {}] bid>={} offset [{}, {})s swing={}s",
        if args.dry_run { "DRY-RUN" } else { "LIVE" },
        args.notional,
        args.min_ask,
        args.max_ask,
        args.min_bid,
        args.min_offset_s,
        args.max_offset_s,
        args.swing_lookback_s,
    );
    if !args.dry_run {
        eprintln!("!!! LIVE: real GTC limit BUY orders will be posted to Polymarket !!!");
    }

    // Bring up the executor and wait for authentication before trading.
    let (exec_tx, ready_rx, _exec_handle) = spawn_executor(ExecConfig {
        host: args.host.clone(),
        private_key,
        chain_id: args.chain_id,
        funder: args.funder.clone(),
        dry_run: args.dry_run,
    });
    match ready_rx.await {
        Ok(Ok(addr)) => eprintln!("executor ready (wallet {addr})"),
        Ok(Err(e)) => return Err(anyhow!("executor failed to start: {e}")),
        Err(_) => return Err(anyhow!("executor task died before signaling readiness")),
    }

    let pm = Arc::new(Polymarket::new()?);
    let condition_id = match &args.explicit_market {
        Some(m) => pm.resolve_condition_id(m).await?,
        None => pm
            .current_btc_updown_5m_condition_id()
            .await
            .context("resolving current btc-updown-5m market")?,
    };
    let snapshot = pm.fetch_snapshot(&condition_id).await?;
    if snapshot.outcomes.is_empty() {
        return Err(anyhow!("market has no outcome tokens"));
    }
    let (yes_token, no_token) = pick_yes_no(&snapshot)?;
    let window_start_ts = current_window_start_ts()?;
    eprintln!("market: {} ({})", snapshot.question, snapshot.condition_id);

    let token_ids: Vec<U256> = snapshot.outcomes.iter().map(|o| o.token_id).collect();
    let (tx, mut rx) = broadcast::channel::<RecordedEvent>(4096);
    let mut pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);
    let _cb = CoinbaseFeed::start(COINBASE_PRODUCT, tx.clone());
    let _kr = KrakenFeed::start(KRAKEN_SYMBOL, tx.clone());
    let _bs = BitstampFeed::start(BITSTAMP_PAIR, tx.clone());

    let (target_tx, mut target_rx) = mpsc::unbounded_channel::<TargetMsg>();
    let mut target_fetcher: Option<JoinHandle<()>> = if auto_roll {
        Some(spawn_target_fetcher(Arc::clone(&pm), target_tx.clone()))
    } else {
        None
    };

    let mut state = State {
        pm: PmState {
            condition_id: snapshot.condition_id.clone(),
            window_start_ts,
            yes: SideState::new(yes_token),
            no: SideState::new(no_token),
        },
        btc: HashMap::new(),
        btc_target: None,
        out,
        exec_tx,
        notional: args.notional,
        min_ask: args.min_ask,
        max_ask: args.max_ask,
        min_bid: args.min_bid,
        min_offset_s: args.min_offset_s,
        max_offset_s: args.max_offset_s,
        swing_lookback: Duration::from_secs(args.swing_lookback_s),
        n_orders: 0,
        n_wins: 0,
        n_losses: 0,
        n_unresolved: 0,
        pnl: Decimal::ZERO,
    };

    let mut next_rollover: Option<Instant> =
        if auto_roll { Some(next_window_boundary()?) } else { None };
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
                    Err(broadcast::error::RecvError::Lagged(n)) => eprintln!("WARN: lagged {n}"),
                    Err(broadcast::error::RecvError::Closed) => break Ok(()),
                }
                loop {
                    match rx.try_recv() {
                        Ok(evt) => apply_event(&mut state, evt),
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Lagged(n)) => eprintln!("WARN: lagged {n}"),
                        Err(broadcast::error::TryRecvError::Closed) => break,
                    }
                }
                // Post any freshly-captured entry.
                submit_pending(&mut state).await;
            }
            Some(r) = resolved_rx.recv() => sink_resolved(&mut state, r),
            Some(msg) = target_rx.recv() => {
                match msg {
                    Ok(value) => state.btc_target = Some(Target { value }),
                    Err(reason) => eprintln!("WARN: {reason}"),
                }
            }
            _ = tokio::time::sleep(rollover_in), if auto_roll => {
                // Settle the closing window's orders (read fills + cancel
                // remainders) before handing off to the async resolver.
                settle_orders(&mut state).await;
                if let Some(old) = snapshot_window(&state) {
                    let pm_c = pm.clone();
                    let tx_c = resolved_tx.clone();
                    resolver_tasks.push(tokio::spawn(async move {
                        resolve_window(old, pm_c, tx_c).await;
                    }));
                }
                match roll_window(&mut state, &tx, &pm, &mut pm_feed, &target_tx, &mut target_fetcher).await {
                    Ok(()) => {
                        next_rollover = next_window_boundary().ok();
                        eprintln!("rollover: now trading {}", state.pm.condition_id);
                    }
                    Err(e) => {
                        eprintln!("WARN: rollover failed: {e:#}");
                        next_rollover = Some(Instant::now() + Duration::from_secs(2));
                    }
                }
                while target_rx.try_recv().is_ok() {}
            }
            _ = summary_tick.tick() => print_summary(&state),
            _ = signal::ctrl_c() => {
                eprintln!("\nshutting down…");
                break Ok(());
            }
        }
    };

    // Settle + resolve the final in-progress window on shutdown.
    settle_orders(&mut state).await;
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
    print_summary(&state);
    outcome
}
