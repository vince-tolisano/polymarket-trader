// dry-trader: runs the live-trader entry strategy against the live book
// WITHOUT posting orders — no wallet, no sizing, nothing spent. It exists to
// collect data: defaults are deliberately LOOSER than live-trader's so runs
// map where the edge lives across the feature space, and each entry is scored
// per-share exactly like live-itm (win = 1 − ask, loss = −ask). CSVs default
// to dry-data/ and follow live-itm's schema (per-share pnl, no order/size
// columns), so there is no notional and no fill simulation to argue with.
//
// !!! KEEP IN SYNC with live-trader/src/main.rs !!!
// The entry criteria, captured features, window/rollover handling, and
// resolution logic here are copies of live-trader (which in turn mirrors
// live-itm/src/main.rs). Any change to that logic must be made in BOTH files
// — only the criteria DEFAULTS are allowed to differ (loose here, strict
// there), so dry data stays comparable to live data under the same flags.
// The CSV schema follows live-itm's write_row, not live-trader's.

use std::collections::{HashMap, HashSet, VecDeque};
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
use tokio::signal;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const COINBASE_PRODUCT: &str = "BTC-USD";
const KRAKEN_SYMBOL: &str = "BTC/USD";
const BITSTAMP_PAIR: &str = "btcusd";
const WINDOW_SECS: u64 = 300;
const VENUES: &[CexVenue] = &[CexVenue::Coinbase, CexVenue::Kraken, CexVenue::Bitstamp];
const MEDIAN_FRESHNESS: Duration = Duration::from_secs(5);
const SWING_BUFFER_RETENTION: Duration = Duration::from_secs(60);
// Rollover retry backoff. Short at first so a transient blip costs at most one
// window, then doubling so a sustained outage can't spin the rollover arm — at
// the old flat 2s a stuck rollover retried ~30x/minute for as long as it lasted.
const ROLLOVER_RETRY_MIN: Duration = Duration::from_secs(2);
const ROLLOVER_RETRY_MAX: Duration = Duration::from_secs(60);

const TARGET_FETCH_RETRIES: u32 = 5;
const TARGET_FETCH_BACKOFF: Duration = Duration::from_secs(2);

/// One recorded entry on a side, captured at trigger time. Identical to
/// live-itm's Entry: no size, no order — the row IS the trade.
#[derive(Clone, Copy)]
struct Entry {
    ask: Decimal,
    offset_s: u64,
    /// Side's ask delta over `swing_lookback` at trigger time.
    swing_at_entry: Option<Decimal>,
    /// Multi-venue BTC median at the instant the entry triggered. Compared
    /// against the window target to log how far in-the-money the side already
    /// was at entry (`price_diff_from_entry` = entry − target).
    btc_at_entry: Option<Decimal>,
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

    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    /// Minimum absolute BTC distance (USD) between the median and the target
    /// required to enter — filters out marginal entries sitting on the strike.
    min_target_dist: Decimal,
    /// Maximum absolute BTC distance (USD) allowed to enter. Decimal::MAX
    /// means uncapped; set it to run the near-strike inverse experiment.
    max_target_dist: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    swing_lookback: Duration,

    n_entries: u64,
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

/// First qualifying tick on a side captures the entry. Mirrors live-trader's
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
    target: Option<Decimal>,
    min_target_dist: Decimal,
    max_target_dist: Decimal,
) {
    side.ask = Some(ask);
    if side.entry.is_some() {
        return;
    }
    if offset_s < min_offset_s || offset_s >= max_offset_s {
        return;
    }
    // Ask band is (min_ask, max_ask]: exclude min_ask itself, include max_ask.
    if ask <= min_ask || ask > max_ask {
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
    // Distance past the strike, as a band [min_target_dist, max_target_dist]
    // in USD. The floor filters out marginal entries sitting right on the
    // strike where a tiny reversal flips the outcome. The ceiling is the
    // INVERSE experiment: out-of-sample data through 2026-07-27 shows edge
    // falling as distance grows (the ask more than prices the distance in),
    // so capping it isolates the near-strike entries the floor throws away.
    // Default ceiling is Decimal::MAX, i.e. no cap — set it explicitly.
    match (btc_median, target) {
        (Some(m), Some(t))
            if (m - t).abs() >= min_target_dist && (m - t).abs() <= max_target_dist => {}
        _ => return,
    }
    side.entry = Some(Entry {
        ask,
        offset_s,
        swing_at_entry: swing,
        btc_at_entry: btc_median,
    });
    let remaining = WINDOW_SECS.saturating_sub(offset_s);
    eprintln!(">>> DRY ENTRY {side_label} @ {ask} +{offset_s}s in, {remaining}s left");
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
    let target_val = state.btc_target.map(|t| t.value);
    let min_target_dist = state.min_target_dist;
    let max_target_dist = state.max_target_dist;
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
                    direction, swing, btc_median, target_val, min_target_dist,
                    max_target_dist,
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
/// triggered — nothing to resolve or log.
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
        yes_entry: state.pm.yes.entry,
        no_entry: state.pm.no.entry,
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
/// See live-trader for the rationale on the patient ~20-minute budget.
async fn poll_pm_winner(
    pm: &Polymarket,
    condition_id: &str,
    yes_token: U256,
    no_token: U256,
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
        yes_entry: old.yes_entry,
        no_entry: old.no_entry,
    });
}

/// Per-share PnL, same as live-itm: win = 1 − ask, loss = −ask.
fn pnl_for(entry_ask: Decimal, win: bool) -> Decimal {
    if win {
        Decimal::ONE - entry_ask
    } else {
        -entry_ask
    }
}

/// True iff the side's ask was falling over the lookback window — i.e.
/// the market was moving against the side we just bought. Symmetric for
/// YES/NO: a down move in the token's own ask is always "the market
/// doubts this side." Unknown swing → None. (Same as live-itm.)
fn entered_on_down_swing(swing: Option<Decimal>) -> Option<bool> {
    let s = swing?;
    Some(s.is_sign_negative() && !s.is_zero())
}

fn sink_resolved(state: &mut State, r: ResolvedWindow) {
    // Prefer the authoritative on-chain winner; fall back to Pyth target-vs-final.
    let (resolved_side, source) = match r.pm_winner_side {
        Some(s) => (Some(s), "pm"),
        None => match (r.target, r.final_pyth) {
            (Some(t), Some(f)) => (Some(if f > t { "YES" } else { "NO " }), "pyth"),
            _ => (None, ""),
        },
    };

    for (side, entry_opt) in [("YES", r.yes_entry), ("NO ", r.no_entry)] {
        let Some(entry) = entry_opt else { continue };
        let (won, pnl) = match resolved_side {
            Some(rs) => {
                let win = rs.trim() == side.trim();
                let p = pnl_for(entry.ask, win);
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
            side.trim(),
            entry,
            r.target,
            r.final_pyth,
            resolved_side.map(|s| s.trim()),
            source,
            won,
            pnl,
        ) {
            eprintln!("WARN: write row: {e:#}");
        }
    }
}

// CSV schema follows live-itm/src/main.rs, NOT live-trader (no order/size/
// notional columns — pnl is per share). Keep column changes in step with
// live-itm so both papertrading datasets stay analyzable with one loader.
fn write_header(w: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        w,
        "window_start_ts,condition_id,side,entry_ask,entry_offset_s,\
         swing_at_entry,entered_on_down_swing,btc_at_entry,\
         target_pyth,final_pyth,price_diff_from_entry,resolved_side,resolved_source,won,pnl"
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
    resolved_source: &str,
    won: Option<bool>,
    pnl: Option<Decimal>,
) -> Result<()> {
    let target_s = target.map(|d| d.to_string()).unwrap_or_default();
    let final_s = final_pyth.map(|d| d.to_string()).unwrap_or_default();
    let resolved_s = resolved_side.unwrap_or("");
    let won_s = won.map(|b| if b { "1" } else { "0" }).unwrap_or("");
    let pnl_s = pnl.map(|d| d.to_string()).unwrap_or_default();
    let swing_s = entry
        .swing_at_entry
        .map(|d| d.to_string())
        .unwrap_or_default();
    let down_s = entered_on_down_swing(entry.swing_at_entry)
        .map(|b| if b { "1" } else { "0" })
        .unwrap_or("");
    let btc_entry_s = entry
        .btc_at_entry
        .map(|d| d.to_string())
        .unwrap_or_default();
    // Signed BTC distance from the window target at entry (entry − target):
    // how far in-the-money the side already was when it triggered. Blank
    // unless both the entry-time median and the target are known.
    let price_diff_s = match (entry.btc_at_entry, target) {
        (Some(entry_px), Some(target_px)) => (entry_px - target_px).to_string(),
        _ => String::new(),
    };
    writeln!(
        w,
        "{window_start_ts},{condition_id},{side},{},{},{swing_s},{down_s},{btc_entry_s},{target_s},{final_s},{price_diff_s},{resolved_s},{resolved_source},{won_s},{pnl_s}",
        entry.ask, entry.offset_s,
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
        // Retry quietly: Hermes routinely has no update yet for a
        // boundary-aligned window-start ts, so per-attempt misses aren't worth
        // a log line. Warn once only if every attempt fails (the median
        // fallback then seeds the target).
        let mut last_reason = String::new();
        for attempt in 1..=TARGET_FETCH_RETRIES {
            if attempt > 1 {
                tokio::time::sleep(TARGET_FETCH_BACKOFF).await;
            }
            match pm.current_btc_updown_5m_target().await {
                Ok(Some(v)) => {
                    let _ = tx.send(Ok(v));
                    return;
                }
                Ok(None) => last_reason = "pyth had no update yet".to_string(),
                Err(e) => last_reason = format!("{e:#}"),
            }
        }
        let _ = tx.send(Err(format!(
            "px target unavailable after {TARGET_FETCH_RETRIES} attempts ({last_reason}); using median fallback"
        )));
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
        "entries={} wins={} losses={} unresolved={} | win_rate={:.1}% pnl_per_share={}",
        state.n_entries, state.n_wins, state.n_losses, state.n_unresolved, win_rate, state.pnl,
    );
}

struct Args {
    out_path: Option<PathBuf>,
    /// Filename stem prefix for the auto-named CSV: `dry-data/<prefix>-<stamp>.csv`.
    /// Lets a second dry run (e.g. the inverse experiment) share the dry-data
    /// mount without its rows mixing into the baseline's `trade-*.csv` glob.
    out_prefix: String,
    explicit_market: Option<String>,
    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    min_target_dist: Decimal,
    max_target_dist: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    swing_lookback_s: u64,
}

fn parse_args() -> Result<Args> {
    // Defaults are deliberately LOOSER than live-trader's — the point of a
    // dry run is to observe entries the live criteria would skip, so the
    // analysis can find where edge starts and stops. The ask band opens a
    // notch below live (0.94 vs 0.95) and min_target_dist drops to 0 to
    // record the marginal near-strike entries live filters out.
    //
    // Gates intentionally kept at live values:
    //  - min_offset: entering at 240s+ is what defines the strategy; also the
    //    trigger takes the FIRST qualifying tick per window, so a low offset
    //    floor would capture early entries at the expense of the late trade.
    //  - min_bid: rows are scored as if filled at the ask, which is fiction on
    //    wide-spread thin books; the bid floor keeps entries where a real fill
    //    was plausible.
    let mut a = Args {
        out_path: None,
        out_prefix: "trade".to_string(),
        explicit_market: None,
        min_ask: Decimal::new(94, 2),
        max_ask: Decimal::new(99, 2),
        min_bid: Decimal::new(50, 2),
        min_target_dist: Decimal::ZERO,
        max_target_dist: Decimal::MAX,
        min_offset_s: 240,
        max_offset_s: WINDOW_SECS - 10,
        swing_lookback_s: 10,
    };
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-o" | "--out" => {
                a.out_path = Some(PathBuf::from(args.next().context("--out needs a path")?))
            }
            "--out-prefix" => {
                let v = args.next().context("--out-prefix needs a value")?;
                // A prefix is a filename stem, not a path — a separator here
                // would silently write outside the mounted dry-data dir.
                if v.is_empty() || v.contains('/') || v.contains('\\') {
                    anyhow::bail!("--out-prefix must be a non-empty filename stem: {v}");
                }
                a.out_prefix = v;
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
            "--min-target-dist" => {
                let v = args.next().context("--min-target-dist needs a value")?;
                a.min_target_dist =
                    v.parse().with_context(|| format!("parsing --min-target-dist {v}"))?;
            }
            "--max-target-dist" => {
                let v = args.next().context("--max-target-dist needs a value")?;
                a.max_target_dist =
                    v.parse().with_context(|| format!("parsing --max-target-dist {v}"))?;
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

    let out_path = args.out_path.clone().unwrap_or_else(|| {
        let stamp = chrono::Local::now().format("%m-%d-%Y-%H.%M");
        PathBuf::from(format!("dry-data/{}-{stamp}.csv", args.out_prefix))
    });
    if let Some(parent) = out_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating dir {}", parent.display()))?;
    }
    let mut out = BufWriter::new(
        File::create(&out_path).with_context(|| format!("creating {}", out_path.display()))?,
    );
    write_header(&mut out)?;

    // Decimal::MAX is the "uncapped" sentinel; printing it raw would dump a
    // 29-digit number into the banner, so render the band instead.
    let dist_band = if args.max_target_dist == Decimal::MAX {
        format!(">={}", args.min_target_dist)
    } else {
        format!(" in [{}, {}]", args.min_target_dist, args.max_target_dist)
    };
    eprintln!(
        "dry-trader (no orders, per-share pnl) | ask in [{}, {}] bid>={} dist{} offset [{}, {})s swing={}s",
        args.min_ask,
        args.max_ask,
        args.min_bid,
        dist_band,
        args.min_offset_s,
        args.max_offset_s,
        args.swing_lookback_s,
    );

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
        min_ask: args.min_ask,
        max_ask: args.max_ask,
        min_bid: args.min_bid,
        min_target_dist: args.min_target_dist,
        max_target_dist: args.max_target_dist,
        min_offset_s: args.min_offset_s,
        max_offset_s: args.max_offset_s,
        swing_lookback: Duration::from_secs(args.swing_lookback_s),
        n_entries: 0,
        n_wins: 0,
        n_losses: 0,
        n_unresolved: 0,
        pnl: Decimal::ZERO,
    };

    let mut next_rollover: Option<Instant> =
        if auto_roll { Some(next_window_boundary()?) } else { None };

    let (resolved_tx, mut resolved_rx) = mpsc::unbounded_channel::<ResolvedWindow>();
    let mut resolver_tasks: Vec<JoinHandle<()>> = Vec::new();
    // Windows already handed to a resolver. A failed roll_window leaves the old
    // window's entries in place, so the retry snapshots the SAME window again;
    // without this guard each retry spawned another resolver, and since each one
    // polls PM for up to 20 minutes they accumulate — hundreds of live tasks,
    // each writing its own duplicate CSV row and holding its own socket. That is
    // what filled the trade logs and exhausted the fd limit on 2026-07-25.
    // Unbounded by design: one u64 per 5-minute window is ~840KB/century.
    let mut dispatched: HashSet<u64> = HashSet::new();
    let mut rollover_backoff = ROLLOVER_RETRY_MIN;
    // Set on shutdown so patient PM polls bail out and fall back to Pyth
    // instead of blocking process exit for up to ~20 minutes each.
    let cancel = Arc::new(AtomicBool::new(false));

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
            }
            Some(r) = resolved_rx.recv() => {
                sink_resolved(&mut state, r);
                print_summary(&state);
            }
            Some(msg) = target_rx.recv() => {
                match msg {
                    Ok(value) => state.btc_target = Some(Target { value }),
                    Err(reason) => eprintln!("WARN: {reason}"),
                }
            }
            _ = tokio::time::sleep(rollover_in), if auto_roll => {
                if let Some(old) = snapshot_window(&state)
                    && dispatched.insert(old.window_start_ts)
                {
                    // Inside the guard: a re-dispatched window would also have
                    // double-counted its entries in the summary.
                    state.n_entries += old.yes_entry.is_some() as u64
                        + old.no_entry.is_some() as u64;
                    let pm_c = pm.clone();
                    let tx_c = resolved_tx.clone();
                    let cancel_c = Arc::clone(&cancel);
                    resolver_tasks.push(tokio::spawn(async move {
                        resolve_window(old, pm_c, tx_c, cancel_c).await;
                    }));
                }
                match roll_window(&mut state, &tx, &pm, &mut pm_feed, &target_tx, &mut target_fetcher).await {
                    Ok(()) => {
                        next_rollover = next_window_boundary().ok();
                        rollover_backoff = ROLLOVER_RETRY_MIN;
                        eprintln!("rollover: now trading {}", state.pm.condition_id);
                        print_summary(&state);
                    }
                    Err(e) => {
                        eprintln!(
                            "WARN: rollover failed (retry in {}s): {e:#}",
                            rollover_backoff.as_secs()
                        );
                        next_rollover = Some(Instant::now() + rollover_backoff);
                        rollover_backoff = (rollover_backoff * 2).min(ROLLOVER_RETRY_MAX);
                    }
                }
                while target_rx.try_recv().is_ok() {}
            }
            _ = signal::ctrl_c() => {
                eprintln!("\nshutting down…");
                break Ok(());
            }
        }
    };

    // Resolve the final in-progress window on shutdown. Signal the patient PM
    // polls to stop so any in-flight resolvers fall straight back to Pyth
    // instead of blocking exit for up to ~20 minutes each.
    cancel.store(true, Ordering::Relaxed);
    // Same guard as the rollover arm: if we're shutting down mid-retry this
    // window may already have a resolver in flight.
    if let Some(old) = snapshot_window(&state)
        && dispatched.insert(old.window_start_ts)
    {
        state.n_entries += old.yes_entry.is_some() as u64 + old.no_entry.is_some() as u64;
        let pm_c = pm.clone();
        let tx_c = resolved_tx.clone();
        let cancel_c = Arc::clone(&cancel);
        resolver_tasks.push(tokio::spawn(async move {
            resolve_window(old, pm_c, tx_c, cancel_c).await;
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
