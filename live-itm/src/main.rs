// live-itm: combines the live-prices TUI with the itm-tracker scalp
// strategy. The TUI shows the same per-venue BTC median, target, and
// outcome book as live-prices, plus a strategy panel that lights up
// during the last minute of the window. The first ask in
// [--min-ask, --max-ask] on either side during the eligible window is
// captured as an entry; at rollover the window is resolved via Pyth
// BTC/USD at window_end_ts and one CSV row is written per triggered side.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Stdout, Write, stdout};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures::StreamExt;
use polymarket_core::{
    BitstampFeed, CexPayload, CexVenue, CoinbaseFeed, Decimal, FeedSource, KrakenFeed,
    MarketSnapshot, Polymarket, PolymarketEvent, PolymarketFeed, PolymarketPayload,
    RecordedEvent, U256,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

const COINBASE_PRODUCT: &str = "BTC-USD";
const KRAKEN_SYMBOL: &str = "BTC/USD";
const BITSTAMP_PAIR: &str = "btcusd";
const WINDOW_SECS: u64 = 300;
const VENUES: &[CexVenue] = &[CexVenue::Coinbase, CexVenue::Kraken, CexVenue::Bitstamp];
const MEDIAN_FRESHNESS: Duration = Duration::from_secs(5);
/// A trade only triggers if the side's order book moved within this window.
/// If the previous book update was more than this ago the book has been
/// stale — the print waking it up is unreliable, so we skip the trigger.
const BOOK_FRESHNESS: Duration = Duration::from_secs(5);

const TARGET_FETCH_RETRIES: u32 = 5;
const TARGET_FETCH_BACKOFF: Duration = Duration::from_secs(2);
const MAX_RECENT_ENTRIES: usize = 12;
const SWING_BUFFER_RETENTION: Duration = Duration::from_secs(60);

const PM_RESOLUTION_INITIAL_WAIT: Duration = Duration::from_secs(5);
const PM_RESOLUTION_POLL_INTERVAL: Duration = Duration::from_secs(5);
/// Polymarket's on-chain winner is the *primary* resolution source, so we
/// poll it patiently: ~20 minutes (240 * 5s) before giving up and falling
/// back to Pyth as a last resort in the CSV. Chainlink Data Streams usually
/// settles in <60s, but the CLOB `closed`/`winner` flags routinely lag the
/// 5-minute window by several minutes; the old 5-minute budget expired before
/// PM ever reported, so almost every row resolved via the Pyth fallback. The
/// resolver runs in its own background task, so a long poll across several
/// subsequent windows costs nothing but a delayed authoritative row (the fast
/// Pyth *preview* still flips the TUI to a tentative result immediately).
const PM_RESOLUTION_MAX_ATTEMPTS: u32 = 240;

#[derive(Clone, Copy)]
struct Entry {
    ask: Decimal,
    offset_s: u64,
    /// BTC median delta over `swing_lookback` at trigger time. `None` if we
    /// didn't have enough history. Negative = BTC fell; positive = rose.
    swing_at_entry: Option<Decimal>,
    /// Multi-venue BTC median at the instant the entry triggered. `None` if no
    /// fresh median was available. Used to measure how far BTC moved between
    /// entry and settlement (`price_diff_from_entry`).
    btc_at_entry: Option<Decimal>,
}

struct OutcomeRow {
    outcome: String,
    token_id: U256,
    bid: Option<Decimal>,
    bid_size: Option<Decimal>,
    ask: Option<Decimal>,
    ask_size: Option<Decimal>,
    last: Option<Decimal>,
    last_book_at: Option<Instant>,
    last_trade_at: Option<Instant>,
    /// Strategy side label ("YES" or "NO ") so trigger output is uniform
    /// regardless of how the market named its outcomes.
    side: &'static str,
    entry: Option<Entry>,
    /// Rolling buffer of (recv_time, ask) used to measure how the side's
    /// ask has moved over the swing-lookback window. Up swing = ask
    /// rising (market favors this side more); down swing = ask falling.
    ask_samples: VecDeque<(Instant, Decimal)>,
}

impl OutcomeRow {
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

    /// Current ask minus the latest sample at-or-before `now - lookback`.
    /// `None` if no current ask or not enough buffer history yet.
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
    bid: Option<Decimal>,
    ask: Option<Decimal>,
    last: Option<Decimal>,
    last_at: Option<Instant>,
    evt_ticker: u64,
    evt_trade: u64,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TargetSource {
    Px,
    Md,
}

impl TargetSource {
    fn tag(self) -> &'static str {
        match self {
            TargetSource::Px => "px",
            TargetSource::Md => "md",
        }
    }
}

#[derive(Clone, Copy)]
struct Target {
    value: Decimal,
    source: TargetSource,
}

/// One recent entry kept for the TUI scrollback panel. Resolution is filled
/// in when the window resolves; until then `won` / `pnl` are None. The
/// `resolved_source` marker shows whether the WIN/LOSS shown is from the
/// instant Pyth preview ("pyth?", tentative) or the authoritative PM
/// resolution ("pm").
#[derive(Clone)]
struct RecentEntry {
    window_start_ts: u64,
    side: &'static str,
    ask: Decimal,
    offset_s: u64,
    swing_at_entry: Option<Decimal>,
    won: Option<bool>,
    pnl: Option<Decimal>,
    resolved_source: Option<&'static str>,
}

struct AppState {
    question: String,
    slug: String,
    condition_id: String,
    window_start_ts: u64,
    outcomes: Vec<OutcomeRow>,
    last_event_at: Option<Instant>,
    last_error: Option<String>,
    btc: HashMap<CexVenue, VenueState>,
    btc_target: Option<Target>,

    min_ask: Decimal,
    max_ask: Decimal,
    min_bid: Decimal,
    /// Minimum absolute BTC distance (USD) between the median and the target
    /// required to enter — filters out marginal entries sitting on the strike.
    min_target_dist: Decimal,
    min_offset_s: u64,
    max_offset_s: u64,
    swing_lookback: Duration,

    n_entries: u64,
    n_wins: u64,
    n_losses: u64,
    n_unresolved: u64,
    pnl: Decimal,

    recent: Vec<RecentEntry>,
}

impl AppState {
    fn apply(&mut self, evt: RecordedEvent) {
        let now = Instant::now();
        self.last_event_at = Some(now);
        match evt {
            RecordedEvent::Polymarket(e) => self.apply_pm(&e, now),
            RecordedEvent::Cex(e) => {
                let v = self.btc.entry(e.venue).or_default();
                v.last_at = Some(now);
                match &e.payload {
                    CexPayload::Ticker {
                        best_bid,
                        best_ask,
                        last,
                        ..
                    } => {
                        v.bid = *best_bid;
                        v.ask = *best_ask;
                        v.last = Some(*last);
                        v.evt_ticker += 1;
                    }
                    CexPayload::Trade { price, .. } => {
                        v.last = Some(*price);
                        v.evt_trade += 1;
                    }
                }
                if self.btc_target.is_none()
                    && VENUES.iter().all(|venue| {
                        self.btc.get(venue).and_then(|s| s.last).is_some()
                    })
                    && let Some(value) = self.btc_median_last()
                {
                    self.btc_target = Some(Target {
                        value,
                        source: TargetSource::Md,
                    });
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
                self.last_error = Some(format!("[{tag}] {message}"));
            }
        }
    }

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

    fn newest_btc_at(&self) -> Option<Instant> {
        VENUES
            .iter()
            .filter_map(|v| self.btc.get(v).and_then(|s| s.last_at))
            .max()
    }

    fn offset_s(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            .saturating_sub(self.window_start_ts)
    }

    fn time_remaining_s(&self) -> u64 {
        WINDOW_SECS.saturating_sub(self.offset_s())
    }

    fn apply_pm(&mut self, e: &PolymarketEvent, now: Instant) {
        let offset_s = self.offset_s();
        let min_ask = self.min_ask;
        let max_ask = self.max_ask;
        let min_bid = self.min_bid;
        let min_off = self.min_offset_s;
        let max_off = self.max_offset_s;
        let window_ts = self.window_start_ts;
        // Direction confirmation: only enter the side BTC is currently
        // showing. None when target or median is missing — skip the trigger
        // entirely in that case.
        let btc_median = self.btc_median_last();
        let target_val = self.btc_target.map(|t| t.value);
        let min_target_dist = self.min_target_dist;
        let direction: Option<&'static str> = match (self.btc_target, btc_median) {
            (Some(t), Some(m)) if m > t.value => Some("YES"),
            (Some(t), Some(m)) if m < t.value => Some("NO "),
            _ => None,
        };
        let swing_lookback = self.swing_lookback;
        // Window lock: first side to fire wins the window. If we already
        // have any entry, only update book state and stop evaluating trigger
        // conditions. Prevents the BTC median crossing the target from
        // letting the other side also fire and giving us both legs of a
        // straddle (guaranteed loser).
        let window_locked = self.outcomes.iter().any(|o| o.entry.is_some());
        match &e.payload {
            PolymarketPayload::Book(b) => {
                if let Some(row) = self.find_mut(&b.asset_id) {
                    let book_fresh = book_fresh(row.last_book_at, now);
                    row.bid = b.bids.first().map(|l| l.price);
                    row.bid_size = b.bids.first().map(|l| l.size);
                    row.ask = b.asks.first().map(|l| l.price);
                    row.ask_size = b.asks.first().map(|l| l.size);
                    row.last_book_at = Some(now);
                    if let Some(ask) = row.ask {
                        row.push_ask_sample(now, ask);
                        let swing = row.ask_move_over(swing_lookback);
                        if !window_locked && book_fresh {
                            try_trigger(row, ask, offset_s, min_ask, max_ask, min_bid, min_off, max_off, direction, swing, btc_median, target_val, min_target_dist, window_ts);
                        }
                    }
                }
            }
            PolymarketPayload::PriceChange(p) => {
                for entry in &p.price_changes {
                    if let Some(row) = self.find_mut(&entry.asset_id) {
                        let book_fresh = book_fresh(row.last_book_at, now);
                        if let Some(bb) = entry.best_bid {
                            row.bid = Some(bb);
                        }
                        if let Some(ba) = entry.best_ask {
                            row.ask = Some(ba);
                            row.push_ask_sample(now, ba);
                            let swing = row.ask_move_over(swing_lookback);
                            if !window_locked && book_fresh {
                                try_trigger(row, ba, offset_s, min_ask, max_ask, min_bid, min_off, max_off, direction, swing, btc_median, target_val, min_target_dist, window_ts);
                            }
                        }
                        row.last_book_at = Some(now);
                    }
                }
            }
            PolymarketPayload::LastTrade(t) => {
                if let Some(row) = self.find_mut(&t.asset_id) {
                    row.last = Some(t.price);
                    row.last_trade_at = Some(now);
                }
            }
        }
        // Push any newly-triggered entries onto the scrollback. We do this
        // after mutating rows so it doesn't conflict with find_mut's borrow.
        let new_entries: Vec<(u64, &'static str, Entry)> = self
            .outcomes
            .iter()
            .filter_map(|r| {
                let e = r.entry?;
                if self.recent.iter().any(|re| {
                    re.window_start_ts == window_ts && re.side == r.side
                }) {
                    None
                } else {
                    Some((window_ts, r.side, e))
                }
            })
            .collect();
        for (ws, side, e) in new_entries {
            self.recent.push(RecentEntry {
                window_start_ts: ws,
                side,
                ask: e.ask,
                offset_s: e.offset_s,
                swing_at_entry: e.swing_at_entry,
                won: None,
                pnl: None,
                resolved_source: None,
            });
            while self.recent.len() > MAX_RECENT_ENTRIES {
                self.recent.remove(0);
            }
        }
    }

    fn find_mut(&mut self, asset_id: &U256) -> Option<&mut OutcomeRow> {
        self.outcomes.iter_mut().find(|o| &o.token_id == asset_id)
    }

    fn enter_new_window(
        &mut self,
        snapshot: MarketSnapshot,
        outcomes: Vec<OutcomeRow>,
        window_start_ts: u64,
    ) {
        self.question = snapshot.question;
        self.slug = snapshot.market_slug;
        self.condition_id = snapshot.condition_id;
        self.outcomes = outcomes;
        self.window_start_ts = window_start_ts;
        self.last_event_at = None;
        self.btc_target = None;
    }
}

/// True iff the side's book moved recently enough to act on. `prev_book_at`
/// is the row's *previous* `last_book_at` (captured before the current event
/// overwrote it). If the last movement was over `BOOK_FRESHNESS` ago — or the
/// book hasn't moved at all this window — the book is stale and we don't trade.
fn book_fresh(prev_book_at: Option<Instant>, now: Instant) -> bool {
    match prev_book_at {
        Some(t) => now.saturating_duration_since(t) <= BOOK_FRESHNESS,
        None => false,
    }
}

#[allow(clippy::too_many_arguments)]
fn try_trigger(
    row: &mut OutcomeRow,
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
    _window_ts: u64,
) {
    if row.entry.is_some() {
        return;
    }
    // Eligible window is [min_offset_s, max_offset_s): too early gives a
    // worse signal, too late (default last 10s) leaves no time to fill and
    // risks the resolution print landing first.
    if offset_s < min_offset_s || offset_s >= max_offset_s {
        return;
    }
    // Ask band is (min_ask, max_ask]: exclude min_ask itself, include max_ask.
    if ask <= min_ask || ask > max_ask {
        return;
    }
    // Require the bid to also be high: filters out wide-spread thin books
    // where both YES and NO asks sit near 1.00 with bids near 0.
    let Some(bid) = row.bid else {
        return;
    };
    if bid < min_bid {
        return;
    }
    // Direction confirmation: only enter the side BTC is currently showing
    // (median > target → YES, median < target → NO). If direction is
    // unknown (no target or median yet) we skip rather than guess.
    if direction != Some(row.side) {
        return;
    }
    // Minimum distance past the strike: BTC must be at least `min_target_dist`
    // (USD) away from the target, filtering out marginal entries sitting right
    // on the strike where a tiny reversal flips the outcome.
    match (btc_median, target) {
        (Some(m), Some(t)) if (m - t).abs() >= min_target_dist => {}
        _ => return,
    }
    row.entry = Some(Entry {
        ask,
        offset_s,
        swing_at_entry: swing,
        btc_at_entry: btc_median,
    });
}

fn outcomes_from_snapshot(snapshot: &MarketSnapshot) -> Vec<OutcomeRow> {
    let yes_idx = snapshot
        .outcomes
        .iter()
        .position(|o| {
            let lc = o.outcome.to_lowercase();
            lc == "yes" || lc == "up" || lc == "above"
        })
        .unwrap_or(0);
    snapshot
        .outcomes
        .iter()
        .enumerate()
        .map(|(i, o)| OutcomeRow {
            outcome: o.outcome.clone(),
            token_id: o.token_id,
            bid: o.bid,
            bid_size: None,
            ask: o.ask,
            ask_size: None,
            last: o.last,
            last_book_at: None,
            last_trade_at: None,
            side: if i == yes_idx { "YES" } else { "NO " },
            entry: None,
            ask_samples: VecDeque::new(),
        })
        .collect()
}

fn current_window_start_ts() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?
        .as_secs();
    Ok(now - (now % WINDOW_SECS))
}

fn next_window_boundary() -> Result<Instant> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?;
    let next_boundary_secs = (now.as_secs() / WINDOW_SECS + 1) * WINDOW_SECS;
    let dur_until = Duration::from_secs(next_boundary_secs) - now;
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
            let reason = match pm.current_btc_updown_5m_target().await {
                Ok(Some(v)) => {
                    let _ = tx.send(Ok(v));
                    return;
                }
                Ok(None) => "pyth returned no update for window-start ts".to_string(),
                Err(e) => format!("{e:#}"),
            };
            let _ = tx.send(Err(format!(
                "px target [{attempt}/{TARGET_FETCH_RETRIES}]: {reason}"
            )));
        }
    })
}

/// Snapshot of a window's entry state, handed off to the async resolver
/// at rollover so the new window starts subscribing immediately. Carries
/// the YES/NO token_ids so the PM resolver can match the on-chain winner
/// token_id back to a side label.
struct OldWindow {
    window_start_ts: u64,
    condition_id: String,
    target: Option<Decimal>,
    yes_entry: Option<Entry>,
    no_entry: Option<Entry>,
    yes_token_id: U256,
    no_token_id: U256,
}

/// Two-stage resolver output. `PythPreview` is the instant inferred-from-Pyth
/// outcome (TUI scrollback only — does NOT write CSV or increment counters).
/// `Final` is the authoritative write — uses PM resolution if available,
/// falls back to Pyth if PM polling exhausted.
enum ResolutionUpdate {
    PythPreview {
        window_start_ts: u64,
        target: Option<Decimal>,
        final_pyth: Option<Decimal>,
        yes_entry: Option<Entry>,
        no_entry: Option<Entry>,
    },
    Final {
        window_start_ts: u64,
        condition_id: String,
        target: Option<Decimal>,
        final_pyth: Option<Decimal>,
        /// `Some` if PM closed and we matched the winner to a side; `None`
        /// if PM polling exhausted or returned an unknown token_id.
        pm_winner_side: Option<&'static str>,
        yes_entry: Option<Entry>,
        no_entry: Option<Entry>,
    },
}

fn snapshot_old_window(state: &AppState) -> Option<OldWindow> {
    let yes_row = state.outcomes.iter().find(|r| r.side == "YES")?;
    let no_row = state.outcomes.iter().find(|r| r.side == "NO ")?;
    let yes_entry = yes_row.entry;
    let no_entry = no_row.entry;
    if yes_entry.is_none() && no_entry.is_none() {
        return None;
    }
    Some(OldWindow {
        window_start_ts: state.window_start_ts,
        condition_id: state.condition_id.clone(),
        target: state.btc_target.map(|t| t.value),
        yes_entry,
        no_entry,
        yes_token_id: yes_row.token_id,
        no_token_id: no_row.token_id,
    })
}

/// Hermes 404s on boundary-aligned recent timestamps. Walk the timestamp
/// back on each retry — the BTC price 1–30s before window end is
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
            Err(_) => {
                if i + 1 == attempts.len() {
                    // give up silently — main loop notices via n_unresolved
                }
            }
        }
    }
    None
}

/// Poll Polymarket's CLOB market endpoint until it closes and reports a
/// winner. Returns `Some("YES")` / `Some("NO ")` matched against the side's
/// token_id, or `None` if polling exhausted or the winner token didn't
/// match either side. Bails early if the shutdown cancel flag is set.
async fn poll_pm_winner(
    pm: &Polymarket,
    condition_id: &str,
    yes_token_id: U256,
    no_token_id: U256,
    cancel: Arc<AtomicBool>,
) -> Option<&'static str> {
    tokio::time::sleep(PM_RESOLUTION_INITIAL_WAIT).await;
    for _ in 0..PM_RESOLUTION_MAX_ATTEMPTS {
        if cancel.load(Ordering::Relaxed) {
            return None;
        }
        match pm.market_winner(condition_id).await {
            Ok(Some(token_id)) => {
                if token_id == yes_token_id {
                    return Some("YES");
                }
                if token_id == no_token_id {
                    return Some("NO ");
                }
                return None;
            }
            Ok(None) | Err(_) => {}
        }
        tokio::time::sleep(PM_RESOLUTION_POLL_INTERVAL).await;
    }
    None
}

/// Two-stage resolution. Polymarket's on-chain winner is the primary,
/// authoritative source; Pyth is only a last resort. First emit a fast
/// Pyth-derived *preview* so the TUI scrollback flips from `pending` to a
/// tentative result quickly (display only — it never writes the CSV). Then
/// poll PM patiently for the on-chain winner (~20 min, see
/// `PM_RESOLUTION_MAX_ATTEMPTS`) and emit the authoritative `Final` update
/// that writes the CSV row and increments counters. Only if PM polling is
/// fully exhausted does `Final` fall back to the Pyth price.
async fn resolve_window(
    old: OldWindow,
    pm: Arc<Polymarket>,
    tx: mpsc::UnboundedSender<ResolutionUpdate>,
    cancel: Arc<AtomicBool>,
) {
    let window_end_ts = old.window_start_ts + WINDOW_SECS;
    let final_pyth = fetch_final_pyth_retry(&pm, window_end_ts).await;
    let _ = tx.send(ResolutionUpdate::PythPreview {
        window_start_ts: old.window_start_ts,
        target: old.target,
        final_pyth,
        yes_entry: old.yes_entry,
        no_entry: old.no_entry,
    });

    let pm_winner_side = poll_pm_winner(
        &pm,
        &old.condition_id,
        old.yes_token_id,
        old.no_token_id,
        cancel,
    )
    .await;

    let _ = tx.send(ResolutionUpdate::Final {
        window_start_ts: old.window_start_ts,
        condition_id: old.condition_id,
        target: old.target,
        final_pyth,
        pm_winner_side,
        yes_entry: old.yes_entry,
        no_entry: old.no_entry,
    });
}

fn pnl_for(entry_ask: Decimal, win: bool) -> Decimal {
    if win {
        Decimal::ONE - entry_ask
    } else {
        -entry_ask
    }
}

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

/// True iff the side's ask was falling over the lookback window — i.e.
/// the market was moving against the side we just bought. Symmetric for
/// YES/NO: a down move in the token's own ask is always "the market
/// doubts this side." Unknown swing → None.
fn entered_on_down_swing(swing: Option<Decimal>) -> Option<bool> {
    let s = swing?;
    Some(s.is_sign_negative() && !s.is_zero())
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
    let swing_s = entry.swing_at_entry.map(|d| d.to_string()).unwrap_or_default();
    let down_s = entered_on_down_swing(entry.swing_at_entry)
        .map(|b| if b { "1" } else { "0" })
        .unwrap_or("");
    let btc_entry_s = entry.btc_at_entry.map(|d| d.to_string()).unwrap_or_default();
    // Signed BTC distance from the window target at entry (entry − target):
    // how far in-the-money the side already was when it triggered. Blank
    // unless both the entry-time median and the target are known.
    let price_diff_s = match (entry.btc_at_entry, target) {
        (Some(entry_px), Some(target_px)) => (entry_px - target_px).to_string(),
        _ => String::new(),
    };
    let side_trim = side.trim();
    writeln!(
        w,
        "{window_start_ts},{condition_id},{side_trim},{},{},{swing_s},{down_s},{btc_entry_s},{target_s},{final_s},{price_diff_s},{resolved_s},{resolved_source},{won_s},{pnl_s}",
        entry.ask, entry.offset_s,
    )?;
    w.flush()?;
    Ok(())
}

fn pyth_resolved_side(
    target: Option<Decimal>,
    final_pyth: Option<Decimal>,
) -> Option<&'static str> {
    match (target, final_pyth) {
        (Some(t), Some(f)) => Some(if f > t { "YES" } else { "NO " }),
        _ => None,
    }
}

fn update_scrollback(
    state: &mut AppState,
    window_start_ts: u64,
    side: &'static str,
    won: Option<bool>,
    pnl: Option<Decimal>,
    source: &'static str,
) {
    for re in state.recent.iter_mut().rev() {
        if re.window_start_ts == window_start_ts && re.side == side {
            re.won = won;
            re.pnl = pnl;
            re.resolved_source = Some(source);
            break;
        }
    }
}

fn sink_resolution(
    state: &mut AppState,
    out: &mut BufWriter<File>,
    update: ResolutionUpdate,
) {
    match update {
        ResolutionUpdate::PythPreview {
            window_start_ts,
            target,
            final_pyth,
            yes_entry,
            no_entry,
        } => {
            let resolved_side = pyth_resolved_side(target, final_pyth);
            for (side, entry_opt) in [("YES", yes_entry), ("NO ", no_entry)] {
                let Some(entry) = entry_opt else { continue };
                let (won, pnl) = match resolved_side {
                    Some(rs) => {
                        let win = rs == side;
                        (Some(win), Some(pnl_for(entry.ask, win)))
                    }
                    None => (None, None),
                };
                update_scrollback(state, window_start_ts, side, won, pnl, "pyth?");
            }
        }
        ResolutionUpdate::Final {
            window_start_ts,
            condition_id,
            target,
            final_pyth,
            pm_winner_side,
            yes_entry,
            no_entry,
        } => {
            let (resolved_side, source) = match pm_winner_side {
                Some(s) => (Some(s), "pm"),
                None => match pyth_resolved_side(target, final_pyth) {
                    Some(s) => (Some(s), "pyth"),
                    None => (None, ""),
                },
            };
            for (side, entry_opt) in [("YES", yes_entry), ("NO ", no_entry)] {
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
                    out,
                    window_start_ts,
                    &condition_id,
                    side,
                    entry,
                    target,
                    final_pyth,
                    resolved_side,
                    source,
                    won,
                    pnl,
                ) {
                    state.last_error = Some(format!("write row: {e:#}"));
                }
                update_scrollback(state, window_start_ts, side, won, pnl, source);
            }
        }
    }
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
    let mut min_target_dist = Decimal::from(35);
    let mut min_offset_s: u64 = 240;
    let mut max_offset_s: u64 = WINDOW_SECS - 10;
    let mut swing_lookback_s: u64 = 10;
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
            "--min-target-dist" => {
                let v = args.next().context("--min-target-dist needs a value")?;
                min_target_dist = v
                    .parse()
                    .with_context(|| format!("parsing --min-target-dist {v}"))?;
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
            "--swing-lookback" => {
                let v = args.next().context("--swing-lookback needs a value")?;
                swing_lookback_s = v
                    .parse()
                    .with_context(|| format!("parsing --swing-lookback {v}"))?;
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
        // Local wall-clock stamp. Windows forbids ':' in filenames, so the
        // hh:mm separator is rendered as '.' (data/mm-dd-yyyy-hh.mm.csv).
        let stamp = chrono::Local::now().format("%m-%d-%Y-%H.%M");
        PathBuf::from(format!("data/{stamp}.csv"))
    });
    if let Some(parent) = out_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating dir {}", parent.display()))?;
    }
    let mut out = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("creating {}", out_path.display()))?,
    );
    write_header(&mut out)?;

    let pm = Arc::new(Polymarket::new()?);
    let condition_id = match &explicit_market {
        Some(a) => pm.resolve_condition_id(a).await?,
        None => pm
            .current_btc_updown_5m_condition_id()
            .await
            .context("resolving current btc-updown-5m market")?,
    };
    let snapshot = pm.fetch_snapshot(&condition_id).await?;
    let outcomes = outcomes_from_snapshot(&snapshot);
    if outcomes.is_empty() {
        return Err(anyhow!("market has no outcome tokens"));
    }
    let window_start_ts = current_window_start_ts()?;

    let mut state = AppState {
        question: snapshot.question.clone(),
        slug: snapshot.market_slug.clone(),
        condition_id: snapshot.condition_id.clone(),
        window_start_ts,
        outcomes,
        last_event_at: None,
        last_error: None,
        btc: HashMap::new(),
        btc_target: None,
        min_ask,
        max_ask,
        min_bid,
        min_target_dist,
        min_offset_s,
        max_offset_s,
        swing_lookback: Duration::from_secs(swing_lookback_s),
        n_entries: 0,
        n_wins: 0,
        n_losses: 0,
        n_unresolved: 0,
        pnl: Decimal::ZERO,
        recent: Vec::new(),
    };

    let token_ids: Vec<U256> = state.outcomes.iter().map(|o| o.token_id).collect();
    let (tx, mut rx) = broadcast::channel::<RecordedEvent>(4096);
    let mut pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);
    let _cb_feed = CoinbaseFeed::start(COINBASE_PRODUCT, tx.clone());
    let _kr_feed = KrakenFeed::start(KRAKEN_SYMBOL, tx.clone());
    let _bs_feed = BitstampFeed::start(BITSTAMP_PAIR, tx.clone());

    let (target_tx, mut target_rx) = mpsc::unbounded_channel::<TargetMsg>();
    let mut target_fetcher: Option<JoinHandle<()>> = if auto_roll {
        Some(spawn_target_fetcher(Arc::clone(&pm), target_tx.clone()))
    } else {
        None
    };

    let (resolved_tx, mut resolved_rx) = mpsc::unbounded_channel::<ResolutionUpdate>();
    let mut resolver_tasks: Vec<JoinHandle<()>> = Vec::new();
    let cancel = Arc::new(AtomicBool::new(false));

    let mut terminal = init_terminal()?;
    let res = run(
        &mut terminal,
        &mut state,
        &mut out,
        &mut rx,
        &tx,
        &pm,
        &mut pm_feed,
        &mut target_rx,
        &target_tx,
        &mut target_fetcher,
        &mut resolved_rx,
        &resolved_tx,
        &mut resolver_tasks,
        Arc::clone(&cancel),
        auto_roll,
    )
    .await;
    restore_terminal()?;

    // Shutdown: tell PM-pollers to bail (they'll fall through to the Pyth
    // fallback in `Final`), then spawn a final resolver for the in-progress
    // window so its CSV row gets written.
    cancel.store(true, Ordering::Relaxed);
    if let Some(old) = snapshot_old_window(&state) {
        let pm_c = pm.clone();
        let tx_c = resolved_tx.clone();
        let cancel_c = Arc::clone(&cancel);
        resolver_tasks.push(tokio::spawn(async move {
            resolve_window(old, pm_c, tx_c, cancel_c).await;
        }));
    }
    drop(resolved_tx);
    for h in resolver_tasks {
        let _ = h.await;
    }
    while let Some(r) = resolved_rx.recv().await {
        sink_resolution(&mut state, &mut out, r);
    }
    out.flush().ok();
    res
}

async fn roll_window(
    state: &mut AppState,
    tx: &broadcast::Sender<RecordedEvent>,
    pm: &Arc<Polymarket>,
    pm_feed: &mut Option<PolymarketFeed>,
    target_tx: &mpsc::UnboundedSender<TargetMsg>,
    target_fetcher: &mut Option<JoinHandle<()>>,
    resolved_tx: &mpsc::UnboundedSender<ResolutionUpdate>,
    resolver_tasks: &mut Vec<JoinHandle<()>>,
    cancel: &Arc<AtomicBool>,
) -> Result<()> {
    if let Some(old) = snapshot_old_window(state) {
        let pm_c = pm.clone();
        let tx_c = resolved_tx.clone();
        let cancel_c = Arc::clone(cancel);
        resolver_tasks.push(tokio::spawn(async move {
            resolve_window(old, pm_c, tx_c, cancel_c).await;
        }));
    }

    let condition_id = pm
        .current_btc_updown_5m_condition_id()
        .await
        .context("resolving new btc-updown-5m market")?;
    let snapshot = pm
        .fetch_snapshot(&condition_id)
        .await
        .context("fetching new market snapshot")?;
    let outcomes = outcomes_from_snapshot(&snapshot);
    if outcomes.is_empty() {
        anyhow::bail!("new market has no outcome tokens");
    }
    let token_ids: Vec<U256> = outcomes.iter().map(|o| o.token_id).collect();

    *pm_feed = None;
    *pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);

    let window_start_ts = current_window_start_ts()?;
    state.enter_new_window(snapshot, outcomes, window_start_ts);

    if let Some(h) = target_fetcher.take() {
        h.abort();
    }
    *target_fetcher = Some(spawn_target_fetcher(Arc::clone(pm), target_tx.clone()));

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    state: &mut AppState,
    out: &mut BufWriter<File>,
    rx: &mut broadcast::Receiver<RecordedEvent>,
    tx: &broadcast::Sender<RecordedEvent>,
    pm: &Arc<Polymarket>,
    pm_feed: &mut Option<PolymarketFeed>,
    target_rx: &mut mpsc::UnboundedReceiver<TargetMsg>,
    target_tx: &mpsc::UnboundedSender<TargetMsg>,
    target_fetcher: &mut Option<JoinHandle<()>>,
    resolved_rx: &mut mpsc::UnboundedReceiver<ResolutionUpdate>,
    resolved_tx: &mpsc::UnboundedSender<ResolutionUpdate>,
    resolver_tasks: &mut Vec<JoinHandle<()>>,
    cancel: Arc<AtomicBool>,
    auto_roll: bool,
) -> Result<()> {
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let mut next_rollover: Option<Instant> = if auto_roll {
        Some(next_window_boundary()?)
    } else {
        None
    };

    loop {
        terminal.draw(|f| render(f, state))?;

        let rollover_in = match next_rollover {
            Some(t) => t.saturating_duration_since(Instant::now()),
            None => Duration::from_secs(86400),
        };

        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(evt) => state.apply(evt),
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        state.last_error = Some(format!("lagged {n} events"));
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
                loop {
                    match rx.try_recv() {
                        Ok(e) => state.apply(e),
                        Err(broadcast::error::TryRecvError::Empty) => break,
                        Err(broadcast::error::TryRecvError::Lagged(n)) => {
                            state.last_error = Some(format!("lagged {n} events"));
                        }
                        Err(broadcast::error::TryRecvError::Closed) => return Ok(()),
                    }
                }
            }
            Some(Ok(term_evt)) = events.next() => {
                if let Event::Key(k) = term_evt
                    && k.kind == KeyEventKind::Press
                    && should_quit(k.code, k.modifiers)
                {
                    return Ok(());
                }
            }
            _ = tokio::time::sleep(rollover_in), if auto_roll => {
                match roll_window(
                    state, tx, pm, pm_feed, target_tx, target_fetcher,
                    resolved_tx, resolver_tasks, &cancel,
                ).await {
                    Ok(()) => {
                        next_rollover = next_window_boundary().ok();
                    }
                    Err(e) => {
                        state.last_error = Some(format!("rollover failed: {e:#}"));
                        next_rollover = Some(Instant::now() + Duration::from_secs(2));
                    }
                }
                while target_rx.try_recv().is_ok() {}
            }
            Some(msg) = target_rx.recv() => {
                match msg {
                    Ok(value) => {
                        state.btc_target = Some(Target { value, source: TargetSource::Px });
                    }
                    Err(reason) => {
                        state.last_error = Some(reason);
                    }
                }
            }
            Some(r) = resolved_rx.recv() => {
                sink_resolution(state, out, r);
            }
            _ = tick.tick() => {}
        }
    }
}

fn should_quit(code: KeyCode, mods: KeyModifiers) -> bool {
    matches!(code, KeyCode::Char('q') | KeyCode::Esc)
        || (matches!(code, KeyCode::Char('c')) && mods.contains(KeyModifiers::CONTROL))
}

fn render(f: &mut ratatui::Frame, state: &AppState) {
    let header_height = 8 + state.last_error.is_some() as u16;
    let chunks = Layout::vertical([
        Constraint::Length(header_height),
        Constraint::Length(7),
        Constraint::Min(6),
        Constraint::Min(4),
        Constraint::Length(3),
    ])
    .split(f.area());

    f.render_widget(header(state), chunks[0]);
    f.render_widget(strategy_panel(state), chunks[1]);
    f.render_widget(table(state), chunks[2]);
    f.render_widget(recent_panel(state), chunks[3]);
    f.render_widget(footer(), chunks[4]);
}

fn header(state: &AppState) -> Paragraph<'_> {
    let status = match state.last_event_at {
        None => Span::styled("connecting…", Style::default().fg(Color::Yellow)),
        Some(t) => {
            let secs = t.elapsed().as_secs_f64();
            let color = if secs < 5.0 {
                Color::Green
            } else if secs < 30.0 {
                Color::Yellow
            } else {
                Color::Red
            };
            Span::styled(
                format!("live • last event {secs:.1}s ago"),
                Style::default().fg(color),
            )
        }
    };

    let fmt_px = |v: Option<Decimal>, prec: usize| match v {
        Some(d) => format!("{d:.*}", prec),
        None => "—".to_string(),
    };

    let median = state.btc_median_last();
    let median_line = {
        let age = match state.newest_btc_at() {
            Some(t) => format!("{:.1}s", t.elapsed().as_secs_f64()),
            None => "—".to_string(),
        };
        let delta = match (median, state.btc_target) {
            (Some(l), Some(t)) => {
                let d = l - t.value;
                let sign = if d.is_sign_negative() { "" } else { "+" };
                format!("{sign}{d:.2}")
            }
            _ => "—".to_string(),
        };
        let tgt_str = match state.btc_target {
            Some(t) => format!("{:.2} ({})", t.value, t.source.tag()),
            None => "—".to_string(),
        };
        format!(
            "med {} | tgt {} Δ {} | age {}",
            fmt_px(median, 2),
            tgt_str,
            delta,
            age,
        )
    };

    let venues_line = {
        let parts: Vec<String> = VENUES
            .iter()
            .map(|venue| {
                let s = state.btc.get(venue);
                let last = s.and_then(|s| s.last);
                let age = s
                    .and_then(|s| s.last_at)
                    .map(|t| format!("{:.1}s", t.elapsed().as_secs_f64()))
                    .unwrap_or_else(|| "—".to_string());
                let evts = s
                    .map(|s| s.evt_ticker + s.evt_trade)
                    .unwrap_or(0);
                format!("{}={} ({} {}t)", venue.as_str(), fmt_px(last, 2), age, evts)
            })
            .collect();
        parts.join(" | ")
    };

    let mut lines = vec![
        Line::from(vec![
            Span::styled("Question:  ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&state.question),
        ]),
        Line::from(vec![
            Span::styled("Slug:      ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&state.slug),
        ]),
        Line::from(vec![
            Span::styled("Condition: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(&state.condition_id),
        ]),
        Line::from(vec![
            Span::styled("BTC med:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(median_line),
        ]),
        Line::from(vec![
            Span::styled("Venues:    ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(venues_line),
        ]),
        Line::from(vec![
            Span::styled("Status:    ", Style::default().add_modifier(Modifier::BOLD)),
            status,
        ]),
    ];

    if let Some(err) = &state.last_error {
        lines.push(Line::from(vec![
            Span::styled("Error:     ", Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
            Span::styled(err.as_str(), Style::default().fg(Color::Red)),
        ]));
    }

    Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(" Polymarket Live "))
}

fn strategy_panel(state: &AppState) -> Paragraph<'_> {
    let offset = state.offset_s();
    let remaining = state.time_remaining_s();
    let eligible = offset >= state.min_offset_s && offset < state.max_offset_s;
    let direction_opt: Option<&'static str> = match (state.btc_target, state.btc_median_last()) {
        (Some(t), Some(m)) if m > t.value => Some("YES"),
        (Some(t), Some(m)) if m < t.value => Some("NO "),
        _ => None,
    };
    let direction_str = match direction_opt {
        Some("YES") => "UP (favor YES)",
        Some("NO ") => "DOWN (favor NO)",
        _ => "—",
    };
    let yes_swing = state
        .outcomes
        .iter()
        .find(|r| r.side == "YES")
        .and_then(|r| r.ask_move_over(state.swing_lookback));
    let no_swing = state
        .outcomes
        .iter()
        .find(|r| r.side == "NO ")
        .and_then(|r| r.ask_move_over(state.swing_lookback));
    let swing_line = swing_line(state.swing_lookback, yes_swing, no_swing);

    let (status_text, status_color) = if eligible {
        ("ACTIVE", Color::Green)
    } else {
        let window_note = if offset >= state.max_offset_s {
            "window closed".to_string()
        } else {
            format!("eligible in {}s", state.min_offset_s.saturating_sub(offset))
        };
        return Paragraph::new(vec![
            Line::from(vec![
                Span::styled("Band:      ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!(
                    "ask in [{}, {}], bid ≥ {}, dir matches BTC   eligible offset [{}, {})s",
                    state.min_ask, state.max_ask, state.min_bid,
                    state.min_offset_s, state.max_offset_s
                )),
            ]),
            Line::from(vec![
                Span::styled("Direction: ", Style::default().add_modifier(Modifier::BOLD)),
                Span::styled(
                    direction_str.to_string(),
                    Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
                ),
            ]),
            swing_line,
            Line::from(vec![
                Span::styled("Window:    ", Style::default().add_modifier(Modifier::BOLD)),
                Span::raw(format!("offset {offset}s, {remaining}s remaining")),
                Span::raw("   "),
                Span::styled(
                    window_note,
                    Style::default().fg(Color::DarkGray),
                ),
            ]),
            Line::from(vec![
                Span::styled("Stats:     ", Style::default().add_modifier(Modifier::BOLD)),
                stats_span(state),
            ]),
        ])
        .block(Block::default().borders(Borders::ALL).title(" Strategy "));
    };

    let yes_entry = state
        .outcomes
        .iter()
        .find(|r| r.side == "YES")
        .and_then(|r| r.entry);
    let no_entry = state
        .outcomes
        .iter()
        .find(|r| r.side == "NO ")
        .and_then(|r| r.entry);

    let fmt_entry = |e: Option<Entry>| match e {
        Some(en) => format!("@ {} (+{}s)", en.ask, en.offset_s),
        None => "—".to_string(),
    };

    Paragraph::new(vec![
        Line::from(vec![
            Span::styled("Band:      ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!(
                "ask in [{}, {}], bid ≥ {}, dir matches BTC   ",
                state.min_ask, state.max_ask, state.min_bid
            )),
            Span::styled(
                format!("[{status_text}]"),
                Style::default().fg(status_color).add_modifier(Modifier::BOLD),
            ),
        ]),
        Line::from(vec![
            Span::styled("Direction: ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled(
                direction_str.to_string(),
                Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD),
            ),
        ]),
        swing_line,
        Line::from(vec![
            Span::styled("Window:    ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(format!("offset {offset}s, {remaining}s remaining")),
        ]),
        Line::from(vec![
            Span::styled("Entries:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::styled("YES ", Style::default().fg(Color::Cyan)),
            Span::raw(fmt_entry(yes_entry)),
            Span::raw("   "),
            Span::styled("NO ", Style::default().fg(Color::Cyan)),
            Span::raw(fmt_entry(no_entry)),
        ]),
        Line::from(vec![
            Span::styled("Stats:     ", Style::default().add_modifier(Modifier::BOLD)),
            stats_span(state),
        ]),
    ])
    .block(Block::default().borders(Borders::ALL).title(" Strategy "))
}

/// Render the per-side ask-swing line for the strategy panel. Each side's
/// ask move is colored red (ask falling = down swing for that side),
/// green (rising), or gray (no data).
fn swing_line<'a>(
    lookback: Duration,
    yes_swing: Option<Decimal>,
    no_swing: Option<Decimal>,
) -> Line<'a> {
    let label = format!("Swing {}s: ", lookback.as_secs());
    let fmt = |s: Option<Decimal>| -> (String, Color) {
        match s {
            None => ("—".to_string(), Color::DarkGray),
            Some(v) => {
                let sign = if v.is_sign_negative() { "" } else { "+" };
                let color = if v.is_zero() {
                    Color::DarkGray
                } else if v.is_sign_negative() {
                    Color::Red
                } else {
                    Color::Green
                };
                (format!("{sign}{v}"), color)
            }
        }
    };
    let (yes_text, yes_color) = fmt(yes_swing);
    let (no_text, no_color) = fmt(no_swing);
    Line::from(vec![
        Span::styled(label, Style::default().add_modifier(Modifier::BOLD)),
        Span::styled("YES ", Style::default().fg(Color::Cyan)),
        Span::styled(yes_text, Style::default().fg(yes_color).add_modifier(Modifier::BOLD)),
        Span::raw("   "),
        Span::styled("NO ", Style::default().fg(Color::Cyan)),
        Span::styled(no_text, Style::default().fg(no_color).add_modifier(Modifier::BOLD)),
    ])
}

fn stats_span(state: &AppState) -> Span<'static> {
    let resolved = state.n_wins + state.n_losses;
    let win_rate = if resolved > 0 {
        state.n_wins as f64 * 100.0 / resolved as f64
    } else {
        0.0
    };
    Span::raw(format!(
        "entries={} wins={} losses={} unresolved={} | win_rate={:.1}% pnl={}",
        state.n_entries, state.n_wins, state.n_losses, state.n_unresolved, win_rate, state.pnl,
    ))
}

fn table(state: &AppState) -> Table<'_> {
    let header_style = Style::default()
        .fg(Color::Black)
        .bg(Color::DarkGray)
        .add_modifier(Modifier::BOLD);

    let header = Row::new([
        "Outcome", "Bid", "BidSz", "Ask", "AskSz", "Mid", "Last", "Δbook", "Δtrade", "Entry",
    ])
    .style(header_style);

    let now = Instant::now();
    let rows: Vec<Row> = state
        .outcomes
        .iter()
        .map(|o| {
            let mid = match (o.bid, o.ask) {
                (Some(b), Some(a)) => Some((b + a) / Decimal::from(2)),
                _ => None,
            };
            let entry_cell = match o.entry {
                Some(e) => Cell::from(format!("{}@{}s", e.ask, e.offset_s))
                    .style(Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                None => Cell::from("—").style(Style::default().fg(Color::DarkGray)),
            };
            Row::new(vec![
                Cell::from(o.outcome.clone()),
                cell_price(o.bid),
                cell_size(o.bid_size),
                cell_price(o.ask),
                cell_size(o.ask_size),
                cell_price(mid),
                cell_price(o.last),
                cell_age(o.last_book_at, now),
                cell_age(o.last_trade_at, now),
                entry_cell,
            ])
        })
        .collect();

    Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
            Constraint::Length(14),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(" Outcomes "))
}

fn recent_panel(state: &AppState) -> Paragraph<'_> {
    let lines: Vec<Line> = if state.recent.is_empty() {
        vec![Line::from(Span::styled(
            "  no entries yet",
            Style::default().fg(Color::DarkGray),
        ))]
    } else {
        state
            .recent
            .iter()
            .rev()
            .map(|re| {
                let result_span = match (re.won, re.pnl) {
                    (Some(true), Some(p)) => Span::styled(
                        format!("  WIN {p}"),
                        Style::default().fg(Color::Green).add_modifier(Modifier::BOLD),
                    ),
                    (Some(false), Some(p)) => Span::styled(
                        format!("  LOSS {p}"),
                        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
                    ),
                    _ => Span::styled(
                        "  pending".to_string(),
                        Style::default().fg(Color::Yellow),
                    ),
                };
                let source_span = match re.resolved_source {
                    Some(src) => Span::styled(
                        format!(" [{src}]"),
                        Style::default().fg(Color::DarkGray),
                    ),
                    None => Span::raw(""),
                };
                let down = entered_on_down_swing(re.swing_at_entry).unwrap_or(false);
                let swing_text = match re.swing_at_entry {
                    Some(s) => {
                        let sign = if s.is_sign_negative() { "" } else { "+" };
                        format!(" swing {sign}{s}")
                    }
                    None => " swing —".to_string(),
                };
                let swing_color = if down { Color::Red } else { Color::DarkGray };
                Line::from(vec![
                    Span::raw(format!(
                        " w {} {} @ {} (+{}s)",
                        re.window_start_ts, re.side, re.ask, re.offset_s
                    )),
                    Span::styled(swing_text, Style::default().fg(swing_color)),
                    result_span,
                    source_span,
                ])
            })
            .collect()
    };
    Paragraph::new(lines).block(Block::default().borders(Borders::ALL).title(" Recent entries "))
}

fn cell_price(v: Option<Decimal>) -> Cell<'static> {
    match v {
        Some(d) => Cell::from(format!("{d:.4}")),
        None => Cell::from("—").style(Style::default().fg(Color::DarkGray)),
    }
}

fn cell_size(v: Option<Decimal>) -> Cell<'static> {
    match v {
        Some(d) => Cell::from(format!("{d:.2}")).style(Style::default().fg(Color::Cyan)),
        None => Cell::from("—").style(Style::default().fg(Color::DarkGray)),
    }
}

fn cell_age(t: Option<Instant>, now: Instant) -> Cell<'static> {
    match t {
        None => Cell::from("—").style(Style::default().fg(Color::DarkGray)),
        Some(t) => {
            let secs = now.saturating_duration_since(t).as_secs_f64();
            let color = if secs < 2.0 {
                Color::Green
            } else if secs < 15.0 {
                Color::Yellow
            } else {
                Color::Red
            };
            Cell::from(format!("{secs:>4.1}s")).style(Style::default().fg(color))
        }
    }
}

fn footer() -> Paragraph<'static> {
    Paragraph::new(Line::from(vec![Span::styled(
        " q/Esc/Ctrl-C to quit ",
        Style::default().fg(Color::DarkGray),
    )]))
    .block(Block::default().borders(Borders::ALL))
}

fn init_terminal() -> Result<Terminal<CrosstermBackend<Stdout>>> {
    enable_raw_mode()?;
    let mut out = stdout();
    execute!(out, EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(out);
    Ok(Terminal::new(backend)?)
}

fn restore_terminal() -> Result<()> {
    disable_raw_mode()?;
    execute!(stdout(), LeaveAlternateScreen)?;
    Ok(())
}
