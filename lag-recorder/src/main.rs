// Lag recorder: detects significant CEX BTC moves, watches whether
// Polymarket YES midprice reacts in the matching direction, and writes
// one CSV row per event (reacted / timeout / rollover). Designed to run
// for hours: auto-resolves the current 5-min market and rolls over at
// every 300s UNIX boundary like `live-prices`.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use polymarket_core::{
    BitstampFeed, CexEvent, CexPayload, CexVenue, CoinbaseFeed, Decimal, FeedSource,
    KrakenFeed, MarketSnapshot, Polymarket, PolymarketEvent, PolymarketFeed,
    PolymarketPayload, RecordedEvent, U256,
};
use tokio::signal;
use tokio::sync::broadcast;

const COINBASE_PRODUCT: &str = "BTC-USD";
const KRAKEN_SYMBOL: &str = "BTC/USD";
const BITSTAMP_PAIR: &str = "btcusd";
const WINDOW_SECS: u64 = 300;
const VENUES: &[CexVenue] = &[CexVenue::Coinbase, CexVenue::Kraken, CexVenue::Bitstamp];

/// A venue's last-trade is included in the median only if updated within
/// this window (matches live-prices). Bitstamp's BTC/USD trade tape is
/// sparse so without this a stale Bitstamp print drags the median.
const MEDIAN_FRESHNESS: Duration = Duration::from_secs(5);

/// Window over which we measure the median move when deciding to trigger.
const TRIGGER_LOOKBACK: Duration = Duration::from_secs(5);

/// Min time between consecutive triggers (avoid double-counting one move).
const TRIGGER_DEBOUNCE: Duration = Duration::from_secs(5);

/// Pending triggers older than this are marked timed-out.
const REACTION_TIMEOUT: Duration = Duration::from_secs(30);

/// USD threshold for the median CEX move that fires a trigger.
fn trigger_threshold_usd() -> Decimal {
    Decimal::from(10)
}

/// Polymarket YES midprice move (price units, 0–1) counted as a reaction.
/// 0.005 = 0.5¢.
fn reaction_threshold() -> Decimal {
    Decimal::new(5, 3)
}

struct VenueState {
    last: Option<Decimal>,
    last_at: Option<Instant>,
}

#[derive(Clone, Copy)]
struct MedianSample {
    t: Instant,
    median: Decimal,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Direction {
    Up,
    Down,
}

impl Direction {
    fn tag(self) -> &'static str {
        match self {
            Direction::Up => "up",
            Direction::Down => "down",
        }
    }
    fn sign(self) -> i32 {
        match self {
            Direction::Up => 1,
            Direction::Down => -1,
        }
    }
}

struct PendingTrigger {
    t_trigger_mono: Instant,
    t_trigger_wall_ms: i64,
    direction: Direction,
    median_before: Decimal,
    median_after: Decimal,
    yes_mid_at_trigger: Decimal,
    no_mid_at_trigger: Option<Decimal>,
}

#[derive(Default, Clone, Copy)]
struct OutcomeBook {
    bid: Option<Decimal>,
    ask: Option<Decimal>,
}

impl OutcomeBook {
    fn mid(&self) -> Option<Decimal> {
        match (self.bid, self.ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::from(2)),
            _ => None,
        }
    }
}

struct PmState {
    yes_token: U256,
    no_token: Option<U256>,
    yes_book: OutcomeBook,
    no_book: OutcomeBook,
    yes_token_hex: String,
    condition_id: String,
    window_start_ts: u64,
}

struct State {
    venues: HashMap<CexVenue, VenueState>,
    median_history: VecDeque<MedianSample>,
    last_trigger_at: Option<Instant>,
    pending: Vec<PendingTrigger>,
    pm: PmState,
    out: BufWriter<File>,
    n_triggers: u64,
    n_reactions: u64,
    n_timeouts: u64,
    n_rollovers: u64,
    lag_samples: Vec<u64>,
}

fn write_header(w: &mut BufWriter<File>) -> Result<()> {
    writeln!(
        w,
        "trigger_wall_ms,direction,median_before,median_after,median_delta,\
         pm_yes_mid_at_trigger,pm_no_mid_at_trigger,status,lag_ms,\
         pm_yes_mid_at_reaction,pm_yes_mid_delta,yes_token_id,condition_id,window_start_ts"
    )?;
    w.flush()?;
    Ok(())
}

fn write_row(
    w: &mut BufWriter<File>,
    t: &PendingTrigger,
    yes_token_hex: &str,
    condition_id: &str,
    window_start_ts: u64,
    status: &str,
    lag_ms: Option<u64>,
    yes_mid_at_reaction: Option<Decimal>,
) -> Result<()> {
    let delta = t.median_after - t.median_before;
    let no_mid_str = t
        .no_mid_at_trigger
        .map(|m| m.to_string())
        .unwrap_or_default();
    let lag_str = lag_ms.map(|l| l.to_string()).unwrap_or_default();
    let reaction_mid_str = yes_mid_at_reaction
        .map(|m| m.to_string())
        .unwrap_or_default();
    let reaction_delta_str = yes_mid_at_reaction
        .map(|m| (m - t.yes_mid_at_trigger).to_string())
        .unwrap_or_default();
    writeln!(
        w,
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        t.t_trigger_wall_ms,
        t.direction.tag(),
        t.median_before,
        t.median_after,
        delta,
        t.yes_mid_at_trigger,
        no_mid_str,
        status,
        lag_str,
        reaction_mid_str,
        reaction_delta_str,
        yes_token_hex,
        condition_id,
        window_start_ts,
    )?;
    w.flush()?;
    Ok(())
}

fn median_of_cex(venues: &HashMap<CexVenue, VenueState>) -> Option<Decimal> {
    let now = Instant::now();
    let mut prices: Vec<Decimal> = VENUES
        .iter()
        .filter_map(|v| {
            let s = venues.get(v)?;
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

/// Pick which token id is "YES" (BTC up). Defaults to outcomes[0] if no
/// label matches the obvious aliases.
fn pick_yes_no(snapshot: &MarketSnapshot) -> (U256, Option<U256>, String) {
    let yes_idx = snapshot
        .outcomes
        .iter()
        .position(|o| {
            let lc = o.outcome.to_lowercase();
            lc == "yes" || lc == "up" || lc == "above"
        })
        .unwrap_or(0);
    let yes = snapshot.outcomes[yes_idx].token_id;
    let yes_hex = format!("0x{yes:x}");
    let no = snapshot
        .outcomes
        .iter()
        .enumerate()
        .find(|(i, _)| *i != yes_idx)
        .map(|(_, o)| o.token_id);
    (yes, no, yes_hex)
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

fn now_wall_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn apply_cex(state: &mut State, e: &CexEvent) {
    let now = Instant::now();
    let v = state.venues.entry(e.venue).or_insert(VenueState {
        last: None,
        last_at: None,
    });
    v.last_at = Some(now);
    match &e.payload {
        CexPayload::Ticker { last, .. } => v.last = Some(*last),
        CexPayload::Trade { price, .. } => v.last = Some(*price),
    }

    let Some(m) = median_of_cex(&state.venues) else {
        return;
    };
    state
        .median_history
        .push_back(MedianSample { t: now, median: m });
    while let Some(front) = state.median_history.front().copied() {
        if now.saturating_duration_since(front.t) > TRIGGER_LOOKBACK {
            state.median_history.pop_front();
        } else {
            break;
        }
    }
    if state.median_history.len() < 2 {
        return;
    }
    let Some(front) = state.median_history.front().copied() else {
        return;
    };
    let delta = m - front.median;
    if delta.abs() < trigger_threshold_usd() {
        return;
    }
    if let Some(t_last) = state.last_trigger_at
        && now.saturating_duration_since(t_last) < TRIGGER_DEBOUNCE
    {
        return;
    }
    let Some(yes_mid) = state.pm.yes_book.mid() else {
        // No PM baseline yet; skip silently. Common at startup.
        return;
    };
    let direction = if delta.is_sign_negative() {
        Direction::Down
    } else {
        Direction::Up
    };
    state.pending.push(PendingTrigger {
        t_trigger_mono: now,
        t_trigger_wall_ms: now_wall_ms(),
        direction,
        median_before: front.median,
        median_after: m,
        yes_mid_at_trigger: yes_mid,
        no_mid_at_trigger: state.pm.no_book.mid(),
    });
    state.last_trigger_at = Some(now);
    state.n_triggers += 1;
    // Reset history so the same move can't re-fire on the next event.
    state.median_history.clear();
    state
        .median_history
        .push_back(MedianSample { t: now, median: m });
}

fn apply_pm(state: &mut State, e: &PolymarketEvent) {
    let yes = state.pm.yes_token;
    let no = state.pm.no_token;
    match &e.payload {
        PolymarketPayload::Book(b) => {
            if b.asset_id == yes {
                state.pm.yes_book.bid = b.bids.first().map(|l| l.price);
                state.pm.yes_book.ask = b.asks.first().map(|l| l.price);
            } else if Some(b.asset_id) == no {
                state.pm.no_book.bid = b.bids.first().map(|l| l.price);
                state.pm.no_book.ask = b.asks.first().map(|l| l.price);
            }
        }
        PolymarketPayload::PriceChange(p) => {
            for entry in &p.price_changes {
                if entry.asset_id == yes {
                    if let Some(bb) = entry.best_bid {
                        state.pm.yes_book.bid = Some(bb);
                    }
                    if let Some(ba) = entry.best_ask {
                        state.pm.yes_book.ask = Some(ba);
                    }
                } else if Some(entry.asset_id) == no {
                    if let Some(bb) = entry.best_bid {
                        state.pm.no_book.bid = Some(bb);
                    }
                    if let Some(ba) = entry.best_ask {
                        state.pm.no_book.ask = Some(ba);
                    }
                }
            }
        }
        PolymarketPayload::LastTrade(_) => {
            // Trades don't move our mid baseline; don't treat as reaction.
        }
    }
    check_pending(state);
}

fn check_pending(state: &mut State) {
    let now = Instant::now();
    let yes_mid_now = state.pm.yes_book.mid();
    let reaction_threshold = reaction_threshold();
    let yes_token_hex = state.pm.yes_token_hex.clone();
    let condition_id = state.pm.condition_id.clone();
    let window_start_ts = state.pm.window_start_ts;

    let mut i = 0;
    while i < state.pending.len() {
        let t = &state.pending[i];

        if now.saturating_duration_since(t.t_trigger_mono) > REACTION_TIMEOUT {
            let owned = state.pending.remove(i);
            state.n_timeouts += 1;
            if let Err(e) = write_row(
                &mut state.out,
                &owned,
                &yes_token_hex,
                &condition_id,
                window_start_ts,
                "timeout",
                None,
                None,
            ) {
                eprintln!("WARN: write timeout row: {e:#}");
            }
            continue;
        }

        if let Some(mid) = yes_mid_now {
            let mid_delta = mid - t.yes_mid_at_trigger;
            let sign = if mid_delta.is_sign_negative() { -1 } else { 1 };
            if sign == t.direction.sign() && mid_delta.abs() >= reaction_threshold {
                let lag_ms = now
                    .saturating_duration_since(t.t_trigger_mono)
                    .as_millis() as u64;
                let owned = state.pending.remove(i);
                state.n_reactions += 1;
                state.lag_samples.push(lag_ms);
                if let Err(e) = write_row(
                    &mut state.out,
                    &owned,
                    &yes_token_hex,
                    &condition_id,
                    window_start_ts,
                    "reacted",
                    Some(lag_ms),
                    Some(mid),
                ) {
                    eprintln!("WARN: write reacted row: {e:#}");
                }
                continue;
            }
        }
        i += 1;
    }
}

fn flush_pending_for_rollover(state: &mut State) {
    let yes_token_hex = state.pm.yes_token_hex.clone();
    let condition_id = state.pm.condition_id.clone();
    let window_start_ts = state.pm.window_start_ts;
    for t in state.pending.drain(..) {
        state.n_rollovers += 1;
        if let Err(e) = write_row(
            &mut state.out,
            &t,
            &yes_token_hex,
            &condition_id,
            window_start_ts,
            "rollover",
            None,
            None,
        ) {
            eprintln!("WARN: write rollover row: {e:#}");
        }
    }
}

fn percentile(samples: &[u64], p: f64) -> Option<u64> {
    if samples.is_empty() {
        return None;
    }
    let mut s = samples.to_vec();
    s.sort();
    let idx = ((s.len() as f64 * p) as usize).min(s.len() - 1);
    Some(s[idx])
}

fn print_summary(state: &State, t_start: Instant) {
    let elapsed = t_start.elapsed().as_secs_f64();
    let p50 = percentile(&state.lag_samples, 0.50);
    let p95 = percentile(&state.lag_samples, 0.95);
    eprintln!(
        "[{:>7.1}s] trig={} reacted={} timeout={} rollover={} | lag p50={}ms p95={}ms",
        elapsed,
        state.n_triggers,
        state.n_reactions,
        state.n_timeouts,
        state.n_rollovers,
        p50.map(|v| v.to_string()).unwrap_or_else(|| "—".into()),
        p95.map(|v| v.to_string()).unwrap_or_else(|| "—".into()),
    );
}

fn apply_event(state: &mut State, evt: RecordedEvent) {
    match evt {
        RecordedEvent::Polymarket(e) => apply_pm(state, &e),
        RecordedEvent::Cex(e) => apply_cex(state, &e),
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
    let (yes_token, no_token, yes_token_hex) = pick_yes_no(&snapshot);
    let window_start_ts = current_window_start_ts()?;
    let token_ids: Vec<U256> = snapshot.outcomes.iter().map(|o| o.token_id).collect();

    // Drop old feed first so its three WS tasks abort cleanly before
    // we start a new one (matches live-prices rollover).
    *pm_feed = None;
    *pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);

    state.pm = PmState {
        yes_token,
        no_token,
        yes_book: OutcomeBook::default(),
        no_book: OutcomeBook::default(),
        yes_token_hex,
        condition_id: snapshot.condition_id,
        window_start_ts,
    };
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    let mut args = std::env::args().skip(1);
    let mut out_path: Option<PathBuf> = None;
    let mut explicit_market: Option<String> = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "-o" | "--out" => {
                out_path = Some(PathBuf::from(
                    args.next().context("--out needs a path")?,
                ));
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
        PathBuf::from(format!("lag-{ts}.csv"))
    });
    let mut out = BufWriter::new(
        File::create(&out_path)
            .with_context(|| format!("creating {}", out_path.display()))?,
    );
    write_header(&mut out)?;
    eprintln!("writing to {}", out_path.display());

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
    let (yes_token, no_token, yes_token_hex) = pick_yes_no(&snapshot);
    let window_start_ts = current_window_start_ts()?;
    eprintln!(
        "market: {} ({})\n  yes token: {}\n  trigger: >${} median move over {}s | reaction: >=0.5¢ in <=30s",
        snapshot.question,
        snapshot.condition_id,
        yes_token_hex,
        trigger_threshold_usd(),
        TRIGGER_LOOKBACK.as_secs(),
    );

    let token_ids: Vec<U256> = snapshot.outcomes.iter().map(|o| o.token_id).collect();
    let (tx, mut rx) = broadcast::channel::<RecordedEvent>(4096);
    let mut pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);
    let _cb = CoinbaseFeed::start(COINBASE_PRODUCT, tx.clone());
    let _kr = KrakenFeed::start(KRAKEN_SYMBOL, tx.clone());
    let _bs = BitstampFeed::start(BITSTAMP_PAIR, tx.clone());

    let mut state = State {
        venues: HashMap::new(),
        median_history: VecDeque::new(),
        last_trigger_at: None,
        pending: Vec::new(),
        pm: PmState {
            yes_token,
            no_token,
            yes_book: OutcomeBook::default(),
            no_book: OutcomeBook::default(),
            yes_token_hex,
            condition_id: snapshot.condition_id.clone(),
            window_start_ts,
        },
        out,
        n_triggers: 0,
        n_reactions: 0,
        n_timeouts: 0,
        n_rollovers: 0,
        lag_samples: Vec::new(),
    };

    let t_start = Instant::now();
    let mut next_rollover: Option<Instant> = if auto_roll {
        Some(next_window_boundary()?)
    } else {
        None
    };
    let mut summary_tick = tokio::time::interval(Duration::from_secs(30));
    summary_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut timeout_tick = tokio::time::interval(Duration::from_secs(1));
    timeout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

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
                flush_pending_for_rollover(&mut state);
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
            _ = timeout_tick.tick() => {
                check_pending(&mut state);
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

    flush_pending_for_rollover(&mut state);
    state.out.flush()?;
    print_summary(&state, t_start);
    outcome
}
