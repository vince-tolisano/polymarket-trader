use std::collections::HashMap;
use std::io::{Stdout, stdout};
use std::sync::Arc;
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
    MarketSnapshot, PolymarketEvent, PolymarketFeed, PolymarketPayload, Polymarket,
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
/// A venue's last-trade is included in the running median only if it was
/// updated within this window. Bitstamp's BTC/USD trade tape is sparse
/// (often 10+ seconds between prints) so without this filter a stale
/// Bitstamp print drags the median against fast Coinbase moves.
const MEDIAN_FRESHNESS: Duration = Duration::from_secs(5);

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
    /// Pyth Network BTC/USD aggregate at the window-start timestamp.
    Px,
    /// Stopgap — median of CEX last-trades captured at first observation.
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

struct AppState {
    question: String,
    slug: String,
    condition_id: String,
    outcomes: Vec<OutcomeRow>,
    last_event_at: Option<Instant>,
    last_error: Option<String>,
    btc: HashMap<CexVenue, VenueState>,
    btc_target: Option<Target>,
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
                    && VENUES.iter().all(|v| {
                        self.btc.get(v).and_then(|s| s.last).is_some()
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

    fn apply_pm(&mut self, e: &PolymarketEvent, now: Instant) {
        match &e.payload {
            PolymarketPayload::Book(b) => {
                if let Some(row) = self.find_mut(&b.asset_id) {
                    row.bid = b.bids.first().map(|l| l.price);
                    row.bid_size = b.bids.first().map(|l| l.size);
                    row.ask = b.asks.first().map(|l| l.price);
                    row.ask_size = b.asks.first().map(|l| l.size);
                    row.last_book_at = Some(now);
                }
            }
            PolymarketPayload::PriceChange(p) => {
                for entry in &p.price_changes {
                    if let Some(row) = self.find_mut(&entry.asset_id) {
                        if let Some(bb) = entry.best_bid {
                            row.bid = Some(bb);
                        }
                        if let Some(ba) = entry.best_ask {
                            row.ask = Some(ba);
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
    }

    fn find_mut(&mut self, asset_id: &U256) -> Option<&mut OutcomeRow> {
        self.outcomes.iter_mut().find(|o| &o.token_id == asset_id)
    }

    fn enter_new_window(&mut self, snapshot: MarketSnapshot, outcomes: Vec<OutcomeRow>) {
        self.question = snapshot.question;
        self.slug = snapshot.market_slug;
        self.condition_id = snapshot.condition_id;
        self.outcomes = outcomes;
        self.last_event_at = None;
        self.btc_target = None;
    }
}

fn outcomes_from_snapshot(snapshot: &MarketSnapshot) -> Vec<OutcomeRow> {
    snapshot
        .outcomes
        .iter()
        .map(|o| OutcomeRow {
            outcome: o.outcome.clone(),
            token_id: o.token_id,
            bid: o.bid,
            bid_size: None,
            ask: o.ask,
            ask_size: None,
            last: o.last,
            last_book_at: None,
            last_trade_at: None,
        })
        .collect()
}

fn next_window_boundary() -> Result<Instant> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system time before unix epoch")?;
    let next_boundary_secs = (now.as_secs() / WINDOW_SECS + 1) * WINDOW_SECS;
    let dur_until = Duration::from_secs(next_boundary_secs) - now;
    Ok(Instant::now() + dur_until)
}

const TARGET_FETCH_RETRIES: u32 = 5;
const TARGET_FETCH_BACKOFF: Duration = Duration::from_secs(2);

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

#[tokio::main]
async fn main() -> Result<()> {
    // rustls 0.23 needs an explicit default crypto provider; with multiple
    // reqwest versions in the dep tree (ours + the sdk's), feature unification
    // doesn't pick one for us. Pick ring at startup so every TLS client
    // (CEX feeds, polymarket WS, reqwest) uses it.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let arg = std::env::args().nth(1);
    let auto_roll = arg.is_none();

    let pm = Arc::new(Polymarket::new()?);
    let condition_id = match arg {
        Some(a) => pm.resolve_condition_id(&a).await?,
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

    let mut state = AppState {
        question: snapshot.question.clone(),
        slug: snapshot.market_slug.clone(),
        condition_id: snapshot.condition_id.clone(),
        outcomes,
        last_event_at: None,
        last_error: None,
        btc: HashMap::new(),
        btc_target: None,
    };

    let token_ids: Vec<U256> = state.outcomes.iter().map(|o| o.token_id).collect();
    let (tx, mut rx) = broadcast::channel::<RecordedEvent>(1024);
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

    let mut terminal = init_terminal()?;
    let res = run(
        &mut terminal,
        &mut state,
        &mut rx,
        &tx,
        &pm,
        &mut pm_feed,
        &mut target_rx,
        &target_tx,
        &mut target_fetcher,
        auto_roll,
    )
    .await;
    restore_terminal()?;
    res
}

async fn roll_window(
    state: &mut AppState,
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
    let outcomes = outcomes_from_snapshot(&snapshot);
    if outcomes.is_empty() {
        anyhow::bail!("new market has no outcome tokens");
    }
    let token_ids: Vec<U256> = outcomes.iter().map(|o| o.token_id).collect();

    *pm_feed = None;
    *pm_feed = Some(PolymarketFeed::start(token_ids, tx.clone())?);

    state.enter_new_window(snapshot, outcomes);

    if let Some(h) = target_fetcher.take() {
        h.abort();
    }
    *target_fetcher = Some(spawn_target_fetcher(Arc::clone(pm), target_tx.clone()));

    Ok(())
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    state: &mut AppState,
    rx: &mut broadcast::Receiver<RecordedEvent>,
    tx: &broadcast::Sender<RecordedEvent>,
    pm: &Arc<Polymarket>,
    pm_feed: &mut Option<PolymarketFeed>,
    target_rx: &mut mpsc::UnboundedReceiver<TargetMsg>,
    target_tx: &mpsc::UnboundedSender<TargetMsg>,
    target_fetcher: &mut Option<JoinHandle<()>>,
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
                match roll_window(state, tx, pm, pm_feed, target_tx, target_fetcher).await {
                    Ok(()) => {
                        next_rollover = next_window_boundary().ok();
                    }
                    Err(e) => {
                        state.last_error = Some(format!("rollover failed: {e:#}"));
                        next_rollover = Some(Instant::now() + Duration::from_secs(2));
                    }
                }
                // Discard any pm-target value queued by the prior window's fetcher
                // before we aborted it; otherwise we'd apply it to the new window.
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
        Constraint::Min(3),
        Constraint::Length(3),
    ])
    .split(f.area());

    f.render_widget(header(state), chunks[0]);
    f.render_widget(table(state), chunks[1]);
    f.render_widget(footer(state), chunks[2]);
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

fn table(state: &AppState) -> Table<'_> {
    let header_style = Style::default()
        .fg(Color::Black)
        .bg(Color::DarkGray)
        .add_modifier(Modifier::BOLD);

    let header = Row::new([
        "Outcome", "Bid", "BidSz", "Ask", "AskSz", "Mid", "Last", "Δbook", "Δtrade",
    ])
    .style(header_style);

    let now = Instant::now();
    let rows = state.outcomes.iter().map(|o| {
        let mid = match (o.bid, o.ask) {
            (Some(b), Some(a)) => Some((b + a) / Decimal::from(2)),
            _ => None,
        };
        Row::new([
            Cell::from(o.outcome.clone()),
            cell_price(o.bid),
            cell_size(o.bid_size),
            cell_price(o.ask),
            cell_size(o.ask_size),
            cell_price(mid),
            cell_price(o.last),
            cell_age(o.last_book_at, now),
            cell_age(o.last_trade_at, now),
        ])
    });

    Table::new(
        rows,
        [
            Constraint::Length(14),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(12),
            Constraint::Length(10),
            Constraint::Length(10),
            Constraint::Length(8),
            Constraint::Length(8),
        ],
    )
    .header(header)
    .block(Block::default().borders(Borders::ALL).title(" Outcomes "))
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

fn footer(_state: &AppState) -> Paragraph<'_> {
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
