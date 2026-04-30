use std::io::{Stdout, stdout};
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use crossterm::event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers};
use crossterm::execute;
use crossterm::terminal::{
    EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode,
};
use futures::StreamExt;
use polymarket_core::{
    CoinbaseFeed, CoinbasePayload, Decimal, FeedSource, PolymarketEvent, PolymarketFeed,
    PolymarketPayload, Polymarket, RecordedEvent, U256,
};
use ratatui::Terminal;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Layout};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Paragraph, Row, Table};
use tokio::sync::broadcast;

const COINBASE_PRODUCT: &str = "BTC-USD";

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

struct AppState {
    question: String,
    slug: String,
    condition_id: String,
    outcomes: Vec<OutcomeRow>,
    last_event_at: Option<Instant>,
    last_error: Option<String>,
    btc_bid: Option<Decimal>,
    btc_ask: Option<Decimal>,
    btc_last: Option<Decimal>,
    last_btc_at: Option<Instant>,
    btc_evt_ticker: u64,
    btc_evt_trade: u64,
}

impl AppState {
    fn apply(&mut self, evt: RecordedEvent) {
        let now = Instant::now();
        self.last_event_at = Some(now);
        match evt {
            RecordedEvent::Polymarket(e) => self.apply_pm(&e, now),
            RecordedEvent::Coinbase(e) => {
                self.last_btc_at = Some(now);
                match &e.payload {
                    CoinbasePayload::Ticker {
                        best_bid,
                        best_ask,
                        last,
                        ..
                    } => {
                        self.btc_bid = Some(*best_bid);
                        self.btc_ask = Some(*best_ask);
                        self.btc_last = Some(*last);
                        self.btc_evt_ticker += 1;
                    }
                    CoinbasePayload::Trade { price, .. } => {
                        self.btc_last = Some(*price);
                        self.btc_evt_trade += 1;
                    }
                }
            }
            RecordedEvent::FeedError {
                source, message, ..
            } => {
                let tag = match source {
                    FeedSource::Polymarket => "polymarket",
                    FeedSource::Coinbase => "coinbase",
                };
                self.last_error = Some(format!("[{tag}] {message}"));
            }
        }
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("usage: live-prices <slug-or-condition_id>"))?;

    let pm = Polymarket::new()?;
    let condition_id = pm.resolve_condition_id(&arg).await?;
    let snapshot = pm.fetch_snapshot(&condition_id).await?;

    let outcomes: Vec<OutcomeRow> = snapshot
        .outcomes
        .into_iter()
        .map(|o| OutcomeRow {
            outcome: o.outcome,
            token_id: o.token_id,
            bid: o.bid,
            bid_size: None,
            ask: o.ask,
            ask_size: None,
            last: o.last,
            last_book_at: None,
            last_trade_at: None,
        })
        .collect();

    if outcomes.is_empty() {
        return Err(anyhow!("market has no outcome tokens"));
    }

    let mut state = AppState {
        question: snapshot.question,
        slug: snapshot.market_slug,
        condition_id,
        outcomes,
        last_event_at: None,
        last_error: None,
        btc_bid: None,
        btc_ask: None,
        btc_last: None,
        last_btc_at: None,
        btc_evt_ticker: 0,
        btc_evt_trade: 0,
    };

    let token_ids: Vec<U256> = state.outcomes.iter().map(|o| o.token_id).collect();
    let (tx, mut rx) = broadcast::channel::<RecordedEvent>(1024);
    let _pm_feed = PolymarketFeed::start(token_ids, tx.clone())?;
    let _cb_feed = CoinbaseFeed::start(COINBASE_PRODUCT, tx);

    let mut terminal = init_terminal()?;
    let res = run(&mut terminal, &mut state, &mut rx).await;
    restore_terminal()?;
    res
}

async fn run(
    terminal: &mut Terminal<CrosstermBackend<Stdout>>,
    state: &mut AppState,
    rx: &mut broadcast::Receiver<RecordedEvent>,
) -> Result<()> {
    let mut events = EventStream::new();
    let mut tick = tokio::time::interval(Duration::from_millis(200));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        terminal.draw(|f| render(f, state))?;

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
            _ = tick.tick() => {}
        }
    }
}

fn should_quit(code: KeyCode, mods: KeyModifiers) -> bool {
    matches!(code, KeyCode::Char('q') | KeyCode::Esc)
        || (matches!(code, KeyCode::Char('c')) && mods.contains(KeyModifiers::CONTROL))
}

fn render(f: &mut ratatui::Frame, state: &AppState) {
    let header_height = 7 + state.last_error.is_some() as u16;
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

    let btc_line = {
        let fmt = |v: Option<Decimal>, prec: usize| match v {
            Some(d) => format!("{d:.*}", prec),
            None => "—".to_string(),
        };
        let age = match state.last_btc_at {
            Some(t) => format!("{:.1}s", t.elapsed().as_secs_f64()),
            None => "—".to_string(),
        };
        format!(
            "last {} | bid {} ask {} | Δrecv {}  [ticker {} trade {}]",
            fmt(state.btc_last, 2),
            fmt(state.btc_bid, 2),
            fmt(state.btc_ask, 2),
            age,
            state.btc_evt_ticker,
            state.btc_evt_trade,
        )
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
            Span::styled("BTC-USD:   ", Style::default().add_modifier(Modifier::BOLD)),
            Span::raw(btc_line),
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
