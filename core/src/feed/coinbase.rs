use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::Decimal;
use crate::event::{
    CoinbaseEvent, CoinbasePayload, EventClock, FeedSource, RecordedEvent, TradeSide,
};

const WS_URL: &str = "wss://advanced-trade-ws.coinbase.com";

pub struct CoinbaseFeed {
    _task: JoinHandle<()>,
}

impl CoinbaseFeed {
    pub fn start(
        product_id: impl Into<String>,
        tx: broadcast::Sender<RecordedEvent>,
    ) -> Self {
        let product_id = product_id.into();
        let task = tokio::spawn(run(product_id, tx));
        Self { _task: task }
    }
}

async fn run(product_id: String, tx: broadcast::Sender<RecordedEvent>) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);
    loop {
        let result = connect_and_pump(&product_id, &tx).await;
        let message = match &result {
            Ok(()) => "ws closed; reconnecting".to_string(),
            Err(e) => format!("ws error: {e:#}"),
        };
        let _ = tx.send(RecordedEvent::FeedError {
            source: FeedSource::Coinbase,
            message,
            clock: EventClock::now(None),
        });
        tokio::time::sleep(backoff).await;
        backoff = if result.is_ok() {
            Duration::from_secs(1)
        } else {
            (backoff * 2).min(max_backoff)
        };
    }
}

async fn connect_and_pump(
    product_id: &str,
    tx: &broadcast::Sender<RecordedEvent>,
) -> Result<()> {
    let (ws_stream, _resp) = connect_async(WS_URL)
        .await
        .with_context(|| format!("connecting to {WS_URL}"))?;
    let (mut sink, mut stream) = ws_stream.split();

    // One subscribe message per channel. heartbeats keeps the connection
    // alive when the others are quiet; ticker gives bid/ask/last in one
    // shot; market_trades gives the full trade tape.
    for channel in &["heartbeats", "ticker", "market_trades"] {
        let msg = format!(
            r#"{{"type":"subscribe","product_ids":["{product_id}"],"channel":"{channel}"}}"#
        );
        sink.send(Message::Text(msg.into()))
            .await
            .with_context(|| format!("subscribing to {channel}"))?;
    }

    loop {
        let msg = match stream.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => return Err(e).context("ws recv"),
            None => return Ok(()),
        };
        match msg {
            Message::Text(text) => match parse_frame(product_id, text.as_str()) {
                Ok(events) => {
                    for evt in events {
                        if tx.send(evt).is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(RecordedEvent::FeedError {
                        source: FeedSource::Coinbase,
                        message: format!("parse: {e:#}"),
                        clock: EventClock::now(None),
                    });
                }
            },
            Message::Ping(data) => {
                sink.send(Message::Pong(data))
                    .await
                    .context("send pong")?;
            }
            Message::Close(_) => return Ok(()),
            _ => {}
        }
    }
}

#[derive(Deserialize)]
struct ChannelOnly<'a> {
    channel: &'a str,
}

#[derive(Deserialize)]
struct TickerEnvelope {
    events: Vec<TickerEvent>,
}

#[derive(Deserialize)]
struct TickerEvent {
    #[serde(default)]
    tickers: Vec<TickerRaw>,
}

#[derive(Deserialize)]
struct TickerRaw {
    product_id: String,
    price: String,
    best_bid: String,
    best_bid_quantity: String,
    best_ask: String,
    best_ask_quantity: String,
}

#[derive(Deserialize)]
struct TradesEnvelope {
    events: Vec<TradesEvent>,
}

#[derive(Deserialize)]
struct TradesEvent {
    #[serde(default)]
    trades: Vec<TradeRaw>,
}

#[derive(Deserialize)]
struct TradeRaw {
    trade_id: String,
    product_id: String,
    price: String,
    size: String,
    side: String,
}

fn parse_frame(product_id: &str, text: &str) -> Result<Vec<RecordedEvent>> {
    let ch: ChannelOnly =
        serde_json::from_str(text).context("decoding channel")?;

    match ch.channel {
        "ticker" | "ticker_batch" => {
            let env: TickerEnvelope =
                serde_json::from_str(text).context("decoding ticker envelope")?;
            let mut out = Vec::new();
            for evt in env.events {
                for t in evt.tickers {
                    if t.product_id != product_id {
                        continue;
                    }
                    let payload = CoinbasePayload::Ticker {
                        best_bid: Decimal::from_str(&t.best_bid)?,
                        best_bid_qty: Decimal::from_str(&t.best_bid_quantity)?,
                        best_ask: Decimal::from_str(&t.best_ask)?,
                        best_ask_qty: Decimal::from_str(&t.best_ask_quantity)?,
                        last: Decimal::from_str(&t.price)?,
                    };
                    out.push(RecordedEvent::Coinbase(Arc::new(CoinbaseEvent {
                        clock: EventClock::now(None),
                        product_id: t.product_id,
                        payload,
                    })));
                }
            }
            Ok(out)
        }
        "market_trades" => {
            let env: TradesEnvelope = serde_json::from_str(text)
                .context("decoding market_trades envelope")?;
            let mut out = Vec::new();
            for evt in env.events {
                for t in evt.trades {
                    if t.product_id != product_id {
                        continue;
                    }
                    let side = if t.side.eq_ignore_ascii_case("BUY") {
                        TradeSide::Buy
                    } else {
                        TradeSide::Sell
                    };
                    let payload = CoinbasePayload::Trade {
                        price: Decimal::from_str(&t.price)?,
                        size: Decimal::from_str(&t.size)?,
                        side,
                        trade_id: t.trade_id,
                    };
                    out.push(RecordedEvent::Coinbase(Arc::new(CoinbaseEvent {
                        clock: EventClock::now(None),
                        product_id: t.product_id,
                        payload,
                    })));
                }
            }
            Ok(out)
        }
        "subscriptions" | "heartbeats" => Ok(vec![]),
        other => Ok(vec![RecordedEvent::FeedError {
            source: FeedSource::Coinbase,
            message: format!("unknown channel: {other}"),
            clock: EventClock::now(None),
        }]),
    }
}
