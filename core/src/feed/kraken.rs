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
    CexEvent, CexPayload, CexVenue, EventClock, FeedSource, RecordedEvent,
};

const WS_URL: &str = "wss://ws.kraken.com/v2";

pub struct KrakenFeed {
    _task: JoinHandle<()>,
}

impl KrakenFeed {
    pub fn start(
        symbol: impl Into<String>,
        tx: broadcast::Sender<RecordedEvent>,
    ) -> Self {
        let symbol = symbol.into();
        let task = tokio::spawn(run(symbol, tx));
        Self { _task: task }
    }
}

async fn run(symbol: String, tx: broadcast::Sender<RecordedEvent>) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);
    loop {
        let result = connect_and_pump(&symbol, &tx).await;
        let message = match &result {
            Ok(()) => "ws closed; reconnecting".to_string(),
            Err(e) => format!("ws error: {e:#}"),
        };
        let _ = tx.send(RecordedEvent::FeedError {
            source: FeedSource::Kraken,
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
    symbol: &str,
    tx: &broadcast::Sender<RecordedEvent>,
) -> Result<()> {
    let (ws_stream, _resp) = connect_async(WS_URL)
        .await
        .with_context(|| format!("connecting to {WS_URL}"))?;
    let (mut sink, mut stream) = ws_stream.split();

    let sub = format!(
        r#"{{"method":"subscribe","params":{{"channel":"ticker","symbol":["{symbol}"]}}}}"#
    );
    sink.send(Message::Text(sub.into()))
        .await
        .context("sending subscribe")?;

    loop {
        let msg = match stream.next().await {
            Some(Ok(m)) => m,
            Some(Err(e)) => return Err(e).context("ws recv"),
            None => return Ok(()),
        };
        match msg {
            Message::Text(text) => match parse_frame(symbol, text.as_str()) {
                Ok(events) => {
                    for evt in events {
                        if tx.send(evt).is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(RecordedEvent::FeedError {
                        source: FeedSource::Kraken,
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
    channel: Option<&'a str>,
    #[serde(default)]
    method: Option<&'a str>,
}

#[derive(Deserialize)]
struct TickerEnvelope {
    #[serde(default)]
    data: Vec<TickerRaw>,
}

#[derive(Deserialize)]
struct TickerRaw {
    symbol: String,
    bid: Decimal,
    bid_qty: Decimal,
    ask: Decimal,
    ask_qty: Decimal,
    last: Decimal,
}

fn parse_frame(symbol: &str, text: &str) -> Result<Vec<RecordedEvent>> {
    let ch: ChannelOnly =
        serde_json::from_str(text).context("decoding channel envelope")?;

    // Acks / status / heartbeats: silently swallow.
    if ch.method.is_some() || ch.channel.is_none() {
        return Ok(vec![]);
    }
    match ch.channel.unwrap() {
        "ticker" => {
            let env: TickerEnvelope =
                serde_json::from_str(text).context("decoding ticker envelope")?;
            let mut out = Vec::with_capacity(env.data.len());
            for t in env.data {
                if t.symbol != symbol {
                    continue;
                }
                let payload = CexPayload::Ticker {
                    best_bid: Some(t.bid),
                    best_bid_qty: Some(t.bid_qty),
                    best_ask: Some(t.ask),
                    best_ask_qty: Some(t.ask_qty),
                    last: t.last,
                };
                out.push(RecordedEvent::Cex(Arc::new(CexEvent {
                    clock: EventClock::now(None),
                    venue: CexVenue::Kraken,
                    product_id: t.symbol,
                    payload,
                })));
            }
            Ok(out)
        }
        "heartbeat" | "status" | "pong" => Ok(vec![]),
        other => Ok(vec![RecordedEvent::FeedError {
            source: FeedSource::Kraken,
            message: format!("unknown channel: {other}"),
            clock: EventClock::now(None),
        }]),
    }
}
