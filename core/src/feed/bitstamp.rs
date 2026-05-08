use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

use crate::event::{
    CexEvent, CexPayload, CexVenue, EventClock, FeedSource, RecordedEvent, TradeSide,
};

const WS_URL: &str = "wss://ws.bitstamp.net";

pub struct BitstampFeed {
    _task: JoinHandle<()>,
}

impl BitstampFeed {
    pub fn start(
        pair: impl Into<String>,
        tx: broadcast::Sender<RecordedEvent>,
    ) -> Self {
        let pair = pair.into();
        let task = tokio::spawn(run(pair, tx));
        Self { _task: task }
    }
}

async fn run(pair: String, tx: broadcast::Sender<RecordedEvent>) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(30);
    loop {
        let result = connect_and_pump(&pair, &tx).await;
        let message = match &result {
            Ok(()) => "ws closed; reconnecting".to_string(),
            Err(e) => format!("ws error: {e:#}"),
        };
        let _ = tx.send(RecordedEvent::FeedError {
            source: FeedSource::Bitstamp,
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
    pair: &str,
    tx: &broadcast::Sender<RecordedEvent>,
) -> Result<()> {
    let (ws_stream, _resp) = connect_async(WS_URL)
        .await
        .with_context(|| format!("connecting to {WS_URL}"))?;
    let (mut sink, mut stream) = ws_stream.split();

    let channel = format!("live_trades_{pair}");
    let sub = format!(
        r#"{{"event":"bts:subscribe","data":{{"channel":"{channel}"}}}}"#
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
            Message::Text(text) => match parse_frame(pair, text.as_str()) {
                Ok(events) => {
                    for evt in events {
                        if tx.send(evt).is_err() {
                            return Ok(());
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.send(RecordedEvent::FeedError {
                        source: FeedSource::Bitstamp,
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
struct EventEnvelope<'a> {
    event: &'a str,
}

#[derive(Deserialize)]
struct TradeEnvelope {
    data: TradeRaw,
}

#[derive(Deserialize)]
struct TradeRaw {
    id: u64,
    price_str: String,
    amount_str: String,
    #[serde(rename = "type")]
    kind: u8,
}

fn parse_frame(pair: &str, text: &str) -> Result<Vec<RecordedEvent>> {
    let env: EventEnvelope =
        serde_json::from_str(text).context("decoding event envelope")?;

    match env.event {
        "trade" => {
            let parsed: TradeEnvelope =
                serde_json::from_str(text).context("decoding trade envelope")?;
            let side = if parsed.data.kind == 0 {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            };
            let payload = CexPayload::Trade {
                price: parsed.data.price_str.parse()?,
                size: parsed.data.amount_str.parse()?,
                side,
                trade_id: Some(parsed.data.id.to_string()),
            };
            Ok(vec![RecordedEvent::Cex(Arc::new(CexEvent {
                clock: EventClock::now(None),
                venue: CexVenue::Bitstamp,
                product_id: pair.to_string(),
                payload,
            }))])
        }
        "bts:subscription_succeeded" | "bts:heartbeat" | "bts:request_reconnect" => {
            Ok(vec![])
        }
        other => Ok(vec![RecordedEvent::FeedError {
            source: FeedSource::Bitstamp,
            message: format!("unknown event: {other}"),
            clock: EventClock::now(None),
        }]),
    }
}
