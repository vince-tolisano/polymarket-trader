use std::sync::Arc;

use anyhow::Result;
use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::event::{
    EventClock, FeedSource, PolymarketEvent, PolymarketPayload, RecordedEvent,
};
use crate::{U256, WsClient};

pub struct PolymarketFeed {
    _tasks: Vec<JoinHandle<()>>,
}

impl PolymarketFeed {
    pub fn start(
        token_ids: Vec<U256>,
        tx: broadcast::Sender<RecordedEvent>,
    ) -> Result<Self> {
        let ws = WsClient::default();
        let book_stream = ws.subscribe_orderbook(token_ids.clone())?;
        let price_stream = ws.subscribe_prices(token_ids.clone())?;
        let trade_stream = ws.subscribe_last_trade_price(token_ids)?;

        let mut tasks = Vec::with_capacity(3);

        let tx_book = tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = Box::pin(book_stream);
            while let Some(r) = s.next().await {
                let evt = match r {
                    Ok(b) => RecordedEvent::Polymarket(Arc::new(PolymarketEvent {
                        clock: EventClock::now(None),
                        payload: PolymarketPayload::Book(b),
                    })),
                    Err(e) => RecordedEvent::FeedError {
                        source: FeedSource::Polymarket,
                        message: format!("book: {e}"),
                        clock: EventClock::now(None),
                    },
                };
                if tx_book.send(evt).is_err() {
                    break;
                }
            }
        }));

        let tx_price = tx.clone();
        tasks.push(tokio::spawn(async move {
            let mut s = Box::pin(price_stream);
            while let Some(r) = s.next().await {
                let evt = match r {
                    Ok(p) => RecordedEvent::Polymarket(Arc::new(PolymarketEvent {
                        clock: EventClock::now(None),
                        payload: PolymarketPayload::PriceChange(p),
                    })),
                    Err(e) => RecordedEvent::FeedError {
                        source: FeedSource::Polymarket,
                        message: format!("price: {e}"),
                        clock: EventClock::now(None),
                    },
                };
                if tx_price.send(evt).is_err() {
                    break;
                }
            }
        }));

        let tx_trade = tx;
        tasks.push(tokio::spawn(async move {
            let mut s = Box::pin(trade_stream);
            while let Some(r) = s.next().await {
                let evt = match r {
                    Ok(t) => RecordedEvent::Polymarket(Arc::new(PolymarketEvent {
                        clock: EventClock::now(None),
                        payload: PolymarketPayload::LastTrade(t),
                    })),
                    Err(e) => RecordedEvent::FeedError {
                        source: FeedSource::Polymarket,
                        message: format!("trade: {e}"),
                        clock: EventClock::now(None),
                    },
                };
                if tx_trade.send(evt).is_err() {
                    break;
                }
            }
        }));

        Ok(Self { _tasks: tasks })
    }
}
