use std::sync::Arc;
use std::time::{Instant, SystemTime};

use crate::{BookUpdate, Decimal, LastTradePrice, PriceChange};

#[derive(Debug, Clone, Copy)]
pub struct EventClock {
    pub recv_mono: Instant,
    pub recv_wall: SystemTime,
    pub venue_ts_ms: Option<u64>,
}

impl EventClock {
    pub fn now(venue_ts_ms: Option<u64>) -> Self {
        Self {
            recv_mono: Instant::now(),
            recv_wall: SystemTime::now(),
            venue_ts_ms,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedSource {
    Polymarket,
    Coinbase,
}

#[derive(Debug, Clone)]
pub enum RecordedEvent {
    Polymarket(Arc<PolymarketEvent>),
    Coinbase(Arc<CoinbaseEvent>),
    FeedError {
        source: FeedSource,
        message: String,
        clock: EventClock,
    },
}

#[derive(Debug)]
pub struct PolymarketEvent {
    pub clock: EventClock,
    pub payload: PolymarketPayload,
}

#[derive(Debug)]
pub enum PolymarketPayload {
    Book(BookUpdate),
    PriceChange(PriceChange),
    LastTrade(LastTradePrice),
}

#[derive(Debug)]
pub struct CoinbaseEvent {
    pub clock: EventClock,
    pub product_id: String,
    pub payload: CoinbasePayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug)]
pub enum CoinbasePayload {
    Ticker {
        best_bid: Decimal,
        best_bid_qty: Decimal,
        best_ask: Decimal,
        best_ask_qty: Decimal,
        last: Decimal,
    },
    Trade {
        price: Decimal,
        size: Decimal,
        side: TradeSide,
        trade_id: String,
    },
}
