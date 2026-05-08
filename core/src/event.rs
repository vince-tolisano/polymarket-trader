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
    Kraken,
    Bitstamp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CexVenue {
    Coinbase,
    Kraken,
    Bitstamp,
}

impl CexVenue {
    pub fn as_str(self) -> &'static str {
        match self {
            CexVenue::Coinbase => "coinbase",
            CexVenue::Kraken => "kraken",
            CexVenue::Bitstamp => "bitstamp",
        }
    }
}

#[derive(Debug, Clone)]
pub enum RecordedEvent {
    Polymarket(Arc<PolymarketEvent>),
    Cex(Arc<CexEvent>),
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
pub struct CexEvent {
    pub clock: EventClock,
    pub venue: CexVenue,
    pub product_id: String,
    pub payload: CexPayload,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug)]
pub enum CexPayload {
    Ticker {
        best_bid: Option<Decimal>,
        best_bid_qty: Option<Decimal>,
        best_ask: Option<Decimal>,
        best_ask_qty: Option<Decimal>,
        last: Decimal,
    },
    Trade {
        price: Decimal,
        size: Decimal,
        side: TradeSide,
        trade_id: Option<String>,
    },
}
