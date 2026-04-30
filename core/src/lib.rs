use anyhow::{Context, Result, anyhow};
use polymarket_client_sdk_v2::clob::types::Side;
use polymarket_client_sdk_v2::clob::types::request::{
    LastTradePriceRequest, MidpointRequest, PriceRequest,
};
use polymarket_client_sdk_v2::clob::{Client as ClobClient, Config};
use polymarket_client_sdk_v2::gamma::Client as GammaClient;
use polymarket_client_sdk_v2::gamma::types::request::MarketBySlugRequest;

pub use alloy_primitives::U256;
pub use polymarket_client_sdk_v2::clob::ws::Client as WsClient;
pub use polymarket_client_sdk_v2::clob::ws::types::response::{
    BookUpdate, LastTradePrice, OrderBookLevel, PriceChange, PriceChangeBatchEntry,
};
pub use rust_decimal::Decimal;

pub mod event;
pub mod feed;

pub use event::{
    CoinbaseEvent, CoinbasePayload, EventClock, FeedSource, PolymarketEvent,
    PolymarketPayload, RecordedEvent, TradeSide,
};
pub use feed::{CoinbaseFeed, PolymarketFeed};

const CLOB_HOST: &str = "https://clob.polymarket.com";

pub struct Polymarket {
    clob: ClobClient,
    gamma: GammaClient,
}

#[derive(Debug, Clone)]
pub struct MarketSnapshot {
    pub question: String,
    pub market_slug: String,
    pub condition_id: String,
    pub outcomes: Vec<OutcomeSnapshot>,
}

#[derive(Debug, Clone)]
pub struct OutcomeSnapshot {
    pub outcome: String,
    pub token_id: U256,
    pub bid: Option<Decimal>,
    pub ask: Option<Decimal>,
    pub mid: Option<Decimal>,
    pub last: Option<Decimal>,
}

impl Polymarket {
    pub fn new() -> Result<Self> {
        let clob =
            ClobClient::new(CLOB_HOST, Config::default()).context("creating clob client")?;
        let gamma = GammaClient::default();
        Ok(Self { clob, gamma })
    }

    pub async fn resolve_condition_id(&self, arg: &str) -> Result<String> {
        if arg.starts_with("0x") || arg.starts_with("0X") {
            return Ok(arg.to_string());
        }
        let req = MarketBySlugRequest::builder().slug(arg).build();
        let market = self
            .gamma
            .market_by_slug(&req)
            .await
            .with_context(|| format!("looking up slug {arg}"))?;
        let cid = market
            .condition_id
            .ok_or_else(|| anyhow!("market {arg} has no condition_id"))?;
        Ok(cid.to_string())
    }

    pub async fn fetch_snapshot(&self, condition_id: &str) -> Result<MarketSnapshot> {
        let market = self
            .clob
            .market(condition_id)
            .await
            .with_context(|| format!("fetching market {condition_id}"))?;

        let mut outcomes = Vec::with_capacity(market.tokens.len());
        for token in &market.tokens {
            let token_id = token.token_id;
            let bid_req = PriceRequest::builder()
                .token_id(token_id)
                .side(Side::Buy)
                .build();
            let ask_req = PriceRequest::builder()
                .token_id(token_id)
                .side(Side::Sell)
                .build();
            let mid_req = MidpointRequest::builder().token_id(token_id).build();
            let last_req = LastTradePriceRequest::builder().token_id(token_id).build();

            let (bid, ask, mid, last) = tokio::join!(
                self.clob.price(&bid_req),
                self.clob.price(&ask_req),
                self.clob.midpoint(&mid_req),
                self.clob.last_trade_price(&last_req),
            );

            outcomes.push(OutcomeSnapshot {
                outcome: token.outcome.clone(),
                token_id,
                bid: bid.ok().map(|r| r.price),
                ask: ask.ok().map(|r| r.price),
                mid: mid.ok().map(|r| r.mid),
                last: last.ok().map(|r| r.price),
            });
        }

        Ok(MarketSnapshot {
            question: market.question,
            market_slug: market.market_slug,
            condition_id: condition_id.to_string(),
            outcomes,
        })
    }
}
