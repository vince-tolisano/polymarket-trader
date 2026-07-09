use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use serde::Deserialize;
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
    CexEvent, CexPayload, CexVenue, EventClock, FeedSource, PolymarketEvent,
    PolymarketPayload, RecordedEvent, TradeSide,
};
pub use feed::{BitstampFeed, CoinbaseFeed, KrakenFeed, PolymarketFeed};

const CLOB_HOST: &str = "https://clob.polymarket.com";
const BTC_UPDOWN_5M_WINDOW_SECS: u64 = 300;
const PYTH_HERMES_HOST: &str = "https://hermes.pyth.network";
// BTC/USD feed id (Pyth crypto aggregator). 32-byte hex, no 0x prefix.
const PYTH_BTC_USD_FEED_ID: &str =
    "e62df6c8b4a85fe1a67db44dc12de5db330f7ac66b72dc658afedf0f4a415b43";

pub struct Polymarket {
    clob: ClobClient,
    gamma: GammaClient,
    pm_proxy: reqwest::Client,
}

#[derive(Deserialize)]
struct PythUpdates {
    #[serde(default)]
    parsed: Vec<PythParsed>,
}

#[derive(Deserialize)]
struct PythParsed {
    price: PythPrice,
}

#[derive(Deserialize)]
struct PythPrice {
    price: String,
    expo: i32,
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
        let pm_proxy = reqwest::Client::builder()
            .user_agent("polymarket-trader/0.1")
            .build()
            .context("building polymarket frontend client")?;
        Ok(Self { clob, gamma, pm_proxy })
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

    /// Returns the BTC reference price for the currently-open 5-min up/down
    /// window, by querying Pyth Network's BTC/USD aggregate at the window's
    /// start timestamp. Pyth is highly correlated with Chainlink Data Streams
    /// (both aggregate CEX feeds) and is publicly accessible without auth.
    /// Returns Ok(None) only if Pyth has no update for that timestamp.
    pub async fn current_btc_updown_5m_target(&self) -> Result<Option<Decimal>> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_secs();
        let window_start = now - (now % BTC_UPDOWN_5M_WINDOW_SECS);
        self.pyth_btc_usd_at(window_start).await
    }

    /// Fetch Pyth Network's BTC/USD aggregate price at a specific unix
    /// timestamp (seconds). Hermes returns the update closest to the requested
    /// time. Returns Ok(None) if Hermes has no update for that timestamp yet
    /// (it 404s on boundary-aligned very-recent timestamps) or no parsed
    /// update was returned.
    pub async fn pyth_btc_usd_at(&self, ts: u64) -> Result<Option<Decimal>> {
        let url = format!(
            "{PYTH_HERMES_HOST}/v2/updates/price/{ts}?ids[]={PYTH_BTC_USD_FEED_ID}"
        );
        let resp = self
            .pm_proxy
            .get(&url)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let updates: PythUpdates = resp
            .error_for_status()
            .with_context(|| format!("status from {url}"))?
            .json()
            .await
            .context("decoding pyth updates")?;

        let Some(parsed) = updates.parsed.into_iter().next() else {
            return Ok(None);
        };
        let raw: i64 = parsed
            .price
            .price
            .parse()
            .with_context(|| format!("parsing pyth raw price {:?}", parsed.price.price))?;
        let expo = parsed.price.expo;
        // Pyth's expo is typically negative; the actual price = raw * 10^expo.
        let value = if expo <= 0 {
            Decimal::new(raw, (-expo) as u32)
        } else {
            Decimal::from(raw) * Decimal::from(10_i64.pow(expo as u32))
        };
        Ok(Some(value))
    }

    pub async fn current_btc_updown_5m_condition_id(&self) -> Result<String> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system time before unix epoch")?
            .as_secs();
        let window_start = now - (now % BTC_UPDOWN_5M_WINDOW_SECS);
        let slug = format!("btc-updown-5m-{window_start}");
        let req = MarketBySlugRequest::builder().slug(&slug).build();
        let market = self
            .gamma
            .market_by_slug(&req)
            .await
            .with_context(|| format!("looking up {slug}"))?;
        let cid = market
            .condition_id
            .ok_or_else(|| anyhow!("{slug} has no condition_id"))?;
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

    /// Look up the on-chain resolved winner via Polymarket's CLOB market
    /// endpoint. Returns `Ok(Some(token_id))` once `market.closed == true`
    /// and a token with `winner == true` is present; `Ok(None)` while the
    /// market is still open / awaiting oracle resolution.
    pub async fn market_winner(&self, condition_id: &str) -> Result<Option<U256>> {
        let market = self
            .clob
            .market(condition_id)
            .await
            .with_context(|| format!("fetching market {condition_id}"))?;
        if !market.closed {
            return Ok(None);
        }
        Ok(market.tokens.iter().find(|t| t.winner).map(|t| t.token_id))
    }
}
