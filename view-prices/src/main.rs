use anyhow::{Result, anyhow};
use polymarket_core::{Decimal, Polymarket};

#[tokio::main]
async fn main() -> Result<()> {
    let arg = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow!("usage: view-prices <condition_id|slug>"))?;

    let pm = Polymarket::new()?;
    let condition_id = pm.resolve_condition_id(&arg).await?;
    let snapshot = pm.fetch_snapshot(&condition_id).await?;

    println!("Market: {}", snapshot.question);
    if !snapshot.market_slug.is_empty() {
        println!("Slug:   {}", snapshot.market_slug);
    }
    println!();

    if snapshot.outcomes.is_empty() {
        println!("(no outcome tokens)");
        return Ok(());
    }

    println!(
        "{:<12}  {:>10}  {:>10}  {:>10}  {:>10}",
        "Outcome", "Bid", "Ask", "Mid", "Last"
    );
    println!("{}", "-".repeat(60));

    let fmt = |o: Option<Decimal>| o.map(|d| d.to_string()).unwrap_or_else(|| "—".into());

    for o in &snapshot.outcomes {
        println!(
            "{:<12}  {:>10}  {:>10}  {:>10}  {:>10}",
            o.outcome,
            fmt(o.bid),
            fmt(o.ask),
            fmt(o.mid),
            fmt(o.last)
        );
    }

    Ok(())
}
