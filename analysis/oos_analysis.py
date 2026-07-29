"""Out-of-sample significance testing for trade logs.

`trade_analysis.py` answers "what happened?". This module answers "could it
have happened by luck?", which is the only question that matters at a 0.4%
loss rate. Two independent checks, because they fail differently:

  zero_edge_test  - binomial on the loss COUNT. Ignores payout sizes, so it
                    can't be fooled by one unusually large win/loss.
  bootstrap_edge  - resamples actual per-dollar PnL. Accounts for payouts,
                    so it catches a result carried by a few outliers.

If they disagree, the result is driven by trade sizing rather than by hit
rate, and neither number should be trusted on its own.

Findings from data collected before 2026-07-21 are in-sample hypotheses; the
DEFAULT_CUTOFF below is the boundary for validating them. See CLAUDE.md.

Usage:
    python oos_analysis.py              # live-data, full report
    python oos_analysis.py --dry        # dry-data (same report, no exec stats)
    python oos_analysis.py --all        # skip the out-of-sample filter

    from oos_analysis import *
    df = load_oos()
    print(zero_edge_test(df).to_string())
"""

import sys
from math import comb
from pathlib import Path

import numpy as np
import pandas as pd

from trade_analysis import load_dry, load_trades

HERE = Path(__file__).parent

# Start of the out-of-sample collection phase (wide dry-trader + $2 live).
DEFAULT_CUTOFF = pd.Timestamp("2026-07-21")

# Resampling draws for the bootstrap. 20k is plenty for a 95% interval and
# still runs in under a second on ~1k trades.
N_BOOTSTRAP = 20_000
SEED = 0


def load_oos(dry=False, cutoff=DEFAULT_CUTOFF, drop_unresolved=True, path=None,
             dedupe=True, pattern=None):
    """Load trades, keeping only runs that started at/after `cutoff`.

    Filters on run_start (from the filename), not window_time, so a run is
    either wholly in or wholly out — mixing criteria regimes inside one
    sample is exactly what the cutoff exists to prevent.

    Rows with won == NaN are trades whose on-chain resolution never landed.
    They are dropped by default; counting them as losses would be wrong and
    counting them as wins would be worse.

    `path` overrides the data directory (defaults to analysis/live-data or
    analysis/dry-data).

    Deduplicates on (window_start_ts, side) — the writers emit one row per
    triggered side, so a window may legitimately hold both a YES and a NO row;
    keying on the timestamp alone would drop one. A repeat of the SAME side in
    the same window is always a logging fault (see dedupe_csvs.py), and since a
    stuck row is overwhelmingly likely to be a win, leaving them in inflates the
    win rate. Kept here as a safety net for freshly-pulled EC2 files: the
    on-disk CSVs were cleaned on 2026-07-27, so this is normally a no-op.
    Pass dedupe=False to inspect the raw rows.

    `pattern` (dry only) selects a filename glob within the directory, for dry
    runs that share dry-data but write a prefixed name — the inverse
    experiment is `pattern="inverse-trade*.csv"`. Default keeps load_dry's
    baseline `trade*.csv`.
    """
    if dry:
        kw = {"pattern": pattern} if pattern else {}
        df = load_dry(path, **kw) if path else load_dry(**kw)
    elif path:
        df = load_trades(path)
    else:
        df = load_trades()

    if dedupe:
        key = ["window_start_ts", "side"]
        n = df.duplicated(subset=key).sum()
        if n:
            print(f"# duplicate window rows: {n} (dropped, kept first)")
        df = df.drop_duplicates(subset=key, keep="first")
    if cutoff is not None:
        df = df[df.run_start >= cutoff]
    if drop_unresolved:
        n = df.won.isna().sum()
        if n:
            print(f"# unresolved: {n} (dropped)")
        df = df[df.won.notna()]
    return df.copy()


def binom_cdf(k, n, p):
    """P(X <= k) for X ~ Binomial(n, p). Stands in for scipy.stats.binom.cdf.

    Built on the term recurrence rather than summing comb() * p**i * q**(n-i)
    directly: comb(700, 350) is a 200-digit integer, and multiplying it by a
    denormal float is both slow and lossy.
    """
    if p <= 0:
        return 1.0
    if p >= 1:
        return 1.0 if k >= n else 0.0
    k = min(int(k), n)
    term = (1 - p) ** n  # P(X = 0)
    total = term
    for i in range(1, k + 1):
        term *= (n - i + 1) / i * p / (1 - p)
        total += term
    return min(total, 1.0)


def zero_edge_test(df, by="intended_ask"):
    """Per ask level: are there fewer losses than a zero-edge strategy predicts?

    Buying at ask `a` pays (1-a) on a win and costs `a` on a loss, so a
    strategy with no edge loses exactly (1-a) of the time -- the ask IS the
    null hypothesis. p_luck is P(losses <= observed) under that null: small
    means the good result is hard to explain by chance.

    Grouping by ask (not pooling) matters because each level has its own
    null. Pooling a 0.96 and a 0.99 trade tests neither.
    """
    rows = {}
    for ask, g in df.groupby(by):
        n = len(g)
        losses = int((g.won == 0).sum())
        rows[ask] = {
            "trades": n,
            "losses": losses,
            "exp_losses": n * (1 - ask),
            "win_rate": g.won.mean(),
            "edge": g.won.mean() - ask,
            "p_luck": binom_cdf(losses, n, 1 - ask),
        }
    return pd.DataFrame(rows).T.rename_axis(by)


def bootstrap_edge(df, n_boot=N_BOOTSTRAP, seed=SEED, col="pnl_per_dollar"):
    """95% CI for mean per-dollar PnL, by resampling the trades themselves.

    Draws len(df) trades with replacement, n_boot times, and takes the 2.5th
    and 97.5th percentiles of the resulting means. Makes no normality
    assumption, which matters here: per-trade PnL is wildly bimodal (+0.01 or
    -0.99), so a textbook t-interval would be misleading.

    A lower bound above 0 is the claim "profitable"; anything straddling 0
    means the sample cannot distinguish this from a coin flip.
    """
    x = df[col].dropna().to_numpy()
    if len(x) == 0:
        return {"n": 0}
    means = np.random.default_rng(seed).choice(x, (n_boot, len(x))).mean(axis=1)
    return {
        "n": len(x),
        "mean_per_trade": x.mean(),
        "ci_lo": np.percentile(means, 2.5),
        "ci_hi": np.percentile(means, 97.5),
        "total": x.sum(),
    }


def bootstrap_table(df, splits=None):
    """bootstrap_edge over several slices, as one comparable table.

    Default splits isolate the top of the ask band from the rest: that is
    where the strategy's whole thesis lives, and pooling them hides it.
    """
    if splits is None:
        splits = {
            "all": df,
            "ask >= 0.99": df[df.intended_ask >= 0.99],
            "ask < 0.99": df[df.intended_ask < 0.99],
        }
    return pd.DataFrame({k: bootstrap_edge(v) for k, v in splits.items()}).T


def losers(df, min_ask=0.99):
    """Every losing trade at/above `min_ask`, with its entry context.

    Not a summary — at these hit rates the losses are countable, and reading
    the individual rows tells you more than any aggregate. Distance at entry
    vs. how far past the target it finished shows whether losses are big
    adverse moves (filterable) or near-misses (not).
    """
    cols = [
        "window_time", "side", "intended_ask", "entry_offset_s",
        "btc_at_entry", "target_pyth", "final_pyth",
        "price_diff_from_entry", "resolved_side",
    ]
    if "realized_pnl" in df:
        cols.append("realized_pnl")
    out = df[(df.intended_ask >= min_ask) & (df.won == 0)]
    out = out[[c for c in cols if c in out]].copy()
    # How far past the target it actually settled: the margin of defeat.
    if "final_pyth" in out:
        out["missed_by"] = (out.final_pyth - out.target_pyth).abs().round(2)
    return out


def loss_leverage(df, min_ask=0.99):
    """How many extra losses would erase the profit at this ask level?

    At ask 0.99 a loss costs 99x what a win pays, so the record is decided by
    a handful of events. This converts the abstract CI into a count: if the
    answer is a small integer, the sample is not evidence yet.
    """
    g = df[df.intended_ask >= min_ask]
    if g.empty:
        return {}
    wins, losses = int((g.won == 1).sum()), int((g.won == 0).sum())
    avg_ask = g.intended_ask.mean()
    win_pay, loss_cost = 1 - avg_ask, avg_ask
    net = wins * win_pay - losses * loss_cost
    return {
        "trades": len(g),
        "wins": wins,
        "losses": losses,
        "exp_losses": round(len(g) * (1 - avg_ask), 2),
        "net_per_share": round(net, 4),
        # Each extra loss costs one payout AND forfeits the win it replaces.
        "losses_to_breakeven": round(net / (win_pay + loss_cost), 2),
    }


def execution_quality(cutoff=DEFAULT_CUTOFF):
    """Order rejections and partial fills. Live only — dry posts nothing.

    Reloads with drop_errors=False because load_trades() discards rejected
    orders by default, which is right for strategy stats and wrong here.
    """
    raw = load_trades(drop_errors=False)
    if cutoff is not None:
        raw = raw[raw.run_start >= cutoff]
    filled = raw[raw.post_error.isna()]
    partial = filled[filled.size_shares != filled.size_matched]
    return {
        "attempted": len(raw),
        "filled": len(filled),
        "rejected": int(raw.post_error.notna().sum()),
        "reject_rate": raw.post_error.notna().mean(),
        "partial_fills": len(partial),
        # Non-Pyth resolutions: these rows resolved on-chain only, so any
        # analysis keyed on final_pyth silently drops them.
        "no_final_pyth": int(filled.final_pyth.isna().sum()),
    }


def report(dry=False, cutoff=DEFAULT_CUTOFF):
    """The full out-of-sample readout, in the order the argument runs."""
    pd.set_option("display.width", 250)
    label = "dry" if dry else "live"
    df = load_oos(dry=dry, cutoff=cutoff)

    span = f"{df.window_time.min()} -> {df.window_time.max()}"
    scope = "ALL DATA" if cutoff is None else f"out-of-sample (runs >= {cutoff.date()})"
    print(f"\n=== {label} trades, {scope} ===")
    print(f"{len(df)} resolved trades from {df.source_file.nunique()} runs")
    print(f"window span: {span}\n")

    print("=== zero-edge test (p_luck < 0.05 = hard to explain by chance) ===")
    print(zero_edge_test(df).round(4).to_string(), "\n")

    print("=== bootstrap 95% CI on per-dollar PnL (CI must exclude 0) ===")
    print(bootstrap_table(df).round(5).to_string(), "\n")

    print("=== leverage of the losses at ask >= 0.99 ===")
    for k, v in loss_leverage(df).items():
        print(f"  {k:22s} {v}")
    print()

    print("=== every loss at ask >= 0.99 ===")
    L = losers(df)
    print(L.to_string(index=False) if len(L) else "  (none)", "\n")

    if not dry:
        print("=== execution quality ===")
        for k, v in execution_quality(cutoff).items():
            print(f"  {k:16s} {v:.4f}" if isinstance(v, float) else f"  {k:16s} {v}")
        print()


if __name__ == "__main__":
    report(
        dry="--dry" in sys.argv,
        cutoff=None if "--all" in sys.argv else DEFAULT_CUTOFF,
    )
