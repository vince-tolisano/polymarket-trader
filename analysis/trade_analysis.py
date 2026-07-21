"""Helpers for analyzing live trade logs.

The trade CSVs in live-data/ were produced by bot runs with different trade
criteria (notional target changed from $5 to $2 on 07-14, ask levels and swing
thresholds also drifted between runs). Comparing raw PnL across runs is
misleading, so everything here tags rows with their source run / criteria
group and offers a normalized `pnl_per_dollar` (realized_pnl / notional_target)
for apples-to-apples comparison.

Usage from a notebook:

    from trade_analysis import *
    df = load_trades()
    summarize_criteria(df)
    summarize_runs(df)
    sweep = sweep_entry_threshold(df, by="criteria_group")
    plot_threshold_sweep(df, by="criteria_group")
    plot_equity_curves(df)
    win_rate_by_ask(df)
    print_feature_scan(df)
"""

from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

HERE = Path(__file__).parent


def load_trades(path=HERE / "live-data", header=HERE / "header.csv", drop_errors=True):
    """Load all trade CSVs, tagging each row with its run and criteria group.

    Adds columns:
      source_file    - CSV the row came from
      run_start      - run start datetime parsed from the filename
      window_time    - window_start_ts as datetime
      criteria_group - label for the trade-criteria regime (currently keyed on
                       notional_target, the clearest config break between runs)
      pnl_per_dollar - realized_pnl normalized by notional_target
    """
    cols = pd.read_csv(header).columns.tolist()
    frames = []
    for f in sorted(Path(path).glob("trade*.csv")):
        d = pd.read_csv(f, skiprows=1, header=None, names=cols)
        d["source_file"] = f.name
        d["run_start"] = datetime.strptime(f.stem.removeprefix("trade-"), "%m-%d-%Y-%H.%M")
        frames.append(d)
    df = pd.concat(frames, ignore_index=True)

    n_errors = df.post_error.notna().sum()
    if n_errors:
        print(f"# errors: {n_errors}" + (" (dropped)" if drop_errors else " (kept)"))
    if drop_errors:
        df = df[df.post_error.isna()].copy()

    df["window_time"] = pd.to_datetime(df.window_start_ts, unit="s")
    df["criteria_group"] = "nt$" + df.notional_target.astype(str)
    df["pnl_per_dollar"] = df.realized_pnl / df.notional_target

    # Candidate entry features (all known at entry time, no lookahead)
    df["dist"] = abs(df.price_diff_from_entry)
    df["time_left"] = 300 - df.entry_offset_s
    df["dist_per_sqrt_t"] = df.dist / np.sqrt(df.time_left.clip(lower=1))
    df["abs_swing"] = abs(df.swing_at_entry)
    return df.sort_values("window_start_ts").reset_index(drop=True)


def load_dry(path=HERE / "dry-data", drop_unresolved=True):
    """Load dry-trader CSVs (live-itm schema: per-share pnl, no sizing).

    Columns are renamed/derived to line up with load_trades() output so
    summarize_runs / feature_scan / sweep_entry_threshold work unchanged:
    entry_ask -> intended_ask, entry_offset_s -> entry_offset_s, and
    pnl_per_dollar = pnl / ask (a share risks its ask price). These files
    carry their own header row, so no header.csv is involved.
    """
    frames = []
    for f in sorted(Path(path).glob("trade*.csv")):
        d = pd.read_csv(f)
        d["source_file"] = f.name
        d["run_start"] = datetime.strptime(f.stem.removeprefix("trade-"), "%m-%d-%Y-%H.%M")
        frames.append(d)
    if not frames:
        raise FileNotFoundError(f"no trade*.csv files in {path}")
    df = pd.concat(frames, ignore_index=True)

    n_unresolved = df.won.isna().sum()
    if n_unresolved:
        print(f"# unresolved: {n_unresolved}" + (" (dropped)" if drop_unresolved else " (kept)"))
    if drop_unresolved:
        df = df[df.won.notna()].copy()

    df = df.rename(columns={"entry_ask": "intended_ask", "pnl": "realized_pnl"})
    df["window_time"] = pd.to_datetime(df.window_start_ts, unit="s")
    df["criteria_group"] = "dry"
    df["pnl_per_dollar"] = df.realized_pnl / df.intended_ask

    df["dist"] = abs(df.price_diff_from_entry)
    df["time_left"] = 300 - df.entry_offset_s
    df["dist_per_sqrt_t"] = df.dist / np.sqrt(df.time_left.clip(lower=1))
    df["abs_swing"] = abs(df.swing_at_entry)
    return df.sort_values("window_start_ts").reset_index(drop=True)


def summarize_runs(df, by="source_file"):
    """Per-run (or per-group) stats. Win rate and both raw and normalized PnL."""
    def stats(g):
        resolved = g[g.won.notna()]
        return pd.Series({
            "trades": len(g),
            "wins": (resolved.won == 1).sum(),
            "losses": (resolved.won == 0).sum(),
            "win_rate": (resolved.won == 1).mean(),
            "avg_ask": g.intended_ask.mean(),
            "notional": g.notional_target.iloc[0] if g.notional_target.nunique() == 1 else np.nan,
            "pnl": g.realized_pnl.sum(),
            "pnl_per_$": g.pnl_per_dollar.sum(),
        })
    return df.groupby(by, sort=True).apply(stats, include_groups=False)


def summarize_criteria(df):
    """Compare the trade-criteria regimes head to head."""
    return summarize_runs(df, by="criteria_group")


def breakeven_win_rate(df):
    """Actual vs required win rate per criteria group.

    Buying at ask a: win nets (1-a) per share, loss costs a per share,
    so the breakeven win rate is simply a.
    """
    def stats(g):
        resolved = g[g.won.notna()]
        return pd.Series({
            "win_rate": (resolved.won == 1).mean(),
            "breakeven": resolved.intended_ask.mean(),
            "edge": (resolved.won == 1).mean() - resolved.intended_ask.mean(),
        })
    return df.groupby("criteria_group").apply(stats, include_groups=False)


def sweep_entry_threshold(df, by=None, max_thresh=100, normalized=True):
    """PnL if you'd only taken trades with abs(price_diff_from_entry) >= t.

    price_diff_from_entry = btc_at_entry - target_pyth, known at entry time,
    so this is a valid entry filter (no lookahead).

    Returns a DataFrame indexed by threshold with one PnL column per group
    (or a single 'pnl' column when by=None). Use .idxmax() for the optimum.
    """
    col = "pnl_per_dollar" if normalized else "realized_pnl"
    thresholds = np.arange(0, max_thresh)
    groups = [("pnl", df)] if by is None else list(df.groupby(by))
    out = {}
    for name, g in groups:
        dist = abs(g.price_diff_from_entry)
        out[name] = [g[dist >= t][col].sum() for t in thresholds]
    return pd.DataFrame(out, index=pd.Index(thresholds, name="threshold"))


def plot_threshold_sweep(df, by=None, max_thresh=100, normalized=True):
    sweep = sweep_entry_threshold(df, by=by, max_thresh=max_thresh, normalized=normalized)
    fig, ax = plt.subplots()
    for col in sweep.columns:
        best = sweep[col].idxmax()
        ax.plot(sweep.index, sweep[col], label=f"{col} (best: {best})")
        ax.axvline(x=best, ls="--", alpha=0.4)
    ax.axhline(y=0, color="r")
    ax.set_xlabel("Min $ price distance from target at entry")
    ax.set_ylabel("PnL per $ risked" if normalized else "PnL")
    ax.set_title("Entry threshold sweep")
    ax.legend()
    return sweep


def plot_equity_curves(df, normalized=True):
    """Cumulative PnL over time, one line per criteria group."""
    col = "pnl_per_dollar" if normalized else "realized_pnl"
    fig, ax = plt.subplots()
    for name, g in df.groupby("criteria_group"):
        g = g.sort_values("window_start_ts")
        ax.plot(g.window_time, g[col].cumsum(), label=name)
    ax.axhline(y=0, color="r", ls="--", alpha=0.5)
    ax.set_ylabel("Cumulative PnL per $ risked" if normalized else "Cumulative PnL")
    ax.set_title("Equity curve by criteria group")
    ax.legend()
    fig.autofmt_xdate()


def plot_losses(df):
    """Where the losses live: hour of day, and entry offset vs price distance."""
    losses = df[df.won == 0]
    wins = df[df.won == 1]
    fig, ax = plt.subplots(1, 2, figsize=(12, 4))

    ax[0].hist(losses.period_of_day * 5 / 60, bins=24, range=(0, 24), edgecolor="black")
    ax[0].set_title("Losses by hour of day")
    ax[0].set_xlabel("Hour (UTC)")
    ax[0].set_ylabel("# Losses")

    ax[1].scatter(wins.entry_offset_s, abs(wins.price_diff_from_entry),
                  color="r", alpha=0.5, label="wins")
    ax[1].scatter(losses.entry_offset_s, abs(losses.price_diff_from_entry),
                  alpha=0.7, label="losses")
    ax[1].set_title("Entry offset vs price distance")
    ax[1].set_xlabel("Entry offset (s)")
    ax[1].set_ylabel("abs $ distance from target")
    ax[1].legend()
    fig.tight_layout()


def feature_scan(df, features=("dist", "time_left", "dist_per_sqrt_t", "abs_swing"), q=4):
    """Bucket each candidate entry feature into quantiles and show the edge.

    Edge = win rate minus average ask paid. That's the number that matters:
    win rate alone is misleading because the ask already prices in safety
    (e.g. win rate rises with dist, but so does the ask, so edge stays flat).
    Buckets with consistently positive edge are candidate trade criteria.
    """
    resolved = df[df.won.notna()]
    out = {}
    for feat in features:
        buckets = pd.qcut(resolved[feat], q, duplicates="drop")
        g = resolved.groupby(buckets, observed=True).agg(
            trades=("won", "size"),
            win_rate=("won", "mean"),
            avg_ask=("intended_ask", "mean"),
            pnl_per_dollar=("pnl_per_dollar", "sum"),
        )
        g["edge"] = g.win_rate - g.avg_ask
        out[feat] = g
    return out


def print_feature_scan(df, **kwargs):
    for feat, table in feature_scan(df, **kwargs).items():
        print(f"--- {feat} ---")
        print(table.round(4).to_string(), "\n")


def win_rate_by_ask(df):
    """Win rate and PnL bucketed by intended ask, vs the breakeven line."""
    resolved = df[df.won.notna()]
    g = resolved.groupby("intended_ask").agg(
        trades=("won", "size"),
        win_rate=("won", "mean"),
        pnl_per_dollar=("pnl_per_dollar", "sum"),
    )
    g["breakeven"] = g.index
    g["edge"] = g.win_rate - g.breakeven
    return g


if __name__ == "__main__":
    df = load_trades()
    print(f"\nLoaded {len(df)} trades from {df.source_file.nunique()} runs\n")
    print("=== By criteria group ===")
    print(summarize_criteria(df).to_string(), "\n")
    print("=== By run ===")
    print(summarize_runs(df).to_string(), "\n")
    print("=== Breakeven check ===")
    print(breakeven_win_rate(df).to_string(), "\n")
    print("=== Win rate by ask ===")
    print(win_rate_by_ask(df).to_string(), "\n")
    print("=== Feature scan (edge = win rate - ask) ===")
    print_feature_scan(df)
