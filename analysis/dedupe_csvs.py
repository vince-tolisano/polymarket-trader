"""Strip duplicate rows from trade CSVs, in place, keeping a backup.

Traders built before 2026-07-27 re-dispatched a window resolver on every failed
rollover retry, so a stuck rollover wrote the same trade hundreds of times (one
live window landed 549 times). The rows are not byte-identical — each redundant
resolver fetched Pyth at a slightly different retry offset, so `final_pyth`
wobbles in the last decimals — but side / ask / won / pnl are identical across
every copy, so keeping the first occurrence loses nothing real.

Key is (window_start_ts, side), not window_start_ts alone: the writers emit one
row per triggered side, so a window may legitimately hold a YES row and a NO
row. On the data as of 2026-07-27 both keys give the same result, but only this
one stays correct if both sides ever trigger in the same window.

Operates on bytes and rewrites only the retained lines verbatim, so numeric
formatting and LF endings survive untouched. Originals are copied to a `raw/`
subdirectory before the first rewrite; `raw/` is never overwritten on re-runs,
so the true pre-clean file is always recoverable. `load_trades()`/`load_dry()`
glob non-recursively, so backups are invisible to the loaders.

Usage:
    python dedupe_csvs.py             # report only, changes nothing
    python dedupe_csvs.py --apply     # rewrite in place, backing up to raw/
"""

import shutil
import sys
from pathlib import Path

HERE = Path(__file__).parent
DATA_DIRS = [HERE / "live-data", HERE / "dry-data"]


def dedupe_file(path, apply=False):
    """Return (n_rows, n_kept). Rewrites `path` only when apply=True."""
    raw = path.read_bytes().split(b"\n")
    trailing_newline = raw and raw[-1] == b""
    if trailing_newline:
        raw = raw[:-1]
    if not raw:
        return 0, 0

    header, rows = raw[0], raw[1:]
    fields = header.decode().strip().split(",")
    ts_i, side_i = fields.index("window_start_ts"), fields.index("side")

    seen, kept = set(), []
    for row in rows:
        if not row.strip():
            continue
        parts = row.split(b",")
        # A malformed short row is kept rather than silently dropped — this
        # script's job is removing known duplicates, not validating schema.
        key = (parts[ts_i], parts[side_i]) if len(parts) > side_i else (row,)
        if key not in seen:
            seen.add(key)
            kept.append(row)

    if apply and len(kept) != len(rows):
        backup_dir = path.parent / "raw"
        backup_dir.mkdir(exist_ok=True)
        backup = backup_dir / path.name
        if not backup.exists():  # never clobber the true original
            shutil.copy2(path, backup)
        out = b"\n".join([header, *kept])
        if trailing_newline:
            out += b"\n"
        path.write_bytes(out)

    return len(rows), len(kept)


def main(apply=False):
    total_rows = total_kept = 0
    for d in DATA_DIRS:
        if not d.exists():
            continue
        print(f"=== {d.name} ===")
        for f in sorted(d.glob("trade*.csv")):
            n, k = dedupe_file(f, apply=apply)
            total_rows += n
            total_kept += k
            note = "" if n == k else f"  <-- dropped {n - k}"
            print(f"  {f.name:32s} {n:5d} -> {k:5d}{note}")
    dropped = total_rows - total_kept
    verb = "dropped" if apply else "would drop"
    print(f"\ntotal {total_rows} -> {total_kept} ({verb} {dropped})")
    if not apply and dropped:
        print("dry run — re-run with --apply to rewrite (originals go to raw/)")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
