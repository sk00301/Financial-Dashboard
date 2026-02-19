"""
NSE Indices - Self-Calibrating Gap Checker
===========================================
Instead of a hardcoded holiday list, this script derives the true NSE
trading calendar directly from your data:

  1. Build a "master calendar" = union of ALL trading dates across every file
     (or use NIFTY_50.csv specifically if present — it's the most complete)
  2. Any weekday NOT in the master calendar = market holiday / closure
  3. For each index file, flag dates that ARE in the master calendar
     but MISSING from that specific file → genuine data gaps

This approach is 100% self-calibrating: no hardcoded holidays needed.
"""

import pandas as pd
from pathlib import Path
import logging
from datetime import date, timedelta

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_DIR          = Path("Data/NSE_Indices_Data")
GAP_THRESHOLD     = 4        # calendar days — gaps ≤ this are skipped entirely
MASTER_INDEX_FILE = "NIFTY_50"   # preferred master calendar source; falls back
                                  # to union of all files if not found

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger()


# ============================================================================
# HELPERS
# ============================================================================

def load_dates(csv_path: Path) -> pd.DatetimeIndex:
    """Load a CSV and return a sorted tz-naive DatetimeIndex."""
    df = pd.read_csv(csv_path, index_col=0, parse_dates=True)
    idx = df.index
    if hasattr(idx, "tz") and idx.tz is not None:
        idx = idx.tz_localize(None)
    return pd.DatetimeIndex(sorted(idx.dropna().unique()))


def build_master_calendar(csv_files: list[Path]) -> set[date]:
    """
    Build the reference trading calendar.
    Prefer NIFTY_50; fall back to union of all files.
    """
    preferred = DATA_DIR / f"{MASTER_INDEX_FILE}.csv"
    if preferred.exists():
        logger.info(f"Master calendar source: {preferred.name}")
        idx = load_dates(preferred)
    else:
        logger.info("NIFTY_50.csv not found — using union of all files as master calendar")
        all_dates: set[date] = set()
        for f in csv_files:
            try:
                all_dates.update(d.date() for d in load_dates(f))
            except Exception:
                pass
        return all_dates

    return {d.date() for d in idx}


def weekdays_in_range(start: date, end: date) -> list[date]:
    """All Mon–Fri between start (exclusive) and end (exclusive)."""
    days, current = [], start + timedelta(days=1)
    while current < end:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


# ============================================================================
# GAP DETECTION
# ============================================================================

def find_real_gaps(
    file_dates: set[date],
    master_calendar: set[date],
    stem: str,
) -> list[dict]:
    """
    For a given file, find every trading day that:
      - exists in the master calendar (i.e. market was open)
      - is within the file's own date range
      - is MISSING from the file
    Groups consecutive missing days into gap records.
    """
    if not file_dates:
        return []

    file_start = min(file_dates)
    file_end   = max(file_dates)

    # Expected trading days for this index (within its own date range)
    expected = sorted(
        d for d in master_calendar
        if file_start <= d <= file_end and d not in file_dates
    )

    if not expected:
        return []

    # Group consecutive missing days into gap records
    gaps: list[dict] = []
    group_start = expected[0]
    prev        = expected[0]

    for d in expected[1:]:
        if (d - prev).days > GAP_THRESHOLD:
            gaps.append({
                "file":          stem,
                "gap_start":     str(group_start),
                "gap_end":       str(prev),
                "missing_days":  (expected[expected.index(prev) - expected.index(group_start) + 1:] or [prev]),
                "missing_count": expected.index(prev) - expected.index(group_start) + 1,
            })
            group_start = d
        prev = d

    # Final group
    gaps.append({
        "file":          stem,
        "gap_start":     str(group_start),
        "gap_end":       str(prev),
        "missing_count": sum(1 for x in expected if group_start <= x <= prev),
        "missing_days":  [x for x in expected if group_start <= x <= prev],
    })

    return gaps


# ============================================================================
# RUNNER
# ============================================================================

def run_gap_check():
    if not DATA_DIR.exists():
        logger.error(f"Directory not found: {DATA_DIR.resolve()}")
        return

    csv_files = sorted(f for f in DATA_DIR.glob("*.csv") if not f.name.startswith("_"))

    if not csv_files:
        logger.warning(f"No CSV files found in {DATA_DIR.resolve()}")
        return

    logger.info("=" * 65)
    logger.info(f"NSE INDEX GAP CHECKER (self-calibrating)")
    logger.info(f"{len(csv_files)} files  |  Directory: {DATA_DIR.resolve()}")
    logger.info("=" * 65)

    # Step 1: Build master trading calendar
    master_calendar = build_master_calendar(csv_files)
    logger.info(f"Master calendar: {len(master_calendar)} unique trading days\n")

    all_gaps:    list[dict] = []
    clean_files: list[str]  = []
    error_files: list[str]  = []

    # Step 2: Check each file
    for csv_path in csv_files:
        try:
            file_dates = {d.date() for d in load_dates(csv_path)}
            gaps = find_real_gaps(file_dates, master_calendar, csv_path.stem)

            if gaps:
                total_missing = sum(g["missing_count"] for g in gaps)
                all_gaps.extend(gaps)
                logger.warning(
                    f"  ✗ {csv_path.stem:<45}  {len(gaps)} gap block(s),"
                    f" {total_missing} missing trading day(s)"
                )
            else:
                clean_files.append(csv_path.stem)
                logger.info(f"  ✓ {csv_path.stem:<45}  complete")

        except Exception as e:
            error_files.append(csv_path.name)
            logger.error(f"  ✗ {csv_path.name}: read error — {e}")

    # Step 3: Detail report
    logger.info("\n" + "=" * 65)
    if not all_gaps:
        logger.info("🎉 All files are complete relative to the master trading calendar!")
    else:
        logger.warning(f"GAPS REPORT  ({sum(g['missing_count'] for g in all_gaps)} missing trading days total)")
        logger.info("=" * 65)

        rows = []
        for g in all_gaps:
            for d in g["missing_days"]:
                rows.append({
                    "file":         g["file"],
                    "missing_date": str(d),
                })

        detail_df = pd.DataFrame(rows).sort_values(["file", "missing_date"])

        for file_name, grp in detail_df.groupby("file"):
            dates_list = grp["missing_date"].tolist()
            logger.warning(f"\n  {file_name}  ({len(dates_list)} missing days):")
            # Print in compact rows of 8
            for i in range(0, len(dates_list), 8):
                logger.warning("    " + "  ".join(dates_list[i:i+8]))

        out_path = DATA_DIR / "_missing_dates.csv"
        detail_df.to_csv(out_path, index=False)
        logger.info(f"\n  Full detail saved → {out_path}")

    # Step 4: Summary
    logger.info("\n" + "=" * 65)
    logger.info("SUMMARY")
    logger.info("=" * 65)
    logger.info(f"  Files checked          : {len(csv_files)}")
    logger.info(f"  Complete files         : {len(clean_files)}")
    logger.info(f"  Files with gaps        : {len(set(g['file'] for g in all_gaps))}")
    logger.info(f"  Total missing days     : {sum(g['missing_count'] for g in all_gaps)}")
    if error_files:
        logger.warning(f"  Read errors           : {error_files}")
    logger.info("=" * 65)


if __name__ == "__main__":
    run_gap_check()