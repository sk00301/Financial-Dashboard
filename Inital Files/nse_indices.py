#!/usr/bin/env python3
"""
NSE Index Historical Data Downloader  (v5 — Yahoo Finance direct API)
=====================================================================
Downloads historical OHLCV data for 46 NSE indices from inception to today.
Uses Yahoo Finance's direct CSV download + v8 chart API (no yfinance library).

Requirements:  pip install requests pandas
"""

import os
import time
import logging
import requests
import pandas as pd
from io import StringIO
from datetime import datetime, timezone

# ── Configuration ──────────────────────────────────────────────────
OUTPUT_DIR    = "nse_index_data"
FROM_EPOCH    = 631152000        # 1990-01-01 UTC  (Unix timestamp)
DELAY_SECONDS = 2                # polite delay between requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ── Yahoo Finance ticker mapping ───────────────────────────────────
# Two patterns on Yahoo:
#   ^SYMBOL     (caret-prefixed index symbols)
#   SYMBOL.NS   (NSE-listed composite symbols)
#
# Not all 46 indices have Yahoo tickers. Those without are marked None.

INDICES = {
    # ── Broad Market ──
    "NIFTY_50":                 "^NSEI",
    "NIFTY_NEXT_50":            "^NSMIDCP",
    "NIFTY_100":                "^CNX100",
    "NIFTY_200":                "^CNX200",
    "NIFTY_500":                "^CRSLDX",
    "NIFTY_MIDCAP_50":          "^NSEMDCP50",
    "NIFTY_MIDCAP_100":         "NIFTY_MIDCAP_100.NS",
    "NIFTY_MIDCAP_150":         "NIFTYMIDCAP150.NS",
    "NIFTY_SMALLCAP_50":        "NIFTYSMLCAP50.NS",
    "NIFTY_SMALLCAP_100":       "^CNXSC",
    "NIFTY_SMALLCAP_250":       "NIFTYSMLCAP250.NS",
    "NIFTY_MIDSMALLCAP_400":    "NIFTYMIDSML400.NS",
    "NIFTY_LARGEMIDCAP_250":    "NIFTY_LARGEMID250.NS",
    "NIFTY_TOTAL_MARKET":       "NIFTY_TOTAL_MKT.NS",
    "NIFTY_MICROCAP_250":       "NIFTY_MICROCAP250.NS",

    # ── Sectoral ──
    "NIFTY_BANK":               "^NSEBANK",
    "NIFTY_AUTO":               "^CNXAUTO",
    "NIFTY_FINANCIAL_SERVICES":  "NIFTY_FIN_SERVICE.NS",
    "NIFTY_FMCG":               "^CNXFMCG",
    "NIFTY_IT":                  "^CNXIT",
    "NIFTY_MEDIA":               "^CNXMEDIA",
    "NIFTY_METAL":               "^CNXMETAL",
    "NIFTY_PHARMA":              "^CNXPHARMA",
    "NIFTY_PSU_BANK":            "^CNXPSUBANK",
    "NIFTY_PRIVATE_BANK":        "NIFTYPVTBANK.NS",
    "NIFTY_REALTY":              "^CNXREALTY",
    "NIFTY_HEALTHCARE":          "NIFTY_HEALTHCARE.NS",
    "NIFTY_CONSUMER_DURABLES":   "NIFTY_CONSR_DURBL.NS",
    "NIFTY_OIL_GAS":             "NIFTY_OIL_AND_GAS.NS",

    # ── Thematic ──
    "NIFTY_COMMODITIES":         "^CNXCMDT",
    "NIFTY_INDIA_CONSUMPTION":   "^CNXCONSUM",
    "NIFTY_CPSE":                "CPSE.NS",
    "NIFTY_ENERGY":              "^CNXENERGY",
    "NIFTY_INFRASTRUCTURE":      "^CNXINFRA",
    "NIFTY_MNC":                 "^CNXMNC",
    "NIFTY_PSE":                 "^CNXPSE",
    "NIFTY_SERVICES_SECTOR":     "^CNXSERVICE",
    "NIFTY_INDIA_DIGITAL":       "NIFTY_IND_DIGITAL.NS",
    "NIFTY_INDIA_MANUFACTURING": "NIFTY_INDIA_MFG.NS",

    # ── Newer / may not exist on Yahoo ──
    "NIFTY_INDIA_DEFENCE":              "NIFTY_IND_DEFENCE.NS",
    "NIFTY_TRANSPORTATION_LOGISTICS":   "NIFTY_TRAN_LOGISTICS.NS",
    "NIFTY_HOUSING":                    None,
    "NIFTY_MOBILITY":                   None,
    "NIFTY_EV_NEW_AGE_AUTOMOTIVE":      None,
    "NIFTY100_ESG":                     "NIFTY100_ESG.NS",
    "NIFTY_CORE_HOUSING":               None,
}

# Alternate tickers to try if primary fails
ALTERNATE_TICKERS = {
    "NIFTY_500":         ["^CNX500", "CNX500.NS"],
    "NIFTY_NEXT_50":     ["NIFTYJR.NS"],
    "NIFTY_100":         ["CNX100.NS"],
    "NIFTY_200":         ["CNX200.NS"],
    "NIFTY_CPSE":        ["NIFTY_CPSE.NS"],
    "NIFTY_PRIVATE_BANK": ["^CNXPVTBANK"],
    "NIFTY_INDIA_DEFENCE": ["NIFTY_INDIA_DEFENCE.NS"],
}

# ── Shared session ─────────────────────────────────────────────────
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

_session = None
_crumb   = None


def get_session():
    """Create or return a Yahoo Finance session with crumb."""
    global _session, _crumb
    if _session is not None:
        return _session, _crumb

    _session = requests.Session()
    _session.headers.update({"User-Agent": UA})

    try:
        # Step 1: Get cookies
        _session.get("https://finance.yahoo.com", timeout=15)
        # Step 2: Get crumb
        crumb_resp = _session.get(
            "https://query2.finance.yahoo.com/v1/test/getcrumb",
            timeout=15
        )
        if crumb_resp.status_code == 200:
            _crumb = crumb_resp.text.strip()
            log.info(f"  Yahoo session established (crumb obtained)")
        else:
            log.warning(f"  Could not get crumb (HTTP {crumb_resp.status_code}), will try without")
            _crumb = None
    except Exception as e:
        log.warning(f"  Session init error: {e}, will try without crumb")
        _crumb = None

    return _session, _crumb


def get_to_epoch():
    """Current UTC timestamp."""
    return int(datetime.now(timezone.utc).timestamp())


def download_csv(ticker: str) -> pd.DataFrame | None:
    """Download via Yahoo Finance v7 CSV endpoint."""
    session, crumb = get_session()

    url = f"https://query1.finance.yahoo.com/v7/finance/download/{ticker}"
    params = {
        "period1": FROM_EPOCH,
        "period2": get_to_epoch(),
        "interval": "1d",
        "events":   "history",
        "includeAdjustedClose": "true",
    }
    if crumb:
        params["crumb"] = crumb

    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code == 200 and "Date" in resp.text[:200]:
            df = pd.read_csv(StringIO(resp.text))
            if len(df) > 0:
                return df
        else:
            log.debug(f"    CSV HTTP {resp.status_code}")
    except Exception as e:
        log.debug(f"    CSV error: {e}")
    return None


def download_v8(ticker: str) -> pd.DataFrame | None:
    """Download via Yahoo Finance v8 chart API (JSON)."""
    session, crumb = get_session()

    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {
        "period1": FROM_EPOCH,
        "period2": get_to_epoch(),
        "interval": "1d",
    }
    if crumb:
        params["crumb"] = crumb

    try:
        resp = session.get(url, params=params, timeout=30)
        if resp.status_code != 200:
            return None

        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None

        timestamps = result[0].get("timestamp", [])
        quote = result[0].get("indicators", {}).get("quote", [{}])[0]
        if not timestamps:
            return None

        df = pd.DataFrame({
            "Date":   pd.to_datetime(timestamps, unit="s").strftime("%Y-%m-%d"),
            "Open":   quote.get("open", []),
            "High":   quote.get("high", []),
            "Low":    quote.get("low", []),
            "Close":  quote.get("close", []),
            "Volume": quote.get("volume", []),
        })
        df = df.dropna(subset=["Open", "High", "Low", "Close"], how="all")
        if len(df) > 0:
            return df
    except Exception as e:
        log.debug(f"    v8 error: {e}")
    return None


def try_download(index_name: str, primary_ticker: str) -> pd.DataFrame | None:
    """Try multiple methods and alternate tickers."""
    tickers = [primary_ticker]
    if index_name in ALTERNATE_TICKERS:
        tickers.extend(ALTERNATE_TICKERS[index_name])

    for ticker in tickers:
        if ticker is None:
            continue

        # Method 1: CSV download
        log.info(f"    → {ticker} (CSV)...")
        df = download_csv(ticker)
        if df is not None and len(df) > 5:
            log.info(f"      ✓ {len(df)} rows")
            return df
        time.sleep(0.5)

        # Method 2: v8 chart API
        log.info(f"    → {ticker} (v8 API)...")
        df = download_v8(ticker)
        if df is not None and len(df) > 5:
            log.info(f"      ✓ {len(df)} rows")
            return df
        time.sleep(0.5)

    return None


def save_csv(df: pd.DataFrame, index_name: str):
    """Save DataFrame to CSV with standardized columns."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filepath = os.path.join(OUTPUT_DIR, f"{index_name}.csv")

    # Standardize column names
    col_map = {}
    for c in df.columns:
        cl = c.lower().strip()
        if cl == "date":        col_map[c] = "date"
        elif cl == "open":      col_map[c] = "open"
        elif cl == "high":      col_map[c] = "high"
        elif cl == "low":       col_map[c] = "low"
        elif cl == "close":     col_map[c] = "close"
        elif cl == "adj close": col_map[c] = "close"   # prefer adj close
        elif cl == "volume":    col_map[c] = "volume"

    df = df.rename(columns=col_map)
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep]
    df = df.sort_values("date").reset_index(drop=True)

    # Clean nulls
    df = df.replace("null", pd.NA)
    df = df.dropna(subset=["open", "high", "low", "close"], how="all")

    df.to_csv(filepath, index=False)
    return filepath, len(df)


def main():
    total = len(INDICES)
    log.info("=" * 70)
    log.info("NSE Index Historical Data Downloader  (v5 — Yahoo Finance)")
    log.info(f"Downloading {total} indices  →  {OUTPUT_DIR}/")
    log.info(f"Date range: 1990-01-01  to  {datetime.now().strftime('%Y-%m-%d')}")
    log.info("=" * 70)

    # Warm up session
    log.info("Initializing Yahoo Finance session...")
    get_session()

    success, failed, skipped = [], [], []

    for i, (index_name, ticker) in enumerate(INDICES.items(), 1):
        label = index_name.replace("_", " ")
        log.info(f"[{i:2d}/{total}]  {label}")

        if ticker is None:
            log.warning(f"    ✗ No Yahoo Finance ticker (very new index)")
            skipped.append(index_name)
            continue

        df = try_download(index_name, ticker)

        if df is not None and len(df) > 5:
            filepath, nrows = save_csv(df, index_name)
            log.info(f"    ✓ Saved {nrows} rows → {filepath}")
            success.append(index_name)
        else:
            log.warning(f"    ✗ FAILED for '{label}'")
            failed.append(index_name)

        if i < total:
            time.sleep(DELAY_SECONDS)

    # ── Summary ──
    log.info("=" * 70)
    log.info("SUMMARY")
    log.info(f"  ✓ Downloaded:  {len(success)}")
    log.info(f"  ✗ Failed:      {len(failed)}")
    log.info(f"  ⊘ Skipped:     {len(skipped)} (no ticker)")
    if failed:
        log.info(f"  Failed: {', '.join(f.replace('_',' ') for f in failed)}")
    if skipped:
        log.info(f"  Skipped: {', '.join(s.replace('_',' ') for s in skipped)}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()