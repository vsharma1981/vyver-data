#!/usr/bin/env python3
"""
Vyver Intelligence — Market Data Fetcher
Runs as a GitHub Action daily at 6am UTC.
Fetches 3yr OHLC for all assets from Yahoo Finance → saves as JSON in data/
"""

import json
import time
import random
import urllib.request
import urllib.error
import ssl
from datetime import datetime, timezone
from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SYMBOLS = [
    "GLD","SLV","USO","BNO","UNG","CORN","WEAT","SOYB","COPX","PDBC",
    "SPY","QQQ","DIA","IWM","VTI","EEM","TLT","HYG",
    "XLF","XLE","XLK","XLV","XLU","XLP","XLY","XLI","XLB","XLRE",
    "ARKK","ARKG","GDX","GDXJ","ICLN","BOTZ","CIBR",
    "VNQ","EFA","EWJ","EWZ","FXI","INDA","VGK","EWT",
    "SCHD","VYM","BND","AGG","LQD","TIP","IEF","SHY","MUB","BLOK","MOO",
    "AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","BRK-B",
    "JPM","V","JNJ","WMT","PG","MA","HD","BAC","XOM","CVX","KO","PFE",
    "MRK","ABBV","LLY","COST","AVGO","NFLX","AMD","INTC","QCOM","CRM",
    "NOW","ADBE","ORCL","GS","MS","AXP","BLK","UNH",
    "AMGN","GILD","BA","RTX","LMT","CAT","DE","HON","GE","DIS",
    "SBUX","NKE","MCD","F","GM","T","VZ","C","WFC","SCHW",
    "FXE","FXB","FXY","FXA","FXC","UUP","UDN",
    "IBIT","FBTC","ETHA","GBTC","BITO","MSTR","COIN","RIOT","MARA",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

ctx = ssl.create_default_context()

def fetch_yahoo(symbol: str) -> dict | None:
    for base in ["https://query1.finance.yahoo.com", "https://query2.finance.yahoo.com"]:
        for attempt in range(2):
            try:
                url = f"{base}/v8/finance/chart/{symbol}?range=3y&interval=1d&includeAdjustedClose=true"
                req = urllib.request.Request(url, headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "application/json, */*",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": f"https://finance.yahoo.com/quote/{symbol}/history/",
                    "Origin": "https://finance.yahoo.com",
                })
                with urllib.request.urlopen(req, context=ctx, timeout=15) as r:
                    data = json.loads(r.read())

                result = data.get("chart", {}).get("result", [{}])[0]
                if not result.get("timestamp"):
                    continue

                timestamps = result["timestamp"]
                quote = result.get("indicators", {}).get("quote", [{}])[0]
                adj_close = result.get("indicators", {}).get("adjclose", [{}])[0].get("adjclose", quote.get("close", []))

                rows = []
                for i, ts in enumerate(timestamps):
                    c = adj_close[i] if i < len(adj_close) and adj_close[i] else None
                    if not c or c <= 0:
                        continue
                    rows.append({
                        "date": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d"),
                        "o": quote.get("open",   [None])[i] or c,
                        "h": quote.get("high",   [None])[i] or c,
                        "l": quote.get("low",    [None])[i] or c,
                        "c": c,
                        "v": quote.get("volume", [None])[i] or 0,
                    })

                if len(rows) < 20:
                    continue

                return {
                    "symbol": symbol,
                    "fetchedAt": datetime.now(timezone.utc).isoformat(),
                    "count": len(rows),
                    "dates":  [r["date"] for r in rows],
                    "open":   [r["o"] for r in rows],
                    "high":   [r["h"] for r in rows],
                    "low":    [r["l"] for r in rows],
                    "close":  [r["c"] for r in rows],
                    "volume": [r["v"] for r in rows],
                }
            except urllib.error.HTTPError as e:
                if e.code == 429:
                    print(f"  Rate limited, waiting 3s...")
                    time.sleep(3)
                elif e.code == 404:
                    return None
                else:
                    time.sleep(1)
            except Exception as e:
                print(f"  Attempt {attempt+1} failed: {e}")
                time.sleep(1)
    return None


def merge_incremental(existing: dict, fresh: dict) -> dict:
    if not existing or not existing.get("dates"):
        return fresh
    last_date = existing["dates"][-1]
    new_rows = [(i, d) for i, d in enumerate(fresh["dates"]) if d > last_date]
    if not new_rows:
        return {**existing, "fetchedAt": fresh["fetchedAt"]}
    idx = new_rows[0][0]
    return {
        **existing,
        "fetchedAt": fresh["fetchedAt"],
        "count": existing["count"] + len(new_rows),
        "dates":  existing["dates"]  + fresh["dates"][idx:],
        "open":   existing["open"]   + fresh["open"][idx:],
        "high":   existing["high"]   + fresh["high"][idx:],
        "low":    existing["low"]    + fresh["low"][idx:],
        "close":  existing["close"]  + fresh["close"][idx:],
        "volume": existing["volume"] + fresh["volume"][idx:],
    }


def main():
    success, failed, skipped = [], [], []
    total = len(SYMBOLS)

    for i, sym in enumerate(SYMBOLS):
        safe  = sym.replace("=", "-").replace("/", "-")
        fpath = DATA_DIR / f"{safe}.json"
        print(f"[{i+1}/{total}] {sym}", end=" ... ", flush=True)

        existing = None
        if fpath.exists():
            try:
                existing = json.loads(fpath.read_text())
            except Exception:
                pass

        if existing and existing.get("fetchedAt"):
            age_hours = (datetime.now(timezone.utc) - datetime.fromisoformat(existing["fetchedAt"])).total_seconds() / 3600
            if age_hours < 12:
                print(f"skipped ({age_hours:.1f}h old)")
                skipped.append(sym)
                continue

        fresh = fetch_yahoo(sym)
        if not fresh:
            print("FAILED")
            failed.append(sym)
        else:
            merged = merge_incremental(existing, fresh)
            fpath.write_text(json.dumps(merged, separators=(",", ":")))
            print(f"OK ({merged['count']} candles)")
            success.append(sym)

        time.sleep(0.5)

    manifest = {
        "lastRun": datetime.now(timezone.utc).isoformat(),
        "symbols": success + skipped,
        "count":   len(success) + len(skipped),
        "failed":  failed,
        "success": len(success),
        "skipped": len(skipped),
    }
    (DATA_DIR / "__manifest__.json").write_text(json.dumps(manifest, indent=2))

    print(f"\n{'='*50}")
    print(f"Done: {len(success)} updated, {len(skipped)} skipped, {len(failed)} failed")
    if failed:
        print(f"Failed: {', '.join(failed)}")


if __name__ == "__main__":
    main()
