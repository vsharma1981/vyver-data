# vyver-data

Market data store for [Vyver Intelligence](https://cheery-pegasus-6872e6.netlify.app).

Updated daily at 6am UTC via GitHub Actions.

## Structure

```
data/
  GLD.json          ← 3yr OHLC for Gold ETF
  SPY.json          ← 3yr OHLC for S&P 500 ETF
  NVDA.json         ← 3yr OHLC for NVIDIA
  __manifest__.json ← last run metadata
scripts/
  fetch_data.py     ← Python script run by GitHub Actions
.github/
  workflows/
    enrich.yml      ← Daily cron workflow
```

## Data format

Each JSON file:
```json
{
  "symbol": "GLD",
  "fetchedAt": "2026-04-11T06:00:00Z",
  "count": 756,
  "dates":  ["2023-04-11", ...],
  "open":   [182.5, ...],
  "high":   [183.2, ...],
  "low":    [181.8, ...],
  "close":  [182.9, ...],
  "volume": [8234567, ...]
}
```

## Manual trigger

Go to **Actions** tab → **Daily Market Data Enrichment** → **Run workflow**
