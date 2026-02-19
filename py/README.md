# Python Utilities

Original prototype scripts used during early development of the scanner. These Python tools talk directly to a local `zephyrd` daemon, output CSV files, and produce graphs — useful for quick ad-hoc analysis outside the main scanner pipeline.

## Prerequisites

- Python 3.8+
- `pip install requests pandas matplotlib`
- A running `zephyrd` daemon on `127.0.0.1:17767`

## Scripts

| Script | Description |
|---|---|
| `prscan.py` | Scan pricing records from the daemon and write to `csvs/pricing_records.csv` |
| `txscan.py` | Scan conversion transactions (requires `pricing_records.csv`) and write to `csvs/txs.csv` |
| `txstats.py` | Print summary stats from `csvs/txs.csv` (fees, counts by type, averages) |
| `graph.py` | Generate matplotlib charts from `csvs/pricing_records.csv` (spot, MA, reserve, stable) |
| `reserveinfo.py` | Reconstruct reserve state from CSVs and print per-block reserve stats |

## Tools

| Script | Description |
|---|---|
| `tools/saveRedisTxsToCSV.py` | Dump the scanner's Redis `txs` hash to a CSV file |

## Usage

Run from the project root:

```sh
# 1. Scan pricing records
python py/prscan.py

# 2. Scan transactions (needs pricing_records.csv)
python py/txscan.py

# 3. Generate stats or graphs
python py/txstats.py
python py/graph.py
```

CSV output goes to `py/csvs/`.

## Note

These scripts predate the Node.js scanner and are not actively maintained. The main scanner provides the same data (and more) via its API and database. These remain useful for quick one-off analysis or cross-checking scanner output against raw daemon data.
