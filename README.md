# zephyrdscanner

Tool for scanning the Zephyr Blockchain.
Powehouse for website data and all 

IMPORTANT NOTE - THIS IS A WIP Project

## Node

`npm run start`

Uses redis to store information.
Polls every minute to check for new blocks/transactions.

Keeps track of:

- Pricing Records (Asset prices over time recorded in each block)
- Conversion Transactions
- Block Rewards (Miner Reward, Reserve Rewards and Old Governance Reward)
- Totals
  - Block Rewards
  - Number of Conversion Transactions (and of each type)
  - Amount of Zeph/ZSD/ZRS/ZYS converted (volume)
  - Fees generated from conversions

## Available Routes

An instance of `zephyrdscanner` is running on `157.245.130.75:4000`. There is a rate limit, so please avoid excessive requests.

### Route: `/`
**Description**: Root endpoint for testing server connection.

**Response**: Returns a simple confirmation message "zephyrdscanner reached".

---

### Route: `/stats`
**Description**: Retrieves statistics on the Zephyr Protocol over different time scales.

**Params**:
- **scale** (required): The granularity of data. Can be `block`, `hour`, or `day`.
- **from** (optional): Starting block number or timestamp (applicable for `hour` and `day` scales).
- **to** (optional): Ending block number or timestamp (applicable for `hour` and `day` scales).
- **fields** (optional): A comma-separated string of fields to request (e.g., `spot_open,spot_close`).

**Example**:
```sh
curl "157.245.130.75:4000/stats?scale=day&from=1729468800&to=1729598400&fields=spot_open,spot_close,moving_average_open,moving_average_close" | jq
```

Notes: 
- When requesting data with block scale, the response structure and available fields differs compared to hour and day scales.

- There is a lot of data, avoid not passing in from and to params where possible.

### Route: `livestats`
**Description**: Retrieves (some) current live stats of Zephyr Protocol from Redis.
**Example**:
```sh
 curl "157.245.130.75:4000/livestats" | jq
```
**Response**: 
```sh
{
  "zeph_price": 3.8466,
  "zsd_rate": 0.26,
  "zsd_price": 1.0001,
  "zrs_rate": 1.5052,
  "zrs_price": 5.7899,
  "zys_price": 1.0352,
  "zeph_circ": 5815118.358150186,
  "zsd_circ": 761083.4546228138,
  "zrs_circ": 1092791.8177687959,
  "zys_circ": 525951.1647029163,
  "zeph_circ_daily_change": 8794.776697436348,
  "zsd_circ_daily_change": 57836.032324298634,
  "zrs_circ_daily_change": -6767.932300000219,
  "zys_circ_daily_change": 43992.33862813725,
  "zeph_in_reserve": 1842773.0392142392,
  "zeph_in_reserve_value": 7088410.772641493,
  "zeph_in_reserve_percent": 0.3168934707290177,
  "zsd_in_yield_reserve": 544445.5256842732,
  "zsd_in_yield_reserve_percent": 0.7153558816412525
}
```

---

## Python

Was used as a prototype for the node version, but has graphing functionality.
Uses CSV files as apposed to redis at the moment.

Run `prscan.py` first to generate the pricing records CSV file.
Run `graph.py` to generate graphs from the CSV files.

Run `txscan.py` to generate the transactions CSV file
Run `txstats.py` to generate stats from the transactions CSV file
