# API Examples

Curl examples for every endpoint. Set `$API` to point at your instance:

```sh
# Local default
export API="http://localhost:4000"

# Public server (rate-limited)
# export API="http://157.245.130.75:4000"
```

---

## Public Routes

### `GET /`

Health check.

```sh
curl "$API/"
```

```
zephyrdscanner reached
```

### `GET /stats`

Protocol statistics at different time scales.

**Params**:

- **scale** (required): `block`, `hour`, or `day`.
- **from** / **to** (optional): Block height (for `block` scale) or UNIX timestamp (for `hour`/`day`).
- **fields** (optional): Comma-separated field filter (e.g. `spot_open,spot_close`).

Note: block-scale responses have a different structure and field set compared to hour/day. Always pass `from`/`to` to avoid fetching the entire chain.

#### Block-scale fields

Block scale returns a single value per field (point-in-time snapshot):

| Category | Fields |
|---|---|
| **Prices** | `spot`, `moving_average`, `reserve`, `reserve_ma`, `stable`, `stable_ma`, `yield_price` |
| **Protocol state** | `zeph_circ`, `zephusd_circ`, `zephrsv_circ`, `zyield_circ`, `zeph_in_reserve`, `zeph_in_reserve_atoms`, `zsd_in_yield_reserve`, `zsd_accrued_in_yield_reserve_from_yield_reward`, `zsd_minted_for_yield` |
| **Reserve** | `reserve_ratio`, `reserve_ratio_ma`, `assets`, `assets_ma`, `liabilities`, `equity`, `equity_ma` |
| **Conversions** | `conversion_transactions_count`, `yield_conversion_transactions_count`, `mint_stable_count`, `mint_stable_volume`, `redeem_stable_count`, `redeem_stable_volume`, `mint_reserve_count`, `mint_reserve_volume`, `redeem_reserve_count`, `redeem_reserve_volume`, `mint_yield_count`, `mint_yield_volume`, `redeem_yield_count`, `redeem_yield_volume` |
| **Fees** | `fees_zeph`, `fees_zephusd`, `fees_zephrsv`, `fees_zephusd_yield`, `fees_zyield` |
| **Block info** | `block_height`, `block_timestamp` |

#### Hour/day-scale fields

Hour and day scales return OHLC values (`_open`, `_close`, `_high`, `_low`) for price and state fields:

| Category | Fields |
|---|---|
| **Prices** | `spot_*`, `moving_average_*`, `reserve_*`, `reserve_ma_*`, `stable_*`, `stable_ma_*`, `zyield_price_*` |
| **Protocol state** | `zeph_circ_*`, `zephusd_circ_*`, `zephrsv_circ_*`, `zyield_circ_*`, `zeph_in_reserve_*`, `zsd_in_yield_reserve_*` |
| **Reserve** | `reserve_ratio_*`, `reserve_ratio_ma_*`, `assets_*`, `assets_ma_*`, `liabilities_*`, `equity_*`, `equity_ma_*` |
| **Conversions** | `conversion_transactions_count`, `yield_conversion_transactions_count`, `mint_stable_count`, `mint_stable_volume`, `redeem_stable_count`, `redeem_stable_volume`, `mint_reserve_count`, `mint_reserve_volume`, `redeem_reserve_count`, `redeem_reserve_volume`, `mint_yield_count`, `mint_yield_volume`, `redeem_yield_count`, `redeem_yield_volume` |
| **Fees** | `fees_zeph`, `fees_zephusd`, `fees_zephrsv`, `fees_zephusd_yield`, `fees_zyield` |
| **Window** | `window_start`, `window_end`, `pending` |

Where `*` = `_open`, `_close`, `_high`, `_low`.

For verbose responses with all fields, see [Appendix: Full stats responses](#appendix-full-stats-responses).

#### Day scale

```sh
curl "$API/stats?scale=day&from=1729468800&to=1729598400&fields=spot_open,spot_close,moving_average_open,moving_average_close" | jq
```

```json
[
  {
    "timestamp": 1729468800,
    "data": {
      "spot_open": 2.78811248,
      "spot_close": 4.13679669,
      "moving_average_open": 2.64336087,
      "moving_average_close": 3.17459297
    }
  },
  {
    "timestamp": 1729555200,
    "data": {
      "spot_open": 4.11934623,
      "spot_close": 4.70709109,
      "moving_average_open": 3.17643853,
      "moving_average_close": 4.20558996
    }
  }
]
```

#### Hour scale

```sh
curl "$API/stats?scale=hour&from=1771520000&to=1771530000&fields=spot_open,spot_close,reserve_ratio_open,reserve_ratio_close" | jq
```

```json
[
  {
    "timestamp": 1771520400,
    "data": {
      "spot_open": 0.56604917,
      "spot_close": 0.57096156,
      "reserve_ratio_open": 3.889176314573164,
      "reserve_ratio_close": 3.923001330200107
    }
  },
  {
    "timestamp": 1771524000,
    "data": {
      "spot_open": 0.570693,
      "spot_close": 0.57152149,
      "reserve_ratio_open": 3.921157472571614,
      "reserve_ratio_close": 3.926929850009239
    }
  }
]
```

#### Block scale

```sh
curl "$API/stats?scale=block&from=500000&to=500002&fields=spot,moving_average" | jq
```

```json
[
  {
    "block_height": 500000,
    "data": {
      "spot": 0.58107726,
      "moving_average": 0.6160339
    }
  },
  {
    "block_height": 500001,
    "data": {
      "spot": 0.58107726,
      "moving_average": 0.61595934
    }
  }
]
```

### `GET /txs`

Conversion transactions with pagination and filtering.

**Params**:

- **from** / **to** (optional): Timestamp bounds (UNIX seconds or ISO-8601).
- **from_index** (optional): Skip the first N results (oldest-first). Also accepts `from=idx:N`.
- **types** (optional): Comma-separated conversion types (e.g. `mint_stable,redeem_stable,mint_reserve`).
- **limit** (optional): Max rows (default `1000`). Use `limit=all` or `limit=0` for everything.
- **offset** (optional): Skip N rows (default `0`).
- **page** / **pageSize** (optional): Page-based pagination (1-indexed).
- **order** (optional): `asc` or `desc` (default `desc`).

```sh
# Latest 2 transactions
curl "$API/txs?order=desc&limit=2" | jq
```

```json
{
  "total": 45237,
  "limit": 2,
  "offset": 0,
  "order": "desc",
  "next_offset": 2,
  "prev_offset": null,
  "results": [
    {
      "hash": "1598457886e12416f4e8a3ba9ca453d0ebf48f666de80a63c2573be3b81130fe",
      "block_height": 715127,
      "block_timestamp": 1771528824,
      "conversion_type": "mint_reserve",
      "conversion_rate": 1.39722381,
      "from_asset": "ZEPH",
      "from_amount": 2.8085,
      "from_amount_atoms": "2808500000000",
      "to_asset": "ZEPHRSV",
      "to_amount": 1.98995677715,
      "to_amount_atoms": "1989956777150",
      "conversion_fee_asset": "ZEPHRSV",
      "conversion_fee_amount": 0.02010057350656566,
      "tx_fee_asset": "ZEPH",
      "tx_fee_amount": 0.0009,
      "tx_fee_atoms": "900000000"
    },
    {
      "hash": "1f664fd8b870fe64032c6c8b0fc8943e68b13bbb0549bfe8dcc041e62c9e7a4e",
      "block_height": 715123,
      "block_timestamp": 1771528115,
      "conversion_type": "mint_reserve",
      "conversion_rate": 1.39701616,
      "from_asset": "ZEPH",
      "from_amount": 0.8442,
      "from_amount_atoms": "844200000000",
      "to_asset": "ZEPHRSV",
      "to_amount": 0.598245039882,
      "to_amount_atoms": "598245039882",
      "conversion_fee_asset": "ZEPHRSV",
      "conversion_fee_amount": 0.006042879190727273,
      "tx_fee_asset": "ZEPH",
      "tx_fee_amount": 0.0016,
      "tx_fee_atoms": "1600000000"
    }
  ]
}
```

```sh
# Filter by conversion type
curl "$API/txs?types=mint_stable,redeem_stable&order=desc&limit=2" | jq
```

```json
{
  "total": 12336,
  "limit": 2,
  "offset": 0,
  "order": "desc",
  "next_offset": 2,
  "prev_offset": null,
  "results": [
    {
      "hash": "38cd88b6143e2b5eb299f4f17d03896d4813c5028c4885fca730bb940e5c4f69",
      "block_height": 713900,
      "block_timestamp": 1771380761,
      "conversion_type": "redeem_stable",
      "conversion_rate": 0.57604121,
      "from_asset": "ZEPHUSD",
      "from_amount": 6.2385,
      "from_amount_atoms": "6238500000000",
      "to_asset": "ZEPH",
      "to_amount": 10.72370039319,
      "to_amount_atoms": "10723700393190",
      "conversion_fee_asset": "ZEPH",
      "conversion_fee_amount": 0.01073443482801802,
      "tx_fee_asset": "ZEPHUSD",
      "tx_fee_amount": 0.000627445848,
      "tx_fee_atoms": "627445848"
    },
    {
      "hash": "0ffa6fdd0abab1957090a80eea65e385e7c449fc681fdb65f9c56169f803f32a",
      "block_height": 713643,
      "block_timestamp": 1771349403,
      "conversion_type": "mint_stable",
      "conversion_rate": 0.5863622,
      "from_asset": "ZEPH",
      "from_amount": 19.5489,
      "from_amount_atoms": "19548900000000",
      "to_asset": "ZEPHUSD",
      "to_amount": 11.377385709669,
      "to_amount_atoms": "11377385709669",
      "conversion_fee_asset": "ZEPHUSD",
      "conversion_fee_amount": 0.01138877448415315,
      "tx_fee_asset": "ZEPH",
      "tx_fee_amount": 0.00258,
      "tx_fee_atoms": "2580000000"
    }
  ]
}
```

```sh
# All mint_yield events
curl "$API/txs?types=mint_yield&limit=all" | jq
```

```sh
# Page through results (page 1, 20 per page)
curl "$API/txs?page=1&pageSize=20" | jq
```

```sh
# Incremental polling — only transactions after the first 26,963 rows
curl "$API/txs?from_index=26963&limit=all" | jq
```

**Paginated response fields**:

- `total`: Total rows matching the filters.
- `limit`: Page size (null when `limit=all`).
- `offset`: Starting index.
- `next_offset` / `prev_offset`: Cursor hints for subsequent requests.
- `results`: Array of transaction objects.

### `GET /blockrewards`

Per-block reward breakdowns (miner, governance, reserve, yield).

**Params**:

- **from** / **to** (optional): Height range.
- **limit** (optional): Max rows (default all in range). Use `limit=all` to disable.
- **order** (optional): `asc` (default) or `desc`.

```sh
curl "$API/blockrewards?order=desc&limit=2" | jq
```

```json
{
  "total": 715157,
  "limit": 2,
  "order": "desc",
  "results": [
    {
      "height": 715156,
      "miner_reward": 4.624577576574,
      "governance_reward": 0,
      "reserve_reward": 2.132583090726,
      "yield_reward": 0.355430515121,
      "miner_reward_atoms": "4624577576574",
      "governance_reward_atoms": "0",
      "reserve_reward_atoms": "2132583090726",
      "yield_reward_atoms": "355430515121",
      "base_reward_atoms": "7108610302421",
      "fee_adjustment_atoms": "3980880000"
    },
    {
      "height": 715155,
      "miner_reward": 4.622066953123,
      "governance_reward": 0,
      "reserve_reward": 2.132585124517,
      "yield_reward": 0.355430854086,
      "miner_reward_atoms": "4622066953123",
      "governance_reward_atoms": "0",
      "reserve_reward_atoms": "2132585124517",
      "yield_reward_atoms": "355430854086",
      "base_reward_atoms": "7108617081726",
      "fee_adjustment_atoms": "1465850000"
    }
  ]
}
```

```sh
# Rewards around height 500,000
curl "$API/blockrewards?from=499990&to=500010" | jq
```

### `GET /pricingrecords`

Pricing records (spot prices, reserve ratios, yield price) by block height.

**Params**:

- **from** / **to** (optional): Height range.
- **limit** (optional): Max rows. Use `limit=all` to disable.
- **order** (optional): `asc` (default) or `desc`.

```sh
curl "$API/pricingrecords?order=desc&limit=2" | jq
```

```json
{
  "total": 715157,
  "limit": 2,
  "order": "desc",
  "results": [
    {
      "height": 715156,
      "timestamp": 1771531716,
      "spot": 0.56729083,
      "moving_average": 0.56187756,
      "reserve": 1.39350071,
      "reserve_ma": 1.38833761,
      "stable": 1.76276425,
      "stable_ma": 1.78003269,
      "yield_price": 1.85862268
    },
    {
      "height": 715155,
      "timestamp": 1771531493,
      "spot": 0.56956725,
      "moving_average": 0.56184321,
      "reserve": 1.39542135,
      "reserve_ma": 1.38830591,
      "stable": 1.75571892,
      "stable_ma": 1.78014431,
      "yield_price": 1.85862174
    }
  ]
}
```

```sh
# First 5 pricing records after the yield fork
curl "$API/pricingrecords?from=360000&limit=5" | jq
```

### `GET /reservesnapshots`

Reserve snapshots captured by the scanner, indexed by `previous_height`.

**Params**:

- **height** (optional): Exact `previous_height` to fetch.
- **from** / **to** (optional): Range of `previous_height` values.
- **limit** (optional): Max rows. Use `limit=all` to disable.
- **order** (optional): `asc` (default) or `desc`.

```sh
curl "$API/reservesnapshots?order=desc&limit=1" | jq
```

```json
{
  "total": 2,
  "limit": 1,
  "order": "desc",
  "results": [
    {
      "captured_at": "2026-02-19T19:45:31.593Z",
      "reserve_height": 715144,
      "previous_height": 715143,
      "hf_version": 11,
      "on_chain": {
        "zeph_reserve_atoms": "3182092910153330065",
        "zeph_reserve": 3182092.91015333,
        "zsd_circ_atoms": "463115664607911912",
        "zsd_circ": 463115.664607912,
        "zrs_circ_atoms": "1697706207950085872",
        "zrs_circ": 1697706.207950086,
        "zyield_circ_atoms": "211096235895093092",
        "zyield_circ": 211096.2358950931,
        "zsd_yield_reserve_atoms": "392345860724802335",
        "zsd_yield_reserve": 392345.8607248023,
        "reserve_ratio_atoms": "3.918545",
        "reserve_ratio": 3.918545,
        "reserve_ratio_ma_atoms": "3.857170",
        "reserve_ratio_ma": 3.85717
      },
      "pricing_record": {
        "spot": 570297550000,
        "stable": 1753470620000,
        "reserve": 1396019550000,
        "stable_ma": 1781521110000,
        "timestamp": 1771530217,
        "reserve_ma": 1387915270000,
        "yield_price": 1858610400000,
        "reserve_ratio": 3918544090000,
        "moving_average": 561416850000,
        "reserve_ratio_ma": 3856927410000
      }
    }
  ]
}
```

### `GET /livestats`

Current protocol state — prices, circulating supplies, reserve info.

```sh
curl "$API/livestats" | jq
```

```json
{
  "zrs_circ": 1697706.207950086,
  "zrs_rate": 1.3935,
  "zsd_circ": 463118.2568207949,
  "zsd_rate": 1.7628,
  "zys_circ": 211096.2358950931,
  "zeph_circ": 10992833.02584816,
  "zrs_price": 0.7905,
  "zsd_price": 1,
  "zys_price": 1.8586,
  "zeph_price": 0.5673,
  "reserve_ratio": 3.897904,
  "zeph_in_reserve": 3182125.254515282,
  "reserve_ratio_ma": 3.860348,
  "zsd_in_yield_reserve": 392348.4529376852,
  "zeph_in_reserve_value": 1805219.656886519,
  "zrs_circ_daily_change": 132.5551603061613,
  "zsd_circ_daily_change": 142.6702269592788,
  "zys_circ_daily_change": -524.1651000000129,
  "zeph_circ_daily_change": 5119.903889458627,
  "zeph_in_reserve_percent": 0.2894727180002593,
  "zys_current_variable_apy": 11.6088,
  "zsd_in_yield_reserve_percent": 0.8471884819032426,
  "zsd_accrued_in_yield_reserve_from_yield_reward": 191205.5633135816
}
```

### `GET /historicalreturns`

Historical yield returns across time periods. Optional `?test=true` for dummy data.

```sh
curl "$API/historicalreturns" | jq
```

```json
{
  "allTime": {
    "return": 85.862268,
    "ZSDAccrued": 191205.5633135816,
    "effectiveApy": 57.2031
  },
  "lastBlock": {
    "return": 0.00005057511056553121,
    "ZSDAccrued": 0.1994767182914075,
    "effectiveApy": 14.0071
  },
  "oneDay": {
    "return": 0.03659154571510685,
    "ZSDAccrued": 142.6702269595116,
    "effectiveApy": 14.0772
  },
  "oneWeek": { "..." : "..." },
  "oneMonth": { "..." : "..." },
  "threeMonths": { "..." : "..." },
  "oneYear": { "..." : "..." }
}
```

### `GET /projectedreturns`

Projected yield returns with low/simple/high estimates.

```sh
curl "$API/projectedreturns" | jq
```

```json
{
  "oneWeek": {
    "low": { "zys_price": 1.8589, "return": 0.0158 },
    "simple": { "zys_price": 1.8593, "return": 0.0377 },
    "high": { "zys_price": 1.861, "return": 0.1267 }
  },
  "oneMonth": {
    "low": { "zys_price": 1.8673, "return": 0.4697 },
    "simple": { "zys_price": 1.8787, "return": 1.0819 },
    "high": { "zys_price": 1.9284, "return": 3.7579 }
  },
  "threeMonths": { "..." : "..." },
  "sixMonths": { "..." : "..." },
  "oneYear": { "..." : "..." }
}
```

### `GET /zyspricehistory`

ZYS price history. Returns an array of data points from the yield fork (block 360,000) onwards, sampled every 30 blocks.

```sh
curl "$API/zyspricehistory" | jq '.[0:2]'
```

```json
[
  {
    "timestamp": 1728819553,
    "block_height": 360000,
    "zys_price": 1000000000000
  },
  {
    "timestamp": 1728825671,
    "block_height": 360030,
    "zys_price": 1001104520000
  }
]
```

Note: `zys_price` is in atoms (divide by 1e12 for display units).

### `GET /apyhistory`

Daily APY history. Returns an array of data points from the yield fork onwards.

```sh
curl "$API/apyhistory" | jq '.[0:2]'
```

```json
[
  {
    "timestamp": 1728864000,
    "block_height": 360000,
    "return": 80.62198446466374,
    "zys_price": 1.814675933782327
  },
  {
    "timestamp": 1728950400,
    "block_height": 360720,
    "return": 79.10966779165916,
    "zys_price": 1.803898111558798
  }
]
```

---

## Private Routes (localhost only)

These routes are restricted to `127.0.0.1` and are not available on the public server.

### `GET /reservediff`

Compare daemon reserve info with cached protocol stats. Returns per-field absolute differences.

```sh
curl "http://127.0.0.1:4000/reservediff" | jq
```

```json
{
  "block_height": 715156,
  "reserve_height": 715156,
  "diffs": [
    {
      "field": "zeph_in_reserve",
      "on_chain": 3182125.254515282,
      "cached": 3182125.255140419,
      "difference": 0.0006251371519999999,
      "difference_atoms": -625137152
    },
    {
      "field": "zephusd_circ",
      "on_chain": 463118.2568207949,
      "cached": 463117.3342761348,
      "difference": 0.922544660096,
      "difference_atoms": 922544660096
    },
    {
      "field": "zephrsv_circ",
      "on_chain": 1697706.2079500859,
      "cached": 1697703.304725703,
      "difference": 2.90322438272,
      "difference_atoms": 2903224382720
    },
    {
      "field": "zyield_circ",
      "on_chain": 211096.2358950931,
      "cached": 211095.9583726932,
      "difference": 0.277522399904,
      "difference_atoms": 277522399904
    },
    {
      "field": "zsd_in_yield_reserve",
      "on_chain": 392348.45293768524,
      "cached": 392348.4529377029,
      "difference": 1.7664e-8,
      "difference_atoms": -17664
    },
    {
      "field": "reserve_ratio",
      "on_chain": 3.897904,
      "cached": 3.897913381966372,
      "difference": 0.000009381966371790895,
      "difference_atoms": -9381966
    }
  ],
  "mismatch": false,
  "source": "rpc",
  "source_height": 715156
}
```

### `GET /retallytotals`

Recompute cumulative totals from existing aggregated data.

```sh
curl "http://127.0.0.1:4000/retallytotals"
```

```
Totals retallied successfully
```

### `GET /redetermineapyhistory`

Force a full APY history rebuild.

```sh
curl "http://127.0.0.1:4000/redetermineapyhistory"
```

```
determineAPYHistory redetermined successfully
```

### `GET /rollback`

Roll the scanner back to a given height and rebuild forward. Use with caution.

```sh
curl "http://127.0.0.1:4000/rollback?height=500000"
```

### `POST /reset`

Rebuild scanner state.

**Params**:

- **scope** (optional): `aggregation` (default) clears derived data and recomputes. `full` flushes the database and rescans from scratch.

```sh
# Rebuild only aggregate data
curl -X POST "http://127.0.0.1:4000/reset"

# Full reset (flush, rescan, reaggregate)
curl -X POST "http://127.0.0.1:4000/reset?scope=full"
```

---

## Appendix: Full stats responses

Verbose examples with every field returned when no `fields` filter is applied.

### Block scale (all fields)

```sh
curl "$API/stats?scale=block&from=500000&to=500001" | jq '.[0]'
```

```json
{
  "block_height": 500000,
  "data": {
    "block_height": 500000,
    "block_timestamp": 1745662585,
    "spot": 0.58107726,
    "moving_average": 0.6160339,
    "reserve": 1.15387707,
    "reserve_ma": 1.19191974,
    "stable": 1.72094154,
    "stable_ma": 1.6245867,
    "yield_price": 1.41341445,
    "zeph_in_reserve": 2158268.466192044,
    "zeph_in_reserve_atoms": "2158268466192044228",
    "zsd_in_yield_reserve": 267842.7449828751,
    "zeph_circ": 7306388.036972235,
    "zephusd_circ": 472311.8939696108,
    "zephrsv_circ": 1166020.322979397,
    "zyield_circ": 189500.2815970207,
    "assets": 1254120.726679276,
    "assets_ma": 1329566.540475303,
    "liabilities": 472311.5856018678,
    "equity": 781809.141077408,
    "equity_ma": 857254.9548734355,
    "reserve_ratio": 2.655282582325705,
    "reserve_ratio_ma": 2.815019959294527,
    "zsd_accrued_in_yield_reserve_from_yield_reward": 123374.3423924997,
    "zsd_minted_for_yield": 0.30836774304,
    "conversion_transactions_count": 0,
    "yield_conversion_transactions_count": 0,
    "mint_stable_count": 0,
    "mint_stable_volume": 0,
    "redeem_stable_count": 0,
    "redeem_stable_volume": 0,
    "mint_reserve_count": 0,
    "mint_reserve_volume": 0,
    "redeem_reserve_count": 0,
    "redeem_reserve_volume": 0,
    "mint_yield_count": 0,
    "mint_yield_volume": 0,
    "redeem_yield_count": 0,
    "redeem_yield_volume": 0,
    "fees_zeph": 0,
    "fees_zephusd": 0,
    "fees_zephrsv": 0,
    "fees_zephusd_yield": 0,
    "fees_zyield": 0
  }
}
```

### Day scale (all fields)

Hour scale has the same fields.

```sh
curl "$API/stats?scale=day&from=1729468800&to=1729555200" | jq '.[0]'
```

```json
{
  "timestamp": 1729468800,
  "data": {
    "spot_open": 2.78811248,
    "spot_close": 4.13679669,
    "spot_high": 4.63097171,
    "spot_low": 2.67370671,
    "moving_average_open": 2.64336087,
    "moving_average_close": 3.17459297,
    "moving_average_high": 3.17459297,
    "moving_average_low": 2.64336087,
    "reserve_open": 1.45413528,
    "reserve_close": 1.51117906,
    "reserve_high": 1.52297288,
    "reserve_low": 1.44743844,
    "reserve_ma_open": 1.44331695,
    "reserve_ma_close": 1.47167482,
    "reserve_ma_high": 1.47167482,
    "reserve_ma_low": 1.44331695,
    "stable_open": 0.35866558,
    "stable_close": 0.24173293,
    "stable_high": 0.3740126,
    "stable_low": 0.2159374,
    "stable_ma_open": 0.38007141,
    "stable_ma_close": 0.32335232,
    "stable_ma_high": 0.38007141,
    "stable_ma_low": 0.32335232,
    "zyield_price_open": 1.02066032,
    "zyield_price_close": 1.02476916,
    "zyield_price_high": 1.02476916,
    "zyield_price_low": 1.02066032,
    "zeph_in_reserve_open": 1824467.254264841,
    "zeph_in_reserve_close": 1833758.530996236,
    "zeph_in_reserve_high": 1833758.530996236,
    "zeph_in_reserve_low": 1824060.792078367,
    "zsd_in_yield_reserve_open": 302200.0948314819,
    "zsd_in_yield_reserve_close": 323529.2720690009,
    "zsd_in_yield_reserve_high": 323529.2720690009,
    "zsd_in_yield_reserve_low": 299969.5903902416,
    "zeph_circ_open": 5780546.819092746,
    "zeph_circ_close": 5789204.811474343,
    "zeph_circ_high": 5789204.811474343,
    "zeph_circ_low": 5780546.819092746,
    "zephusd_circ_open": 505658.730499313,
    "zephusd_circ_close": 528877.9556190433,
    "zephusd_circ_high": 528877.9556190433,
    "zephusd_circ_low": 504027.1236729651,
    "zephrsv_circ_open": 1129950.719668795,
    "zephrsv_circ_close": 1128858.634268795,
    "zephrsv_circ_high": 1129950.719668795,
    "zephrsv_circ_low": 1128858.634268795,
    "zyield_circ_open": 296081.3692588892,
    "zyield_circ_close": 315707.5934878832,
    "zyield_circ_high": 315707.9517225102,
    "zyield_circ_low": 293607.4720126264,
    "assets_open": 5086819.920967137,
    "assets_close": 7585886.22128449,
    "assets_high": 8490349.192826733,
    "assets_low": 4879413.412239272,
    "assets_ma_open": 4822725.348520022,
    "assets_ma_close": 5821436.941178177,
    "assets_ma_high": 5821436.941178177,
    "assets_ma_low": 4822725.348520022,
    "liabilities_open": 505657.1429872374,
    "liabilities_close": 528876.0909176503,
    "liabilities_high": 528876.0909176503,
    "liabilities_low": 504025.4780486419,
    "equity_open": 4581162.7779799,
    "equity_close": 7057010.130366839,
    "equity_high": 7961683.828578634,
    "equity_low": 4372949.848946858,
    "equity_ma_open": 4317068.205532785,
    "equity_ma_close": 5292560.850260527,
    "equity_ma_high": 5292560.850260527,
    "equity_ma_low": 4317068.205532785,
    "reserve_ratio_open": 10.05982015979457,
    "reserve_ratio_close": 14.34340926269187,
    "reserve_ratio_high": 16.05996868151602,
    "reserve_ratio_low": 9.634283225666264,
    "reserve_ratio_ma_open": 9.537540239279771,
    "reserve_ratio_ma_close": 11.0071849364138,
    "reserve_ratio_ma_high": 11.0071849364138,
    "reserve_ratio_ma_low": 9.537540239279771,
    "conversion_transactions_count": 70,
    "yield_conversion_transactions_count": 49,
    "mint_stable_count": 46,
    "mint_stable_volume": 27043.49619964839,
    "redeem_stable_count": 13,
    "redeem_stable_volume": 5033.395,
    "mint_reserve_count": 0,
    "mint_reserve_volume": 0,
    "redeem_reserve_count": 11,
    "redeem_reserve_volume": 1092.0854,
    "mint_yield_count": 39,
    "mint_yield_volume": 24195.45782899397,
    "redeem_yield_count": 10,
    "redeem_yield_volume": 4569.2336,
    "fees_zeph": 17.62538519264172,
    "fees_zephusd": 27.07056676641482,
    "fees_zephrsv": 0,
    "fees_zephusd_yield": 4.66937335591906,
    "fees_zyield": 24.21967750650047,
    "pending": false,
    "window_start": 1729468800,
    "window_end": 1729555200
  }
}
```
