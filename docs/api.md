# Zephyrd Scanner API

Generated 2025-10-27T23:57:05.253Z from TypeScript definitions.

---

## GET /stats (scale=block)

**Type:** array<object (BlockStatsRow)>

GET /stats (scale=block) payload.

### Items

Type: object (BlockStatsRow)

#### BlockStatsRow Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| block_height | number | yes |  |
| data | object (ProtocolStats) | yes |  |

##### ProtocolStats Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| block_height | number | yes |  |
| block_timestamp | number | yes |  |
| spot | number | yes |  |
| moving_average | number | yes |  |
| reserve | number | yes |  |
| reserve_ma | number | yes |  |
| stable | number | yes |  |
| stable_ma | number | yes |  |
| yield_price | number | yes |  |
| zeph_in_reserve | number | yes |  |
| zeph_in_reserve_atoms | string | no |  |
| zsd_in_yield_reserve | number | yes |  |
| zeph_circ | number | yes |  |
| zephusd_circ | number | yes |  |
| zephrsv_circ | number | yes |  |
| zyield_circ | number | yes |  |
| assets | number | yes |  |
| assets_ma | number | yes |  |
| liabilities | number | yes |  |
| equity | number | yes |  |
| equity_ma | number | yes |  |
| reserve_ratio | number | yes |  |
| reserve_ratio_ma | number | yes |  |
| zsd_accrued_in_yield_reserve_from_yield_reward | number | yes |  |
| zsd_minted_for_yield | number | yes |  |
| conversion_transactions_count | number | yes |  |
| yield_conversion_transactions_count | number | yes |  |
| mint_reserve_count | number | yes |  |
| mint_reserve_volume | number | yes |  |
| fees_zephrsv | number | yes |  |
| redeem_reserve_count | number | yes |  |
| redeem_reserve_volume | number | yes |  |
| fees_zephusd | number | yes |  |
| mint_stable_count | number | yes |  |
| mint_stable_volume | number | yes |  |
| redeem_stable_count | number | yes |  |
| redeem_stable_volume | number | yes |  |
| fees_zeph | number | yes |  |
| mint_yield_count | number | yes |  |
| mint_yield_volume | number | yes |  |
| redeem_yield_count | number | yes |  |
| redeem_yield_volume | number | yes |  |
| fees_zephusd_yield | number | yes |  |
| fees_zyield | number | yes |  |


---

## GET /stats (scale=hour|day)

**Type:** array<object (AggregatedStatsRow)>

GET /stats (scale=hour|day) payload.

### Items

Type: object (AggregatedStatsRow)

#### AggregatedStatsRow Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| timestamp | number | yes |  |
| data | object (AggregatedData) | yes |  |

##### AggregatedData Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| spot_open | number | yes |  |
| spot_close | number | yes |  |
| spot_high | number | yes |  |
| spot_low | number | yes |  |
| moving_average_open | number | yes |  |
| moving_average_close | number | yes |  |
| moving_average_high | number | yes |  |
| moving_average_low | number | yes |  |
| reserve_open | number | yes |  |
| reserve_close | number | yes |  |
| reserve_high | number | yes |  |
| reserve_low | number | yes |  |
| reserve_ma_open | number | yes |  |
| reserve_ma_close | number | yes |  |
| reserve_ma_high | number | yes |  |
| reserve_ma_low | number | yes |  |
| stable_open | number | yes |  |
| stable_close | number | yes |  |
| stable_high | number | yes |  |
| stable_low | number | yes |  |
| stable_ma_open | number | yes |  |
| stable_ma_close | number | yes |  |
| stable_ma_high | number | yes |  |
| stable_ma_low | number | yes |  |
| zyield_price_open | number | yes |  |
| zyield_price_close | number | yes |  |
| zyield_price_high | number | yes |  |
| zyield_price_low | number | yes |  |
| zeph_in_reserve_open | number | yes |  |
| zeph_in_reserve_close | number | yes |  |
| zeph_in_reserve_high | number | yes |  |
| zeph_in_reserve_low | number | yes |  |
| zsd_in_yield_reserve_open | number | yes |  |
| zsd_in_yield_reserve_close | number | yes |  |
| zsd_in_yield_reserve_high | number | yes |  |
| zsd_in_yield_reserve_low | number | yes |  |
| zeph_circ_open | number | yes |  |
| zeph_circ_close | number | yes |  |
| zeph_circ_high | number | yes |  |
| zeph_circ_low | number | yes |  |
| zephusd_circ_open | number | yes |  |
| zephusd_circ_close | number | yes |  |
| zephusd_circ_high | number | yes |  |
| zephusd_circ_low | number | yes |  |
| zephrsv_circ_open | number | yes |  |
| zephrsv_circ_close | number | yes |  |
| zephrsv_circ_high | number | yes |  |
| zephrsv_circ_low | number | yes |  |
| zyield_circ_open | number | yes |  |
| zyield_circ_close | number | yes |  |
| zyield_circ_high | number | yes |  |
| zyield_circ_low | number | yes |  |
| assets_open | number | yes |  |
| assets_close | number | yes |  |
| assets_high | number | yes |  |
| assets_low | number | yes |  |
| assets_ma_open | number | yes |  |
| assets_ma_close | number | yes |  |
| assets_ma_high | number | yes |  |
| assets_ma_low | number | yes |  |
| liabilities_open | number | yes |  |
| liabilities_close | number | yes |  |
| liabilities_high | number | yes |  |
| liabilities_low | number | yes |  |
| equity_open | number | yes |  |
| equity_close | number | yes |  |
| equity_high | number | yes |  |
| equity_low | number | yes |  |
| equity_ma_open | number | yes |  |
| equity_ma_close | number | yes |  |
| equity_ma_high | number | yes |  |
| equity_ma_low | number | yes |  |
| reserve_ratio_open | number | yes |  |
| reserve_ratio_close | number | yes |  |
| reserve_ratio_high | number | yes |  |
| reserve_ratio_low | number | yes |  |
| reserve_ratio_ma_open | number | yes |  |
| reserve_ratio_ma_close | number | yes |  |
| reserve_ratio_ma_high | number | yes |  |
| reserve_ratio_ma_low | number | yes |  |
| conversion_transactions_count | number | yes |  |
| yield_conversion_transactions_count | number | yes |  |
| mint_reserve_count | number | yes |  |
| mint_reserve_volume | number | yes |  |
| fees_zephrsv | number | yes |  |
| redeem_reserve_count | number | yes |  |
| redeem_reserve_volume | number | yes |  |
| fees_zephusd | number | yes |  |
| mint_stable_count | number | yes |  |
| mint_stable_volume | number | yes |  |
| redeem_stable_count | number | yes |  |
| redeem_stable_volume | number | yes |  |
| fees_zeph | number | yes |  |
| mint_yield_count | number | yes |  |
| mint_yield_volume | number | yes |  |
| fees_zyield | number | yes |  |
| redeem_yield_count | number | yes |  |
| redeem_yield_volume | number | yes |  |
| fees_zephusd_yield | number | yes |  |
| pending | boolean | no |  |
| window_start | number | no |  |
| window_end | number | no |  |


---

## GET /txs

**Type:** object (TransactionsResponse)

GET /txs payload.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| total | number | yes |  |
| results | array<object (TransactionRecord)> | yes |  |
| limit | number | null | yes |  |
| offset | number | yes |  |
| order | string (enum: asc, desc) | yes |  |
| next_offset | number | null | yes |  |
| prev_offset | number | null | yes |  |

#### results Items

Type: object (TransactionRecord)

##### TransactionRecord Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| hash | string | yes |  |
| block_height | number | yes |  |
| block_timestamp | number | yes |  |
| conversion_type | string | yes |  |
| conversion_rate | number | null | no |  |
| from_asset | string | null | no |  |
| from_amount | number | null | no |  |
| from_amount_atoms | string | no |  |
| to_asset | string | null | no |  |
| to_amount | number | null | no |  |
| to_amount_atoms | string | no |  |
| conversion_fee_asset | string | null | no |  |
| conversion_fee_amount | number | null | no |  |
| tx_fee_asset | string | null | no |  |
| tx_fee_amount | number | null | no |  |
| tx_fee_atoms | string | no |  |


---

## GET /blockrewards

**Type:** object (BlockRewardQueryResult)

GET /blockrewards response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| total | number | yes |  |
| results | array<object (BlockRewardRecord)> | yes |  |
| limit | number | null | yes |  |
| order | string (enum: asc, desc) | yes |  |

#### results Items

Type: object (BlockRewardRecord)

##### BlockRewardRecord Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| height | number | yes |  |
| miner_reward | number | yes |  |
| governance_reward | number | yes |  |
| reserve_reward | number | yes |  |
| yield_reward | number | yes |  |
| miner_reward_atoms | string | no |  |
| governance_reward_atoms | string | no |  |
| reserve_reward_atoms | string | no |  |
| yield_reward_atoms | string | no |  |
| base_reward_atoms | string | no |  |
| fee_adjustment_atoms | string | no |  |


---

## GET /pricingrecords

**Type:** object (PricingRecordQueryResult)

GET /pricingrecords response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| total | number | yes |  |
| results | array<object (PricingRecord)> | yes |  |
| limit | number | null | yes |  |
| order | string (enum: asc, desc) | yes |  |

#### results Items

Type: object (PricingRecord)

##### PricingRecord Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| height | number | yes |  |
| timestamp | number | no |  |
| spot | number | no |  |
| moving_average | number | no |  |
| reserve | number | no |  |
| reserve_ma | number | no |  |
| stable | number | no |  |
| stable_ma | number | no |  |
| yield_price | number | no |  |


---

## GET /reservesnapshots

**Type:** object (ReserveSnapshotQueryResult)

GET /reservesnapshots response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| total | number | yes |  |
| results | array<object (ReserveSnapshot)> | yes |  |
| limit | number | null | yes |  |
| order | string (enum: asc, desc) | yes |  |

#### results Items

Type: object (ReserveSnapshot)

##### ReserveSnapshot Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| captured_at | string | yes |  |
| reserve_height | number | yes |  |
| previous_height | number | yes |  |
| hf_version | number | yes |  |
| on_chain | object | yes |  |
| pricing_record | object | no |  |
| raw | object | no |  |

###### on_chain Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| zeph_reserve_atoms | string | yes |  |
| zeph_reserve | number | yes |  |
| zsd_circ_atoms | string | yes |  |
| zsd_circ | number | yes |  |
| zrs_circ_atoms | string | yes |  |
| zrs_circ | number | yes |  |
| zyield_circ_atoms | string | yes |  |
| zyield_circ | number | yes |  |
| zsd_yield_reserve_atoms | string | yes |  |
| zsd_yield_reserve | number | yes |  |
| reserve_ratio_atoms | string | yes |  |
| reserve_ratio | number | null | yes |  |
| reserve_ratio_ma_atoms | string | no |  |
| reserve_ratio_ma | number | null | no |  |

###### pricing_record Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| moving_average | number | yes |  |
| reserve | number | yes |  |
| reserve_ma | number | yes |  |
| reserve_ratio | number | yes |  |
| reserve_ratio_ma | number | yes |  |
| signature | string | yes |  |
| spot | number | yes |  |
| stable | number | yes |  |
| stable_ma | number | yes |  |
| timestamp | number | yes |  |
| yield_price | number | yes |  |

###### raw Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| assets | string | yes |  |
| assets_ma | string | yes |  |
| equity | string | yes |  |
| equity_ma | string | yes |  |
| height | number | yes |  |
| hf_version | number | yes |  |
| liabilities | string | yes |  |
| num_reserves | string | yes |  |
| num_stables | string | yes |  |
| num_zyield | string | yes |  |
| pr | object | no |  |
| reserve_ratio | string | yes |  |
| reserve_ratio_ma | string | yes |  |
| status | string | yes |  |
| zeph_reserve | string | yes |  |
| zyield_reserve | string | yes |  |

###### pr Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| moving_average | number | yes |  |
| reserve | number | yes |  |
| reserve_ma | number | yes |  |
| reserve_ratio | number | yes |  |
| reserve_ratio_ma | number | yes |  |
| signature | string | yes |  |
| spot | number | yes |  |
| stable | number | yes |  |
| stable_ma | number | yes |  |
| timestamp | number | yes |  |
| yield_price | number | yes |  |


---

## GET /livestats

**Type:** object (LiveStats)

GET /livestats response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| zeph_price | number | yes |  |
| zsd_rate | number | yes |  |
| zsd_price | number | yes |  |
| zrs_rate | number | yes |  |
| zrs_price | number | yes |  |
| zys_price | number | yes |  |
| zeph_circ | number | yes |  |
| zsd_circ | number | yes |  |
| zrs_circ | number | yes |  |
| zys_circ | number | yes |  |
| zeph_circ_daily_change | number | yes |  |
| zsd_circ_daily_change | number | yes |  |
| zrs_circ_daily_change | number | yes |  |
| zys_circ_daily_change | number | yes |  |
| zeph_in_reserve | number | yes |  |
| zeph_in_reserve_value | number | yes |  |
| zeph_in_reserve_percent | number | yes |  |
| zsd_in_yield_reserve | number | yes |  |
| zsd_in_yield_reserve_percent | number | yes |  |
| zsd_accrued_in_yield_reserve_from_yield_reward | number | yes |  |
| zys_current_variable_apy | number | null | yes |  |
| reserve_ratio | number | yes |  |
| reserve_ratio_ma | number | null | yes |  |


---

## GET /historicalreturns

**Type:** object (HistoricalReturns)

GET /historicalreturns response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| lastBlock | object (HistoricalReturnEntry) | yes |  |
| oneDay | object (HistoricalReturnEntry) | yes |  |
| oneWeek | object (HistoricalReturnEntry) | yes |  |
| oneMonth | object (HistoricalReturnEntry) | yes |  |
| threeMonths | object (HistoricalReturnEntry) | yes |  |
| oneYear | object (HistoricalReturnEntry) | yes |  |
| allTime | object (HistoricalReturnEntry) | yes |  |

#### HistoricalReturnEntry Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| return | number | yes |  |
| ZSDAccrued | number | yes |  |
| effectiveApy | number | null | yes |  |


---

## GET /projectedreturns

**Type:** object (ProjectedReturns)

GET /projectedreturns response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| oneWeek | object (ProjectedReturnTier) | yes |  |
| oneMonth | object (ProjectedReturnTier) | yes |  |
| threeMonths | object (ProjectedReturnTier) | yes |  |
| sixMonths | object (ProjectedReturnTier) | yes |  |
| oneYear | object (ProjectedReturnTier) | yes |  |

#### ProjectedReturnTier Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| low | object (ProjectedReturnScenario) | yes |  |
| simple | object (ProjectedReturnScenario) | yes |  |
| high | object (ProjectedReturnScenario) | yes |  |

##### ProjectedReturnScenario Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| zys_price | number | yes |  |
| return | number | yes |  |


---

## GET /zyspricehistory

**Type:** array<object (ZysPriceHistoryEntry)>

GET /zyspricehistory payload.

### Items

Type: object (ZysPriceHistoryEntry)

#### ZysPriceHistoryEntry Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| timestamp | number | yes |  |
| block_height | number | yes |  |
| zys_price | number | yes |  |


---

## GET /apyhistory

**Type:** array<object (ApyHistoryEntry)>

GET /apyhistory payload.

### Items

Type: object (ApyHistoryEntry)

#### ApyHistoryEntry Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| timestamp | number | yes |  |
| block_height | number | yes |  |
| return | number | yes |  |
| zys_price | number | yes |  |


---

## GET /reservediff

**Type:** object (ReserveDiffReport)

GET /reservediff response schema.

### Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| block_height | number | yes |  |
| reserve_height | number | yes |  |
| diffs | array<object (ReserveDiffEntry)> | yes |  |
| mismatch | boolean | yes |  |
| source | string (enum: rpc, snapshot) | yes |  |
| source_height | number | no |  |
| snapshot_path | string | no |  |

#### diffs Items

Type: object (ReserveDiffEntry)

##### ReserveDiffEntry Fields

| Field | Type | Required | Description |
| ----- | ---- | -------- | ----------- |
| field | string | yes |  |
| on_chain | number | yes |  |
| cached | number | yes |  |
| difference | number | yes |  |
| difference_atoms | number | no |  |
| note | string | no |  |


