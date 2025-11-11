-- CreateEnum
CREATE TYPE "HistoricalReturnRange" AS ENUM ('LAST_BLOCK', 'ONE_DAY', 'ONE_WEEK', 'ONE_MONTH', 'THREE_MONTHS', 'ONE_YEAR', 'ALL_TIME');

-- CreateEnum
CREATE TYPE "ProjectionTimeframe" AS ENUM ('ONE_WEEK', 'ONE_MONTH', 'THREE_MONTHS', 'SIX_MONTHS', 'ONE_YEAR');

-- CreateEnum
CREATE TYPE "ProjectionScenario" AS ENUM ('LOW', 'SIMPLE', 'HIGH');

-- CreateTable
CREATE TABLE "pricing_records" (
    "block_height" INTEGER NOT NULL,
    "timestamp" INTEGER NOT NULL,
    "spot" DOUBLE PRECISION NOT NULL,
    "moving_average" DOUBLE PRECISION NOT NULL,
    "reserve" DOUBLE PRECISION NOT NULL,
    "reserve_ma" DOUBLE PRECISION NOT NULL,
    "stable" DOUBLE PRECISION NOT NULL,
    "stable_ma" DOUBLE PRECISION NOT NULL,
    "yield_price" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "pricing_records_pkey" PRIMARY KEY ("block_height")
);

-- CreateTable
CREATE TABLE "block_rewards" (
    "block_height" INTEGER NOT NULL,
    "miner_reward" DOUBLE PRECISION NOT NULL,
    "governance_reward" DOUBLE PRECISION NOT NULL,
    "reserve_reward" DOUBLE PRECISION NOT NULL,
    "yield_reward" DOUBLE PRECISION NOT NULL,
    "miner_reward_atoms" TEXT,
    "governance_reward_atoms" TEXT,
    "reserve_reward_atoms" TEXT,
    "yield_reward_atoms" TEXT,
    "base_reward_atoms" TEXT,
    "fee_adjustment_atoms" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "block_rewards_pkey" PRIMARY KEY ("block_height")
);

-- CreateTable
CREATE TABLE "transactions" (
    "hash" TEXT NOT NULL,
    "block_height" INTEGER NOT NULL,
    "block_timestamp" INTEGER NOT NULL,
    "conversion_type" TEXT NOT NULL,
    "conversion_rate" DOUBLE PRECISION NOT NULL,
    "from_asset" TEXT NOT NULL,
    "from_amount" DOUBLE PRECISION NOT NULL,
    "from_amount_atoms" TEXT,
    "to_asset" TEXT NOT NULL,
    "to_amount" DOUBLE PRECISION NOT NULL,
    "to_amount_atoms" TEXT,
    "conversion_fee_asset" TEXT NOT NULL,
    "conversion_fee_amount" DOUBLE PRECISION NOT NULL,
    "tx_fee_asset" TEXT NOT NULL,
    "tx_fee_amount" DOUBLE PRECISION NOT NULL,
    "tx_fee_atoms" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "transactions_pkey" PRIMARY KEY ("hash")
);

-- CreateTable
CREATE TABLE "protocol_stats" (
    "block_height" INTEGER NOT NULL,
    "block_timestamp" INTEGER NOT NULL,
    "spot" DOUBLE PRECISION NOT NULL,
    "moving_average" DOUBLE PRECISION NOT NULL,
    "reserve" DOUBLE PRECISION NOT NULL,
    "reserve_ma" DOUBLE PRECISION NOT NULL,
    "stable" DOUBLE PRECISION NOT NULL,
    "stable_ma" DOUBLE PRECISION NOT NULL,
    "yield_price" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_atoms" TEXT,
    "zsd_in_yield_reserve" DOUBLE PRECISION NOT NULL,
    "zeph_circ" DOUBLE PRECISION NOT NULL,
    "zephusd_circ" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ" DOUBLE PRECISION NOT NULL,
    "zyield_circ" DOUBLE PRECISION NOT NULL,
    "assets" DOUBLE PRECISION NOT NULL,
    "assets_ma" DOUBLE PRECISION NOT NULL,
    "liabilities" DOUBLE PRECISION NOT NULL,
    "equity" DOUBLE PRECISION NOT NULL,
    "equity_ma" DOUBLE PRECISION NOT NULL,
    "reserve_ratio" DOUBLE PRECISION,
    "reserve_ratio_ma" DOUBLE PRECISION,
    "zsd_accrued_in_yield_reserve_from_yield_reward" DOUBLE PRECISION NOT NULL,
    "zsd_minted_for_yield" DOUBLE PRECISION NOT NULL,
    "conversion_transactions_count" INTEGER NOT NULL,
    "yield_conversion_transactions_count" INTEGER NOT NULL,
    "mint_reserve_count" INTEGER NOT NULL,
    "mint_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephrsv" DOUBLE PRECISION NOT NULL,
    "redeem_reserve_count" INTEGER NOT NULL,
    "redeem_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd" DOUBLE PRECISION NOT NULL,
    "mint_stable_count" INTEGER NOT NULL,
    "mint_stable_volume" DOUBLE PRECISION NOT NULL,
    "redeem_stable_count" INTEGER NOT NULL,
    "redeem_stable_volume" DOUBLE PRECISION NOT NULL,
    "fees_zeph" DOUBLE PRECISION NOT NULL,
    "mint_yield_count" INTEGER NOT NULL,
    "mint_yield_volume" DOUBLE PRECISION NOT NULL,
    "redeem_yield_count" INTEGER NOT NULL,
    "redeem_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd_yield" DOUBLE PRECISION NOT NULL,
    "fees_zyield" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "protocol_stats_pkey" PRIMARY KEY ("block_height")
);

-- CreateTable
CREATE TABLE "protocol_stats_hourly" (
    "window_start" INTEGER NOT NULL,
    "window_end" INTEGER,
    "pending" BOOLEAN NOT NULL DEFAULT false,
    "spot_open" DOUBLE PRECISION NOT NULL,
    "spot_close" DOUBLE PRECISION NOT NULL,
    "spot_high" DOUBLE PRECISION NOT NULL,
    "spot_low" DOUBLE PRECISION NOT NULL,
    "moving_average_open" DOUBLE PRECISION NOT NULL,
    "moving_average_close" DOUBLE PRECISION NOT NULL,
    "moving_average_high" DOUBLE PRECISION NOT NULL,
    "moving_average_low" DOUBLE PRECISION NOT NULL,
    "reserve_open" DOUBLE PRECISION NOT NULL,
    "reserve_close" DOUBLE PRECISION NOT NULL,
    "reserve_high" DOUBLE PRECISION NOT NULL,
    "reserve_low" DOUBLE PRECISION NOT NULL,
    "reserve_ma_open" DOUBLE PRECISION NOT NULL,
    "reserve_ma_close" DOUBLE PRECISION NOT NULL,
    "reserve_ma_high" DOUBLE PRECISION NOT NULL,
    "reserve_ma_low" DOUBLE PRECISION NOT NULL,
    "stable_open" DOUBLE PRECISION NOT NULL,
    "stable_close" DOUBLE PRECISION NOT NULL,
    "stable_high" DOUBLE PRECISION NOT NULL,
    "stable_low" DOUBLE PRECISION NOT NULL,
    "stable_ma_open" DOUBLE PRECISION NOT NULL,
    "stable_ma_close" DOUBLE PRECISION NOT NULL,
    "stable_ma_high" DOUBLE PRECISION NOT NULL,
    "stable_ma_low" DOUBLE PRECISION NOT NULL,
    "zyield_price_open" DOUBLE PRECISION NOT NULL,
    "zyield_price_close" DOUBLE PRECISION NOT NULL,
    "zyield_price_high" DOUBLE PRECISION NOT NULL,
    "zyield_price_low" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_open" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_close" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_high" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_low" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_open" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_close" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_high" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_low" DOUBLE PRECISION NOT NULL,
    "zeph_circ_open" DOUBLE PRECISION NOT NULL,
    "zeph_circ_close" DOUBLE PRECISION NOT NULL,
    "zeph_circ_high" DOUBLE PRECISION NOT NULL,
    "zeph_circ_low" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_open" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_close" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_high" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_low" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_open" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_close" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_high" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_low" DOUBLE PRECISION NOT NULL,
    "zyield_circ_open" DOUBLE PRECISION NOT NULL,
    "zyield_circ_close" DOUBLE PRECISION NOT NULL,
    "zyield_circ_high" DOUBLE PRECISION NOT NULL,
    "zyield_circ_low" DOUBLE PRECISION NOT NULL,
    "assets_open" DOUBLE PRECISION NOT NULL,
    "assets_close" DOUBLE PRECISION NOT NULL,
    "assets_high" DOUBLE PRECISION NOT NULL,
    "assets_low" DOUBLE PRECISION NOT NULL,
    "assets_ma_open" DOUBLE PRECISION NOT NULL,
    "assets_ma_close" DOUBLE PRECISION NOT NULL,
    "assets_ma_high" DOUBLE PRECISION NOT NULL,
    "assets_ma_low" DOUBLE PRECISION NOT NULL,
    "liabilities_open" DOUBLE PRECISION NOT NULL,
    "liabilities_close" DOUBLE PRECISION NOT NULL,
    "liabilities_high" DOUBLE PRECISION NOT NULL,
    "liabilities_low" DOUBLE PRECISION NOT NULL,
    "equity_open" DOUBLE PRECISION NOT NULL,
    "equity_close" DOUBLE PRECISION NOT NULL,
    "equity_high" DOUBLE PRECISION NOT NULL,
    "equity_low" DOUBLE PRECISION NOT NULL,
    "equity_ma_open" DOUBLE PRECISION NOT NULL,
    "equity_ma_close" DOUBLE PRECISION NOT NULL,
    "equity_ma_high" DOUBLE PRECISION NOT NULL,
    "equity_ma_low" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_open" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_close" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_high" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_low" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_open" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_close" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_high" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_low" DOUBLE PRECISION NOT NULL,
    "conversion_transactions_count" INTEGER NOT NULL,
    "yield_conversion_transactions_count" INTEGER NOT NULL,
    "mint_reserve_count" INTEGER NOT NULL,
    "mint_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephrsv" DOUBLE PRECISION NOT NULL,
    "redeem_reserve_count" INTEGER NOT NULL,
    "redeem_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd" DOUBLE PRECISION NOT NULL,
    "mint_stable_count" INTEGER NOT NULL,
    "mint_stable_volume" DOUBLE PRECISION NOT NULL,
    "redeem_stable_count" INTEGER NOT NULL,
    "redeem_stable_volume" DOUBLE PRECISION NOT NULL,
    "fees_zeph" DOUBLE PRECISION NOT NULL,
    "mint_yield_count" INTEGER NOT NULL,
    "mint_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zyield" DOUBLE PRECISION NOT NULL,
    "redeem_yield_count" INTEGER NOT NULL,
    "redeem_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd_yield" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "protocol_stats_hourly_pkey" PRIMARY KEY ("window_start")
);

-- CreateTable
CREATE TABLE "protocol_stats_daily" (
    "window_start" INTEGER NOT NULL,
    "window_end" INTEGER,
    "pending" BOOLEAN NOT NULL DEFAULT false,
    "spot_open" DOUBLE PRECISION NOT NULL,
    "spot_close" DOUBLE PRECISION NOT NULL,
    "spot_high" DOUBLE PRECISION NOT NULL,
    "spot_low" DOUBLE PRECISION NOT NULL,
    "moving_average_open" DOUBLE PRECISION NOT NULL,
    "moving_average_close" DOUBLE PRECISION NOT NULL,
    "moving_average_high" DOUBLE PRECISION NOT NULL,
    "moving_average_low" DOUBLE PRECISION NOT NULL,
    "reserve_open" DOUBLE PRECISION NOT NULL,
    "reserve_close" DOUBLE PRECISION NOT NULL,
    "reserve_high" DOUBLE PRECISION NOT NULL,
    "reserve_low" DOUBLE PRECISION NOT NULL,
    "reserve_ma_open" DOUBLE PRECISION NOT NULL,
    "reserve_ma_close" DOUBLE PRECISION NOT NULL,
    "reserve_ma_high" DOUBLE PRECISION NOT NULL,
    "reserve_ma_low" DOUBLE PRECISION NOT NULL,
    "stable_open" DOUBLE PRECISION NOT NULL,
    "stable_close" DOUBLE PRECISION NOT NULL,
    "stable_high" DOUBLE PRECISION NOT NULL,
    "stable_low" DOUBLE PRECISION NOT NULL,
    "stable_ma_open" DOUBLE PRECISION NOT NULL,
    "stable_ma_close" DOUBLE PRECISION NOT NULL,
    "stable_ma_high" DOUBLE PRECISION NOT NULL,
    "stable_ma_low" DOUBLE PRECISION NOT NULL,
    "zyield_price_open" DOUBLE PRECISION NOT NULL,
    "zyield_price_close" DOUBLE PRECISION NOT NULL,
    "zyield_price_high" DOUBLE PRECISION NOT NULL,
    "zyield_price_low" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_open" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_close" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_high" DOUBLE PRECISION NOT NULL,
    "zeph_in_reserve_low" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_open" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_close" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_high" DOUBLE PRECISION NOT NULL,
    "zsd_in_yield_reserve_low" DOUBLE PRECISION NOT NULL,
    "zeph_circ_open" DOUBLE PRECISION NOT NULL,
    "zeph_circ_close" DOUBLE PRECISION NOT NULL,
    "zeph_circ_high" DOUBLE PRECISION NOT NULL,
    "zeph_circ_low" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_open" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_close" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_high" DOUBLE PRECISION NOT NULL,
    "zephusd_circ_low" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_open" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_close" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_high" DOUBLE PRECISION NOT NULL,
    "zephrsv_circ_low" DOUBLE PRECISION NOT NULL,
    "zyield_circ_open" DOUBLE PRECISION NOT NULL,
    "zyield_circ_close" DOUBLE PRECISION NOT NULL,
    "zyield_circ_high" DOUBLE PRECISION NOT NULL,
    "zyield_circ_low" DOUBLE PRECISION NOT NULL,
    "assets_open" DOUBLE PRECISION NOT NULL,
    "assets_close" DOUBLE PRECISION NOT NULL,
    "assets_high" DOUBLE PRECISION NOT NULL,
    "assets_low" DOUBLE PRECISION NOT NULL,
    "assets_ma_open" DOUBLE PRECISION NOT NULL,
    "assets_ma_close" DOUBLE PRECISION NOT NULL,
    "assets_ma_high" DOUBLE PRECISION NOT NULL,
    "assets_ma_low" DOUBLE PRECISION NOT NULL,
    "liabilities_open" DOUBLE PRECISION NOT NULL,
    "liabilities_close" DOUBLE PRECISION NOT NULL,
    "liabilities_high" DOUBLE PRECISION NOT NULL,
    "liabilities_low" DOUBLE PRECISION NOT NULL,
    "equity_open" DOUBLE PRECISION NOT NULL,
    "equity_close" DOUBLE PRECISION NOT NULL,
    "equity_high" DOUBLE PRECISION NOT NULL,
    "equity_low" DOUBLE PRECISION NOT NULL,
    "equity_ma_open" DOUBLE PRECISION NOT NULL,
    "equity_ma_close" DOUBLE PRECISION NOT NULL,
    "equity_ma_high" DOUBLE PRECISION NOT NULL,
    "equity_ma_low" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_open" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_close" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_high" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_low" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_open" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_close" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_high" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_ma_low" DOUBLE PRECISION NOT NULL,
    "conversion_transactions_count" INTEGER NOT NULL,
    "yield_conversion_transactions_count" INTEGER NOT NULL,
    "mint_reserve_count" INTEGER NOT NULL,
    "mint_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephrsv" DOUBLE PRECISION NOT NULL,
    "redeem_reserve_count" INTEGER NOT NULL,
    "redeem_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd" DOUBLE PRECISION NOT NULL,
    "mint_stable_count" INTEGER NOT NULL,
    "mint_stable_volume" DOUBLE PRECISION NOT NULL,
    "redeem_stable_count" INTEGER NOT NULL,
    "redeem_stable_volume" DOUBLE PRECISION NOT NULL,
    "fees_zeph" DOUBLE PRECISION NOT NULL,
    "mint_yield_count" INTEGER NOT NULL,
    "mint_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zyield" DOUBLE PRECISION NOT NULL,
    "redeem_yield_count" INTEGER NOT NULL,
    "redeem_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd_yield" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "protocol_stats_daily_pkey" PRIMARY KEY ("window_start")
);

-- CreateTable
CREATE TABLE "reserve_snapshots" (
    "previous_height" INTEGER NOT NULL,
    "reserve_height" INTEGER NOT NULL,
    "captured_at" TIMESTAMP(3) NOT NULL,
    "hf_version" INTEGER NOT NULL,
    "zeph_reserve_atoms" TEXT NOT NULL,
    "zeph_reserve" DOUBLE PRECISION NOT NULL,
    "zsd_circ_atoms" TEXT NOT NULL,
    "zsd_circ" DOUBLE PRECISION NOT NULL,
    "zrs_circ_atoms" TEXT NOT NULL,
    "zrs_circ" DOUBLE PRECISION NOT NULL,
    "zyield_circ_atoms" TEXT NOT NULL,
    "zyield_circ" DOUBLE PRECISION NOT NULL,
    "zsd_yield_reserve_atoms" TEXT NOT NULL,
    "zsd_yield_reserve" DOUBLE PRECISION NOT NULL,
    "reserve_ratio_atoms" TEXT NOT NULL,
    "reserve_ratio" DOUBLE PRECISION,
    "reserve_ratio_ma_atoms" TEXT,
    "reserve_ratio_ma" DOUBLE PRECISION,
    "pricingRecord" JSONB,
    "raw" JSONB,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "reserve_snapshots_pkey" PRIMARY KEY ("previous_height")
);

-- CreateTable
CREATE TABLE "reserve_mismatch_reports" (
    "block_height" INTEGER NOT NULL,
    "reserve_height" INTEGER NOT NULL,
    "mismatch" BOOLEAN NOT NULL,
    "source" TEXT NOT NULL,
    "source_height" INTEGER,
    "snapshot_path" TEXT,
    "diffs" JSONB NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "reserve_mismatch_reports_pkey" PRIMARY KEY ("block_height")
);

-- CreateTable
CREATE TABLE "totals" (
    "id" INTEGER NOT NULL DEFAULT 1,
    "conversion_transactions" DOUBLE PRECISION NOT NULL,
    "yield_conversion_transactions" DOUBLE PRECISION NOT NULL,
    "mint_reserve_count" DOUBLE PRECISION NOT NULL,
    "mint_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephrsv" DOUBLE PRECISION NOT NULL,
    "redeem_reserve_count" DOUBLE PRECISION NOT NULL,
    "redeem_reserve_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd" DOUBLE PRECISION NOT NULL,
    "mint_stable_count" DOUBLE PRECISION NOT NULL,
    "mint_stable_volume" DOUBLE PRECISION NOT NULL,
    "redeem_stable_count" DOUBLE PRECISION NOT NULL,
    "redeem_stable_volume" DOUBLE PRECISION NOT NULL,
    "fees_zeph" DOUBLE PRECISION NOT NULL,
    "mint_yield_count" DOUBLE PRECISION NOT NULL,
    "mint_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zyield" DOUBLE PRECISION NOT NULL,
    "redeem_yield_count" DOUBLE PRECISION NOT NULL,
    "redeem_yield_volume" DOUBLE PRECISION NOT NULL,
    "fees_zephusd_yield" DOUBLE PRECISION NOT NULL,
    "miner_reward" DOUBLE PRECISION NOT NULL,
    "governance_reward" DOUBLE PRECISION NOT NULL,
    "reserve_reward" DOUBLE PRECISION NOT NULL,
    "yield_reward" DOUBLE PRECISION NOT NULL,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "totals_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "scanner_state" (
    "key" TEXT NOT NULL,
    "value" TEXT NOT NULL,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "scanner_state_pkey" PRIMARY KEY ("key")
);

-- CreateTable
CREATE TABLE "live_stats_cache" (
    "id" INTEGER NOT NULL DEFAULT 1,
    "payload" JSONB NOT NULL,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "live_stats_cache_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "historical_returns" (
    "range" "HistoricalReturnRange" NOT NULL,
    "return_pct" DOUBLE PRECISION NOT NULL,
    "zsd_accrued" DOUBLE PRECISION NOT NULL,
    "effective_apy" DOUBLE PRECISION,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "historical_returns_pkey" PRIMARY KEY ("range")
);

-- CreateTable
CREATE TABLE "projected_returns" (
    "timeframe" "ProjectionTimeframe" NOT NULL,
    "scenario" "ProjectionScenario" NOT NULL,
    "zys_price" DOUBLE PRECISION NOT NULL,
    "return_pct" DOUBLE PRECISION NOT NULL,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "projected_returns_pkey" PRIMARY KEY ("timeframe","scenario")
);

-- CreateTable
CREATE TABLE "apy_history" (
    "id" BIGSERIAL NOT NULL,
    "timestamp" INTEGER NOT NULL,
    "block_height" INTEGER NOT NULL,
    "return_pct" DOUBLE PRECISION NOT NULL,
    "zys_price" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "apy_history_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "zys_price_history" (
    "id" BIGSERIAL NOT NULL,
    "timestamp" INTEGER NOT NULL,
    "block_height" INTEGER NOT NULL,
    "zys_price" DOUBLE PRECISION NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "zys_price_history_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "idx_ct_block_height" ON "transactions"("block_height");

-- CreateIndex
CREATE INDEX "idx_ct_block_timestamp" ON "transactions"("block_timestamp");

-- CreateIndex
CREATE INDEX "idx_apy_timestamp" ON "apy_history"("timestamp");

-- CreateIndex
CREATE UNIQUE INDEX "zys_price_history_block_height_key" ON "zys_price_history"("block_height");

-- CreateIndex
CREATE INDEX "idx_zys_timestamp" ON "zys_price_history"("timestamp");
