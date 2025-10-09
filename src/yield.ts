// Stats for ZSD Yield
// Yield Reserve circ, ZYS price and circ, yield conversions count and fees, are all available in /stats and are handled in aggregator.ts
// This is for populating historical returns and projected returns.
import redis from "./redis";
import { UNAUDITABLE_ZEPH_MINT } from "./constants";
import {
  AggregatedData,
  getAggregatedProtocolStatsFromRedis,
  getCurrentBlockHeight,
  getLatestProtocolStats,
  getPricingRecordFromBlock,
  getRedisHeight,
  getReserveInfo,
} from "./utils";
import {
  logHistoricalReturns,
  logProjectedAccruals,
  logProjectedAssumptions,
  logProjectedBaseStats,
  logProjectedReturns,
} from "./logger";

const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;
const VERSION_2_3_0_HF_V11_BLOCK_HEIGHT = 536000; // Post Audit, asset type changes.
const BLOCKS_PER_DAY = 720;
const DAYS_PER_MONTH = 30;
const MONTHS_PER_YEAR = 12;
const BLOCKS_PER_YEAR = BLOCKS_PER_DAY * DAYS_PER_MONTH * MONTHS_PER_YEAR;

type HistoricalReturnKey =
  | "lastBlock"
  | "oneDay"
  | "oneWeek"
  | "oneMonth"
  | "threeMonths"
  | "oneYear"
  | "allTime";

type HistoricalReturnEntry = {
  return: number;
  ZSDAccrued: number;
  effectiveApy: number | null;
};

type HistoricalReturns = Record<HistoricalReturnKey, HistoricalReturnEntry>;

const HISTORICAL_TIMEFRAME_BLOCKS: Record<Exclude<HistoricalReturnKey, "allTime">, number> = {
  lastBlock: 1,
  oneDay: BLOCKS_PER_DAY,
  oneWeek: BLOCKS_PER_DAY * 7,
  oneMonth: BLOCKS_PER_DAY * DAYS_PER_MONTH,
  threeMonths: BLOCKS_PER_DAY * DAYS_PER_MONTH * 3,
  oneYear: BLOCKS_PER_YEAR,
};

function calculateEffectiveApy(returnPercentage: number, timeframeBlocks: number): number | null {
  if (!Number.isFinite(returnPercentage) || timeframeBlocks <= 0) {
    return null;
  }

  const periodsPerYear = BLOCKS_PER_YEAR / timeframeBlocks;
  if (!Number.isFinite(periodsPerYear) || periodsPerYear <= 0) {
    return null;
  }

  const returnDecimal = returnPercentage / 100;
  const base = 1 + returnDecimal;

  if (base <= 0) {
    return null;
  }

  const apy = Math.pow(base, periodsPerYear) - 1;
  if (!Number.isFinite(apy)) {
    return null;
  }

  return Number((apy * 100).toFixed(4));
}

// Historical Returns:
export async function determineHistoricalReturns() {
  // ----------------------------------------------------------
  // Last block       [0.001% | +1 ZSD from block reward]
  // 1 Day            [0.01%  | +720 ZSD from block reward]
  // 1 Week           [0.07%  | +5040 ZSD from block reward]
  // 1 Month          [0.3%   | +21600 ZSD from block reward]
  // 3 Months         [0.9%   | +64800 ZSD from block reward]
  // 1 Year           [3.65%  | +262800 ZSD from block reward]
  // ALL TIME         [3.65%  | +262800 ZSD from block reward]
  // -----------------------------------------------------------

  try {
    const currentBlockHeight = await getRedisHeight();

    if (currentBlockHeight < VERSION_2_HF_V6_BLOCK_HEIGHT) {
      console.log("BEFORE 2.0.0 FORK HEIGHT");
      return
    }

    // Determine ZYS gains by referencing the "yield_price" in the "pricing_records" key for the block height(s). and comparing to now.
    let currentPricingRecord = await getPricingRecordFromBlock(currentBlockHeight);

    const currentZYSPrice = currentPricingRecord ? currentPricingRecord.yield_price : 0;
    if (!currentZYSPrice) {
      console.log("No valid ZYS price found, ending processing historical returns");
      return;
    }

    const previousBlockHeight = currentBlockHeight - 1;
    const onedayagoBlockHeight = currentBlockHeight - 720;
    const oneweekagoBlockHeight = currentBlockHeight - 720 * 7;
    const onemonthagoBlockHeight = currentBlockHeight - 720 * 30;
    const threemonthsagoBlockHeight = currentBlockHeight - 720 * 30 * 3;
    const oneyearagoBlockHeight = currentBlockHeight - 720 * 30 * 12;

    // get the "yield_price" from the pricing records for the block heights, we could have missing rates for some pricing records although it is unlikely.
    // for now if we get any missing pricing records we will just end the processing to wait until the next run to update redis yield info.

    const previousPricingRecord = await getPricingRecordFromBlock(Math.max(previousBlockHeight, VERSION_2_HF_V6_BLOCK_HEIGHT));
    const onedayagoPricingRecord = await getPricingRecordFromBlock(Math.max(onedayagoBlockHeight, VERSION_2_HF_V6_BLOCK_HEIGHT));
    const oneweekagoPricingRecord = await getPricingRecordFromBlock(Math.max(oneweekagoBlockHeight, VERSION_2_HF_V6_BLOCK_HEIGHT));
    const onemonthagoPricingRecord = await getPricingRecordFromBlock(Math.max(onemonthagoBlockHeight, VERSION_2_HF_V6_BLOCK_HEIGHT));
    const threemonthsagoPricingRecord = await getPricingRecordFromBlock(Math.max(threemonthsagoBlockHeight, VERSION_2_HF_V6_BLOCK_HEIGHT));
    const oneyearagoPricingRecord = await getPricingRecordFromBlock(Math.max(oneyearagoBlockHeight, VERSION_2_HF_V6_BLOCK_HEIGHT));

    if (!previousPricingRecord || !onedayagoPricingRecord || !oneweekagoPricingRecord || !onemonthagoPricingRecord || !threemonthsagoPricingRecord || !oneyearagoPricingRecord) {
      console.log("Missing pricing records, ending processing historical returns");
      console.log("--------------------");
      console.log("Current Pricing Record:");
      console.log(currentPricingRecord);
      console.log("--------------------");
      console.log("Previous Pricing Record:");
      console.log(previousPricingRecord);
      console.log("--------------------");
      return;
    }

    const previousZYSPrice = previousPricingRecord.yield_price;
    const onedayagoZYSPrice = onedayagoPricingRecord.yield_price;
    const oneweekagoZYSPrice = oneweekagoPricingRecord.yield_price;
    const onemonthagoZYSPrice = onemonthagoPricingRecord.yield_price;
    const threemonthsagoZYSPrice = threemonthsagoPricingRecord.yield_price;
    const oneyearagoZYSPrice = oneyearagoPricingRecord.yield_price;
    const initialZYSPrice = 1000000000000; // This is the initial price of ZYS in ZSD at the start of the ZSD Yield Update at block 360000.

    if (!previousZYSPrice || !onedayagoZYSPrice || !oneweekagoZYSPrice || !onemonthagoZYSPrice || !threemonthsagoZYSPrice || !oneyearagoZYSPrice) {
      console.log("Missing ZYS prices, ending processing historical returns");
      console.log("--------------------");
      console.log("Current Pricing Record:");
      console.log(currentPricingRecord);
      console.log("--------------------");
      console.log("Previous Pricing Record:");
      console.log(previousPricingRecord);
      console.log("--------------------");
      return;
    }


    // Calculate the %age returns
    const previousReturn = ((currentZYSPrice - previousZYSPrice) / previousZYSPrice) * 100;
    const onedayagoReturn = ((currentZYSPrice - onedayagoZYSPrice) / onedayagoZYSPrice) * 100;
    const oneweekagoReturn = ((currentZYSPrice - oneweekagoZYSPrice) / oneweekagoZYSPrice) * 100;
    const onemonthagoReturn = ((currentZYSPrice - onemonthagoZYSPrice) / onemonthagoZYSPrice) * 100;
    const threemonthsagoReturn = ((currentZYSPrice - threemonthsagoZYSPrice) / threemonthsagoZYSPrice) * 100;
    const oneyearagoReturn = ((currentZYSPrice - oneyearagoZYSPrice) / oneyearagoZYSPrice) * 100;
    const alltimeReturn = ((currentZYSPrice - initialZYSPrice) / initialZYSPrice) * 100;

    /* 
    / DETERMINE HOW MUCH ZSD WAS AUTO-MINTED FROM THE ZEPH REWARD
    */

    // We do this when we are aggregating the data, so aggreation for this block should be done before this function is called.
    // DONE:
    // blockData.zsd_minted_for_yield = zsd_auto_minted;
    // blockData.zsd_accrued_in_yield_reserve_from_yield_reward += zsd_auto_minted;
    // We check RR's at each block to determine if zsd was auto-minted from the zeph reward.

    // Get all time zsd auto-minted from the zeph reward from zsd_accrued_in_yield_reserve_from_yield_reward in latest protocol_stats

    // Fetch current block's data to determine all time zsd auto-minted from the zeph reward
    const currentProtocolStatsDataJson = await redis.hget("protocol_stats", (currentBlockHeight).toString());
    let currentProtocolStatsData = currentProtocolStatsDataJson ? JSON.parse(currentProtocolStatsDataJson) : {};

    const total_zsd_accrued_in_yield_reserve_from_yield_reward = currentProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;
    if (!total_zsd_accrued_in_yield_reserve_from_yield_reward) {
      console.log("Error getting currentProtocolStats - zsd_accrued_in_yield_reserve_from_yield_reward not found, ending processing historical returns");
      return;
    }

    const alltimeZSDAccrued = total_zsd_accrued_in_yield_reserve_from_yield_reward;

    // Last block
    const previousProtocolStatsDataJson = await redis.hget("protocol_stats", (previousBlockHeight).toString());
    let previousProtocolStatsData = previousProtocolStatsDataJson ? JSON.parse(previousProtocolStatsDataJson) : {};
    const previousZSDAccrued = alltimeZSDAccrued - previousProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;

    // 1 Day
    const onedayagoProtocolStatsDataJson = await redis.hget("protocol_stats", (onedayagoBlockHeight).toString());
    let onedayagoProtocolStatsData = onedayagoProtocolStatsDataJson ? JSON.parse(onedayagoProtocolStatsDataJson) : {};
    const onedayagoZSDAccrued = alltimeZSDAccrued - onedayagoProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;

    // 1 Week
    const oneweekagoProtocolStatsDataJson = await redis.hget("protocol_stats", (oneweekagoBlockHeight).toString());
    let oneweekagoProtocolStatsData = oneweekagoProtocolStatsDataJson ? JSON.parse(oneweekagoProtocolStatsDataJson) : {};
    const oneweekagoZSDAccrued = alltimeZSDAccrued - oneweekagoProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;

    // 1 Month
    const onemonthagoProtocolStatsDataJson = await redis.hget("protocol_stats", (onemonthagoBlockHeight).toString());
    let onemonthagoProtocolStatsData = onemonthagoProtocolStatsDataJson ? JSON.parse(onemonthagoProtocolStatsDataJson) : {};
    const onemonthagoZSDAccrued = alltimeZSDAccrued - onemonthagoProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;

    // 3 Months
    const threemonthsagoProtocolStatsDataJson = await redis.hget("protocol_stats", (threemonthsagoBlockHeight).toString());
    let threemonthsagoProtocolStatsData = threemonthsagoProtocolStatsDataJson ? JSON.parse(threemonthsagoProtocolStatsDataJson) : {};
    const threemonthsagoZSDAccrued = alltimeZSDAccrued - threemonthsagoProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;

    // 1 Year
    const oneyearagoProtocolStatsDataJson = await redis.hget("protocol_stats", (oneyearagoBlockHeight).toString());
    let oneyearagoProtocolStatsData = oneyearagoProtocolStatsDataJson ? JSON.parse(oneyearagoProtocolStatsDataJson) : {};
    const oneyearagoZSDAccrued = alltimeZSDAccrued - oneyearagoProtocolStatsData.zsd_accrued_in_yield_reserve_from_yield_reward;


    const historicalStats: HistoricalReturns = {
      lastBlock: { return: previousReturn, ZSDAccrued: previousZSDAccrued, effectiveApy: null },
      oneDay: { return: onedayagoReturn, ZSDAccrued: onedayagoZSDAccrued, effectiveApy: null },
      oneWeek: { return: oneweekagoReturn, ZSDAccrued: oneweekagoZSDAccrued, effectiveApy: null },
      oneMonth: { return: onemonthagoReturn, ZSDAccrued: onemonthagoZSDAccrued, effectiveApy: null },
      threeMonths: { return: threemonthsagoReturn, ZSDAccrued: threemonthsagoZSDAccrued, effectiveApy: null },
      oneYear: { return: oneyearagoReturn, ZSDAccrued: oneyearagoZSDAccrued, effectiveApy: null },
      allTime: { return: alltimeReturn, ZSDAccrued: alltimeZSDAccrued, effectiveApy: null }
    };

    (Object.entries(HISTORICAL_TIMEFRAME_BLOCKS) as Array<[
      Exclude<HistoricalReturnKey, "allTime">,
      number
    ]>).forEach(([key, timeframeBlocks]) => {
      const entry = historicalStats[key];
      if (!entry) {
        return;
      }

      const apy = calculateEffectiveApy(entry.return, timeframeBlocks);
      if (apy !== null) {
        entry.effectiveApy = apy;
      }
    });

    const blocksSinceLaunch = currentBlockHeight - VERSION_2_HF_V6_BLOCK_HEIGHT;
    if (blocksSinceLaunch > 0) {
      const apy = calculateEffectiveApy(historicalStats.allTime.return, blocksSinceLaunch);
      if (apy !== null) {
        historicalStats.allTime.effectiveApy = apy;
      }
    }

    // save to redis
    await redis.set("historical_returns", JSON.stringify(historicalStats));

    logHistoricalReturns([
      {
        period: "Last block",
        returnPct: historicalStats.lastBlock.return,
        zsdAccrued: historicalStats.lastBlock.ZSDAccrued,
        apy: historicalStats.lastBlock.effectiveApy,
      },
      {
        period: "1 Day",
        returnPct: historicalStats.oneDay.return,
        zsdAccrued: historicalStats.oneDay.ZSDAccrued,
        apy: historicalStats.oneDay.effectiveApy,
      },
      {
        period: "1 Week",
        returnPct: historicalStats.oneWeek.return,
        zsdAccrued: historicalStats.oneWeek.ZSDAccrued,
        apy: historicalStats.oneWeek.effectiveApy,
      },
      {
        period: "1 Month",
        returnPct: historicalStats.oneMonth.return,
        zsdAccrued: historicalStats.oneMonth.ZSDAccrued,
        apy: historicalStats.oneMonth.effectiveApy,
      },
      {
        period: "3 Months",
        returnPct: historicalStats.threeMonths.return,
        zsdAccrued: historicalStats.threeMonths.ZSDAccrued,
        apy: historicalStats.threeMonths.effectiveApy,
      },
      {
        period: "1 Year",
        returnPct: historicalStats.oneYear.return,
        zsdAccrued: historicalStats.oneYear.ZSDAccrued,
        apy: historicalStats.oneYear.effectiveApy,
      },
      {
        period: "All Time",
        returnPct: historicalStats.allTime.return,
        zsdAccrued: historicalStats.allTime.ZSDAccrued,
        apy: historicalStats.allTime.effectiveApy,
      },
    ]);


  }
  catch (error) {
    console.error("Error determining historical returns:", error);
  }

}



// Projected Returns:
export async function determineProjectedReturns(test = false) {
  async function getStats(test = false) {
    // test = true // uncomment to force dummy data
    if (test) {
      // return dummy protocol stats for testing route
      const dummyProtocolStats = {
        currentBlockHeight: VERSION_2_HF_V6_BLOCK_HEIGHT,
        zeph_price: 1.40,
        zys_price: 1.00,
        zsd_circ: 449_132.29,
        zys_circ: 238_861,
        zsd_in_reserve: 239_119,
        reserve_ratio: 5.38,
        usedFallbackPricing: false,
      };
      return dummyProtocolStats;
    }

    const currentBlockHeight = await getRedisHeight();
    // OLD: Using Protocol Stats from Redis

    // const currentProtocolStats = await redis.hget("protocol_stats", currentBlockHeight.toString());
    // const currentProtocolStatsData: ProtocolStats = currentProtocolStats ? JSON.parse(currentProtocolStats) : {};
    // if (!currentProtocolStatsData) {
    //     console.log("Error in determineProjectedReturns getting currentProtocolStatsData, ending processing projected returns");
    //     return { currentBlockHeight: 0, zeph_price: 0, zys_price: 0, zsd_circ: 0, zys_circ: 0, zsd_in_reserve: 0, reserve_ratio: 0 };
    // }

    // const zeph_price = currentProtocolStatsData.spot
    // const zsd_circ = currentProtocolStatsData.zephusd_circ;
    // const reserve_ratio = currentProtocolStatsData.reserve_ratio;

    // // Pre 2.0.0 fork height these will be 0
    // let zys_price = currentProtocolStatsData.yield_price;
    // let zys_circ = currentProtocolStatsData.zyield_circ;
    // let zsd_in_reserve = currentProtocolStatsData.zsd_in_yield_reserve;

    // Use Reserve Info from Daemon
    const reserveInfo = await getReserveInfo();

    // leave early if we don't have reserve info
    if (!reserveInfo || !reserveInfo.result) {
      console.log("Error in determineProjectedReturns getting reserveInfo, ending processing projected returns");
      return {
        currentBlockHeight: 0,
        zeph_price: 0,
        zys_price: 0,
        zsd_circ: 0,
        zys_circ: 0,
        zsd_in_reserve: 0,
        reserve_ratio: 0,
        usedFallbackPricing: false,
      };
    }

    const DEATOMIZE = 10 ** -12;

    let spotAtoms = reserveInfo.result.pr?.spot;
    let yieldPriceAtoms = reserveInfo.result.pr?.yield_price;
    let usedFallbackPricing = false;

    if (
      typeof spotAtoms !== "number" ||
      !Number.isFinite(spotAtoms) ||
      typeof yieldPriceAtoms !== "number" ||
      !Number.isFinite(yieldPriceAtoms)
    ) {
      const latestStats = await getLatestProtocolStats();
      if (latestStats) {
        if (typeof spotAtoms !== "number" || !Number.isFinite(spotAtoms)) {
          spotAtoms = latestStats.spot;
          usedFallbackPricing = true;
        }
        if (typeof yieldPriceAtoms !== "number" || !Number.isFinite(yieldPriceAtoms)) {
          yieldPriceAtoms = latestStats.yield_price;
          usedFallbackPricing = true;
        }
      }
    }

    if (
      typeof spotAtoms !== "number" ||
      !Number.isFinite(spotAtoms) ||
      typeof yieldPriceAtoms !== "number" ||
      !Number.isFinite(yieldPriceAtoms)
    ) {
      console.log("Error in determineProjectedReturns getting pricing data, ending processing projected returns");
      return {
        currentBlockHeight: 0,
        zeph_price: 0,
        zys_price: 0,
        zsd_circ: 0,
        zys_circ: 0,
        zsd_in_reserve: 0,
        reserve_ratio: 0,
        usedFallbackPricing: false,
      };
    }

    if (usedFallbackPricing) {
    }

    const zeph_price = Number((spotAtoms * DEATOMIZE).toFixed(4));
    let zys_price = Number((yieldPriceAtoms * DEATOMIZE).toFixed(4));
    let zsd_circ = Number((Number(reserveInfo.result.num_stables) * DEATOMIZE).toFixed(4));
    let zys_circ = Number((Number(reserveInfo.result.num_zyield) * DEATOMIZE).toFixed(4));
    let zsd_in_reserve = Number((Number(reserveInfo.result.zyield_reserve) * DEATOMIZE).toFixed(4));
    const reserve_ratio = Number(reserveInfo.result.reserve_ratio);

    if (currentBlockHeight < VERSION_2_HF_V6_BLOCK_HEIGHT) {
      // Setting some values in order to calculate projected returns pre-fork
      zys_price = 1;
      zys_circ = zsd_circ / 2;
      zsd_in_reserve = zsd_circ / 2;
    }

    return {
      currentBlockHeight,
      zeph_price,
      zys_price,
      zsd_circ,
      zys_circ,
      zsd_in_reserve,
      reserve_ratio,
      usedFallbackPricing,
    };
  }
  // ----------------------------------------------------------
  // 1 Week           [Low: 0.60% | Simple: 1.00% | High: 2.60%]
  // 1 Month          [Low: 1.90% | Simple: 3.00% | High: 5.60%]
  // 3 Months         [Low: 5.50% | Simple: 12.50% | High: 20.60%]
  // 6 Months         [Low: 10.50% | Simple: 20.50% | High: 30.60%]
  // 1 Year           [Low: 20.50% | Simple: 30.50% | High: 40.60%]
  // -----------------------------------------------------------
  console.log(`[projected] running (test=${test})`);

  // We need to calcuate the projected returns based on the amount of zeph emmissions that will occur in the future.
  // for a simple projection we can use pre-calculated zeph emmissions for each time period and assume competition for the yield will remain the same, and zeph's price will remain the same.
  // for a low projection we can assume a lower zeph price and higher competition for the yield.
  // for a high projection we can assume a higher zeph price and lower competition for the yield.
  // Price is an in-built factor in competition due to the reserve ratio restrictions in the djed stablecoin system.
  // We can determine a high competition state where a higher percentage of zsd is staked (compared to current, up to 100%) and the reserve ratio is low
  // We can determine a low competition state where a lower percentage of zsd is staked (compared to current, down to say 50%) and the reserve ratio is high


  const {
    currentBlockHeight,
    zeph_price,
    zys_price,
    zsd_circ,
    zys_circ,
    zsd_in_reserve,
    reserve_ratio,
    usedFallbackPricing,
  } =
    await getStats(test);
  // check none of the values are 0
  if (!currentBlockHeight || !zeph_price || !zys_price || !zsd_circ || !zys_circ || !zsd_in_reserve || !reserve_ratio) {
    console.log("Error in determineProjectedReturns getting stats, ending processing projected returns");
    console.log(`currentBlockHeight: ${currentBlockHeight}
                    zeph_price: ${zeph_price}
                    zys_price: ${zys_price}
                    zsd_circ: ${zsd_circ}
                    zys_circ: ${zys_circ}
                    zsd_in_reserve: ${zsd_in_reserve}
                    reserve_ratio: ${reserve_ratio}`);
    return;
  }

  logProjectedBaseStats({
    blockHeight: currentBlockHeight,
    zephPrice: zeph_price,
    zysPrice: zys_price,
    zsdCirc: zsd_circ,
    zysCirc: zys_circ,
    zsdReserve: zsd_in_reserve,
    reserveRatio: reserve_ratio,
    fallbackPricing: usedFallbackPricing,
  });


  const zeph_price_200RR = (zeph_price / reserve_ratio) * 2;
  const zeph_price_800RR = (zeph_price / reserve_ratio) * 8;

  const precalcuatedBlockRewards = await getPrecalculatedBlockRewards(currentBlockHeight);
  if (!precalcuatedBlockRewards) {
    console.log("Error in determineProjectedReturns getting precalcuatedBlockRewards, ending processing projected returns");
    return;
  }

  const oneweek_block_height = currentBlockHeight + 720;
  const onemonth_block_height = currentBlockHeight + (720 * 30);
  const threemonths_block_height = currentBlockHeight + (720 * 30 * 3);
  const sixmonths_block_height = currentBlockHeight + (720 * 30 * 6);
  const oneyear_block_height = currentBlockHeight + (720 * 30 * 12);

  // work out how much zsd will be minted in the future based on the current price of zeph.
  let accured_zsd_total = { low: 0, simple: 0, high: 0 };

  // let oneweek_accured_zsd = 0;
  // let onemonth_accured_zsd = 0;
  // let threemonths_accured_zsd = 0;
  // let sixmonths_accured_zsd = 0;
  // let oneyear_accured_zsd = 0;

  let oneweek_accured_zsd = { low: 0, simple: 0, high: 0 };
  let onemonth_accured_zsd = { low: 0, simple: 0, high: 0 };
  let threemonths_accured_zsd = { low: 0, simple: 0, high: 0 };
  let sixmonths_accured_zsd = { low: 0, simple: 0, high: 0 };
  let oneyear_accured_zsd = { low: 0, simple: 0, high: 0 };

  for (let block = currentBlockHeight; block <= oneyear_block_height; block++) {
    const total_block_reward = precalcuatedBlockRewards[block].block_reward;
    const yield_reward = total_block_reward * 0.05; // 5%

    const zsd_auto_minted_simple = yield_reward * zeph_price;
    const zsd_auto_minted_low = yield_reward * zeph_price_200RR;
    const zsd_auto_minted_high = yield_reward * zeph_price_800RR;


    accured_zsd_total.simple += zsd_auto_minted_simple;
    accured_zsd_total.low += zsd_auto_minted_low;
    accured_zsd_total.high += zsd_auto_minted_high;

    if (block === oneweek_block_height) {
      oneweek_accured_zsd.simple = accured_zsd_total.simple;
      oneweek_accured_zsd.low = accured_zsd_total.low;
      oneweek_accured_zsd.high = accured_zsd_total.high;
    }
    if (block === onemonth_block_height) {
      onemonth_accured_zsd.simple = accured_zsd_total.simple;
      onemonth_accured_zsd.low = accured_zsd_total.low;
      onemonth_accured_zsd.high = accured_zsd_total.high;
    }
    if (block === threemonths_block_height) {
      threemonths_accured_zsd.simple = accured_zsd_total.simple;
      threemonths_accured_zsd.low = accured_zsd_total.low;
      threemonths_accured_zsd.high = accured_zsd_total.high;
    }
    if (block === sixmonths_block_height) {
      sixmonths_accured_zsd.simple = accured_zsd_total.simple;
      sixmonths_accured_zsd.low = accured_zsd_total.low;
      sixmonths_accured_zsd.high = accured_zsd_total.high;
    }
    if (block === oneyear_block_height) {
      oneyear_accured_zsd.simple = accured_zsd_total.simple;
      oneyear_accured_zsd.low = accured_zsd_total.low;
      oneyear_accured_zsd.high = accured_zsd_total.high;
    }
  }

  logProjectedAccruals([
    { period: "1 Week", low: oneweek_accured_zsd.low, simple: oneweek_accured_zsd.simple, high: oneweek_accured_zsd.high },
    {
      period: "1 Month",
      low: onemonth_accured_zsd.low,
      simple: onemonth_accured_zsd.simple,
      high: onemonth_accured_zsd.high,
    },
    {
      period: "3 Months",
      low: threemonths_accured_zsd.low,
      simple: threemonths_accured_zsd.simple,
      high: threemonths_accured_zsd.high,
    },
    {
      period: "6 Months",
      low: sixmonths_accured_zsd.low,
      simple: sixmonths_accured_zsd.simple,
      high: sixmonths_accured_zsd.high,
    },
    {
      period: "1 Year",
      low: oneyear_accured_zsd.low,
      simple: oneyear_accured_zsd.simple,
      high: oneyear_accured_zsd.high,
    },
  ]);


  // Let determine what the simple projection of ZYS price would be
  // ZYS price = ZSD in reserve / ZYS in circulation
  const simple_projection_oneweek_zys_price = (zsd_in_reserve + oneweek_accured_zsd.simple) / zys_circ;
  const simple_projection_onemonth_zys_price = (zsd_in_reserve + onemonth_accured_zsd.simple) / zys_circ;
  const simple_projection_threemonths_zys_price = (zsd_in_reserve + threemonths_accured_zsd.simple) / zys_circ;
  const simple_projection_sixmonths_zys_price = (zsd_in_reserve + sixmonths_accured_zsd.simple) / zys_circ;
  const simple_projection_oneyear_zys_price = (zsd_in_reserve + oneyear_accured_zsd.simple) / zys_circ;

  // Calculate lower bound
  // Reserve Ratio = 200%
  // For RR to be 200% we need to calculate what the ZEPH price is to get the reserve ratio to 200%
  // percentage_of_zsd_staked = 100%

  const zsd_in_reserve_high_competition = zsd_circ;
  let additional_zys = 0;
  if (zys_circ < zsd_in_reserve_high_competition) {
    // we need to adjust so that the equivilant amount of zsd staked has zys minted
    additional_zys = zsd_in_reserve_high_competition / zys_price - zys_circ; // this is assuming that 100% of zsd is staked and needs to be adjusted if we change our low projection to <100% staked
  }


  const low_projection_oneweek_zys_price = (zsd_in_reserve_high_competition + oneweek_accured_zsd.low) / (zys_circ + additional_zys);
  const low_projection_onemonth_zys_price = (zsd_in_reserve_high_competition + onemonth_accured_zsd.low) / (zys_circ + additional_zys);
  const low_projection_threemonths_zys_price = (zsd_in_reserve_high_competition + threemonths_accured_zsd.low) / (zys_circ + additional_zys);
  const low_projection_sixmonths_zys_price = (zsd_in_reserve_high_competition + sixmonths_accured_zsd.low) / (zys_circ + additional_zys);
  const low_projection_oneyear_zys_price = (zsd_in_reserve_high_competition + oneyear_accured_zsd.low) / (zys_circ + additional_zys);

  const zsd_in_reserve_low_competition = zsd_circ / 2; // 50% of all ZSD is staked

  // zys price = (zsd staked in reserve + zsd accured in the future) / zys in circulation
  const simulated_zys_circ = zsd_in_reserve_low_competition / zys_price;
  // Calculate higher bound
  // Reserve Ratio = 800%
  const high_projection_oneweek_zys_price = (zsd_in_reserve_low_competition + oneweek_accured_zsd.high) / simulated_zys_circ;
  const high_projection_onemonth_zys_price = (zsd_in_reserve_low_competition + onemonth_accured_zsd.high) / simulated_zys_circ;
  const high_projection_threemonths_zys_price = (zsd_in_reserve_low_competition + threemonths_accured_zsd.high) / simulated_zys_circ;
  const high_projection_sixmonths_zys_price = (zsd_in_reserve_low_competition + sixmonths_accured_zsd.high) / simulated_zys_circ;
  const high_projection_oneyear_zys_price = (zsd_in_reserve_low_competition + oneyear_accured_zsd.high) / simulated_zys_circ;

  logProjectedAssumptions([
    { label: "zeph_price_200_rr", value: zeph_price_200RR },
    { label: "zeph_price_800_rr", value: zeph_price_800RR },
    { label: "additional_zys", value: additional_zys },
    { label: "zsd_reserve_low_comp", value: zsd_in_reserve_low_competition },
    { label: "simulated_zys_circ", value: simulated_zys_circ },
  ]);


  // Lets determine the percentage returns for each time period
  const simple_projection_oneweek_returns = ((simple_projection_oneweek_zys_price - zys_price) / zys_price) * 100;
  const simple_projection_onemonth_returns = ((simple_projection_onemonth_zys_price - zys_price) / zys_price) * 100;
  const simple_projection_threemonths_returns = ((simple_projection_threemonths_zys_price - zys_price) / zys_price) * 100;
  const simple_projection_sixmonths_returns = ((simple_projection_sixmonths_zys_price - zys_price) / zys_price) * 100;
  const simple_projection_oneyear_returns = ((simple_projection_oneyear_zys_price - zys_price) / zys_price) * 100;


  const low_projection_oneweek_returns = ((low_projection_oneweek_zys_price - zys_price) / zys_price) * 100;
  const low_projection_onemonth_returns = ((low_projection_onemonth_zys_price - zys_price) / zys_price) * 100;
  const low_projection_threemonths_returns = ((low_projection_threemonths_zys_price - zys_price) / zys_price) * 100;
  const low_projection_sixmonths_returns = ((low_projection_sixmonths_zys_price - zys_price) / zys_price) * 100;
  const low_projection_oneyear_returns = ((low_projection_oneyear_zys_price - zys_price) / zys_price) * 100;

  const high_projection_oneweek_returns = ((high_projection_oneweek_zys_price - zys_price) / zys_price) * 100;
  const high_projection_onemonth_returns = ((high_projection_onemonth_zys_price - zys_price) / zys_price) * 100;
  const high_projection_threemonths_returns = ((high_projection_threemonths_zys_price - zys_price) / zys_price) * 100;
  const high_projection_sixmonths_returns = ((high_projection_sixmonths_zys_price - zys_price) / zys_price) * 100;
  const high_projection_oneyear_returns = ((high_projection_oneyear_zys_price - zys_price) / zys_price) * 100;

  const projectedStats = {
    oneWeek: {
      low: {
        zys_price: parseFloat(low_projection_oneweek_zys_price.toFixed(4)),
        return: parseFloat(low_projection_oneweek_returns.toFixed(4))
      },
      simple: {
        zys_price: parseFloat(simple_projection_oneweek_zys_price.toFixed(4)),
        return: parseFloat(simple_projection_oneweek_returns.toFixed(4))
      },
      high: {
        zys_price: parseFloat(high_projection_oneweek_zys_price.toFixed(4)),
        return: parseFloat(high_projection_oneweek_returns.toFixed(4))
      }
    },
    oneMonth: {
      low: {
        zys_price: parseFloat(low_projection_onemonth_zys_price.toFixed(4)),
        return: parseFloat(low_projection_onemonth_returns.toFixed(4))
      },
      simple: {
        zys_price: parseFloat(simple_projection_onemonth_zys_price.toFixed(4)),
        return: parseFloat(simple_projection_onemonth_returns.toFixed(4))
      },
      high: {
        zys_price: parseFloat(high_projection_onemonth_zys_price.toFixed(4)),
        return: parseFloat(high_projection_onemonth_returns.toFixed(4))
      }
    },
    threeMonths: {
      low: {
        zys_price: parseFloat(low_projection_threemonths_zys_price.toFixed(4)),
        return: parseFloat(low_projection_threemonths_returns.toFixed(4))
      },
      simple: {
        zys_price: parseFloat(simple_projection_threemonths_zys_price.toFixed(4)),
        return: parseFloat(simple_projection_threemonths_returns.toFixed(4))
      },
      high: {
        zys_price: parseFloat(high_projection_threemonths_zys_price.toFixed(4)),
        return: parseFloat(high_projection_threemonths_returns.toFixed(4))
      }
    },
    sixMonths: {
      low: {
        zys_price: parseFloat(low_projection_sixmonths_zys_price.toFixed(4)),
        return: parseFloat(low_projection_sixmonths_returns.toFixed(4))
      },
      simple: {
        zys_price: parseFloat(simple_projection_sixmonths_zys_price.toFixed(4)),
        return: parseFloat(simple_projection_sixmonths_returns.toFixed(4))
      },
      high: {
        zys_price: parseFloat(high_projection_sixmonths_zys_price.toFixed(4)),
        return: parseFloat(high_projection_sixmonths_returns.toFixed(4))
      }
    },
    oneYear: {
      low: {
        zys_price: parseFloat(low_projection_oneyear_zys_price.toFixed(4)),
        return: parseFloat(low_projection_oneyear_returns.toFixed(4))
      },
      simple: {
        zys_price: parseFloat(simple_projection_oneyear_zys_price.toFixed(4)),
        return: parseFloat(simple_projection_oneyear_returns.toFixed(4))
      },
      high: {
        zys_price: parseFloat(high_projection_oneyear_zys_price.toFixed(4)),
        return: parseFloat(high_projection_oneyear_returns.toFixed(4))
      }
    }
  };


  logProjectedReturns([
    {
      period: "1 Week",
      lowAmount: projectedStats.oneWeek.low.zys_price,
      lowPct: projectedStats.oneWeek.low.return,
      simpleAmount: projectedStats.oneWeek.simple.zys_price,
      simplePct: projectedStats.oneWeek.simple.return,
      highAmount: projectedStats.oneWeek.high.zys_price,
      highPct: projectedStats.oneWeek.high.return,
    },
    {
      period: "1 Month",
      lowAmount: projectedStats.oneMonth.low.zys_price,
      lowPct: projectedStats.oneMonth.low.return,
      simpleAmount: projectedStats.oneMonth.simple.zys_price,
      simplePct: projectedStats.oneMonth.simple.return,
      highAmount: projectedStats.oneMonth.high.zys_price,
      highPct: projectedStats.oneMonth.high.return,
    },
    {
      period: "3 Months",
      lowAmount: projectedStats.threeMonths.low.zys_price,
      lowPct: projectedStats.threeMonths.low.return,
      simpleAmount: projectedStats.threeMonths.simple.zys_price,
      simplePct: projectedStats.threeMonths.simple.return,
      highAmount: projectedStats.threeMonths.high.zys_price,
      highPct: projectedStats.threeMonths.high.return,
    },
    {
      period: "6 Months",
      lowAmount: projectedStats.sixMonths.low.zys_price,
      lowPct: projectedStats.sixMonths.low.return,
      simpleAmount: projectedStats.sixMonths.simple.zys_price,
      simplePct: projectedStats.sixMonths.simple.return,
      highAmount: projectedStats.sixMonths.high.zys_price,
      highPct: projectedStats.sixMonths.high.return,
    },
    {
      period: "1 Year",
      lowAmount: projectedStats.oneYear.low.zys_price,
      lowPct: projectedStats.oneYear.low.return,
      simpleAmount: projectedStats.oneYear.simple.zys_price,
      simplePct: projectedStats.oneYear.simple.return,
      highAmount: projectedStats.oneYear.high.zys_price,
      highPct: projectedStats.oneYear.high.return,
    },
  ]);

  // save to redis
  if (!test) {
    await redis.set("projected_returns", JSON.stringify(projectedStats));
  }

}


async function getPrecalculatedBlockRewards(current_block_height: number) {

  // For pre 2.0.0 fork to do the projections before the fork
  if (current_block_height < VERSION_2_HF_V6_BLOCK_HEIGHT) {
    current_block_height = VERSION_2_HF_V6_BLOCK_HEIGHT;
  }
  // Calculate the block rewards for each block and save this info to redis if we don't already have it
  // We need to calculate over 1 year in advance to get the projected returns for 1 year.
  // We may as well calculate 5 years in advance.

  const oneyear_block_height = current_block_height + (720 * 30 * 12);
  console.log(`getPrecalculatedBlockRewards: current_block_height: ${current_block_height}, oneyear_block_height: ${oneyear_block_height}`);

  // check if the redis key "precalculated_block_rewards" exists
  let precalculatedBlockRewards = await redis.get("precalculated_block_rewards");

  if (precalculatedBlockRewards) {
    // ensure we have enough precalculated block rewards to cover the next year
    const precalculatedBlockRewardsJson = JSON.parse(precalculatedBlockRewards);
    const lastPreCalcBlockHeight = precalculatedBlockRewardsJson[precalculatedBlockRewardsJson.length - 1].block_height;
    // Ensure we have enough precalculated block rewards to cover the next year
    if (lastPreCalcBlockHeight >= oneyear_block_height) {
      return precalculatedBlockRewardsJson;
    }
  }

  console.log(`We don't have any/enough precalculated block rewards from block 0 to block ${oneyear_block_height}...`);
  // We haven't got the precalculated block rewards or we don't have enough to cover the next year, so we need to calculate them.
  const ending_pre_calc_block_height = current_block_height + (720 * 30 * 12 * 5); // 5 years in advance
  await precalculateBlockRewards(current_block_height, ending_pre_calc_block_height);

  // get the precalculated block rewards from redis
  precalculatedBlockRewards = await redis.get("precalculated_block_rewards");
  if (precalculatedBlockRewards) return JSON.parse(precalculatedBlockRewards);

  return null;
}

async function precalculateBlockRewards(current_block_height: number, ending_block_height: number) {

  console.log(`precalculateBlockRewards: current_block_height: ${current_block_height}, ending_block_height: ${ending_block_height}`);

  const starting_supply = 500_000; // 500k ZEPH (treasury)
  const emmission_speed_factor = 20;
  const tail_emmission_reward = 0.6; // 0.6 ZEPH per block

  const precalculatedBlockRewards = [];

  let current_supply = starting_supply;

  for (let block_height = 0; block_height <= ending_block_height; block_height++) {

    const block_reward = calculateBlockRewardWithSpeedFactor(current_supply, emmission_speed_factor, tail_emmission_reward);

    const blockData = {
      block_height: block_height,
      block_reward: block_reward
    };

    precalculatedBlockRewards.push(blockData);

    // Update the current supply
    if (block_height !== VERSION_2_3_0_HF_V11_BLOCK_HEIGHT + 1) {
      current_supply += block_reward;
    } else {
      console.log(`HFv11 detected at block ${block_height}, setting current supply to audited amount...`);
      console.log(`Current supply before HFv11: ${current_supply}`);

      const audited_zeph_amount = 7_828_285.273529857474;
      const minted_unauditable_zeph_amount = UNAUDITABLE_ZEPH_MINT; // Amount minted after HFv11
      current_supply = audited_zeph_amount + minted_unauditable_zeph_amount; // Audited amount at HFv11 plus unauditable mint

      console.log(`Current supply after HFv11: ${current_supply}`);
    }

  }

  // print out a few block rewards to ensure they are correct
  // block 1, block 1000, block 89300, block 100000, current block height
  console.log(`Block 1: ${precalculatedBlockRewards[1].block_reward}`);
  console.log(`Block 1000: ${precalculatedBlockRewards[1000].block_reward}`);
  console.log(`Block 89300: ${precalculatedBlockRewards[89300].block_reward}`);
  console.log(`Block 100000: ${precalculatedBlockRewards[100000].block_reward}`);
  console.log(`Current Block (${current_block_height}): ${precalculatedBlockRewards[current_block_height].block_reward}`);

  // save to redis
  await redis.set("precalculated_block_rewards", JSON.stringify(precalculatedBlockRewards));

}

// Function to calculate the block reward for a given supply
function calculateBlockRewardWithSpeedFactor(
  supply: number,
  emissionSpeedFactor: number,
  tailEmissionReward: number = 0.6
) {

  // Convert supply and tail emission reward to atomic units
  const atomicSupply = Math.floor(supply * 10 ** 12);

  // Block reward calculation formula (in atomic units)
  const atomicBaseReward = (BigInt(2 ** 64) - BigInt(1) - BigInt(atomicSupply)) >> BigInt(emissionSpeedFactor);

  // Convert the base reward back to standard units
  const baseRewardInStandardUnits = Number(atomicBaseReward) / 10 ** 12;

  // Ensure the block reward doesn't fall below the tail emission reward
  return Math.max(baseRewardInStandardUnits, tailEmissionReward);
}

export async function getHistoricalReturnsFromRedis(test = false) {
  if (test) {
    // return dummy historical stats for testing route
    const dummyHistoricalStats = {
      lastBlock: { return: 0.01, ZSDAccrued: 1, effectiveApy: 0.05 },
      oneDay: { return: 0.70, ZSDAccrued: 720, effectiveApy: 279.44 },
      oneWeek: { return: 2.60, ZSDAccrued: 5040, effectiveApy: 197.06 },
      oneMonth: { return: 3.90, ZSDAccrued: 21600, effectiveApy: 59.52 },
      threeMonths: { return: 12.50, ZSDAccrued: 64800, effectiveApy: 62.31 },
      oneYear: { return: 25.60, ZSDAccrued: 262800, effectiveApy: 25.60 },
      allTime: { return: 55.60, ZSDAccrued: 562800, effectiveApy: 55.60 },
    };

    return dummyHistoricalStats;

  }
  const historicalStats = await redis.get("historical_returns");
  if (!historicalStats) {
    return null;
  }
  return JSON.parse(historicalStats);
}


export async function getProjectedReturnsFromRedis(test = false) {

  if (test) {
    // return dummy projected stats for testing route
    const dummyProjectedStats = {
      oneWeek: { low: { zys_price: 1.010, return: 0.01 }, simple: { zys_price: 1.05, return: 0.05 }, high: { zys_price: 1.10, return: 0.10 } },
      oneMonth: { low: { zys_price: 1.05, return: 0.05 }, simple: { zys_price: 1.10, return: 0.10 }, high: { zys_price: 1.20, return: 0.20 } },
      threeMonths: { low: { zys_price: 1.10, return: 0.10 }, simple: { zys_price: 1.20, return: 0.20 }, high: { zys_price: 1.30, return: 0.30 } },
      sixMonths: { low: { zys_price: 1.20, return: 0.20 }, simple: { zys_price: 1.30, return: 0.30 }, high: { zys_price: 1.40, return: 0.40 } },
      oneYear: { low: { zys_price: 1.30, return: 0.30 }, simple: { zys_price: 1.40, return: 0.40 }, high: { zys_price: 1.50, return: 0.50 } },
    };

    return dummyProjectedStats;
  }

  const projectedStats = await redis.get("projected_returns");
  if (!projectedStats) {
    return null;
  }
  return JSON.parse(projectedStats);
}


export async function determineAPYHistory(reset = false) {
  console.log(`Determining APY history... reset: ${reset}`);
  if (reset) {
    // If reset is true, we want to clear the existing APY history in Redis
    await redis.del("apy_history");
    console.log("APY history reset in Redis.");
  }
  // Effectively what we need to do is use the same logic as the projected returns for the "simple" calculation to determine what the effective APY was for each day.
  // We want to calculate the effective apy daily for all the days we have available from block 360_000 to the current block height.
  // We can just use the _close figures
  // We also should calc the current apy using the most recent data like we do in projected returns and include that data

  try {
    // Define relevant fields to request from aggregated data
    const relevantAggregatedFields: (keyof AggregatedData)[] = [
      "spot_close",
      "zyield_price_close",
      "zephusd_circ_close",
      "zsd_in_yield_reserve_close",
      "zyield_circ_close",
      "reserve_ratio_close"
    ];

    // Get historical protocol stats from block 360,000 to the current block height
    let fromTimestamp = "1728819352"; // ~~360,000 block height
    let blockHeightPosition = 360_000;
    const currentRedisAPYHistory = await getAPYHistoryFromRedis() as { timestamp: number; block_height: number; return: number; zys_price: number }[];
    // If we have historical data, we can start from the 2nd to last timestamp
    if (currentRedisAPYHistory && currentRedisAPYHistory.length > 1) {
      fromTimestamp = currentRedisAPYHistory[currentRedisAPYHistory.length - 2].timestamp.toString();
      blockHeightPosition = currentRedisAPYHistory[currentRedisAPYHistory.length - 2].block_height;
      console.log(`We have existing APY history, starting from timestamp: ${fromTimestamp} && block height: ${blockHeightPosition}`);
    }
    const historicalData = await getAggregatedProtocolStatsFromRedis("day", fromTimestamp, undefined, relevantAggregatedFields);

    if (!historicalData || historicalData.length === 0) {
      console.log("No historical data found, ending APY calculation");
      return;
    }

    // use existing apy history if we have it but remove the last 2 entries
    // const apyHistory: { timestamp: number; block_height: number; return: number; zys_price: number }[] = [];
    const apyHistory = currentRedisAPYHistory ? currentRedisAPYHistory.slice(0, -2) : [];

    let missingDataCount = 0;


    for (const dataPoint of historicalData) {
      // we need to run the same calculations as the projected returns to determine the effective APY for each day
      const precalculatedBlockRewards = await getPrecalculatedBlockRewards(blockHeightPosition);
      const oneyear_block_height = blockHeightPosition + (720 * 30 * 12);
      let oneyear_accured_zsd_simple = 0;

      for (let block = blockHeightPosition; block <= oneyear_block_height; block++) {
        const total_block_reward = precalculatedBlockRewards[block].block_reward;
        const yield_reward = total_block_reward * 0.05; // 5%
        const zsd_auto_minted_simple = yield_reward * dataPoint.data.spot_close;
        oneyear_accured_zsd_simple += zsd_auto_minted_simple;
      }

      const zsd_in_reserve = dataPoint.data.zsd_in_yield_reserve_close;
      const zys_cric = dataPoint.data.zyield_circ_close;
      const zys_price = dataPoint.data.zyield_price_close;

      const simple_projection_oneyear_zys_price = (zsd_in_reserve + oneyear_accured_zsd_simple) / zys_cric;
      const simple_projection_oneyear_returns = ((simple_projection_oneyear_zys_price - zys_price) / zys_price) * 100;

      function isValidNumber(val: unknown): val is number {
        return typeof val === 'number' && val !== 0 && Number.isFinite(val);
      }

      if (
        !isValidNumber(simple_projection_oneyear_returns) ||
        !isValidNumber(simple_projection_oneyear_zys_price)
      ) {
        console.log(`Skipping data point at block height ${blockHeightPosition} due to missing returns or zys price`);
        missingDataCount++;

        // DEBUG - LOG OUT ALL VARIABLES TO DETERMINE WHY:
        console.log(`\tData Point: ${JSON.stringify(dataPoint)}`);
        console.log(`\tBlock Height Position: ${blockHeightPosition}`);
        console.log(`\tZSD in Reserve: ${zsd_in_reserve}`);
        console.log(`\tZYS Circulation: ${zys_cric}`);
        console.log(`\tZYS Price: ${zys_price}`);
        console.log(`\tSimple Projection One Year Returns: ${simple_projection_oneyear_returns}`);
        console.log(`\tSimple Projection One Year ZYS Price: ${simple_projection_oneyear_zys_price}`);

        continue; // Skip this iteration if returns or zys price is not available
      }

      // if (blockHeightPosition == 360720) {
      //   // DEBUG - LOG OUT ALL VARIABLES TO DETERMINE WHY:
      //   console.log(`Data Point: ${JSON.stringify(dataPoint)}`);
      //   console.log(`Block Height Position: ${blockHeightPosition}`);
      //   // console.log(`Precalculated Block Rewards: ${JSON.stringify(precalculatedBlockRewards)}`);
      //   console.log(`ZSD in Reserve: ${zsd_in_reserve}`);
      //   console.log(`ZYS Circulation: ${zys_cric}`);
      //   console.log(`ZYS Price: ${zys_price}`);
      //   console.log(`Simple Projection One Year Returns: ${simple_projection_oneyear_returns}`);
      //   console.log(`Simple Projection One Year ZYS Price: ${simple_projection_oneyear_zys_price}`);
      //   return; // DEBUG - stop here to inspect the values
      // }

      apyHistory.push({ timestamp: dataPoint.timestamp, block_height: blockHeightPosition, return: simple_projection_oneyear_returns, zys_price: simple_projection_oneyear_zys_price });

      blockHeightPosition += 720; // 720 blocks per day
    }

    // Get current effective APY from projected returns (for the "simple" scenario)
    const projectedReturns = await getProjectedReturnsFromRedis();
    if (!projectedReturns) {
      console.log("Unable to get projected returns, ending APY calculation");
      return;
    }

    const currentTimestamp = Math.floor(Date.now() / 1000);
    const currentAPY = projectedReturns.oneYear.simple.return;
    const predictedZysPrice = projectedReturns.oneYear.simple.zys_price;
    const redisHieght = await getCurrentBlockHeight();
    apyHistory.push({ timestamp: currentTimestamp, block_height: redisHieght, return: currentAPY, zys_price: predictedZysPrice });

    // Store APY history to Redis
    await redis.set("apy_history", JSON.stringify(apyHistory));

    console.log(`determineAPYHistory | APY history successfully stored in Redis`);
    console.log(`determineAPYHistory | Total data points processed: ${historicalData.length}`);
    console.log(`determineAPYHistory | Total missing data points: ${missingDataCount}`);


  } catch (error) {
    console.error("Error determining APY history:", error);
  }
}


export async function getAPYHistoryFromRedis() {
  const apyHistory = await redis.get("apy_history");
  if (!apyHistory) {
    return null;
  }
  return JSON.parse(apyHistory);
}
