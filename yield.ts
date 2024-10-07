// Stats for ZSD Yield
// Yield Reserve circ, ZYS price and circ, yield conversions count and fees, are all available in /stats and are handled in aggregator.ts
// This is for populating historical returns and projected returns.
import redis from "./redis";
import { getCurrentBlockHeight, getPricingRecordFromBlock } from "./utils";


const VERSION_2_HF_V6_BLOCK_HEIGHT = 360000;




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
        const currentBlockHeight = await getCurrentBlockHeight();

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

        const previousPricingRecord = await getPricingRecordFromBlock(previousBlockHeight);
        const onedayagoPricingRecord = await getPricingRecordFromBlock(onedayagoBlockHeight);
        const oneweekagoPricingRecord = await getPricingRecordFromBlock(oneweekagoBlockHeight);
        const onemonthagoPricingRecord = await getPricingRecordFromBlock(onemonthagoBlockHeight);
        const threemonthsagoPricingRecord = await getPricingRecordFromBlock(threemonthsagoBlockHeight);
        const oneyearagoPricingRecord = await getPricingRecordFromBlock(oneyearagoBlockHeight);

        if (!previousPricingRecord || !onedayagoPricingRecord || !oneweekagoPricingRecord || !onemonthagoPricingRecord || !threemonthsagoPricingRecord || !oneyearagoPricingRecord) {
            console.log("Missing pricing records, ending processing historical returns");
            return;
        }

        const previousZYSPrice = previousPricingRecord.yield_price;
        const onedayagoZYSPrice = onedayagoPricingRecord.yield_price;
        const oneweekagoZYSPrice = oneweekagoPricingRecord.yield_price;
        const onemonthagoZYSPrice = onemonthagoPricingRecord.yield_price;
        const threemonthsagoZYSPrice = threemonthsagoPricingRecord.yield_price;
        const oneyearagoZYSPrice = oneyearagoPricingRecord.yield_price;
        const initialZYSPrice = 1; // This is the initial price of ZYS in ZSD at the start of the ZSD Yield Update at block 360000.

        if (!previousZYSPrice || !onedayagoZYSPrice || !oneweekagoZYSPrice || !onemonthagoZYSPrice || !threemonthsagoZYSPrice || !oneyearagoZYSPrice) {
            console.log("Missing ZYS prices, ending processing historical returns");
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


        const historicalStats = {
            lastBlock: { return: previousReturn, ZSDAccrued: previousZSDAccrued },
            oneDay: { return: onedayagoReturn, ZSDAccrued: onedayagoZSDAccrued },
            oneWeek: { return: oneweekagoReturn, ZSDAccrued: oneweekagoZSDAccrued },
            oneMonth: { return: onemonthagoReturn, ZSDAccrued: onemonthagoZSDAccrued },
            threeMonths: { return: threemonthsagoReturn, ZSDAccrued: threemonthsagoZSDAccrued },
            oneYear: { return: oneyearagoReturn, ZSDAccrued: oneyearagoZSDAccrued },
            allTime: { return: alltimeReturn, ZSDAccrued: alltimeZSDAccrued }
        };


        // save to redis
        await redis.set("historical_returns", JSON.stringify(historicalStats));

        console.log("----------------------------------------------------------");
        console.log(`Last block       [${historicalStats.lastBlock.return}%   | +${historicalStats.lastBlock.ZSDAccrued} ZSD from block reward]`);
        console.log(`1 Day            [${historicalStats.oneDay.return}%  | +${historicalStats.oneDay.ZSDAccrued} ZSD from block reward]`);
        console.log(`1 Week           [${historicalStats.oneWeek.return}%  | +${historicalStats.oneWeek.ZSDAccrued} ZSD from block reward]`);
        console.log(`1 Month          [${historicalStats.oneMonth.return}%  | +${historicalStats.oneMonth.ZSDAccrued} ZSD from block reward]`);
        console.log(`3 Months         [${historicalStats.threeMonths.return}%  | +${historicalStats.threeMonths.ZSDAccrued} ZSD from block reward]`);
        console.log(`1 Year           [${historicalStats.oneYear.return}%  | +${historicalStats.oneYear.ZSDAccrued} ZSD from block reward]`);
        console.log(`ALL TIME         [${historicalStats.allTime.return}%  | +${historicalStats.allTime.ZSDAccrued} ZSD from block reward]`);
        console.log("----------------------------------------------------------");


    }
    catch (error) {
        console.error("Error determining historical returns:", error);
    }

}

export async function getHistoricalReturnsFromRedis(test = false) {
    if (test) {
        // return dummy historical stats for testing route
        const dummyHistoricalStats = {
            lastBlock: { return: 0.01, ZSDAccrued: 1 },
            oneDay: { return: 0.70, ZSDAccrued: 720 },
            oneWeek: { return: 2.60, ZSDAccrued: 5040 },
            oneMonth: { return: 3.90, ZSDAccrued: 21600 },
            threeMonths: { return: 12.50, ZSDAccrued: 64800 },
            oneYear: { return: 25.60, ZSDAccrued: 262800 },
            allTime: { return: 55.60, ZSDAccrued: 562800 },
        };

        return dummyHistoricalStats;

    }
    const historicalStats = await redis.get("historical_returns");
    if (!historicalStats) {
        return null;
    }
    return JSON.parse(historicalStats);
}
