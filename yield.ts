// Stats for ZSD Yield
// Yield Reserve circ, ZYS price and circ, yield conversions count and fees, are all available in /stats and are handled in aggregator.ts
// This is for populating historical returns and projected returns.
import redis from "./redis";
import { ProtocolStats, getCurrentBlockHeight, getPricingRecordFromBlock, getRedisHeight } from "./utils";

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



// Projected Returns:
export async function determineProjectedReturns(test = false) {
    async function getStats(test = false) {
        if (test) {
            // return dummy protocol stats for testing route
            const dummyProtocolStats = {
                currentBlockHeight: VERSION_2_HF_V6_BLOCK_HEIGHT,
                zeph_price: 1.34,
                zys_price: 1.00,
                zsd_circ: 449_132.29,
                zys_circ: 200_000,
                zsd_in_reserve: 200_000,
                reserve_ratio: 5.03,
            };
            return dummyProtocolStats;
        }


        const currentBlockHeight = await getRedisHeight();
        const currentProtocolStats = await redis.hget("protocol_stats", currentBlockHeight.toString());
        const currentProtocolStatsData: ProtocolStats = currentProtocolStats ? JSON.parse(currentProtocolStats) : {};
        if (!currentProtocolStatsData) {
            console.log("Error in determineProjectedReturns getting currentProtocolStatsData, ending processing projected returns");
            return { currentBlockHeight: 0, zeph_price: 0, zys_price: 0, zsd_circ: 0, zys_circ: 0, zsd_in_reserve: 0, reserve_ratio: 0 };
        }

        const zeph_price = currentProtocolStatsData.spot_close;
        const zsd_circ = currentProtocolStatsData.zephusd_circ_close;
        const reserve_ratio = currentProtocolStatsData.reserve_ratio_close;

        // Pre 2.0.0 fork height these will be 0
        let zys_price = currentProtocolStatsData.zyield_price_close;
        let zys_circ = currentProtocolStatsData.zyield_circ_close;
        let zsd_in_reserve = currentProtocolStatsData.zsd_in_yield_reserve_close;

        if (currentBlockHeight < VERSION_2_HF_V6_BLOCK_HEIGHT) {
            zys_price = 1;
            zys_circ = zsd_circ / 2;
            zsd_in_reserve = zsd_circ / 2;
        }

        return { currentBlockHeight, zeph_price, zys_price, zsd_circ, zys_circ, zsd_in_reserve, reserve_ratio };
    }
    // ----------------------------------------------------------
    // 1 Week           [Low: 0.60% | Simple: 1.00% | High: 2.60%]
    // 1 Month          [Low: 1.90% | Simple: 3.00% | High: 5.60%]
    // 3 Months         [Low: 5.50% | Simple: 12.50% | High: 20.60%]
    // 6 Months         [Low: 10.50% | Simple: 20.50% | High: 30.60%]
    // 1 Year           [Low: 20.50% | Simple: 30.50% | High: 40.60%]
    // -----------------------------------------------------------
    console.log("Determining projected returns...");

    // We need to calcuate the projected returns based on the amount of zeph emmissions that will occur in the future.
    // for a simple projection we can use pre-calculated zeph emmissions for each time period and assume competition for the yield will remain the same, and zeph's price will remain the same.
    // for a low projection we can assume a lower zeph price and higher competition for the yield.
    // for a high projection we can assume a higher zeph price and lower competition for the yield.
    // Price is an in-built factor in competition due to the reserve ratio restrictions in the djed stablecoin system.
    // We can determine a high competition state where a higher percentage of zsd is staked (compared to current, up to 100%) and the reserve ratio is low
    // We can determine a low competition state where a lower percentage of zsd is staked (compared to current, down to say 50%) and the reserve ratio is high


    const { currentBlockHeight, zeph_price, zys_price, zsd_circ, zys_circ, zsd_in_reserve, reserve_ratio } = await getStats(test);
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

    console.log(`determineProjectedReturns - test: ${test}`)
    console.log(`Starting Stats:`)
    console.log(`currentBlockHeight: ${currentBlockHeight}
                    zeph_price: ${zeph_price}
                    zys_price: ${zys_price}
                    zsd_circ: ${zsd_circ}
                    zys_circ: ${zys_circ}
                    zsd_in_reserve: ${zsd_in_reserve}
                    reserve_ratio: ${reserve_ratio}`);


    const zeph_price_200RR = zeph_price / reserve_ratio * 2;
    const zeph_price_800RR = zeph_price / reserve_ratio * 8;

    console.log(`zeph_price_200RR: ${zeph_price_200RR} - High Competition/Low Price`)
    console.log(`zeph_price_800RR: ${zeph_price_800RR} - Low Competition/High Price`)

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

    console.log(`Projected ZSD Accured in the future:`)
    console.log(`1 Week: [Low: ${oneweek_accured_zsd.low} | Simple: ${oneweek_accured_zsd.simple} | High: ${oneweek_accured_zsd.high}]`)
    console.log(`1 Month: [Low: ${onemonth_accured_zsd.low} | Simple: ${onemonth_accured_zsd.simple} | High: ${onemonth_accured_zsd.high}]`)
    console.log(`3 Months: [Low: ${threemonths_accured_zsd.low} | Simple: ${threemonths_accured_zsd.simple} | High: ${threemonths_accured_zsd.high}]`)
    console.log(`6 Months: [Low: ${sixmonths_accured_zsd.low} | Simple: ${sixmonths_accured_zsd.simple} | High: ${sixmonths_accured_zsd.high}]`)
    console.log(`1 Year: [Low: ${oneyear_accured_zsd.low} | Simple: ${oneyear_accured_zsd.simple} | High: ${oneyear_accured_zsd.high}]`)


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
        additional_zys = (zsd_in_reserve_high_competition / zys_price) - zys_circ; // this is assuming that 100% of zsd is staked and needs to be adjusted if we change our low projection to <100% staked
        console.log(`Additional simulated ZYS minted: ${additional_zys}`)
    }


    const low_projection_oneweek_zys_price = (zsd_in_reserve_high_competition + oneweek_accured_zsd.low) / (zys_circ + additional_zys);
    const low_projection_onemonth_zys_price = (zsd_in_reserve_high_competition + onemonth_accured_zsd.low) / (zys_circ + additional_zys);
    const low_projection_threemonths_zys_price = (zsd_in_reserve_high_competition + threemonths_accured_zsd.low) / (zys_circ + additional_zys);
    const low_projection_sixmonths_zys_price = (zsd_in_reserve_high_competition + sixmonths_accured_zsd.low) / (zys_circ + additional_zys);
    const low_projection_oneyear_zys_price = (zsd_in_reserve_high_competition + oneyear_accured_zsd.low) / (zys_circ + additional_zys);

    // Calculate higher bound
    const zsd_in_reserve_low_competition = zsd_circ / 2; // 50% of all ZSD is staked
    // Reserve Ratio = 800%
    const high_projection_oneweek_zys_price = (zsd_in_reserve_low_competition + oneweek_accured_zsd.high) / zys_circ;
    const high_projection_onemonth_zys_price = (zsd_in_reserve_low_competition + onemonth_accured_zsd.high) / zys_circ;
    const high_projection_threemonths_zys_price = (zsd_in_reserve_low_competition + threemonths_accured_zsd.high) / zys_circ;
    const high_projection_sixmonths_zys_price = (zsd_in_reserve_low_competition + sixmonths_accured_zsd.high) / zys_circ;
    const high_projection_oneyear_zys_price = (zsd_in_reserve_low_competition + oneyear_accured_zsd.high) / zys_circ;


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


    console.log("----------------------------------------------------------");
    console.log(`1 Week           [Low: ${projectedStats.oneWeek.low.zys_price} ZSD (${projectedStats.oneWeek.low.return}%) | Simple: ${projectedStats.oneWeek.simple.zys_price} ZSD (${projectedStats.oneWeek.simple.return})% | High: ${projectedStats.oneWeek.high.zys_price} ZSD (${projectedStats.oneWeek.high.return})%]`);
    console.log(`1 Month          [Low: ${projectedStats.oneMonth.low.zys_price} ZSD (${projectedStats.oneMonth.low.return}%) | Simple: ${projectedStats.oneMonth.simple.zys_price} ZSD (${projectedStats.oneMonth.simple.return})% | High: ${projectedStats.oneMonth.high.zys_price} ZSD (${projectedStats.oneMonth.high.return})%]`);
    console.log(`3 Months         [Low: ${projectedStats.threeMonths.low.zys_price} ZSD (${projectedStats.threeMonths.low.return}%) | Simple: ${projectedStats.threeMonths.simple.zys_price} ZSD (${projectedStats.threeMonths.simple.return})% | High: ${projectedStats.threeMonths.high.zys_price} ZSD (${projectedStats.threeMonths.high.return})%]`);
    console.log(`6 Months         [Low: ${projectedStats.sixMonths.low.zys_price} ZSD (${projectedStats.sixMonths.low.return}%) | Simple: ${projectedStats.sixMonths.simple.zys_price} ZSD (${projectedStats.sixMonths.simple.return})% | High: ${projectedStats.sixMonths.high.zys_price} ZSD (${projectedStats.sixMonths.high.return})%]`);
    console.log(`1 Year           [Low: ${projectedStats.oneYear.low.zys_price} ZSD (${projectedStats.oneYear.low.return}%) | Simple: ${projectedStats.oneYear.simple.zys_price} ZSD (${projectedStats.oneYear.simple.return})% | High: ${projectedStats.oneYear.high.zys_price} ZSD (${projectedStats.oneYear.high.return})%]`);
    console.log("----------------------------------------------------------");

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
        current_supply += block_reward;

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
