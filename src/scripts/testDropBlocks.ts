/**
 * Test helper: delete protocol_stats rows for specified block heights
 * and reset the aggregator height so the scanner must recover them.
 *
 * The aggregator is set to the max deleted height (not min-1), so the
 * scanner tries to aggregate the block AFTER the gap. This forces the
 * recovery path: prevBlockData is missing, triggering re-aggregation.
 *
 * Usage:
 *   npx tsx src/scripts/testDropBlocks.ts 89350            # single gap
 *   npx tsx src/scripts/testDropBlocks.ts 89350 89351 89352 # consecutive gap (cascade recovery)
 */
import dotenv from "dotenv";
dotenv.config();

import { getPrismaClient } from "../db/index";

async function main() {
  const heights = process.argv.slice(2).map(Number).filter(Number.isFinite);

  if (heights.length === 0) {
    console.error("Usage: npx tsx src/scripts/testDropBlocks.ts <height> [height2] [height3] ...");
    process.exit(1);
  }

  const prisma = getPrismaClient();

  // Show the rows before deleting
  const before = await prisma.protocolStatsBlock.findMany({
    where: { blockHeight: { in: heights } },
    select: { blockHeight: true, zephCirc: true, zephusdCirc: true, zephrsvCirc: true },
    orderBy: { blockHeight: "asc" },
  });

  console.log(`\nFound ${before.length} rows for heights [${heights.join(", ")}]:`);
  for (const row of before) {
    console.log(`  height=${row.blockHeight}  zeph_circ=${row.zephCirc}  zephusd_circ=${row.zephusdCirc}  zephrsv_circ=${row.zephrsvCirc}`);
  }

  // Delete the rows
  const deleted = await prisma.protocolStatsBlock.deleteMany({
    where: { blockHeight: { in: heights } },
  });
  console.log(`\nDeleted ${deleted.count} protocol_stats rows.`);

  // Set aggregator height to the max deleted height.
  // The scanner will try to aggregate maxDeleted+1 next, which needs
  // prevBlockData from maxDeleted (now missing) — triggering recovery.
  const maxDeleted = Math.max(...heights);
  const newAggregatorHeight = maxDeleted;

  await prisma.scannerState.upsert({
    where: { key: "height_aggregator" },
    update: { value: newAggregatorHeight.toString() },
    create: { key: "height_aggregator", value: newAggregatorHeight.toString() },
  });
  console.log(`Set height_aggregator to ${newAggregatorHeight} (scanner will process ${newAggregatorHeight + 1} next, needing prevBlockData from the gap)`);

  // Also reset the redis aggregator height if redis is available
  try {
    const redis = (await import("../redis")).default;
    await redis.set("height_aggregator", newAggregatorHeight.toString());
    console.log(`Set redis height_aggregator to ${newAggregatorHeight}`);
    await redis.quit();
  } catch {
    console.log("Redis not available, skipped redis reset.");
  }

  await prisma.$disconnect();
  console.log("\nDone. Re-run the scanner to test recovery.");
  console.log(`Expected: scanner aggregates ${newAggregatorHeight + 1}, finds prevBlockData (${maxDeleted}) missing, triggers re-aggregation.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
