import { scanPricingRecords } from "./pr";
import { scanTransactions } from "./tx";
import { getTotalsFromRedis } from "./utils";

// 1 min set interval for scanning
setInterval(async () => {
  await scanPricingRecords();
  console.log("--------------------");
  await scanTransactions();
  console.log("--------------------");
  const totals = await getTotalsFromRedis();
  console.log(totals);
}, 60000);
