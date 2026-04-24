// 500K ZEPH pre-mine in genesis block (block 0, vout[0]).
// Added to zeph_circ by the aggregator at block 0; kept separate from mined rewards.
export const INITIAL_TREASURY_ZEPH = 500_000;
// HF Version 1 (DJED reserve protocol start).
// Pre-V1 blocks have zero-value pricing records and no conversion transactions.
export const HF_V1_BLOCK_HEIGHT = 89_300;
// Chain timestamp of HF_V1_BLOCK_HEIGHT (block 89,300). Used as a fresh-scan
// aggregation floor so we don't emit noisy pre-HF3 hourly/daily buckets —
// hardcoded so the floor is correct even when the pricing record for block
// 89,300 isn't yet in the store (mid-scan).
export const HF_V1_BLOCK_TIMESTAMP = 1_696_152_427;
// HF Version 11 (V11/V2.3.0): asset type changes (ZEPH→ZPH, ZEPHUSD→ZSD).
// Circ values reset to audited amounts; UNAUDITABLE_ZEPH_MINT added to supply.
export const HF_V11_BLOCK_HEIGHT = 536_000;
// Amount of ZEPH minted post-HFv11 (block 536,000, vout[2]) that is not captured
// by processTx (which only reads vout[0]). Used in logger total and aggregator V11 reset.
export const UNAUDITABLE_ZEPH_MINT = 1_921_650;
// Audited ZEPH total at V11 boundary (end of block 535,999).
// This is higher than emission-only total because ~145K inflation bug coins were
// audited through. The audit HF was specifically designed to handle unknown supply.
// Value from daemon's V2 total_asset_supply ZPH at the V11 transition.
export const AUDITED_ZEPH_AT_V11 = 7_828_285.273529857474;
