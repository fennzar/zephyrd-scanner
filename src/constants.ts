// 500K ZEPH pre-mine in genesis block (block 0, vout[0]).
// Added to zeph_circ by the aggregator at block 0; kept separate from mined rewards.
export const INITIAL_TREASURY_ZEPH = 500_000;
// HF Version 1 (DJED reserve protocol start).
// Pre-V1 blocks have zero-value pricing records and no conversion transactions.
export const HF_V1_BLOCK_HEIGHT = 89_300;
// HF Version 11 (V11/V2.3.0): asset type changes (ZEPH→ZPH, ZEPHUSD→ZSD).
// Circ values reset to audited amounts; UNAUDITABLE_ZEPH_MINT added to supply.
export const HF_V11_BLOCK_HEIGHT = 536_000;
// Amount of ZEPH minted post-HFv11 (block 536,000, vout[2]) that is not captured
// by processTx (which only reads vout[0]). Used in logger total and aggregator V11 reset.
export const UNAUDITABLE_ZEPH_MINT = 1_921_650;
