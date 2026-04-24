import { describe, expect, test } from "bun:test";
import {
  toCanonicalProtocolStats,
  toCanonicalAggregated,
  toCanonicalPricing,
  toCanonicalTx,
  toCanonicalReserveSnapshot,
  toCanonicalBlockStatsRows,
  toCanonicalAggregatedStatsRows,
  toCanonicalAsset,
  toCanonicalConversionType,
  resolveInputFieldBlock,
  resolveInputFieldAggregated,
  resolveInputAsset,
  resolveInputConversionType,
  ASSET_ALIAS,
  CONVERSION_TYPE_ALIAS,
  BLOCK_FIELD_RENAMES,
  AGGREGATE_FIELD_RENAMES,
} from "../src/api-schema";

describe("asset renames", () => {
  test("legacy → canonical", () => {
    expect(toCanonicalAsset("ZEPHUSD")).toBe("ZSD");
    expect(toCanonicalAsset("ZEPHRSV")).toBe("ZRS");
    expect(toCanonicalAsset("ZYIELD")).toBe("ZYS");
  });

  test("ZEPH unchanged", () => {
    expect(toCanonicalAsset("ZEPH")).toBe("ZEPH");
  });

  test("null/undefined passthrough", () => {
    expect(toCanonicalAsset(null)).toBe(null);
    expect(toCanonicalAsset(undefined)).toBe(undefined);
  });

  test("unknown asset passthrough", () => {
    expect(toCanonicalAsset("FOO")).toBe("FOO");
  });
});

describe("conversion type renames", () => {
  test("legacy → canonical", () => {
    expect(toCanonicalConversionType("mint_stable")).toBe("mint_zsd");
    expect(toCanonicalConversionType("redeem_stable")).toBe("redeem_zsd");
    expect(toCanonicalConversionType("mint_reserve")).toBe("mint_zrs");
    expect(toCanonicalConversionType("redeem_reserve")).toBe("redeem_zrs");
    expect(toCanonicalConversionType("mint_yield")).toBe("mint_zys");
    expect(toCanonicalConversionType("redeem_yield")).toBe("redeem_zys");
  });

  test("unknown passthrough", () => {
    expect(toCanonicalConversionType("na")).toBe("na");
  });
});

describe("toCanonicalProtocolStats (block scale)", () => {
  const legacy = {
    block_height: 100,
    block_timestamp: 1700000000,
    spot: 0.4,
    moving_average: 0.41,
    stable: 2.5,
    stable_ma: 2.51,
    reserve: 1.3,
    reserve_ma: 1.31,
    yield_price: 1.9,
    zephusd_circ: 300000,
    zephrsv_circ: 2000000,
    zyield_circ: 160000,
    zeph_circ: 11000000,
    zeph_in_reserve: 3500000,
    zsd_in_yield_reserve: 310000,
    assets: 1400000,
    assets_ma: 1401000,
    liabilities: 300000,
    equity: 1100000,
    equity_ma: 1101000,
    reserve_ratio: 3.76,
    reserve_ratio_ma: 3.75,
    zsd_accrued_in_yield_reserve_from_yield_reward: 198000,
    zsd_minted_for_yield: 100,
    conversion_transactions_count: 5,
    yield_conversion_transactions_count: 0,
    mint_reserve_count: 1,
    mint_reserve_volume: 100,
    fees_zephrsv: 0.1,
    redeem_reserve_count: 1,
    redeem_reserve_volume: 50,
    fees_zephusd: 0.2,
    mint_stable_count: 2,
    mint_stable_volume: 200,
    redeem_stable_count: 1,
    redeem_stable_volume: 50,
    fees_zeph: 0.3,
    mint_yield_count: 0,
    mint_yield_volume: 0,
    redeem_yield_count: 0,
    redeem_yield_volume: 0,
    fees_zephusd_yield: 0.05,
    fees_zyield: 0.01,
  };

  const result = toCanonicalProtocolStats(legacy as any);

  test("renames pricing fields", () => {
    expect(result.zeph_price).toBe(0.4);
    expect(result.zeph_price_ma).toBe(0.41);
    expect(result.zsd_rate).toBe(2.5);
    expect(result.zsd_rate_ma).toBe(2.51);
    expect(result.zrs_rate).toBe(1.3);
    expect(result.zrs_rate_ma).toBe(1.31);
    expect(result.zys_price).toBe(1.9);
  });

  test("renames circ fields", () => {
    expect(result.zsd_circ).toBe(300000);
    expect(result.zrs_circ).toBe(2000000);
    expect(result.zys_circ).toBe(160000);
    expect(result.zeph_circ).toBe(11000000);
  });

  test("renames activity fields", () => {
    expect(result.mint_zsd_count).toBe(2);
    expect(result.mint_zsd_volume).toBe(200);
    expect(result.redeem_zsd_count).toBe(1);
    expect(result.mint_zrs_count).toBe(1);
    expect(result.mint_zys_count).toBe(0);
    expect(result.fees_zsd).toBe(0.2);
    expect(result.fees_zrs).toBe(0.1);
    expect(result.fees_zys).toBe(0.01);
    expect(result.fees_zsd_yield).toBe(0.05);
  });

  test("no legacy keys leak", () => {
    const keys = Object.keys(result);
    for (const legacyKey of Object.keys(BLOCK_FIELD_RENAMES)) {
      expect(keys).not.toContain(legacyKey);
    }
  });

  test("unchanged fields pass through", () => {
    expect(result.block_height).toBe(100);
    expect(result.reserve_ratio).toBe(3.76);
    expect(result.zsd_accrued_in_yield_reserve_from_yield_reward).toBe(198000);
  });
});

describe("toCanonicalAggregated (hour/day scale)", () => {
  const legacy = {
    spot_open: 0.4,
    spot_close: 0.41,
    spot_high: 0.42,
    spot_low: 0.39,
    stable_open: 2.5,
    stable_close: 2.51,
    stable_high: 2.52,
    stable_low: 2.49,
    reserve_open: 1.3,
    reserve_close: 1.31,
    reserve_high: 1.32,
    reserve_low: 1.29,
    zyield_price_open: 1.9,
    zyield_price_close: 1.91,
    zyield_price_high: 1.92,
    zyield_price_low: 1.89,
    zephusd_circ_open: 300000,
    zephusd_circ_close: 300100,
    zephusd_circ_high: 300200,
    zephusd_circ_low: 299900,
    zephrsv_circ_open: 2000000,
    zephrsv_circ_close: 2000100,
    zephrsv_circ_high: 2000200,
    zephrsv_circ_low: 1999900,
    zyield_circ_open: 160000,
    zyield_circ_close: 160100,
    zyield_circ_high: 160200,
    zyield_circ_low: 159900,
    mint_stable_count: 2,
    mint_stable_volume: 200,
    fees_zephusd: 0.2,
    fees_zephrsv: 0.1,
    fees_zyield: 0.01,
    fees_zephusd_yield: 0.05,
  };

  const result = toCanonicalAggregated(legacy as any);

  test("renames OHLC pricing fields", () => {
    expect(result.zeph_price_open).toBe(0.4);
    expect(result.zeph_price_close).toBe(0.41);
    expect(result.zsd_rate_close).toBe(2.51);
    expect(result.zrs_rate_high).toBe(1.32);
    expect(result.zys_price_low).toBe(1.89);
  });

  test("renames OHLC circ fields", () => {
    expect(result.zsd_circ_open).toBe(300000);
    expect(result.zrs_circ_close).toBe(2000100);
    expect(result.zys_circ_high).toBe(160200);
  });

  test("renames activity fields (no OHLC)", () => {
    expect(result.mint_zsd_count).toBe(2);
    expect(result.mint_zsd_volume).toBe(200);
    expect(result.fees_zsd).toBe(0.2);
    expect(result.fees_zsd_yield).toBe(0.05);
  });

  test("no legacy keys leak", () => {
    const keys = Object.keys(result);
    for (const legacyKey of Object.keys(AGGREGATE_FIELD_RENAMES)) {
      expect(keys).not.toContain(legacyKey);
    }
  });
});

describe("toCanonicalPricing", () => {
  const legacy = {
    height: 500000,
    timestamp: 1700000000,
    spot: 0.4,
    moving_average: 0.41,
    reserve: 1.3,
    reserve_ma: 1.31,
    stable: 2.5,
    stable_ma: 2.51,
    yield_price: 1.9,
  };

  const result = toCanonicalPricing(legacy as any);

  test("renames height → block_height", () => {
    expect(result.block_height).toBe(500000);
    expect((result as any).height).toBeUndefined();
  });

  test("renames pricing fields", () => {
    expect(result.zeph_price).toBe(0.4);
    expect(result.zeph_price_ma).toBe(0.41);
    expect(result.zrs_rate).toBe(1.3);
    expect(result.zsd_rate).toBe(2.5);
    expect(result.zys_price).toBe(1.9);
  });

  test("timestamp unchanged", () => {
    expect(result.timestamp).toBe(1700000000);
  });
});

describe("toCanonicalTx", () => {
  const legacy = {
    hash: "abc",
    block_height: 100,
    block_timestamp: 1700000000,
    conversion_type: "mint_stable",
    conversion_rate: 2.5,
    from_asset: "ZEPH",
    from_amount: 1,
    to_asset: "ZEPHUSD",
    to_amount: 2.5,
    conversion_fee_asset: "ZEPHUSD",
    conversion_fee_amount: 0.01,
    tx_fee_asset: "ZEPH",
    tx_fee_amount: 0.001,
  };

  const result = toCanonicalTx(legacy as any);

  test("renames conversion_type", () => {
    expect(result.conversion_type).toBe("mint_zsd");
  });

  test("renames asset values", () => {
    expect(result.from_asset).toBe("ZEPH");
    expect(result.to_asset).toBe("ZSD");
    expect(result.conversion_fee_asset).toBe("ZSD");
    expect(result.tx_fee_asset).toBe("ZEPH");
  });

  test("amounts unchanged", () => {
    expect(result.from_amount).toBe(1);
    expect(result.to_amount).toBe(2.5);
  });
});

describe("toCanonicalReserveSnapshot", () => {
  const legacy: any = {
    captured_at: "2026-01-01T00:00:00Z",
    reserve_height: 500000,
    previous_height: 499999,
    hf_version: 11,
    on_chain: {
      zeph_reserve_atoms: "1",
      zeph_reserve: 1,
      zsd_circ_atoms: "2",
      zsd_circ: 2,
      zrs_circ_atoms: "3",
      zrs_circ: 3,
      zyield_circ_atoms: "4",
      zyield_circ: 4,
      zsd_yield_reserve_atoms: "5",
      zsd_yield_reserve: 5,
      reserve_ratio_atoms: "6",
      reserve_ratio: 6,
    },
  };

  const result = toCanonicalReserveSnapshot(legacy);

  test("renames zyield_circ → zys_circ", () => {
    expect(result.on_chain.zys_circ).toBe(4);
    expect(result.on_chain.zys_circ_atoms).toBe("4");
    expect((result.on_chain as any).zyield_circ).toBeUndefined();
    expect((result.on_chain as any).zyield_circ_atoms).toBeUndefined();
  });

  test("other fields preserved", () => {
    expect(result.on_chain.zsd_circ).toBe(2);
    expect(result.on_chain.zrs_circ).toBe(3);
    expect(result.hf_version).toBe(11);
  });
});

describe("toCanonicalBlockStatsRows / toCanonicalAggregatedStatsRows", () => {
  test("block rows preserve block_height + translate data", () => {
    const rows = [{ block_height: 100, data: { spot: 0.4, zephusd_circ: 300 } as any }];
    const result = toCanonicalBlockStatsRows(rows);
    expect(result[0].block_height).toBe(100);
    expect(result[0].data.zeph_price).toBe(0.4);
    expect(result[0].data.zsd_circ).toBe(300);
  });

  test("aggregated rows preserve timestamp + translate data", () => {
    const rows = [{ timestamp: 1700000000, data: { spot_close: 0.4, zephusd_circ_close: 300 } as any }];
    const result = toCanonicalAggregatedStatsRows(rows);
    expect(result[0].timestamp).toBe(1700000000);
    expect(result[0].data.zeph_price_close).toBe(0.4);
    expect(result[0].data.zsd_circ_close).toBe(300);
  });
});

describe("input resolvers (canonical or legacy → legacy internal)", () => {
  test("resolveInputFieldBlock: canonical input → legacy", () => {
    expect(resolveInputFieldBlock("zsd_circ")).toBe("zephusd_circ");
    expect(resolveInputFieldBlock("zeph_price")).toBe("spot");
    expect(resolveInputFieldBlock("zys_price")).toBe("yield_price");
    expect(resolveInputFieldBlock("mint_zsd_count")).toBe("mint_stable_count");
  });

  test("resolveInputFieldBlock: legacy input passes through", () => {
    expect(resolveInputFieldBlock("zephusd_circ")).toBe("zephusd_circ");
    expect(resolveInputFieldBlock("spot")).toBe("spot");
    expect(resolveInputFieldBlock("mint_stable_count")).toBe("mint_stable_count");
  });

  test("resolveInputFieldBlock: non-renamed field passes through", () => {
    expect(resolveInputFieldBlock("block_height")).toBe("block_height");
    expect(resolveInputFieldBlock("reserve_ratio")).toBe("reserve_ratio");
  });

  test("resolveInputFieldAggregated: canonical → legacy with OHLC", () => {
    expect(resolveInputFieldAggregated("zsd_circ_close")).toBe("zephusd_circ_close");
    expect(resolveInputFieldAggregated("zeph_price_open")).toBe("spot_open");
    expect(resolveInputFieldAggregated("zys_price_high")).toBe("zyield_price_high");
    expect(resolveInputFieldAggregated("fees_zsd")).toBe("fees_zephusd");
  });

  test("resolveInputFieldAggregated: legacy passes through", () => {
    expect(resolveInputFieldAggregated("zephusd_circ_close")).toBe("zephusd_circ_close");
    expect(resolveInputFieldAggregated("spot_open")).toBe("spot_open");
  });

  test("resolveInputAsset: canonical → legacy, legacy passthrough, unknown passthrough", () => {
    expect(resolveInputAsset("ZSD")).toBe("ZEPHUSD");
    expect(resolveInputAsset("ZRS")).toBe("ZEPHRSV");
    expect(resolveInputAsset("ZYS")).toBe("ZYIELD");
    expect(resolveInputAsset("ZEPH")).toBe("ZEPH");
    expect(resolveInputAsset("ZEPHUSD")).toBe("ZEPHUSD");
  });

  test("resolveInputConversionType: canonical → legacy, legacy passthrough", () => {
    expect(resolveInputConversionType("mint_zsd")).toBe("mint_stable");
    expect(resolveInputConversionType("redeem_zys")).toBe("redeem_yield");
    expect(resolveInputConversionType("mint_stable")).toBe("mint_stable");
    expect(resolveInputConversionType("na")).toBe("na");
  });
});

describe("round-trip: translate legacy then resolve canonical back to legacy", () => {
  test("block field round-trip", () => {
    for (const [legacy, canonical] of Object.entries(BLOCK_FIELD_RENAMES)) {
      expect(resolveInputFieldBlock(canonical)).toBe(legacy);
    }
  });

  test("aggregate field round-trip", () => {
    for (const [legacy, canonical] of Object.entries(AGGREGATE_FIELD_RENAMES)) {
      expect(resolveInputFieldAggregated(canonical)).toBe(legacy);
    }
  });

  test("asset round-trip", () => {
    for (const [legacy, canonical] of Object.entries(ASSET_ALIAS)) {
      expect(resolveInputAsset(canonical)).toBe(legacy);
    }
  });

  test("conversion type round-trip", () => {
    for (const [legacy, canonical] of Object.entries(CONVERSION_TYPE_ALIAS)) {
      expect(resolveInputConversionType(canonical)).toBe(legacy);
    }
  });
});
