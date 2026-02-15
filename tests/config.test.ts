import { describe, expect, test } from "bun:test";
import {
  getDataStoreMode,
  useRedis,
  usePostgres,
  dualWriteEnabled,
  getStartBlock,
  getEndBlock,
} from "../src/config";

describe("config module", () => {
  test("getDataStoreMode() returns 'postgres'", () => {
    expect(getDataStoreMode()).toBe("postgres");
  });

  test("useRedis() returns false", () => {
    expect(useRedis()).toBe(false);
  });

  test("usePostgres() returns true", () => {
    expect(usePostgres()).toBe(true);
  });

  test("dualWriteEnabled() returns false", () => {
    expect(dualWriteEnabled()).toBe(false);
  });

  test("getStartBlock() parses START_BLOCK from env", () => {
    const result = getStartBlock();
    expect(typeof result).toBe("number");
    expect(Number.isFinite(result)).toBe(true);
  });

  test("getEndBlock() parses END_BLOCK from env", () => {
    const result = getEndBlock();
    expect(typeof result).toBe("number");
    expect(Number.isFinite(result)).toBe(true);
  });
});
