import { Prisma } from "@prisma/client";
import { ReserveSnapshot } from "../utils";
import { getPrismaClient } from "./index";

function toPrismaData(snapshot: ReserveSnapshot): Prisma.ReserveSnapshotUpsertArgs["create"] {
  return {
    previousHeight: snapshot.previous_height,
    reserveHeight: snapshot.reserve_height,
    capturedAt: new Date(snapshot.captured_at),
    hfVersion: snapshot.hf_version,
    zephReserveAtoms: snapshot.on_chain.zeph_reserve_atoms,
    zephReserve: snapshot.on_chain.zeph_reserve,
    zsdCircAtoms: snapshot.on_chain.zsd_circ_atoms,
    zsdCirc: snapshot.on_chain.zsd_circ,
    zrsCircAtoms: snapshot.on_chain.zrs_circ_atoms,
    zrsCirc: snapshot.on_chain.zrs_circ,
    zyieldCircAtoms: snapshot.on_chain.zyield_circ_atoms,
    zyieldCirc: snapshot.on_chain.zyield_circ,
    zsdYieldReserveAtoms: snapshot.on_chain.zsd_yield_reserve_atoms,
    zsdYieldReserve: snapshot.on_chain.zsd_yield_reserve,
    reserveRatioAtoms: snapshot.on_chain.reserve_ratio_atoms,
    reserveRatio: snapshot.on_chain.reserve_ratio ?? null,
    reserveRatioMaAtoms: snapshot.on_chain.reserve_ratio_ma_atoms,
    reserveRatioMa: snapshot.on_chain.reserve_ratio_ma ?? null,
    pricingRecord: snapshot.pricing_record as Prisma.InputJsonValue | undefined,
    rawPayload: snapshot.raw as Prisma.InputJsonValue | undefined,
  };
}

export async function upsertReserveSnapshot(snapshot: ReserveSnapshot): Promise<void> {
  const prisma = getPrismaClient();
  const data = toPrismaData(snapshot);
  await prisma.reserveSnapshot.upsert({
    where: { previousHeight: snapshot.previous_height },
    update: data,
    create: data,
  });
}

export async function getLatestReserveSnapshotRow(): Promise<ReserveSnapshot | null> {
  const prisma = getPrismaClient();
  const row = await prisma.reserveSnapshot.findFirst({
    orderBy: { previousHeight: "desc" },
  });
  return row ? fromPrisma(row) : null;
}

export async function getReserveSnapshotByPreviousHeight(previousHeight: number): Promise<ReserveSnapshot | null> {
  const prisma = getPrismaClient();
  const row = await prisma.reserveSnapshot.findUnique({
    where: { previousHeight },
  });
  return row ? fromPrisma(row) : null;
}

export interface ReserveSnapshotRangeQuery {
  previousHeight?: number;
  fromPreviousHeight?: number;
  toPreviousHeight?: number;
  limit?: number;
  order?: "asc" | "desc";
}

export interface ReserveSnapshotRangeResult {
  total: number;
  rows: ReserveSnapshot[];
}

export async function queryReserveSnapshots(
  options: ReserveSnapshotRangeQuery = {}
): Promise<ReserveSnapshotRangeResult> {
  const prisma = getPrismaClient();
  const { previousHeight, fromPreviousHeight, toPreviousHeight, limit, order = "asc" } = options;

  const where: Prisma.ReserveSnapshotWhereInput = {};
  if (previousHeight != null) {
    where.previousHeight = previousHeight;
  } else {
    const heightFilter: Prisma.IntFilter = {};
    if (fromPreviousHeight != null) {
      heightFilter.gte = fromPreviousHeight;
    }
    if (toPreviousHeight != null) {
      heightFilter.lte = toPreviousHeight;
    }
    if (Object.keys(heightFilter).length > 0) {
      where.previousHeight = heightFilter;
    }
  }

  const resolvedLimit = limit && Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : undefined;
  const sortOrder = order === "desc" ? "desc" : "asc";

  const [total, rows] = await Promise.all([
    prisma.reserveSnapshot.count({ where }),
    prisma.reserveSnapshot.findMany({
      where,
      orderBy: { previousHeight: sortOrder },
      take: resolvedLimit,
    }),
  ]);

  return {
    total,
    rows: rows.map(fromPrisma),
  };
}

function fromPrisma(row: {
  previousHeight: number;
  reserveHeight: number;
  capturedAt: Date;
  hfVersion: number;
  zephReserveAtoms: string;
  zephReserve: number;
  zsdCircAtoms: string;
  zsdCirc: number;
  zrsCircAtoms: string;
  zrsCirc: number;
  zyieldCircAtoms: string;
  zyieldCirc: number;
  zsdYieldReserveAtoms: string;
  zsdYieldReserve: number;
  reserveRatioAtoms: string;
  reserveRatio: number | null;
  reserveRatioMaAtoms: string | null;
  reserveRatioMa: number | null;
  pricingRecord: Prisma.JsonValue | null;
  rawPayload: Prisma.JsonValue | null;
}): ReserveSnapshot {
  return {
    captured_at: row.capturedAt.toISOString(),
    reserve_height: row.reserveHeight,
    previous_height: row.previousHeight,
    hf_version: row.hfVersion,
    on_chain: {
      zeph_reserve_atoms: row.zephReserveAtoms,
      zeph_reserve: row.zephReserve,
      zsd_circ_atoms: row.zsdCircAtoms,
      zsd_circ: row.zsdCirc,
      zrs_circ_atoms: row.zrsCircAtoms,
      zrs_circ: row.zrsCirc,
      zyield_circ_atoms: row.zyieldCircAtoms,
      zyield_circ: row.zyieldCirc,
      zsd_yield_reserve_atoms: row.zsdYieldReserveAtoms,
      zsd_yield_reserve: row.zsdYieldReserve,
      reserve_ratio_atoms: row.reserveRatioAtoms,
      reserve_ratio: row.reserveRatio,
      reserve_ratio_ma_atoms: row.reserveRatioMaAtoms ?? undefined,
      reserve_ratio_ma: row.reserveRatioMa ?? undefined,
    },
    pricing_record: row.pricingRecord as ReserveSnapshot["pricing_record"],
    raw: row.rawPayload as ReserveSnapshot["raw"],
  };
}
