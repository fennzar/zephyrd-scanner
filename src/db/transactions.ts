import { Prisma } from "@prisma/client";

import { getPrismaClient } from "./index";

export interface ConversionTransactionRecord {
  hash: string;
  blockHeight: number;
  blockTimestamp: number;
  conversionType: string;
  conversionRate: number;
  fromAsset: string;
  fromAmount: number;
  fromAmountAtoms?: string;
  toAsset: string;
  toAmount: number;
  toAmountAtoms?: string;
  conversionFeeAsset: string;
  conversionFeeAmount: number;
  txFeeAsset: string;
  txFeeAmount: number;
  txFeeAtoms?: string;
}

function toPrisma(record: ConversionTransactionRecord): Prisma.ConversionTransactionCreateManyInput {
  return {
    hash: record.hash,
    blockHeight: record.blockHeight,
    blockTimestamp: record.blockTimestamp,
    conversionType: record.conversionType,
    conversionRate: record.conversionRate,
    fromAsset: record.fromAsset,
    fromAmount: record.fromAmount,
    fromAmountAtoms: record.fromAmountAtoms,
    toAsset: record.toAsset,
    toAmount: record.toAmount,
    toAmountAtoms: record.toAmountAtoms,
    conversionFeeAsset: record.conversionFeeAsset,
    conversionFeeAmount: record.conversionFeeAmount,
    txFeeAsset: record.txFeeAsset,
    txFeeAmount: record.txFeeAmount,
    txFeeAtoms: record.txFeeAtoms,
  };
}

export async function insertTransactions(records: ConversionTransactionRecord[]): Promise<void> {
  if (records.length === 0) {
    return;
  }
  const prisma = getPrismaClient();
  await prisma.conversionTransaction.createMany({
    data: records.map(toPrisma),
    skipDuplicates: true,
  });
}

export async function deleteTransactionsAboveHeight(blockHeight: number): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.conversionTransaction.deleteMany({
    where: {
      blockHeight: {
        gt: blockHeight,
      },
    },
  });
}

export async function deleteAllTransactions(): Promise<void> {
  const prisma = getPrismaClient();
  await prisma.conversionTransaction.deleteMany();
}

export interface TransactionQueryOptionsDb {
  fromTimestamp?: number;
  toTimestamp?: number;
  types?: string[];
  limit?: number;
  offset?: number;
  order?: "asc" | "desc";
  fromIndex?: number;
}

export interface TransactionQueryResultDb {
  total: number;
  offset: number;
  results: ConversionTransactionRecord[];
}

export async function queryTransactions(options: TransactionQueryOptionsDb): Promise<TransactionQueryResultDb> {
  const prisma = getPrismaClient();
  const where: Prisma.ConversionTransactionWhereInput = {};
  if (options.types && options.types.length > 0) {
    where.conversionType = { in: options.types };
  }
  const blockTimestamp: Prisma.IntFilter = {};
  if (options.fromTimestamp != null) {
    blockTimestamp.gte = options.fromTimestamp;
  }
  if (options.toTimestamp != null) {
    blockTimestamp.lte = options.toTimestamp;
  }
  if (Object.keys(blockTimestamp).length > 0) {
    where.blockTimestamp = blockTimestamp;
  }

  const order: "asc" | "desc" = options.order === "asc" ? "asc" : "desc";
  let orderDirection: Prisma.SortOrder = order === "asc" ? "asc" : "desc";
  let skip = Math.max(options.offset ?? 0, 0);
  let take = options.limit && options.limit > 0 ? Math.floor(options.limit) : undefined;

  if (options.fromIndex != null && options.fromIndex > 0) {
    orderDirection = "asc";
    skip = Math.floor(options.fromIndex);
  }

  const orderBy: Prisma.ConversionTransactionOrderByWithRelationInput[] = [
    { blockTimestamp: orderDirection },
    { blockHeight: orderDirection },
    { hash: orderDirection },
  ];

  const [totalCount, rows] = await Promise.all([
    prisma.conversionTransaction.count({ where }),
    prisma.conversionTransaction.findMany({
      where,
      orderBy,
      skip,
      take,
    }),
  ]);

  let mapped = rows.map((row) => ({
    hash: row.hash,
    blockHeight: row.blockHeight,
    blockTimestamp: row.blockTimestamp,
    conversionType: row.conversionType,
    conversionRate: row.conversionRate,
    fromAsset: row.fromAsset,
    fromAmount: row.fromAmount,
    fromAmountAtoms: row.fromAmountAtoms ?? undefined,
    toAsset: row.toAsset,
    toAmount: row.toAmount,
    toAmountAtoms: row.toAmountAtoms ?? undefined,
    conversionFeeAsset: row.conversionFeeAsset,
    conversionFeeAmount: row.conversionFeeAmount,
    txFeeAsset: row.txFeeAsset,
    txFeeAmount: row.txFeeAmount,
    txFeeAtoms: row.txFeeAtoms ?? undefined,
  }));

  if (options.fromIndex != null && options.fromIndex > 0 && order === "desc") {
    mapped = mapped.reverse();
  }

  return {
    total: Math.max(totalCount - skip, 0),
    offset: skip,
    results: mapped,
  };
}
