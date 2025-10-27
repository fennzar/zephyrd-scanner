import type {
  AggregatedData,
  BlockRewardQueryResult,
  PricingRecordQueryResult,
  ProtocolStats,
  ReserveDiffReport,
  ReserveSnapshotQueryResult,
  TransactionQueryResult,
  LiveStats,
} from "./utils";
import type { HistoricalReturns, ProjectedReturns, ApyHistoryEntry } from "./yield";
import type { ZysPriceHistoryEntry } from "./pr";

export interface BlockStatsRow {
  block_height: number;
  data: ProtocolStats;
}

export type BlockStatsResponse = BlockStatsRow[];

export interface AggregatedStatsRow {
  timestamp: number;
  data: AggregatedData;
}

export type AggregatedStatsResponse = AggregatedStatsRow[];

export interface TransactionsResponse extends TransactionQueryResult {
  next_offset: number | null;
  prev_offset: number | null;
}

export type BlockRewardsResponse = BlockRewardQueryResult;

export type PricingRecordsResponse = PricingRecordQueryResult;

export type ReserveSnapshotsResponse = ReserveSnapshotQueryResult;

export type LiveStatsResponse = LiveStats;

export type HistoricalReturnsResponse = HistoricalReturns;

export type ProjectedReturnsResponse = ProjectedReturns;

export type ZysPriceHistoryResponse = ZysPriceHistoryEntry[];

export type ApyHistoryResponse = ApyHistoryEntry[];

export type ReserveDiffResponse = ReserveDiffReport;
