export interface PricingRecordInput {
  blockHeight: number;
  timestamp: number;
  spot: number;
  movingAverage: number;
  reserve: number;
  reserveMa: number;
  stable: number;
  stableMa: number;
  yieldPrice: number;
}

export interface PricingRecordResult extends PricingRecordInput {}

export interface PricingStore {
  save(record: PricingRecordInput): Promise<void>;
  get(blockHeight: number): Promise<PricingRecordResult | null>;
  getLatestHeight(): Promise<number>;
}

export interface ScannerStateStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string): Promise<void>;
}

export interface DataStores {
  pricing: PricingStore;
  scannerState: ScannerStateStore;
}
