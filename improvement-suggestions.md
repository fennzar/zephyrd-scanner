# Aggregator Performance Roadmap

## Stage 1 – Low-Risk Optimisations (init completed, needs testing)

- Batch Redis reads with pipelines (pricing records, block rewards, transactions).
- Short-circuit when no pending blocks to aggregate.
- Buffer `protocol_stats` writes and flush in chunks instead of per-block writes.
- Reduce logging frequency during large backfills.

## Stage 2 – Structural Redesign

- Move per-block adjustments into Redis-side scripts/Lua for single round trips.
- Maintain `txs_by_block` as an append-only index to avoid repeat lookups.
- Use rolling window data structures for hourly/daily buckets instead of recalculating end-to-end each run.
- Consider a queue-driven ETL pipeline with checkpointing for parallel processing and easier replays.
