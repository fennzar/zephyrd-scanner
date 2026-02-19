/**
 * Concurrent RPC fetch utility.
 *
 * Fetches blocks in chunks with configurable concurrency,
 * then yields them in order for sequential processing.
 */

export const RPC_CONCURRENCY = Number(process.env.RPC_CONCURRENCY ?? "10");
export const RPC_CHUNK_SIZE = Number(process.env.RPC_CHUNK_SIZE ?? "500");

/**
 * Fetch a range of items concurrently, returning results in order.
 *
 * @param items - Array of inputs to fetch
 * @param fetcher - Async function that fetches a single item
 * @param concurrency - Max concurrent fetches (default: RPC_CONCURRENCY)
 * @returns Array of results in the same order as inputs
 */
export async function fetchConcurrent<T, R>(
  items: T[],
  fetcher: (item: T) => Promise<R>,
  concurrency: number = RPC_CONCURRENCY,
): Promise<R[]> {
  const results: R[] = new Array(items.length);
  let nextIndex = 0;

  const worker = async () => {
    while (true) {
      const idx = nextIndex++;
      if (idx >= items.length) return;
      results[idx] = await fetcher(items[idx]);
    }
  };

  const workers = Array.from({ length: Math.min(concurrency, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

/**
 * Process a height range in chunks: fetch concurrently, then process sequentially.
 *
 * @param startHeight - First height to process (inclusive)
 * @param endHeight - Last height to process (inclusive)
 * @param fetcher - Async function that fetches data for a height
 * @param processor - Async function that processes one fetched result. Return false to abort.
 * @param chunkSize - Number of heights per chunk (default: RPC_CHUNK_SIZE)
 * @param concurrency - Max concurrent fetches per chunk (default: RPC_CONCURRENCY)
 * @returns true if completed, false if aborted by processor
 */
export async function processHeightRange<R>(
  startHeight: number,
  endHeight: number,
  fetcher: (height: number) => Promise<R>,
  processor: (height: number, data: R) => Promise<boolean | void>,
  chunkSize: number = RPC_CHUNK_SIZE,
  concurrency: number = RPC_CONCURRENCY,
): Promise<boolean> {
  for (let chunkStart = startHeight; chunkStart <= endHeight; chunkStart += chunkSize) {
    const chunkEnd = Math.min(chunkStart + chunkSize - 1, endHeight);
    const heights = Array.from({ length: chunkEnd - chunkStart + 1 }, (_, i) => chunkStart + i);

    const results = await fetchConcurrent(heights, fetcher, concurrency);

    for (let i = 0; i < heights.length; i++) {
      const cont = await processor(heights[i], results[i]);
      if (cont === false) return false;
    }
  }
  return true;
}
