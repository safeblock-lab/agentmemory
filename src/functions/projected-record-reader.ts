import type { StateKV } from "../state/kv.js";

const PROJECTED_PAGE_LIMIT = 256;
const PROJECTED_COUNT_PAGE_LIMIT = 1_024;
const RECORD_READ_CONCURRENCY = 8;

async function* projectedRecordIds(
  kv: StateKV,
  scope: string,
  pageLimit: number,
): AsyncGenerator<string> {
  const seenIds = new Set<string>();
  for await (const page of kv.pages<{ id: string }>(scope, {
    fields: ["id"],
    limit: pageLimit,
  })) {
    for (const { id } of page.items) {
      if (typeof id !== "string" || id.length === 0) {
        throw new Error("STATE_GRAPH_PROJECTED_RECORD_ID_INVALID");
      }
      if (seenIds.has(id)) {
        throw new Error("STATE_GRAPH_PROJECTED_RECORD_ID_DUPLICATE");
      }
      seenIds.add(id);
      yield id;
    }
  }
}

export function iterateProjectedRecordIds(
  kv: StateKV,
  scope: string,
): AsyncGenerator<string> {
  return projectedRecordIds(kv, scope, PROJECTED_PAGE_LIMIT);
}

/**
 * Read records without asking the state engine to serialize whole values in a
 * page. Projected pages carry only IDs; point reads are bounded and preserve
 * page order so callers can stream the result without buffering the scope.
 */
export async function* iterateProjectedRecords<T extends { id: string }>(
  kv: StateKV,
  scope: string,
): AsyncGenerator<T> {
  const batchIds: string[] = [];
  const readBatch = async function* (ids: string[]): AsyncGenerator<T> {
    const records = await Promise.all(ids.map(async (id) => {
        const record = await kv.get<T>(scope, id);
        if (!record) throw new Error("STATE_GRAPH_PROJECTED_RECORD_MISSING");
        if (record.id !== id) throw new Error("STATE_GRAPH_PROJECTED_RECORD_ID_MISMATCH");
        return record;
    }));
    for (const record of records) yield record;
  };
  for await (const id of iterateProjectedRecordIds(kv, scope)) {
    batchIds.push(id);
    if (batchIds.length === RECORD_READ_CONCURRENCY) {
      yield* readBatch(batchIds.splice(0));
    }
  }
  if (batchIds.length > 0) yield* readBatch(batchIds);
}

export async function collectProjectedRecords<T extends { id: string }>(
  kv: StateKV,
  scope: string,
): Promise<T[]> {
  const records: T[] = [];
  for await (const record of iterateProjectedRecords<T>(kv, scope)) {
    records.push(record);
  }
  return records;
}

export async function countProjectedRecordIds(
  kv: StateKV,
  scope: string,
): Promise<number> {
  let count = 0;
  for await (const _id of projectedRecordIds(kv, scope, PROJECTED_COUNT_PAGE_LIMIT)) count++;
  return count;
}
