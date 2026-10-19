import type { StateKV } from "../state/kv.js";
import {
  MAX_STALE_LIST_RESTARTS,
  StatePageError,
} from "../state/state-pages.js";
import { KV } from "../state/schema.js";
import type { Memory } from "../types.js";
import { PROCEDURAL_MIN_SESSIONS_PER_PATTERN } from "../functions/consolidation-status.js";

const COUNT_PAGE_LIMIT = 256;
const COUNT_PAGE_MAX_BYTES = 1_048_576;

type PatternCountFields = Pick<Memory, "isLatest" | "type"> & {
  sessionIds?: string[];
};

async function countStateRecords<T>(
  kv: StateKV,
  scope: string,
  matches: (value: T) => boolean = () => true,
  fields?: string[],
): Promise<number> {
  for (let staleRestarts = 0; ; staleRestarts++) {
    let count = 0;
    try {
      for await (const page of kv.pages<T>(scope, {
        limit: COUNT_PAGE_LIMIT,
        maxBytes: COUNT_PAGE_MAX_BYTES,
        fields: fields ?? ["id"],
      })) {
        for (const value of page.items) {
          if (matches(value)) count++;
        }
      }
      return count;
    } catch (error) {
      if (
        !(error instanceof StatePageError) ||
        error.code !== "STATE_PAGE_CURSOR_STALE" ||
        staleRestarts >= MAX_STALE_LIST_RESTARTS
      ) {
        throw error;
      }
    }
  }
}

export async function readConsolidationCounts(kv: StateKV) {
  const [summaries, recurringPatterns, semanticFacts, procedures, relations] =
    await Promise.all([
      countStateRecords(kv, KV.summaries).catch(() => null),
      countStateRecords<PatternCountFields>(
        kv,
        KV.memories,
        (memory) =>
          memory.isLatest &&
          memory.type === "pattern" &&
          (memory.sessionIds?.length ?? 0) >=
            PROCEDURAL_MIN_SESSIONS_PER_PATTERN,
        ["isLatest", "type", "sessionIds"],
      ).catch(() => null),
      countStateRecords(kv, KV.semantic).catch(() => null),
      countStateRecords(kv, KV.procedural).catch(() => null),
      countStateRecords(kv, KV.relations).catch(() => null),
    ]);

  return { summaries, recurringPatterns, semanticFacts, procedures, relations };
}
