import { Buffer } from "node:buffer";
import type { StatePageRequest } from "../src/state/state-pages.js";

export function statePageFixture(
  entries: Iterable<readonly [string, unknown]>,
  request: StatePageRequest,
): { items: unknown[]; next_cursor: string | null } {
  const prefix = `${request.scope}:`;
  const values = [...entries]
    .filter(([key]) => key.startsWith(prefix))
    .map(([, value]) => value);
  const offset = request.cursor === undefined ? 0 : Number(request.cursor);
  if (!Number.isSafeInteger(offset) || offset < 0 || offset > values.length) {
    throw Object.assign(new Error("STATE_PAGE_CURSOR_INVALID"), { code: "STATE_PAGE_CURSOR_INVALID" });
  }

  const items: unknown[] = [];
  let index = offset;
  while (index < values.length && items.length < request.limit) {
    const candidateItems = [...items, structuredClone(values[index])];
    const candidateCursor = index + 1 < values.length ? String(index + 1) : null;
    const encoded = JSON.stringify({ items: candidateItems, next_cursor: candidateCursor });
    if (encoded === undefined || Buffer.byteLength(encoded, "utf8") > request.max_bytes) {
      if (items.length === 0) {
        throw Object.assign(new Error("STATE_RECORD_TOO_LARGE"), { code: "STATE_RECORD_TOO_LARGE" });
      }
      break;
    }

    items.push(structuredClone(values[index]));
    index += 1;
  }

  return {
    items,
    next_cursor: index < values.length ? String(index) : null,
  };
}
