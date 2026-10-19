import { createHash } from 'node:crypto';
import type { CompressedObservation } from '../types.js';
import type { StateKV } from '../state/kv.js';
import { MAX_STALE_LIST_RESTARTS, StatePageError, type StateScopeRevision } from '../state/state-pages.js';

interface SnapshotCursor {
  version: number; generation: string; scope_fingerprint: string; revision: string; after_position: number;
  mode?: string; fields?: string[];
}
function convertCursor(cursor: string, scope: string, revision: StateScopeRevision, projected: boolean): string {
  let value: SnapshotCursor;
  try { value = JSON.parse(Buffer.from(cursor, 'base64url').toString('utf8')); }
  catch { throw new StatePageError('STATE_PAGE_CURSOR_INVALID'); }
  const expected = projected
    ? ['after_position', 'generation', 'revision', 'scope_fingerprint', 'version']
    : ['after_position', 'fields', 'generation', 'mode', 'revision', 'scope_fingerprint', 'version'];
  if (JSON.stringify(Object.keys(value).sort()) !== JSON.stringify(expected)
    || value.version !== (projected ? 2 : 3)
    || !Number.isSafeInteger(value.after_position) || value.after_position < 0
    || value.scope_fingerprint !== createHash('sha256').update(scope).digest('base64url')
    || (!projected && (value.mode !== 'projected' || JSON.stringify(value.fields) !== '["id"]')))
    throw new StatePageError('STATE_PAGE_CURSOR_INVALID');
  if (value.generation !== revision.generation || value.revision !== revision.revision)
    throw new StatePageError('STATE_PAGE_CURSOR_STALE');
  const full = { version: 2, generation: value.generation, scope_fingerprint: value.scope_fingerprint,
    revision: value.revision, after_position: value.after_position };
  return Buffer.from(JSON.stringify(projected ? { ...full, version: 3, mode: 'projected', fields: ['id'] } : full)).toString('base64url');
}
async function firstPage<T>(kv: StateKV, scope: string, cursor?: string, projected = false) {
  for await (const page of kv.pages<T>(scope, { limit: projected ? 1 : 256, maxBytes: 1_048_576,
    ...(cursor ? { cursor } : {}), ...(projected ? { fields: ['id'] } : {}) })) return page;
  throw new StatePageError('STATE_PAGE_INVALID_RESPONSE');
}
async function readSnapshot(kv: StateKV, scope: string): Promise<CompressedObservation[]> {
  const before = await kv.scopeRevision(scope);
  const observations: CompressedObservation[] = [];
  const seen = new Set<string>();
  let cursor: string | undefined;
  do {
    if (cursor) {
      if (seen.has(cursor) || seen.size >= 256) throw new StatePageError('STATE_PAGE_CURSOR_INVALID');
      seen.add(cursor);
    }
    try {
      const page = await firstPage<CompressedObservation>(kv, scope, cursor);
      observations.push(...page.items);
      cursor = page.next_cursor ?? undefined;
    } catch (error) {
      if (!(error instanceof StatePageError) || error.code !== 'STATE_RECORD_TOO_LARGE') throw error;
      const projectedCursor = cursor ? convertCursor(cursor, scope, before, true) : undefined;
      const page = await firstPage<Pick<CompressedObservation, 'id'>>(kv, scope, projectedCursor, true);
      const id = page.items.length === 1 ? page.items[0]?.id : undefined;
      if (typeof id !== 'string' || !id) throw new StatePageError('STATE_PAGE_INVALID_RESPONSE');
      const row = await kv.getVersioned<CompressedObservation>(scope, id);
      if (!row.exists || !row.value || row.value.id !== id) throw new StatePageError('STATE_PAGE_CURSOR_STALE');
      // Publication evidence hashes every field of the original object.
      observations.push(row.value);
      cursor = page.next_cursor ? convertCursor(page.next_cursor, scope, before, false) : undefined;
    }
  } while (cursor);
  const after = await kv.scopeRevision(scope);
  if (before.generation !== after.generation || before.revision !== after.revision)
    throw new StatePageError('STATE_PAGE_CURSOR_STALE');
  return observations;
}
export async function readSummarySnapshotSource(kv: StateKV, scope: string): Promise<CompressedObservation[]> {
  for (let restart = 0; ; restart++) {
    try { return await readSnapshot(kv, scope); }
    catch (error) {
      if (!(error instanceof StatePageError) || error.code !== 'STATE_PAGE_CURSOR_STALE'
        || restart >= MAX_STALE_LIST_RESTARTS) throw error;
    }
  }
}
