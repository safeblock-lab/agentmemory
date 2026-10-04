import type { StateKV } from './kv.js';
import type { IndexedVector } from './indexed-vector.js';
import type { EmbeddingProvider } from '../types.js';
import { KV } from './schema.js';
import { retrievalRequest, type IndexedStatus } from './indexed-retrieval.js';

const preparations = new WeakMap<StateKV, Promise<number>>();
export function prepareIndexedCorpus(kv: StateKV, vector: IndexedVector, provider: EmbeddingProvider): Promise<number> {
  const existing = preparations.get(kv);
  if (existing) return existing;
  const preparation = prepare(kv, vector, provider).finally(() => preparations.delete(kv));
  preparations.set(kv, preparation);
  return preparation;
}
async function prepare(kv: StateKV, vector: IndexedVector, provider: EmbeddingProvider): Promise<number> {
  await kv.set(KV.config, 'indexed-corpus', { ...vector.identity, status: 'pending' });
  await vector.configure();
  const write = async (items: Array<{ id: string; sessionId: string; text: string; scope: string }>) => {
    for (let offset = 0; offset < items.length; offset += 32) {
      const batch = items.slice(offset, offset + 32);
      const texts = batch.map(item => item.text.slice(0, 16_000));
      const embeddings = await provider.embedBatch(texts);
      if (embeddings.length !== batch.length) throw new Error('Indexed preparation returned an incomplete embedding batch.');
      for (let i = 0; i < batch.length; i++) {
        if (embeddings[i].length !== provider.dimensions) throw new Error('Indexed preparation embedding dimensions changed.');
        await vector.add(batch[i].id, batch[i].sessionId, embeddings[i], texts[i], batch[i].scope);
      }
    }
  };
  let page: { coverage_ready: boolean; items: Array<{ source_scope: string; id: string; source_version: string }> };
  do {
    page = await retrievalRequest(kv, { action: 'source_prepare', ...vector.identity, max_rows: 256, max_bytes: 1_048_576 });
    if (typeof page.coverage_ready !== 'boolean' || !Array.isArray(page.items) || page.items.length > 256) throw new Error('Invalid native source preparation response.');
    for (let offset = 0; offset < page.items.length; offset += 32) {
      const batch: Array<{ id: string; sessionId: string; text: string; scope: string }> = [];
      for (const ref of page.items.slice(offset, offset + 32)) {
        if (ref.source_scope !== KV.memories && !ref.source_scope.startsWith('mem:obs:')) throw new Error('Invalid indexed source scope.');
        const source = await kv.getVersioned<{ title: string; narrative?: string; content?: string; sessionId?: string; sessionIds?: string[] }>(ref.source_scope, ref.id);
        if (!source.exists || !source.value || source.version !== ref.source_version) throw new Error('STATE_SEMANTIC_SOURCE_STALE: Source changed during preparation.');
        const memory = ref.source_scope === KV.memories;
        batch.push({ id: ref.id, scope: ref.source_scope, sessionId: memory ? source.value.sessionIds?.[0] ?? 'memory' : source.value.sessionId ?? ref.source_scope.slice('mem:obs:'.length), text: `${source.value.title} ${memory ? source.value.content : source.value.narrative}` });
      }
      await write(batch);
    }
  } while (!page.coverage_ready);
  for (const scope of [KV.graphNodes, KV.graphEdges]) {
    let result: { status: string };
    do {
      result = await retrievalRequest(kv, { action: 'index_prepare', scope, max_rows: 256, max_bytes: 1_048_576 });
    } while (result.status === 'pending');
    if (result.status !== 'ready') throw new Error('Indexed graph preparation did not become ready.');
  }
  await vector.flush();
  const status = await retrievalRequest<IndexedStatus>(kv, { action: 'index_status' });
  const count = Number(status.semantic.find(item => item.index_id === vector.identity.index_id)?.count);
  if (!Number.isSafeInteger(count) || count < 0) throw new Error('Invalid indexed corpus count.');
  await kv.set(KV.config, 'indexed-corpus', { ...vector.identity, status: 'ready', count });
  try { await vector.ready(); }
  catch (error) { await vector.markIncomplete(); throw error; }
  return count;
}
