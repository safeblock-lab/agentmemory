import type { IIIClient } from 'iii-sdk';
import type { StateKV } from '../state/kv.js';
import type { IndexedVector } from '../state/indexed-vector.js';
import type { EmbeddingProvider } from '../types.js';
import { IndexedLocalEmbedding } from '../state/indexed-embedding.js';
import { recordAudit } from './audit.js';
import { logger } from '../logger.js';
import { prepareSelectedObservations, SelectedObservationPreparationPending } from './consolidation-selection.js';

export function registerIndexedSourceRecovery(sdk: IIIClient, kv: StateKV,
  vector: IndexedVector | null, provider: EmbeddingProvider | null): void {
  sdk.registerFunction('mem::indexed-source-recover', async (input: unknown) => {
    if (!input || typeof input !== 'object' || Array.isArray(input)) { logger.warn('Indexed recovery input rejected', { phase: 'validate_input', keys: [] }); throw Error('STATE_TX_INVALID_REQUEST: recovery phase=validate_input'); }
    const request = input as Record<string, unknown>;
    if (Object.keys(request).some(key => !['mode', 'index_id', 'model', 'dimensions', 'generation', 'max_dirty_sources', 'project', '_caller_worker_id'].includes(key))
      || request.project !== undefined && (typeof request.project !== 'string' || !request.project.trim() || request.project !== request.project.trim() || Buffer.byteLength(request.project) > 256)
      || (request._caller_worker_id !== undefined && (typeof request._caller_worker_id !== 'string' || request._caller_worker_id.length < 1 || request._caller_worker_id.length > 128))
      || request.mode !== 'local_dirty_only' || request.index_id !== 'observations'
      || request.model !== 'Xenova/all-MiniLM-L6-v2' || request.dimensions !== 384
      || request.generation !== 'local-v1' || request.max_dirty_sources !== 128) {
      logger.warn('Indexed recovery input rejected', { phase: 'validate_input', keys: Object.keys(request) });
      throw Error('STATE_TX_INVALID_REQUEST: recovery phase=validate_input');
    }
    if (!vector || !(provider instanceof IndexedLocalEmbedding)
      || vector.identity.index_id !== request.index_id || vector.identity.model !== request.model
      || vector.identity.dimensions !== request.dimensions || vector.identity.generation !== request.generation) {
      throw Error('STATE_SEMANTIC_IDENTITY_MISMATCH');
    }
    const project = typeof request.project === 'string' ? request.project : undefined;
    const prepareProject = async (selectedProject: string): Promise<SelectedObservationPreparationPending | undefined> => {
      try { await prepareSelectedObservations(kv, vector.identity, selectedProject); return undefined; }
      catch (error) { if (!(error instanceof SelectedObservationPreparationPending)) throw error; return error; }
    };
    let pending = project ? await prepareProject(project) : undefined;
    if (project) {
      if (pending && pending.diagnostics.reason !== 'project_source_not_ready') return {
        success: true, repairedSources: 0, identity: vector.identity, localOnly: true, ready: false,
        readinessScope: 'project', project, status: 'awaiting_dependency', queued: false, completed: false, diagnostics: pending.diagnostics,
      };
    }
    const repairedSources = await vector.recoverDirtySources(provider);
    await vector.ready();
    if (project) pending = await prepareProject(project);
    await recordAudit(kv, 'update', 'mem::config', ['indexed-corpus'], {
      reason: 'bounded_local_dirty_recovery', repairedSources, ...vector.identity, ...(project ? { project, projectReady: !pending } : {}),
    });
    return { success: true, repairedSources, identity: vector.identity, localOnly: true, ready: !pending,
      readinessScope: project ? 'project' : 'global', ...(project ? { project } : {}),
      status: pending ? 'awaiting_dependency' : 'ready', queued: false, completed: !pending,
      ...(pending ? { diagnostics: pending.diagnostics } : {}) };
  });
}
