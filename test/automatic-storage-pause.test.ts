import { afterEach, describe, expect, it, vi } from 'vitest';
vi.mock('iii-sdk', () => ({ InvocationError: class InvocationError extends Error {}, TriggerAction: { Void: () => ({}) } }));
import { isAutomaticStorageMaintenancePaused, runAutomaticStorageMaintenance, runAutomaticTemporalRecovery } from '../src/index.js';
import { createProjectIndexMaintenance } from '../src/state/indexed-retrieval.js';
import type { StateKV } from '../src/state/kv.js';

afterEach(() => { vi.unstubAllEnvs(); vi.useRealTimers(); });

describe('storage-only automatic maintenance opt-in', () => {
  it.each(['', '0', 'true', '1'])('keeps provider recovery paused with storage flag %j', async (flag) => {
    vi.stubEnv('AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED', '1');
    vi.stubEnv('AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED', flag);
    const trigger = vi.fn(async () => ({ success: true }));
    const sdk = { trigger } as unknown as Parameters<typeof runAutomaticStorageMaintenance>[0];
    await runAutomaticStorageMaintenance(sdk, () => {});
    await runAutomaticTemporalRecovery(sdk);
    expect(trigger.mock.calls).toEqual(flag === '1'
      ? [[{ function_id: 'mem::storage-maintenance', payload: { action: 'tick' } }]]
      : []);
  });

  it('permits bounded catalog and vector progress while provider recovery stays paused', async () => {
    vi.useFakeTimers();
    vi.stubEnv('AGENTMEMORY_BACKGROUND_RECOVERY_PAUSED', '1');
    vi.stubEnv('AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED', '0');
    let saved: unknown = null;
    const kv = {
      indexedRetrieval: true,
      get: vi.fn(async () => saved),
      set: vi.fn(async (_scope: string, _key: string, value: unknown) => { saved = value; }),
      retrieval: vi.fn(async (_input: unknown) => ({ status: 'pending', processed: 16, cursor: '16', revision: '2' })),
    };
    const tick = createProjectIndexMaintenance(kv as unknown as StateKV, isAutomaticStorageMaintenancePaused, () => false, () => {});
    await tick();
    expect(kv.get).not.toHaveBeenCalled();
    vi.stubEnv('AGENTMEMORY_STORAGE_MAINTENANCE_WHILE_PAUSED', '1');
    const trigger = vi.fn();
    for (let cut = 0; cut < 3; cut++) {
      await tick();
      await runAutomaticTemporalRecovery({ trigger } as unknown as Parameters<typeof runAutomaticTemporalRecovery>[0]);
      vi.advanceTimersByTime(60_001);
    }
    expect(kv.retrieval.mock.calls.map(([input]) => input)).toEqual([
      { action: 'project_catalog_prepare', source: 'sessions', max_rows: 16, max_bytes: 65_536 },
      { action: 'project_catalog_prepare', source: 'summaries', max_rows: 16, max_bytes: 65_536 },
      { action: 'project_vectors_prepare', index_id: 'observations', max_rows: 16, max_bytes: 65_536 },
    ]);
    expect(trigger).not.toHaveBeenCalled();
  });
});
