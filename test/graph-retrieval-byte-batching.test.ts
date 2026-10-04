import { describe, expect, it, vi } from 'vitest';
import { InvocationError } from 'iii-sdk';
import { GraphRetrievalReader, GraphRetrievalResourceError } from '../src/functions/graph-retrieval-read.js';
import type { IndexedEdge } from '../src/state/indexed-retrieval.js';
import type { StateKV } from '../src/state/kv.js';
import type { GraphEdge, GraphNode } from '../src/types.js';
import { graphStateHarness } from './helpers/graph-state-harness.js';

const MiB = 1024 * 1024;
const resourceFailure = { code: 'STATE_RETRIEVAL_RESOURCE_LIMIT' };
const request = { entityNames: ['seed'], observationIds: [], entityDepth: 1, observationDepth: 0 };
const invocationResourceFailure = () => new InvocationError({
  code: 'STATE_RETRIEVAL_RESOURCE_LIMIT',
  message: 'State retrieval response exceeded the engine resource limit.',
  function_id: 'mem::state-retrieval',
});
const rejectedFailures: Array<[string, Error]> = [
  ['non-resource SDK InvocationError', new InvocationError({
    code: 'STATE_INDEX_NOT_READY',
    message: 'Native indexed retrieval is not ready.',
    function_id: 'mem::state-retrieval',
  })],
  ['untyped Error carrying the resource code', Object.assign(
    new Error('STATE_RETRIEVAL_RESOURCE_LIMIT'),
    { code: 'STATE_RETRIEVAL_RESOURCE_LIMIT' },
  )],
];

function fixture(count: number, reasoningBytes = 0) {
  const harness = graphStateHarness();
  const nodes: GraphNode[] = Array.from({ length: count }, (_, index) => ({
    id: `n${index}`, type: 'concept', name: `seed ${index}`, properties: {},
    sourceObservationIds: [], createdAt: '2026-10-03T00:00:00Z',
  }));
  const edges: IndexedEdge[] = nodes.map((node, index) => ({
    key: `e${index}`, position: String(count - index), value: {
      id: `e${index}`, sourceNodeId: node.id, targetNodeId: node.id,
      type: 'related_to', weight: 0.8, sourceObservationIds: [],
      createdAt: '2026-10-03T00:00:00Z', tcommit: '2026-10-03T00:00:00Z',
      isLatest: true, context: { reasoning: 'x'.repeat(reasoningBytes) },
    },
  }));
  for (const node of nodes) harness.seed('mem:graph:nodes', node.id, node as never);
  const edgeRequest = vi.fn(async (payload: Record<string, unknown>): Promise<unknown> => {
    const ids = new Set(payload.node_ids as string[]);
    const items = edges.filter(item => ids.has(item.value.sourceNodeId) || ids.has(item.value.targetNodeId));
    return Buffer.byteLength(JSON.stringify(items)) > Number(payload.max_bytes)
      ? resourceFailure : { generation: '1', items };
  });
  const retrieval = vi.fn(async (payload: Record<string, unknown>) => {
    if (payload.action === 'graph_edges') return edgeRequest(payload);
    if (payload.action === 'graph_seeds') return {
      generation: '1', items: nodes.map((node, index) => ({
        key: node.id, id: node.id, position: String(index), entity: true, observation: false,
      })),
    };
    throw new Error('Unexpected indexed request');
  });
  Object.assign(harness.kv, { indexedRetrieval: true, retrieval });
  const reader = new GraphRetrievalReader(harness.kv as unknown as StateKV);
  return { reader, nodes, edges, edgeRequest, retrieval, harness };
}

function orphanFixture() {
  const harness = graphStateHarness();
  const nodes: GraphNode[] = [
    { id: 'seed', type: 'concept', name: 'seed', properties: {}, sourceObservationIds: [], createdAt: '2026-10-03T00:00:00Z' },
    { id: 'neighbor', type: 'concept', name: 'neighbor', properties: {}, sourceObservationIds: [], createdAt: '2026-10-03T00:00:00Z' },
  ];
  const edges: IndexedEdge[] = [
    {
      key: 'valid', position: '0', value: {
        id: 'valid', sourceNodeId: 'seed', targetNodeId: 'neighbor', type: 'related_to', weight: 0.8,
        sourceObservationIds: [], createdAt: '2026-10-03T00:00:00Z', tcommit: '2026-10-03T00:00:00Z', isLatest: true,
      },
    },
    {
      key: 'orphan-edge', position: '1', value: {
        id: 'orphan-edge', sourceNodeId: 'seed', targetNodeId: 'orphan', type: 'related_to', weight: 0.8,
        sourceObservationIds: [], createdAt: '2026-10-03T00:00:00Z', tcommit: '2026-10-03T00:00:00Z', isLatest: true,
      },
    },
  ];
  for (const node of nodes) harness.seed('mem:graph:nodes', node.id, node as never);
  const edgeRequest = vi.fn(async (payload: Record<string, unknown>): Promise<unknown> => {
    const ids = new Set(payload.node_ids as string[]);
    return {
      generation: '1',
      items: edges.filter(item => ids.has(item.value.sourceNodeId) || ids.has(item.value.targetNodeId)),
    };
  });
  const retrieval = vi.fn(async (payload: Record<string, unknown>) => {
    if (payload.action === 'graph_edges') return edgeRequest(payload);
    if (payload.action === 'graph_seeds') return {
      generation: '1', items: [{ key: 'seed', id: 'seed', position: '0', entity: true, observation: false }],
    };
    throw new Error('Unexpected indexed request');
  });
  Object.assign(harness.kv, { indexedRetrieval: true, retrieval });
  return {
    reader: new GraphRetrievalReader(harness.kv as unknown as StateKV),
    harness,
    nodes,
    edges,
    retrieval,
  };
}

describe('graph edge adaptive byte batching', () => {
  it('losslessly splits a real oversized 64-ID response and deduplicates in insertion order', async () => {
    const { reader, nodes, edges, edgeRequest } = fixture(64, 140_000);
    const shared: GraphEdge = { ...edges[0].value, id: 'shared', sourceNodeId: 'n0', targetNodeId: 'n63', context: undefined };
    edges.push({ key: 'shared', position: '0', value: shared });
    expect(Buffer.byteLength(JSON.stringify(edges))).toBeGreaterThan(8 * MiB);
    edgeRequest.mockRejectedValueOnce(invocationResourceFailure());
    const result = await reader.readSubgraph(request);
    const calls = edgeRequest.mock.calls.map(([payload]) => payload);
    expect(calls.map(payload => (payload.node_ids as string[]).length)).toEqual([64, 32, 32]);
    expect(calls.slice(1).flatMap(payload => payload.node_ids)).toEqual(nodes.map(node => node.id));
    expect(calls.every(payload => payload.max_bytes === 8 * MiB && payload.max_items === 16_384)).toBe(true);
    expect([...result.nodes.keys()]).toEqual(nodes.map(node => node.id));
    expect(result.edges).toHaveLength(65);
    expect(result.edges.map(edge => edge.sourceNodeId)).toEqual(['n0', ...nodes.toReversed().map(node => node.id)]);
  });

  it('recursively splits both halves without omitting an odd-sized frontier', async () => {
    const { reader, edges, edgeRequest } = fixture(5);
    edgeRequest.mockImplementation(async payload => {
      const ids = payload.node_ids as string[];
      return ids.length > 1 ? resourceFailure : { generation: '1', items: edges.filter(item => ids.includes(item.value.sourceNodeId)) };
    });
    expect((await reader.readSubgraph(request)).edges).toHaveLength(5);
    expect(edgeRequest.mock.calls.map(([payload]) => payload.node_ids)).toEqual([
      ['n0', 'n1', 'n2', 'n3', 'n4'], ['n0', 'n1'], ['n0'], ['n1'],
      ['n2', 'n3', 'n4'], ['n2'], ['n3', 'n4'], ['n3'], ['n4'],
    ]);
  });

  it('propagates a singleton resource failure', async () => {
    const { reader, edgeRequest } = fixture(1);
    edgeRequest.mockResolvedValue(resourceFailure);
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code: resourceFailure.code });
    expect(edgeRequest).toHaveBeenCalledTimes(1);
    await expect(reader.readTemporalEntity('seed 0')).rejects.toMatchObject({ code: resourceFailure.code });
    expect(edgeRequest).toHaveBeenCalledTimes(2);
  });

  it('does not split a singleton SDK resource rejection', async () => {
    const { reader, edgeRequest } = fixture(1);
    edgeRequest.mockRejectedValueOnce(invocationResourceFailure());
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code: resourceFailure.code });
    expect(edgeRequest).toHaveBeenCalledTimes(1);
  });

  it.each(['STATE_INDEX_NOT_READY', 'STATE_GRAPH_RECOVERY_REQUIRED', 'STATE_RETRIEVAL_RESOURCE_LIMIT_EXTRA'])(
    'does not split a %s failure', async code => {
      const { reader, edgeRequest } = fixture(4);
      edgeRequest.mockResolvedValue({ code });
      await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code });
      expect(edgeRequest).toHaveBeenCalledTimes(1);
    },
  );

  it.each(rejectedFailures)('preserves a %s rejection without splitting', async (_description, error) => {
    const { reader, edgeRequest } = fixture(4);
    edgeRequest.mockRejectedValueOnce(error);
    await expect(reader.readSubgraph(request)).rejects.toBe(error);
    expect(edgeRequest).toHaveBeenCalledTimes(1);
  });

  it('propagates a transport failure without splitting', async () => {
    const { reader, edgeRequest } = fixture(4);
    const failure = new Error('transport failed');
    edgeRequest.mockRejectedValue(failure);
    await expect(reader.readSubgraph(request)).rejects.toBe(failure);
    expect(edgeRequest).toHaveBeenCalledTimes(1);
  });

  it.each(['parent', 'child'])('cancels after the %s response without fetching pending children', async stage => {
    const { reader, edges, edgeRequest } = fixture(4);
    const controller = new AbortController();
    const failure = new Error('cancelled');
    edgeRequest.mockImplementation(async payload => {
      const ids = payload.node_ids as string[];
      if (ids.length === 4) {
        if (stage === 'parent') controller.abort(failure);
        return resourceFailure;
      }
      controller.abort(failure);
      return { generation: '1', items: edges.slice(0, 2) };
    });
    await expect(reader.readSubgraph(request, controller.signal)).rejects.toBe(failure);
    expect(edgeRequest).toHaveBeenCalledTimes(stage === 'parent' ? 1 : 2);
  });

  it('rejects differing child generations instead of returning partial results', async () => {
    const { reader, edges, edgeRequest } = fixture(4);
    edgeRequest.mockImplementation(async payload => {
      const ids = payload.node_ids as string[];
      return ids.length === 4 ? resourceFailure : {
        generation: ids[0] === 'n0' ? '1' : '2',
        items: edges.filter(item => ids.includes(item.value.sourceNodeId)),
      };
    });
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code: 'STATE_GRAPH_RECOVERY_REQUIRED' });
    expect(edgeRequest).toHaveBeenCalledTimes(3);
  });

  it('rejects invalid child positions without retrying', async () => {
    const { reader, edges, edgeRequest } = fixture(4);
    edgeRequest.mockImplementation(async payload => (payload.node_ids as string[]).length === 4
      ? resourceFailure : { generation: '1', items: [{ ...edges[0], position: '-1' }] });
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code: 'STATE_TX_INVALID_REQUEST' });
    expect(edgeRequest).toHaveBeenCalledTimes(2);
  });

  it('enforces the unchanged 64 MiB aggregate indexed limit after successful splits', async () => {
    const { reader, edgeRequest } = fixture(10, 7 * MiB);
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({
      code: resourceFailure.code, message: expect.stringContaining('64 MiB'),
    });
    expect(edgeRequest.mock.calls.filter(([payload]) => (payload.node_ids as string[]).length === 1)).toHaveLength(10);
    expect(edgeRequest).toHaveBeenCalledTimes(19);
  });

  it('enforces the unchanged working-set budget even when merged responses fit 64 MiB', async () => {
    const { reader, nodes, harness } = fixture(9, 7 * MiB);
    for (const node of nodes) {
      node.properties = { description: 'x'.repeat(300_000) };
      harness.seed('mem:graph:nodes', node.id, node as never);
    }
    await expect(reader.readSubgraph(request)).rejects.toBeInstanceOf(GraphRetrievalResourceError);
  });

  it('omits a missing expansion neighbor and its edge while retaining valid graph results', async () => {
    const { reader } = orphanFixture();
    const warning = vi.spyOn(console, 'warn').mockImplementation(() => {});
    try {
      const result = await reader.readSubgraph(request);
      expect([...result.nodes.keys()]).toEqual(['seed', 'neighbor']);
      expect(result.edges.map(edge => edge.sourceNodeId + '->' + edge.targetNodeId)).toEqual(['seed->neighbor']);
      expect(warning).toHaveBeenCalledOnce();
      expect(warning.mock.calls[0][0]).toContain('1 missing expansion neighbor');
      expect(warning.mock.calls[0][0]).not.toContain('orphan');
    } finally {
      warning.mockRestore();
    }
  });

  it('keeps missing selected seeds fatal', async () => {
    const { reader, retrieval } = orphanFixture();
    retrieval.mockImplementation(async payload => payload.action === 'graph_seeds'
      ? { generation: '1', items: [{ key: 'orphan', id: 'orphan', position: '0', entity: true, observation: false }] }
      : { generation: '1', items: [] });
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code: 'GRAPH_RETRIEVAL_NODE_RESOLUTION_FAILED' });
  });

  it('keeps malformed expansion identities fatal', async () => {
    const { reader, harness } = orphanFixture();
    const originalGet = harness.kv.get.bind(harness.kv);
    harness.kv.get = async <T>(scope: string, key: string): Promise<T | null> => {
      const value = await originalGet<T>(scope, key);
      return key === 'neighbor' && value
        ? { ...(value as object), id: 'unexpected' } as T
        : value;
    };
    await expect(reader.readSubgraph(request)).rejects.toMatchObject({ code: 'GRAPH_RETRIEVAL_NODE_RESOLUTION_FAILED' });
  });

  it('keeps non-resource expansion node transport failures fatal', async () => {
    const { reader, harness } = orphanFixture();
    const failure = new Error('node transport failed');
    const originalGet = harness.kv.get.bind(harness.kv);
    harness.kv.get = async <T>(scope: string, key: string): Promise<T | null> => {
      if (key === 'neighbor') throw failure;
      return originalGet<T>(scope, key);
    };
    await expect(reader.readSubgraph(request)).rejects.toBe(failure);
  });
});
