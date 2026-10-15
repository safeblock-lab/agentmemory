import { vi } from "vitest";
vi.mock("iii-sdk", () => ({ InvocationError: class InvocationError extends Error { code = "TEST_INVOCATION_ERROR"; } }));
import { KV } from "../../src/state/schema.js";
import { registerReflectFunctions } from "../../src/functions/reflect.js";

export function reflectHarness() {
  const rows = new Map<string, unknown>();
  const functions = new Map<string, (data: any) => Promise<any>>();
  const published: unknown[] = [];
  const identity = { index_id: "observations", model: "local-test", dimensions: 2, generation: "local-v1" };
  const graph = [KV.graphNodes, KV.graphEdges].map(scope => ({ scope, status: "ready", revision: "1" }));
  const index = { ...identity, status: "ready", count: 3, lexical_count: 3, lexical_ready: true,
    source_kind: "agentmemory", source_prepared: true, coverage_ready: true, dirty_count: 0 };
  const projectionFailures = new Map<string, string>();
  const candidates = [{ obsId: "one", sessionId: "session", score: 1 }, { obsId: "two", sessionId: "session", score: 0.8 }];
  const kv = {
    get: vi.fn(async (scope: string, key: string) => structuredClone(rows.get(`${scope}/${key}`) ?? null)),
    set: vi.fn(async (scope: string, key: string, value: unknown) => { rows.set(`${scope}/${key}`, structuredClone(value)); return value; }),
    delete: vi.fn(async (scope: string, key: string) => { rows.delete(`${scope}/${key}`); }),
    list: vi.fn(async (scope: string) => [...rows].filter(([key]) => key.startsWith(`${scope}/`)).map(([, value]) => structuredClone(value))),
    pages: vi.fn(async function* (scope: string, options: unknown) {
      const items = [...rows].filter(([key]) => key.startsWith(`${scope}/`)).map(([, value]) => structuredClone(value));
      yield { items: items.slice(0, 16), next_cursor: items.length > 16 ? "next" : undefined };
      throw new Error("recovery_must_stop_after_one_page");
    }),
    retrieval: vi.fn(async (request: any): Promise<any> => {
      if (request.action === "index_status") return { version: 1, capabilities: ["state::indexed_graph_v1", "state::semantic_lsh_v1"], graph, semantic: [index] };
      if (request.action === "keyword_search" || request.action === "semantic_search") return { ...identity, items: structuredClone(candidates.filter(item => (rows.get(`${KV.memories}/${item.obsId}`) as { project?: string })?.project === request.project)) };
      if (request.action === "project_catalog_page") return { status: "ready", items: [{ project: "/app" }], has_more: false };
      if (request.action === "reflect_legacy_work") return { item: structuredClone(rows.get(`legacy/${request.key}`)), version: "1", fingerprint: "legacy-version-fingerprint" };
      if (request.action === "shared_source_projection") {
        const ref = request.sources[0]; const value: any = rows.get(`${ref.scope}/${ref.key}`);
        if (!value) return { items: [], deferred: [{ ...ref, reason: "missing" }] };
        const offset = Number(ref.provenance_cursor ?? "0");
        const provenance = (value.provenance ?? [{ session_id: "session", project: "/app" }]).slice(offset, offset + 12);
        return { items: [{ ...ref, version: value.version ?? "1", fingerprint: JSON.stringify(value), value: structuredClone(value), provenance,
          next_cursor: offset + 12 < (value.provenance?.length ?? 1) ? String(offset + 12) : undefined }], deferred: [] };
      }
      if (request.action === "reflect_sources") {
        const ref = request.sources[0];
        const value: any = rows.get(`${ref.scope}/${ref.key}`);
        const reason = projectionFailures.get(ref.key) ?? (!value ? "missing" : value.project !== request.project ? "project_mismatch" : ref.expected_version && ref.expected_version !== (value.version ?? "1") ? "stale" : undefined);
        return reason ? { items: [], deferred: [{ ...ref, reason }] } : { items: [{ ...ref, version: value.version ?? "1", fingerprint: JSON.stringify(value), value: structuredClone(value) }], deferred: [] };
      }
      throw new Error(`unexpected_retrieval:${request.action}`);
    }),
  };
  const sdk = {
    registerFunction: (id: string, handler: (data: any) => Promise<any>) => functions.set(id, handler),
    registerTrigger: vi.fn(),
    trigger: vi.fn(async (input: any) => {
      if (input.function_id === "iii::durable::publish") { published.push(input.payload); return {}; }
      const handler = functions.get(input.function_id);
      if (!handler) throw new Error(`unexpected_function:${input.function_id}`);
      return handler(input.payload);
    }),
  };
  const provider = { name: "test", compress: vi.fn(), summarize: vi.fn(async () => '<insights><insight confidence="0.8" title="Boundaries">Validate input at boundaries.</insight></insights>') };
  const embedding = { name: identity.model, dimensions: 2, embed: vi.fn(async () => [0.5, 0.5]) };
  rows.set(`${KV.config}/indexed-corpus`, { ...identity, status: "ready" });
  for (const id of ["one", "two"]) {
    rows.set(`${KV.memories}/${id}`, { id, project: "/app", title: id, content: `Boundary validation ${id}`, concepts: [], files: [], strength: 0.8, createdAt: "2026-10-01" });
    rows.set(`${KV.semanticVectors(identity.index_id)}/${id}`, { sourceScope: KV.memories, sourceVersion: "1" });
  }
  registerReflectFunctions(sdk as never, kv as never, provider as never, undefined, undefined, undefined, embedding as never);
  const call = (function_id: string, payload: unknown = {}) => sdk.trigger({ function_id, payload });
  return { rows, kv, sdk, provider, embedding, call, published, candidates, projectionFailures, index, graph };
}
