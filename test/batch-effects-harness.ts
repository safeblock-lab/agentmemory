import { StateKV } from "../src/state/kv.js";
import type { StateJsonValue } from "../src/state/state-transactions.js";
import type { StatePageRequest } from "../src/state/state-pages.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";
import { statePageFixture } from "./state-page-fixture.js";
import { KV } from "../src/state/schema.js";
import { batchEffectKey } from "../src/state/batch-effects.js";

type CanonicalOperationKind = "upsert" | "evolve" | "decay";

interface CanonicalReceiptSeed {
  scope: string;
  partition: string;
  operation_kind: CanonicalOperationKind;
  target_id: string;
  effect_key: string;
  operation_fingerprint: string;
  canonical_id: string;
  row_version: string;
  created: boolean;
  result_fingerprint: string;
  normalized_key?: string;
}

function canonicalReceiptIdentity(receipt: Pick<CanonicalReceiptSeed,
  "scope" | "partition" | "operation_kind" | "effect_key" | "target_id" | "normalized_key">): string {
  return receipt.normalized_key === undefined
    ? JSON.stringify(["target", receipt.scope, receipt.partition, receipt.operation_kind, receipt.effect_key, receipt.target_id])
    : JSON.stringify(["normalized", receipt.scope, receipt.partition, receipt.normalized_key, receipt.operation_kind, receipt.effect_key]);
}

export function effectHarness(options: { nativeGraphWire?: boolean } = {}) {
  const store = new Map<string, unknown>();
  const canonicalReceipts = new Map<string, CanonicalReceiptSeed>();
  const handlers = new Map<string, (data: never) => Promise<unknown>>();
  const revisions = new Map<string, number>();
  let crash: { scope: string; after: boolean; remaining: number } | undefined;
  const sdk = {
    registerFunction(id: string, handler: (data: never) => Promise<unknown>) { handlers.set(id, handler); },
    async trigger(input: { function_id: string; payload: Record<string, unknown> }): Promise<unknown> {
      const { scope, key, value } = input.payload as { scope: string; key: string; value: unknown };
      const address = `${scope}:${key}`;
      if (input.function_id === "state::get") return structuredClone(store.get(address) ?? null);
      if (input.function_id === "state::list") return structuredClone([...store].filter(([id]) => id.startsWith(`${scope}:`)).map(([, value]) => value));
      if (input.function_id === "state::list_page") {
        return statePageFixture(store.entries(), input.payload as unknown as StatePageRequest);
      }
      if (input.function_id === "state::scope_revision_v1") return { generation: "1", revision: String(revisions.get(scope) ?? 0) };
      if (input.function_id === "state::retrieval") {
        const request = input.payload;
        if (request.action === "canonical_apply" && request.receipt_only === true) {
          const probe = request as unknown as CanonicalReceiptSeed;
          const receipt = canonicalReceipts.get(canonicalReceiptIdentity(probe));
          if (!receipt) return { status: "not_applied" };
          if (receipt.operation_fingerprint !== probe.operation_fingerprint) throw new Error("STATE_TX_CONFLICT");
          return {
            status: "already_applied",
            applied: false,
            created: receipt.created,
            canonical_id: receipt.canonical_id,
            row_version: receipt.row_version,
            result_fingerprint: receipt.result_fingerprint,
            receipt: {
              operation_kind: receipt.operation_kind,
              effect_key: receipt.effect_key,
              operation_fingerprint: receipt.operation_fingerprint,
              canonical_id: receipt.canonical_id,
              row_version: receipt.row_version,
              created: receipt.created,
              result_fingerprint: receipt.result_fingerprint,
            },
          };
        }
        if (request.action === "graph_read") {
          const values = (scope: string) => [...store].filter(([id]) => id.startsWith(`${scope}:`)).map(([, value]) => value as Record<string, unknown>);
          const authorizedObservationIds = new Set([...store.values()].filter(value => {
            const observation = value as { id?: string; sessionId?: string };
            return observation?.sessionId && (store.get(`${KV.sessions}:${observation.sessionId}`) as { project?: string } | undefined)?.project === request.project;
          }).map(value => (value as { id: string }).id));
          const nodes = values(KV.graphNodes).filter(node => node.project === request.project || (node.properties as Record<string, unknown> | undefined)?.project === request.project || (node.sourceObservationIds as string[] | undefined)?.some(id => authorizedObservationIds.has(id)));
          const nodeIds = new Set(nodes.map(node => node.id));
          const edges = values(KV.graphEdges).filter(edge => nodeIds.has(edge.sourceNodeId) && nodeIds.has(edge.targetNodeId));
          let items: unknown[] = [];
          if (request.mode === "search") items = nodes.filter(node => String(node.name).toLowerCase().includes(String(request.query).toLowerCase())).map(node => ({ id: node.id, key: node.id, entity: true, observation: false }));
          else if (request.mode === "neighbors" || request.mode === "history") items = edges.filter(edge => (request.node_ids as string[] | undefined)?.some(id => id === edge.sourceNodeId || id === edge.targetNodeId) || (request.edge_ids as string[] | undefined)?.includes(String(edge.id))).map(edge => ({ key: edge.id, value: edge }));
          else if (request.mode !== "aggregate") throw new Error(`Unsupported harness graph mode: ${request.mode}`);
          const offset = request.cursor ? Number(request.cursor) : 0;
          const selected = items.slice(offset, offset + Number(request.limit));
          const has_more = offset + selected.length < items.length;
          const page = { status: "ready", project: request.project, revision: "1", items: selected, has_more, ...(has_more ? { next_cursor: String(offset + selected.length) } : {}),
            ...(request.mode === "aggregate" ? { aggregate: { node_count: nodes.filter(node => (request.node_ids as string[]).includes(String(node.id))).length, edge_count: edges.filter(edge => (request.edge_ids as string[]).includes(String(edge.id))).length } } : {}) };
          return Buffer.byteLength(JSON.stringify(page)) > Number(request.max_bytes) ? { code: "STATE_RETRIEVAL_RESOURCE_LIMIT" } : page;
        }
        if (request.action === "reflect_sources") {
          const items: unknown[] = [], deferred: unknown[] = [];
          for (const ref of request.sources as Array<{ scope: string; key: string; expected_version?: string }>) {
            const value = store.get(`${ref.scope}:${ref.key}`) as Record<string, unknown> | undefined;
            const version = String(revisions.get(`${ref.scope}:${ref.key}`) ?? 0);
            if (!value || value.project !== request.project || ref.expected_version && ref.expected_version !== version) deferred.push({ ...ref, reason: "stale" });
            else {
              const entry = { ...ref, value: structuredClone(value), version, fingerprint: batchEffectKey(JSON.stringify(value)) };
              if (Buffer.byteLength(JSON.stringify({ items: [...items, entry], deferred })) > Number(request.max_bytes)) deferred.push({ ...ref, reason: "oversized" });
              else items.push(entry);
            }
          }
          return { items, deferred };
        }
        if (request.action === "batch_work_payload") {
          const items: unknown[] = [], deferred: unknown[] = [];
          for (const id of request.keys as string[]) {
            const work = store.get(`${KV.fireworksBatchWorkItems}:${id}`);
            const entry = { key: id, version: String(revisions.get(`${KV.fireworksBatchWorkItems}:${id}`) ?? 0), fingerprint: batchEffectKey(JSON.stringify(work ?? null)), value: structuredClone(work) };
            if (!work) deferred.push({ key: id, reason: "missing" });
            else if (items.length >= Number(request.max_items) || Buffer.byteLength(JSON.stringify({ items: [...items, entry], deferred })) > Number(request.max_bytes)) deferred.push({ key: id, reason: "oversized" });
            else items.push(entry);
          }
          return { items, deferred };
        }
        if (request.action === "batch_work_submission_patch") {
          const refs = request.items as Array<{ key: string; expected_version: string; expected_fingerprint: string }>;
          const selected = refs.map(ref => store.get(`${KV.fireworksBatchWorkItems}:${ref.key}`) as Record<string, unknown> | undefined);
          if (selected.some((work, i) => !work || work.state !== "queued" || batchEffectKey(JSON.stringify(work)) !== refs[i].expected_fingerprint || String(revisions.get(`${KV.fireworksBatchWorkItems}:${refs[i].key}`) ?? 0) !== refs[i].expected_version || store.has(`mem:reflect:control:legacy-bridge:${refs[i].key}`))) return { applied: false, reason: "stale" };
          for (const work of selected) await sdk.trigger({ function_id: "state::set", payload: { scope: KV.fireworksBatchWorkItems, key: work!.id, value: { ...work, state: "submitted", batchJobId: request.batch_job_id, updatedAt: request.updated_at } } });
          return { applied: true };
        }
        throw new Error(`Unsupported harness retrieval: ${request.action}`);
      }
      if (input.function_id === "state::delete") return store.delete(address);
      if (input.function_id === "state::set") {
        const fail = crash?.scope === scope && --crash.remaining === 0;
        const after = crash?.after;
        if (fail) crash = undefined;
        if (fail && !after) throw new Error("injected crash before write");
        store.set(address, structuredClone(value));
        revisions.set(scope, (revisions.get(scope) ?? 0) + 1);
        revisions.set(address, (revisions.get(address) ?? 0) + 1);
        if (fail) throw new Error("injected lost write acknowledgement");
        return structuredClone(value);
      }
      const handler = handlers.get(input.function_id);
      if (!handler) throw new Error(`unregistered ${input.function_id}`);
      return handler(input.payload as never);
    },
  };
  const rawSdk = { ...sdk, trigger: sdk.trigger.bind(sdk) };
  const kv = new StateKV(rawSdk as never);
  let graphWire: ReturnType<typeof installGraphStateWire> | undefined;
  if (options.nativeGraphWire !== false) {
    const graphKv = {
      get: kv.get.bind(kv),
      set: kv.set.bind(kv),
      delete: kv.delete.bind(kv),
      async list<T>(scope: string): Promise<T[]> {
        return await rawSdk.trigger({ function_id: "state::list", payload: { scope } }) as T[];
      },
    };
    graphWire = installGraphStateWire(sdk as never, graphKv as never);
    Object.assign(kv, {
      getVersioned: graphKv.getVersioned,
      lease: graphKv.lease,
      commitBatch: graphKv.commitBatch,
      values: graphKv.values,
      async set<T>(scope: string, key: string, value: T): Promise<T> {
        return await sdk.trigger({ function_id: "state::set", payload: { scope, key, value } }) as T;
      },
      async delete(scope: string, key: string): Promise<void> {
        await sdk.trigger({ function_id: "state::delete", payload: { scope, key } });
      },
    });
  }
  return {
    kv, sdk, store,
    drainDeliveries: (limit?: number) => {
      if (!graphWire) throw new Error("Native graph wire is not installed");
      return graphWire.drainDeliveries(limit);
    },
    get scopedCommitFailures() { return graphWire?.scopedCommitFailures ?? 0; },
    seed(scope: string, key: string, value: unknown) {
      if (graphWire) graphWire.seed(scope, key, value as StateJsonValue);
      else store.set(`${scope}:${key}`, structuredClone(value));
    },
    seedCanonicalReceipt(receipt: CanonicalReceiptSeed) {
      const identity = canonicalReceiptIdentity(receipt);
      if (canonicalReceipts.has(identity)) throw new Error("STATE_TX_CONFLICT");
      canonicalReceipts.set(identity, structuredClone(receipt));
    },
    crash(scope: string, after = false, remaining = 1) { crash = { scope, after, remaining }; },
    crashGraphCommit(scope: string, afterApply = false, matchingCommitOrdinal = 1) {
      if (!graphWire) throw new Error("Native graph wire is not installed");
      graphWire.failCommitForScope(scope, afterApply, matchingCommitOrdinal);
    },
    failNextCommitBeforeApply() {
      if (!graphWire) throw new Error("Native graph wire is not installed");
      graphWire.failCommitBeforeApply();
    },
    loseNextCommitAcknowledgment() {
      if (!graphWire) throw new Error("Native graph wire is not installed");
      graphWire.loseAcknowledgment();
    },
    call<T = Record<string, unknown>>(id: string, data: Record<string, unknown> = {}): Promise<T> {
      return sdk.trigger({ function_id: id, payload: data }) as Promise<T>;
    },
  };
}
