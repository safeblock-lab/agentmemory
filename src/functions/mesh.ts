import type { ISdk } from "iii-sdk";
import type { StateKV } from "../state/kv.js";
import { KV, generateId } from "../state/schema.js";
import { withKeyedLock } from "../state/keyed-mutex.js";
import { withBatchWriterLocks, withBatchRecordLocks, preserveBatchProvenance } from "../state/batch-effects.js";
import { recordAudit } from "./audit.js";
import type {
  MeshPeer,
  Memory,
  Action,
  SemanticMemory,
  ProceduralMemory,
  MemoryRelation,
  GraphNode,
  GraphEdge,
} from "../types.js";
import { lookup } from "node:dns/promises";
import { BlockList, isIP } from "node:net";
import { request } from "node:https";

const forbidden = new BlockList();
for (const [address, prefix] of [["0.0.0.0", 8], ["10.0.0.0", 8], ["100.64.0.0", 10], ["127.0.0.0", 8], ["169.254.0.0", 16], ["172.16.0.0", 12], ["192.0.0.0", 24], ["192.0.2.0", 24], ["192.88.99.0", 24], ["192.168.0.0", 16], ["198.18.0.0", 15], ["198.51.100.0", 24], ["203.0.113.0", 24], ["224.0.0.0", 3]] as const) forbidden.addSubnet(address, prefix, "ipv4");
for (const [address, prefix] of [["2001::", 23], ["2001:db8::", 32], ["2002::", 16], ["3fff::", 20]] as const) forbidden.addSubnet(address, prefix, "ipv6");
const globalV6 = new BlockList();
globalV6.addSubnet("2000::", 3, "ipv6");

function isPrivateIP(ip: string): boolean {
  const family = isIP(ip);
  if (family === 4) return forbidden.check(ip, "ipv4");
  return family !== 6 || !globalV6.check(ip, "ipv6") || forbidden.check(ip, "ipv6");
}

async function resolveMeshUrl(urlStr: string): Promise<{ url: URL; address: string; family: number }> {
  const url = new URL(urlStr);
  const host = url.hostname.replace(/^\[|\]$/g, "").toLowerCase();
  if (url.protocol !== "https:" || url.username || url.password || url.hash || host === "localhost" || host.endsWith(".localhost") || host.endsWith(".local")) throw new Error("Mesh destination blocked");
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    const resolved = isIP(host) ? [{ address: host, family: isIP(host) }] : await Promise.race([
      lookup(host, { all: true, verbatim: true }),
      new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error("Mesh DNS timed out")), 5000); }),
    ]);
    if (!resolved.length || resolved.length > 16 || resolved.some(({ address }) => isPrivateIP(address))) throw new Error("Mesh destination blocked");
    return { url, ...resolved[0] };
  } finally { clearTimeout(timer); }
}

async function isAllowedUrl(urlStr: string): Promise<boolean> {
  try {
    await resolveMeshUrl(urlStr);
    return true;
  } catch {
    return false;
  }
}

const MAX_MESH_BYTES = 8 * 1024 * 1024;
async function meshRequest(urlStr: string, options: { method?: string; headers: Record<string, string>; body?: string }) {
  if (options.body && Buffer.byteLength(options.body) > MAX_MESH_BYTES) throw new Error("Mesh request too large");
  const resolved = await resolveMeshUrl(urlStr);
  const identity = resolved.url.hostname.replace(/^\[|\]$/g, "");
  return new Promise<{ ok: boolean; status: number; json: () => Promise<unknown> }>((resolve, reject) => {
    // Pin the socket to the validated address; preserve Host and TLS identity.
    const req = request(resolved.url, {
      hostname: resolved.address, family: resolved.family,
      servername: isIP(identity) ? undefined : identity,
      method: options.method ?? "GET", headers: { ...options.headers, Host: resolved.url.host },
      signal: AbortSignal.timeout(30000), agent: false,
    }, (response) => {
      const status = response.statusCode ?? 0;
      if (status >= 300 && status < 400) { response.destroy(); reject(new Error("Mesh redirects blocked")); return; }
      let bytes = 0;
      const chunks: Buffer[] = [];
      response.on("data", (chunk: Buffer) => {
        bytes += chunk.length;
        if (bytes > MAX_MESH_BYTES) { response.destroy(new Error("Mesh response too large")); return; }
        chunks.push(chunk);
      });
      response.on("error", reject);
      response.on("end", () => resolve({ ok: status >= 200 && status < 300, status, json: async () => JSON.parse(Buffer.concat(chunks).toString("utf8")) }));
    });
    req.on("error", () => reject(new Error("Mesh request failed")));
    req.end(options.body);
  });
}

const DEFAULT_SHARED_SCOPES = [
  "memories",
  "actions",
  "semantic",
  "procedural",
  "relations",
  "graph:nodes",
  "graph:edges",
];

interface MeshSyncPayload {
  memories?: Memory[];
  actions?: Action[];
  semantic?: SemanticMemory[];
  procedural?: ProceduralMemory[];
  relations?: MemoryRelation[];
  graphNodes?: GraphNode[];
  graphEdges?: GraphEdge[];
}

async function lwwMergeList<T extends { id: string }>(
  kv: StateKV,
  scope: string,
  items: T[] | undefined,
  _lockPrefix: string,
  tsField: "updatedAt" | "createdAt",
): Promise<number> {
  if (!items || !Array.isArray(items)) return 0;
  let count = 0;
  for (const item of items) {
    if (!item || !item.id || typeof item.id !== "string") continue;
    const ts = (item as Record<string, unknown>)[tsField];
    if (typeof ts !== "string" || Number.isNaN(new Date(ts).getTime())) continue;
    const wrote = await withBatchRecordLocks([[scope, item.id]], async () => {
      const existing = await kv.get<T>(scope, item.id);
      if (!existing) {
        await kv.set(scope, item.id, preserveBatchProvenance(existing, item));
        return true;
      }
      const existingTs = (existing as Record<string, unknown>)[tsField] as string;
      if (new Date(ts) > new Date(existingTs)) {
        await kv.set(scope, item.id, preserveBatchProvenance(existing, item));
        return true;
      }
      return false;
    });
    if (wrote) count++;
  }
  return count;
}

function graphNodeTs(node: GraphNode): string {
  return node.updatedAt || node.createdAt;
}

async function lwwMergeGraphNodes(
  kv: StateKV,
  items: GraphNode[] | undefined,
): Promise<number> {
  if (!items || !Array.isArray(items)) return 0;
  let count = 0;
  for (const item of items) {
    if (!item || !item.id || typeof item.id !== "string") continue;
    const ts = graphNodeTs(item);
    if (!ts || Number.isNaN(new Date(ts).getTime())) continue;
    const wrote = await withBatchRecordLocks([[KV.graphNodes, item.id]], async () => {
      const existing = await kv.get<GraphNode>(KV.graphNodes, item.id);
      if (!existing) {
        await kv.set(KV.graphNodes, item.id, preserveBatchProvenance(existing, item));
        return true;
      }
      if (new Date(ts) > new Date(graphNodeTs(existing))) {
        await kv.set(KV.graphNodes, item.id, preserveBatchProvenance(existing, item));
        return true;
      }
      return false;
    });
    if (wrote) count++;
  }
  return count;
}

export function registerMeshFunction(
  sdk: ISdk,
  kv: StateKV,
  meshAuthToken?: string,
): void {
  sdk.registerFunction("mem::mesh-register",
    async (data: {
      url: string;
      name: string;
      sharedScopes?: string[];
      syncFilter?: { project?: string };
    }) => {
      if (!data || typeof data !== "object") {
        return { success: false, error: "payload required" };
      }
      if (!data.url || !data.name) {
        return { success: false, error: "url and name are required" };
      }

      if (!(await isAllowedUrl(data.url))) {
        return { success: false, error: "URL blocked: private/local address not allowed" };
      }

      const existing = await kv.list<MeshPeer>(KV.mesh);
      const duplicate = existing.find((p) => p.url === data.url);
      if (duplicate) {
        return { success: false, error: "peer already registered", peerId: duplicate.id };
      }

      const peer: MeshPeer = {
        id: generateId("peer"),
        url: data.url,
        name: data.name,
        status: "disconnected",
        sharedScopes: data.sharedScopes || DEFAULT_SHARED_SCOPES,
        syncFilter: data.syncFilter,
      };

      await kv.set(KV.mesh, peer.id, peer);
      await recordAudit(kv, "mesh_sync", "mem::mesh-register", [peer.id], {
        action: "mesh.register",
        peerId: peer.id,
        name: peer.name,
        url: peer.url,
        sharedScopes: peer.sharedScopes,
      });
      return { success: true, peer };
    },
  );

  sdk.registerFunction("mem::mesh-list",
    async () => {
      const peers = await kv.list<MeshPeer>(KV.mesh);
      return { success: true, peers };
    },
  );

  sdk.registerFunction("mem::mesh-sync",
    async (data: { peerId?: string; scopes?: string[]; direction?: "push" | "pull" | "both" }) => {
      if (!meshAuthToken) {
        return {
          success: false,
          error: "mesh sync requires AGENTMEMORY_SECRET",
        };
      }
      if (!data || typeof data !== "object") {
        data = {};
      }

      const direction = data.direction || "both";
      let peers: MeshPeer[];

      if (data.peerId) {
        const peer = await kv.get<MeshPeer>(KV.mesh, data.peerId);
        if (!peer) return { success: false, error: "peer not found" };
        peers = [peer];
      } else {
        peers = await kv.list<MeshPeer>(KV.mesh);
      }

      const results: Array<{
        peerId: string;
        peerName: string;
        pushed: number;
        pulled: number;
        errors: string[];
      }> = [];

      for (const peer of peers) {
        const result = {
          peerId: peer.id,
          peerName: peer.name,
          pushed: 0,
          pulled: 0,
          errors: [] as string[],
        };

        peer.status = "syncing";
        await kv.set(KV.mesh, peer.id, peer);
        await recordAudit(kv, "mesh_sync", "mem::mesh-sync", [peer.id], {
          action: "mesh.sync.start",
          direction,
          scopes: data.scopes || peer.sharedScopes,
        });

        const scopes = data.scopes || peer.sharedScopes;

        try {
          if (!(await isAllowedUrl(peer.url))) {
            result.errors.push("peer URL blocked: private/local address not allowed");
            peer.status = "error";
            await kv.set(KV.mesh, peer.id, peer);
            await recordAudit(kv, "mesh_sync", "mem::mesh-sync", [peer.id], {
              action: "mesh.sync.error",
              error: "peer URL blocked: private/local address not allowed",
            });
            results.push(result);
            continue;
          }

          if (direction === "push" || direction === "both") {
            const pushData = await collectSyncData(kv, scopes, peer.lastSyncAt, peer.syncFilter);
            try {
              const response = await meshRequest(`${peer.url}/agentmemory/mesh/receive`, {
                method: "POST",
                headers: {
                  "Content-Type": "application/json",
                  Authorization: `Bearer ${meshAuthToken}`,
                },
                body: JSON.stringify(pushData),
              });
              if (response.ok) {
                const body = (await response.json()) as { accepted: number };
                result.pushed = body.accepted || 0;
              } else {
                result.errors.push(`push failed: HTTP ${response.status}`);
              }
            } catch (err) {
              result.errors.push(`push failed: ${String(err)}`);
            }
          }

          if (direction === "pull" || direction === "both") {
            try {
              const response = await meshRequest(
                `${peer.url}/agentmemory/mesh/export?since=${peer.lastSyncAt || ""}`,
                {
                  headers: {
                    Authorization: `Bearer ${meshAuthToken}`,
                  },
                },
              );
              if (response.ok) {
                const pullData = (await response.json()) as {
                  memories?: Memory[];
                  actions?: Action[];
                };
                result.pulled = await applySyncData(kv, pullData, scopes);
              } else {
                result.errors.push(`pull failed: HTTP ${response.status}`);
              }
            } catch (err) {
              result.errors.push(`pull failed: ${String(err)}`);
            }
          }

          peer.status = result.errors.length > 0 ? "error" : "connected";
          if (result.errors.length === 0) {
            peer.lastSyncAt = new Date().toISOString();
          }
        } catch (err) {
          peer.status = "disconnected";
          result.errors.push(String(err));
        }

        await kv.set(KV.mesh, peer.id, peer);
        await recordAudit(kv, "mesh_sync", "mem::mesh-sync", [peer.id], {
          action: result.errors.length > 0 ? "mesh.sync.error" : "mesh.sync.complete",
          direction,
          scopes,
          pushed: result.pushed,
          pulled: result.pulled,
          errors: result.errors,
          lastSyncAt: peer.lastSyncAt,
        });
        results.push(result);
      }

      return { success: true, results };
    },
  );

  sdk.registerFunction("mem::mesh-receive",
    async (data: MeshSyncPayload) => {
      if (!data || typeof data !== "object") {
        return { success: false, error: "payload required" };
      }
      return withBatchWriterLocks(kv, ["graph", "consolidation", "crystallize"], async () => {
        let accepted = 0;

        accepted += await lwwMergeList(kv, KV.memories, data.memories, "mem:memory", "updatedAt");
        accepted += await lwwMergeList(kv, KV.actions, data.actions, "mem:action", "updatedAt");
        accepted += await lwwMergeList(kv, KV.semantic, data.semantic, "mem:semantic", "updatedAt");
        accepted += await lwwMergeList(kv, KV.procedural, data.procedural, "mem:procedural", "updatedAt");
        if (data.relations && Array.isArray(data.relations)) {
          for (const rel of data.relations) {
            if (!rel.sourceId || !rel.targetId || !rel.type) continue;
            const relKey = `${rel.sourceId}:${rel.targetId}:${rel.type}`;
            await withKeyedLock(`mem:relation:${relKey}`, async () => {
              const existing = await kv.get<MemoryRelation>(KV.relations, relKey);
              if (!existing) {
                await kv.set(KV.relations, relKey, rel);
                await recordAudit(kv, "mesh_sync", "mem::mesh-receive", [relKey], {
                  action: "mesh.receive.relation",
                  accepted: true,
                });
                accepted++;
              }
            });
          }
        }
        accepted += await lwwMergeGraphNodes(kv, data.graphNodes);
        accepted += await lwwMergeList(kv, KV.graphEdges, data.graphEdges, "mem:gedge", "createdAt");
        await recordAudit(kv, "mesh_sync", "mem::mesh-receive", [], {
          action: "mesh.receive",
          accepted,
        });

        return { success: true, accepted };
      });
    },
  );

  sdk.registerFunction("mem::mesh-remove",
    async (data: { peerId: string }) => {
      if (!data || typeof data !== "object" || !data.peerId) {
        return { success: false, error: "peerId is required" };
      }
      await kv.delete(KV.mesh, data.peerId);
      await recordAudit(kv, "mesh_sync", "mem::mesh-remove", [data.peerId], {
        action: "mesh.remove",
      });
      return { success: true };
    },
  );
}

function deltaFilter<T>(
  items: T[],
  sinceTime: number,
  tsField: "updatedAt" | "createdAt",
): T[] {
  return items.filter(
    (item) => new Date((item as Record<string, unknown>)[tsField] as string).getTime() > sinceTime,
  );
}

async function collectSyncData(
  kv: StateKV,
  scopes: string[],
  since?: string,
  syncFilter?: { project?: string },
): Promise<MeshSyncPayload> {
  const result: MeshSyncPayload = {};
  const parsed = since ? new Date(since).getTime() : 0;
  const sinceTime = Number.isNaN(parsed) ? 0 : parsed;

  if (scopes.includes("memories")) {
    const all = await kv.list<Memory>(KV.memories);
    result.memories = deltaFilter(all, sinceTime, "updatedAt");
  }

  if (scopes.includes("actions")) {
    let all = await kv.list<Action>(KV.actions);
    if (syncFilter?.project) {
      all = all.filter((a) => a.project === syncFilter.project);
    }
    result.actions = deltaFilter(all, sinceTime, "updatedAt");
  }

  const projectScoped = !!syncFilter?.project;

  if (scopes.includes("semantic") && !projectScoped) {
    const all = await kv.list<SemanticMemory>(KV.semantic);
    result.semantic = deltaFilter(all, sinceTime, "updatedAt");
  }

  if (scopes.includes("procedural") && !projectScoped) {
    const all = await kv.list<ProceduralMemory>(KV.procedural);
    result.procedural = deltaFilter(all, sinceTime, "updatedAt");
  }

  if (scopes.includes("relations") && !projectScoped) {
    const all = await kv.list<MemoryRelation>(KV.relations);
    result.relations = deltaFilter(all, sinceTime, "createdAt");
  }

  if (scopes.includes("graph:nodes") && !projectScoped) {
    const all = await kv.list<GraphNode>(KV.graphNodes);
    result.graphNodes = all.filter(
      (n) => new Date(graphNodeTs(n)).getTime() > sinceTime,
    );
  }

  if (scopes.includes("graph:edges") && !projectScoped) {
    const all = await kv.list<GraphEdge>(KV.graphEdges);
    result.graphEdges = deltaFilter(all, sinceTime, "createdAt");
  }

  return result;
}

async function applySyncData(
  kv: StateKV,
  data: MeshSyncPayload,
  scopes: string[],
): Promise<number> {
  return withBatchWriterLocks(kv, ["graph", "consolidation", "crystallize"], async () => {
    let applied = 0;

    if (scopes.includes("memories")) {
      applied += await lwwMergeList(kv, KV.memories, data.memories, "mem:memory", "updatedAt");
    }
    if (scopes.includes("actions")) {
      applied += await lwwMergeList(kv, KV.actions, data.actions, "mem:action", "updatedAt");
    }
    if (scopes.includes("semantic")) {
      applied += await lwwMergeList(kv, KV.semantic, data.semantic, "mem:semantic", "updatedAt");
    }
    if (scopes.includes("procedural")) {
      applied += await lwwMergeList(kv, KV.procedural, data.procedural, "mem:procedural", "updatedAt");
    }
    if (scopes.includes("relations") && data.relations) {
      for (const rel of data.relations) {
        if (!rel.sourceId || !rel.targetId || !rel.type) continue;
        const relKey = `${rel.sourceId}:${rel.targetId}:${rel.type}`;
        const wrote = await withKeyedLock(`mem:relation:${relKey}`, async () => {
          const existing = await kv.get<MemoryRelation>(KV.relations, relKey);
          if (!existing) {
            await kv.set(KV.relations, relKey, rel);
            return true;
          }
          return false;
        });
        if (wrote) applied++;
      }
    }
    if (scopes.includes("graph:nodes")) {
      applied += await lwwMergeGraphNodes(kv, data.graphNodes);
    }
    if (scopes.includes("graph:edges")) {
      applied += await lwwMergeList(kv, KV.graphEdges, data.graphEdges, "mem:gedge", "createdAt");
    }

    return applied;
  });
}
