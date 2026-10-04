import { StateKV } from "../src/state/kv.js";
import type { StateJsonValue } from "../src/state/state-transactions.js";
import type { StatePageRequest } from "../src/state/state-pages.js";
import { installGraphStateWire } from "./helpers/graph-state-harness.js";
import { statePageFixture } from "./state-page-fixture.js";

export function effectHarness(options: { nativeGraphWire?: boolean } = {}) {
  const store = new Map<string, unknown>();
  const handlers = new Map<string, (data: never) => Promise<unknown>>();
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
      if (input.function_id === "state::delete") return store.delete(address);
      if (input.function_id === "state::set") {
        const fail = crash?.scope === scope && --crash.remaining === 0;
        const after = crash?.after;
        if (fail) crash = undefined;
        if (fail && !after) throw new Error("injected crash before write");
        store.set(address, structuredClone(value));
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
    get scopedCommitFailures() { return graphWire?.scopedCommitFailures ?? 0; },
    seed(scope: string, key: string, value: unknown) {
      if (graphWire) graphWire.seed(scope, key, value as StateJsonValue);
      else store.set(`${scope}:${key}`, structuredClone(value));
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
