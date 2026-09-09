import { StateKV } from "../src/state/kv.js";

export function effectHarness() {
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
  return {
    kv: new StateKV(sdk as never), sdk, store,
    crash(scope: string, after = false, remaining = 1) { crash = { scope, after, remaining }; },
    call<T = Record<string, unknown>>(id: string, data: Record<string, unknown> = {}): Promise<T> {
      return sdk.trigger({ function_id: id, payload: data }) as Promise<T>;
    },
  };
}
