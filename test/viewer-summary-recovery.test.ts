import { readFileSync } from "node:fs";
import vm from "node:vm";
import { describe, expect, it, vi } from "vitest";

const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
const functions = html.slice(html.indexOf("    async function summarizeSession("), html.indexOf("    async function loadLessons()"));
const clickBranch = html.slice(html.indexOf("      if (action === 'summarize-session')"), html.indexOf("      if (action === 'toggle-audit')"));
const token = () => ({ operationId: "recovery-operation", step: 0, jobId: "failed-job", project: "project",
  sessionId: "session", sourceFingerprint: "original-source", round: 1, unitId: "last-unit", deliveryId: "recovery-delivery" });
const done = { success: true, completed: true, summary: { request: "Summarized" } };
const failed = (continuation: unknown) => ({ success: false, retained: true,
  error: "summary_publication_invalid_response", continuation });

function fixture(...responses: unknown[]) {
  const apiPost = vi.fn(async (_path: string, _payload: { sessionId: string; continuation?: unknown }) => {
    const response = responses.shift();
    if (response instanceof Error) throw response;
    return response;
  });
  const timers: Array<() => void> = [];
  const button = { textContent: "Summarize", disabled: false, isConnected: true,
    summaryRecovery: null as unknown, getAttribute: () => "session" };
  const context = vm.createContext({ apiPost, setTimeout: (callback: () => void) => timers.push(callback) });
  vm.runInContext(functions, context);
  const summarize = context.summarizeSession as (id: string, target: typeof button) => Promise<void>;
  let pending: Promise<void> | undefined;
  context.summarizeSession = (id: string, target: typeof button) => { pending = summarize(id, target); };
  vm.runInContext(`function click(action, target) { ${clickBranch} }`, context);
  return { apiPost, button, timers,
    summarize: (id = "session", target = button) => summarize(id, target),
    click: async () => { context.click("summarize-session", button); await pending; },
    reset: () => { timers.splice(0).forEach(callback => callback()); } };
}

describe("viewer explicit summary recovery", () => {
  it("retains server token without automatic regeneration, then sends it unchanged on a later click", async () => {
    const continuation = token();
    const view = fixture(failed(continuation), done, done);
    await view.click();
    expect(view.apiPost).toHaveBeenCalledTimes(1);
    expect(view.apiPost).toHaveBeenNthCalledWith(1, "summarize", { sessionId: "session" });
    expect(view.button.textContent).toBe("Retry / Regenerate");
    expect(view.button.disabled).toBe(false);
    expect(view.button.summaryRecovery).toBe(continuation);
    expect(view.timers).toHaveLength(0);
    await view.click();
    expect(view.apiPost).toHaveBeenNthCalledWith(2, "summarize", { sessionId: "session", continuation });
    expect(view.apiPost.mock.calls[1][1].continuation).toBe(continuation);
    expect(view.button.textContent).toBe("Done");
    expect(view.button.summaryRecovery).toBeNull();
    view.reset();
    await view.click();
    expect(view.apiPost).toHaveBeenNthCalledWith(3, "summarize", { sessionId: "session" });
  });

  it("preserves ordinary successful bounded continuation and loading behavior", async () => {
    const continuation = token();
    const view = fixture({ success: true, completed: false, continuation, maxRequests: 2, deadline: Date.now() + 60000 }, done);
    const pending = view.click();
    expect(view.button.disabled).toBe(true);
    expect(view.button.textContent).toBe("Summarizing...");
    await view.click();
    await pending;
    expect(view.apiPost).toHaveBeenCalledTimes(2);
    expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "session", continuation });
    expect(view.button.textContent).toBe("Done");
    view.reset();
    expect(view.button).toMatchObject({ textContent: "Summarize", disabled: false, summaryRecovery: null });
  });

  it.each(["omitted", "expired"])("continues within the request ceiling when the workflow deadline is %s", async deadlineMode => {
    const continuation = token();
    const pending = { success: true, completed: false, continuation,
      ...(deadlineMode === "expired" ? { deadline: Date.now() - 1 } : {}) };
    const view = fixture({ ...pending, maxRequests: 2 }, pending, done);
    await view.click();
    expect(view.apiPost).toHaveBeenCalledTimes(3);
    expect(view.apiPost).toHaveBeenNthCalledWith(2, "summarize", { sessionId: "session", continuation });
    expect(view.apiPost).toHaveBeenNthCalledWith(3, "summarize", { sessionId: "session", continuation });
    expect(view.button.textContent).toBe("Done");
  });

  it("stops a successful loop at a recoverable failure until another click", async () => {
    const continuation = token();
    const recovery = { ...token(), operationId: "new-recovery", deliveryId: "new-delivery" };
    const view = fixture({ success: true, completed: false, continuation, maxRequests: 2, deadline: Date.now() + 60000 }, failed(recovery), done);
    await view.click();
    expect(view.apiPost).toHaveBeenCalledTimes(2);
    expect(view.button.textContent).toBe("Retry / Regenerate");
    await view.click();
    expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "session", continuation: recovery });
  });

  it.each([null, [], {}, { ...token(), extra: true }, { ...token(), sessionId: "other" },
    { ...token(), step: -1 }, { ...token(), step: 0.5 }, { ...token(), round: Number.MAX_SAFE_INTEGER + 1 },
    { ...token(), round: "0" }, { ...token(), operationId: " " }, { ...token(), unitId: " unit" },
    { ...token(), sourceFingerprint: "x".repeat(513) }, { ...token(), deliveryId: undefined }])(
    "discards malformed or foreign failure token %j", async continuation => {
      const view = fixture(failed(continuation), done);
      await view.click();
      expect(view.apiPost).toHaveBeenCalledTimes(1);
      expect(view.button.summaryRecovery).toBeNull();
      expect(view.button.textContent).toBe("Failed");
      view.reset();
      await view.click();
      expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "session" });
    });

  it("never sends a retained token for a different session", async () => {
    const view = fixture(failed(token()), done);
    await view.click();
    await view.summarize("other");
    expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "other" });
    expect(view.button.summaryRecovery).toBeNull();
  });

  it("revalidates a retained token before sending", async () => {
    const continuation = token();
    const view = fixture(failed(continuation), done);
    await view.click();
    continuation.step = -1;
    await view.click();
    expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "session" });
  });

  it.each([failed(null), new Error("network failure")])("clears recovery after an unsupported failure %j", async response => {
    const view = fixture(failed(token()), response, done);
    await view.click();
    await view.click();
    expect(view.button.summaryRecovery).toBeNull();
    expect(view.button.textContent).toBe("Failed");
    view.reset();
    await view.click();
    expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "session" });
  });

  it("does not transfer recovery to a replacement button or act on a detached button", async () => {
    const view = fixture(failed(token()), done);
    await view.click();
    view.button.isConnected = false;
    await view.click();
    expect(view.apiPost).toHaveBeenCalledTimes(1);
    const replacement = { ...view.button, isConnected: true, summaryRecovery: null };
    await view.summarize("session", replacement);
    expect(view.apiPost).toHaveBeenLastCalledWith("summarize", { sessionId: "session" });
  });

  it("keeps the existing native button and focused element during recovery", async () => {
    const view = fixture(failed(token()));
    const focusedButton = view.button;
    await view.click();
    expect(view.button).toBe(focusedButton);
    expect(view.button.textContent).toBe("Retry / Regenerate");
    expect(html).toContain('data-action="summarize-session"');
  });
});
