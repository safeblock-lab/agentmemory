import { readFileSync } from "node:fs";
import vm from "node:vm";
import { TextEncoder } from "node:util";
import { describe, expect, it, vi } from "vitest";

const html = readFileSync(new URL("../src/viewer/index.html", import.meta.url), "utf8");
const functions = html.slice(html.indexOf("    var projectReflections ="), html.indexOf("    async function endSession("));
const branch = html.slice(html.indexOf("      if (action === 'reflect-project')"), html.indexOf("      if (action === 'summarize-session')"));
const token = (cut = 1) => ({ jobId: "job", project: "project", requestId: "request", checkpoint: `checkpoint-${cut}`, cut });
const response = (status = "pending", continuation: unknown = token()) => ({ success: true, jobId: "job", requestId: "request",
  status, completed: status === "completed", terminal: ["completed", "partial", "failed"].includes(status), continuation,
  processedClusters: 2, appliedInsights: 3, clustersSkipped: 1,
  coverage: { requestedFamilies: ["memory", "observation"], excludedFamilies: ["fact", "lesson", "crystal"], complete: status === "completed" } });
const escape = (value: unknown) => String(value).replace(/[&<>"']/g, char => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[char]!);

function fixture(...responses: unknown[]) {
  const status = { textContent: "Ready" };
  const list = { textContent: "", innerHTML: "", isConnected: true, reflectionRequest: undefined as unknown };
  const panel = { querySelector: (selector: string) => selector.includes("insights") ? list : status };
  let project = "project";
  const button = { textContent: "Reflect project", disabled: false, isConnected: true,
    getAttribute: () => project, closest: () => panel };
  const apiPost = vi.fn(async (_path: string, _payload: unknown) => {
    const result = responses.shift();
    if (result instanceof Error) throw result;
    return await result;
  });
  const apiGet = vi.fn(async (_path: string): Promise<unknown> => ({ success: true, insights: [] }));
  const context = vm.createContext({ apiPost, apiGet, TextEncoder, crypto: { randomUUID: () => "request" }, esc: escape });
  vm.runInContext(functions, context);
  const reflect = context.reflectProject as (project: string, target: typeof button) => Promise<void>;
  let pending: Promise<void> | undefined;
  context.reflectProject = (id: string, target: typeof button) => { pending = reflect(id, target); };
  vm.runInContext(`function click(action, target) { ${branch} }`, context);
  return { button, status, list, apiPost, apiGet, context,
    click: async () => { context.click("reflect-project", button); await pending; },
    switchProject: (value: string) => { project = value; },
    load: () => (context.loadReflectionInsights as (btn: typeof button, project: string) => Promise<void>)(button, project) };
}

describe("viewer bounded project reflection", () => {
  it("uses business fields and advances one cut per explicit click with stable identity", async () => {
    const view = fixture({ ...response(), deadline: Date.now() - 1 }, response("waiting_index", token(2)), response("completed", null));
    await view.click();
    expect(view.apiPost).toHaveBeenCalledTimes(1);
    expect(view.apiPost).toHaveBeenLastCalledWith("reflect", { project: "project", requestId: "request", manual: true, maxClusters: 1 });
    expect(view.button.textContent).toBe("Continue reflection");
    await view.click();
    expect(view.apiPost).toHaveBeenLastCalledWith("reflect", { project: "project", requestId: "request", manual: true, maxClusters: 1, continuation: token() });
    expect(view.status.textContent).toContain("Waiting for index");
    await view.click();
    expect(view.apiPost).toHaveBeenLastCalledWith("reflect", { project: "project", requestId: "request", manual: true, maxClusters: 1, continuation: token(2) });
    expect(view.status.textContent).toContain("Completed bounded selection");
    expect(view.button.disabled).toBe(true);
  });

  it.each(["pending", "waiting_index", "waiting_graph", "waiting_scope", "waiting_budget", "waiting_batch", "blocked", "partial", "failed", "completed"])("truthfully renders %s, coverage and counts", async status => {
    const view = fixture(response(status, ["completed", "partial", "failed"].includes(status) ? null : token()));
    await view.click();
    expect(view.status.textContent).toContain("Clusters processed: 2");
    expect(view.status.textContent).toContain("Insights applied: 3");
    expect(view.status.textContent).toContain("Selected families: memory, observation");
    expect(view.status.textContent).toContain("Global family coverage is incomplete");
    expect(view.button.disabled).toBe(["completed", "partial", "failed"].includes(status));
    expect(view.apiPost).toHaveBeenCalledTimes(1);
    if (status === "partial") expect(view.status.textContent).toContain("Partial: selection unfinished");
  });

  it("guards duplicate clicks while pending and preserves the native button", async () => {
    let resolve!: (value: unknown) => void;
    const view = fixture(new Promise(done => { resolve = done; }));
    const pending = view.click();
    expect(view.button.disabled).toBe(true);
    expect(view.status.textContent).toContain("Pending");
    await view.click();
    resolve(response());
    await pending;
    expect(view.apiPost).toHaveBeenCalledTimes(1);
    expect(view.button.textContent).toBe("Continue reflection");
  });

  it.each([null, new Error("secret transport detail")])("retains exact original payload after unknown outcome %j", async failure => {
    const view = fixture(response(), failure, response("blocked", token(2)));
    await view.click();
    await view.click();
    const original = view.apiPost.mock.calls[1][1];
    expect(view.button.textContent).toBe("Resume reflection");
    expect(view.status.textContent).not.toContain("secret");
    expect(view.apiPost).toHaveBeenCalledTimes(2);
    await view.click();
    expect(view.apiPost.mock.calls[2][1]).toBe(original);
  });

  it.each([{ ...token(), project: "other" }, { ...token(), requestId: "other" }, { ...token(), jobId: "other" },
    { ...token(), cut: 0 }, { ...token(), cut: 257 }, { ...token(), cut: 1.5 }, { ...token(), extra: true }])("pauses mismatched continuation %j", async continuation => {
    const view = fixture(response("pending", continuation));
    await view.click();
    await view.click();
    expect(view.apiPost).toHaveBeenCalledTimes(1);
    expect(view.status.textContent).toContain("could not be validated");
  });

  it("refuses repeated and skipped cuts or a changed job", async () => {
    for (const next of [response("pending", token()), response("pending", token(3)), { ...response("pending", token(2)), jobId: "foreign" }]) {
      const view = fixture(response(), next);
      await view.click();
      await view.click();
      expect(view.button.disabled).toBe(true);
      expect(view.status.textContent).toContain("could not be validated");
    }
  });

  it.each(["detach", "switch"])("ignores %s responses and retains ambiguous request", async mode => {
    let resolve!: (value: unknown) => void;
    const view = fixture(new Promise(done => { resolve = done; }));
    const pending = view.click();
    if (mode === "detach") view.button.isConnected = false;
    else view.switchProject("other");
    resolve(response());
    await pending;
    expect(view.status.textContent).toContain("Pending");
    expect(view.apiGet).not.toHaveBeenCalled();
    view.button.isConnected = true;
    view.switchProject("project");
    await view.click();
    expect(view.apiPost.mock.calls[1][1]).toBe(view.apiPost.mock.calls[0][1]);
  });

  it("keeps partial terminal and permits a replacement panel to resume only the original request", async () => {
    const terminal = fixture({ ...response("partial", null), terminal: false });
    await terminal.click();
    expect(terminal.button.disabled).toBe(true);
    expect(terminal.status.textContent).toContain("Partial");
    let resolve!: (value: unknown) => void;
    const view = fixture(new Promise(done => { resolve = done; }), response());
    const pending = view.click();
    view.button.isConnected = false;
    const replacement = { ...view.button, isConnected: true };
    (view.context.mountReflection as (btn: typeof replacement) => void)(replacement);
    expect(replacement.disabled).toBe(true);
    resolve(response());
    await pending;
    expect(replacement.textContent).toBe("Resume reflection");
    await (view.context.reflectProject as (project: string, btn: typeof replacement) => Promise<void>)("project", replacement);
    expect(view.apiPost.mock.calls[1][1]).toBe(view.apiPost.mock.calls[0][1]);
  });

  it("loads separate scoped canonical insights with safe escaping and empty/error states", async () => {
    const view = fixture();
    view.switchProject('<project&"');
    const markup = (view.context.reflectionPanel as (project: string) => string)('<project&"');
    expect(markup).toContain("&lt;project&amp;&quot;");
    expect(markup).toContain('role="status"');
    view.apiGet.mockResolvedValueOnce({ insights: [{ project: '<project&"', title: "<img>", content: "<script>" }, { project: "other", title: "foreign" }] });
    await view.load();
    expect(view.apiGet).toHaveBeenLastCalledWith('insights?project=%3Cproject%26%22&limit=20');
    expect(view.list.innerHTML).toContain("&lt;script&gt;");
    expect(view.list.innerHTML).not.toContain("foreign");
    await view.load();
    expect(view.list.innerHTML).toContain("No canonical insights");
    view.apiGet.mockResolvedValueOnce(null);
    await view.load();
    expect(view.list.textContent).toContain("Could not load");
  });

  it.each(["detach", "switch", "newer"])("ignores stale insight results after %s", async mode => {
    let resolve!: (value: unknown) => void;
    const view = fixture();
    view.apiGet.mockImplementationOnce(() => new Promise(done => { resolve = done; }));
    const pending = view.load();
    expect(view.list.textContent).toContain("Loading");
    if (mode === "detach") view.button.isConnected = false;
    if (mode === "switch") view.switchProject("other");
    if (mode === "newer") await view.load();
    resolve({ insights: [{ project: "project", title: "stale", content: "stale" }] });
    await pending;
    expect(view.list.innerHTML).not.toContain("stale");
  });
});
