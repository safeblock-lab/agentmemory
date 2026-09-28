import { readFileSync } from "node:fs";
import { describe, expect, it, vi } from "vitest";

const viewer = readFileSync("src/viewer/index.html", "utf8");
const updateCode = viewer.slice(
  viewer.indexOf("var UPDATE_ACTIVE_PHASES ="),
  viewer.indexOf("    async function loadHealth()"),
);

type UpdateState = {
  support: { supported: boolean; reason?: string } | null;
  status: { phase: string; currentVersion?: string; targetVersion?: string; message?: string; errorCode?: string } | null;
  csrfToken: string;
  check: { currentVersion: string; version: string; available: boolean } | null;
  checking: boolean;
  starting: boolean;
  error: string;
  reconnecting: boolean;
  pollTimer: number | null;
  pollMisses: number;
};

type UpdateFunctions = {
  checkReleaseUpdate: () => Promise<void>;
  startReleaseUpdateFromViewer: () => Promise<void>;
  scheduleUpdatePoll: () => void;
  renderUpdateCard: () => string;
  loadUpdateStatus: (promptForSecret?: boolean) => Promise<void>;
};

function sandbox(fetcher: (url: string, options?: RequestInit) => Promise<Response>, enteredSecret = "viewer-secret") {
  const update: UpdateState = {
    support: null, status: null, csrfToken: "", check: null, checking: false,
    starting: false, error: "", reconnecting: false, pollTimer: null, pollMisses: 0,
  };
  const state = { health: { update, report: { service: { version: "0.9.58" } } } };
  const timers: Array<() => Promise<void>> = [];
  const focus = vi.fn();
  const dialogs: Array<{ innerHTML: string; input: { value: string }; removed: boolean }> = [];
  const document = {
    querySelector: () => ({ focus }),
    body: { appendChild: vi.fn() },
    createElement: (tag: string) => {
      if (tag !== "dialog") throw new Error(`Unexpected ${tag}`);
      const listeners: Record<string, (event: { preventDefault: () => void }) => void> = {};
      const form = { addEventListener: (name: string, handler: typeof listeners[string]) => { listeners[`form:${name}`] = handler; } };
      const cancel = { addEventListener: (name: string, handler: typeof listeners[string]) => { listeners[`cancel:${name}`] = handler; } };
      const input = { value: enteredSecret, focus: vi.fn() };
      const dialog = {
        className: "", innerHTML: "", input, removed: false,
        setAttribute: vi.fn(),
        querySelector: (selector: string) => selector === "form" ? form : selector === "#update-secret-input" ? input : cancel,
        addEventListener: (name: string, handler: typeof listeners[string]) => { listeners[name] = handler; },
        showModal: () => { queueMicrotask(() => listeners["form:submit"]({ preventDefault: () => {} })); },
        close: vi.fn(),
        remove: () => { dialog.removed = true; },
      };
      dialogs.push(dialog);
      return dialog;
    },
  };
  const esc = (value: unknown) => String(value).replace(/[&<>"']/g, (char) => ({
    "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
  })[char] || char);
  const functions = new Function(
    "state", "fetch", "setTimeout", "renderHealth", "document", "esc", "window",
    `${updateCode}\nreturn { checkReleaseUpdate, startReleaseUpdateFromViewer, scheduleUpdatePoll, renderUpdateCard, loadUpdateStatus };`,
  )(
    state, fetcher, (callback: () => Promise<void>) => { timers.push(callback); return timers.length; },
    () => {}, document, esc, {},
  ) as UpdateFunctions;
  return { update, timers, focus, dialogs, ...functions };
}

describe("viewer release update controls", () => {
  it("shows an accessible disabled control and its support reason", () => {
    const ui = sandbox(async () => { throw new Error("Unexpected network call"); });
    ui.update.support = { supported: false, reason: "Native Windows global npm installation required." };
    const html = ui.renderUpdateCard();
    expect(html).toContain('aria-labelledby="update-title"');
    expect(html).toContain('role="status" aria-live="polite"');
    expect(html).toContain('button class="btn" disabled');
    expect(html).toContain("Native Windows global npm installation required.");
  });

  it("checks only on user action, confirms versions, and sends one bodyless start with a fresh token", async () => {
    const fetcher = vi.fn(async (url: string, options?: RequestInit) => {
      if (url === "/update/check") return Response.json({ currentVersion: "0.9.58", version: "0.9.59", tag: "v0.9.59", available: true });
      if (url === "/update/status") return Response.json({ support: { supported: true }, status: { phase: "idle", currentVersion: "0.9.58" }, csrfToken: "fresh-token" });
      if (url === "/update/start") return Response.json({ phase: "stopping", currentVersion: "0.9.58", targetVersion: "0.9.59" }, { status: 202 });
      throw new Error(`Unexpected ${url} ${options?.method}`);
    });
    const ui = sandbox(fetcher);
    ui.update.status = { phase: "idle", currentVersion: "0.9.58" };
    expect(fetcher).not.toHaveBeenCalled();
    await ui.checkReleaseUpdate();
    expect(ui.dialogs).toHaveLength(1);
    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(fetcher.mock.calls[0][0]).toBe("/update/check");
    expect(fetcher.mock.calls[0][1]).toMatchObject({ headers: { "X-AgentMemory-Update-Secret": "viewer-secret" } });
    expect(ui.renderUpdateCard()).toContain("Release v0.9.59 is available (currently v0.9.58)");
    expect(ui.renderUpdateCard()).toContain('data-action="confirm-update"');
    expect(ui.focus).toHaveBeenCalled();
    await ui.startReleaseUpdateFromViewer();
    expect(ui.dialogs).toHaveLength(1);
    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(fetcher.mock.calls[1][1]).toMatchObject({ headers: { "X-AgentMemory-Update-Secret": "viewer-secret" } });
    const [path, request] = fetcher.mock.calls[2];
    expect(path).toBe("/update/start");
    expect(request).toMatchObject({ method: "POST", mode: "same-origin", credentials: "same-origin", headers: { "X-AgentMemory-Update-Token": "fresh-token", "X-AgentMemory-Update-Secret": "viewer-secret" } });
    expect(request).not.toHaveProperty("body");
    expect(ui.update.check).toBeNull();
    expect(ui.renderUpdateCard()).toContain("Stopping AgentMemory");
    expect(ui.timers).toHaveLength(1);
    await ui.startReleaseUpdateFromViewer();
    expect(fetcher).toHaveBeenCalledTimes(3);
  });

  it("keeps polling through temporary network loss until the restarted service reports completion", async () => {
    let calls = 0;
    const ui = sandbox(async () => {
      calls++;
      if (calls === 1) throw new Error("Socket closed");
      if (calls === 2) return Response.json({ support: { supported: true }, status: { phase: "starting", currentVersion: "0.9.58", targetVersion: "0.9.59" }, csrfToken: "new-token" });
      return Response.json({ support: { supported: true }, status: { phase: "complete", currentVersion: "0.9.58", targetVersion: "0.9.59" }, csrfToken: "new-token" });
    });
    ui.update.support = { supported: true };
    ui.update.status = { phase: "stopping", currentVersion: "0.9.58", targetVersion: "0.9.59" };
    ui.scheduleUpdatePoll();
    await ui.timers.shift()?.();
    expect(ui.renderUpdateCard()).toContain("Reconnecting to AgentMemory");
    expect(ui.timers).toHaveLength(1);
    await ui.timers.shift()?.();
    expect(ui.renderUpdateCard()).toContain("Starting AgentMemory");
    await ui.timers.shift()?.();
    expect(ui.renderUpdateCard()).toContain("Update complete");
    expect(ui.renderUpdateCard()).toContain("Current version: v0.9.59");
    expect(ui.timers).toHaveLength(0);
  });

  it("continues reconnecting after fifteen minutes and shows safe error codes", async () => {
    const ui = sandbox(async () => { throw new Error("Socket closed"); });
    ui.update.support = { supported: true };
    ui.update.status = { phase: "starting", errorCode: "UPDATE_RESTART_FAILED" };
    ui.update.pollMisses = 450;
    ui.scheduleUpdatePoll();
    await ui.timers.shift()?.();
    expect(ui.update.pollMisses).toBe(451);
    expect(ui.timers).toHaveLength(1);
    expect(ui.renderUpdateCard()).toContain("Error code: UPDATE_RESTART_FAILED");
  });

  it("requires a new check after a rejected token", async () => {
    let statusCalls = 0;
    const ui = sandbox(async (url) => {
      if (url === "/update/status") {
        statusCalls++;
        return Response.json({ support: { supported: true }, status: { phase: "idle", currentVersion: "0.9.58" }, csrfToken: `token-${statusCalls}` });
      }
      return Response.json({ error: "Invalid update token." }, { status: 403 });
    });
    ui.update.support = { supported: true };
    ui.update.check = { currentVersion: "0.9.58", version: "0.9.59", available: true };
    await ui.startReleaseUpdateFromViewer();
    expect(statusCalls).toBe(2);
    expect(ui.update.check).toBeNull();
    expect(ui.renderUpdateCard()).toContain("Update authorization expired. Check for a release again.");
  });

  it("makes no automatic update call and reports a rejected secret without posting", async () => {
    const fetcher = vi.fn(async (url: string) => {
      if (url === "/update/check") return Response.json({ error: "Update secret is missing or incorrect." }, { status: 401 });
      throw new Error(`Unexpected ${url}`);
    });
    const ui = sandbox(fetcher, "incorrect-secret");
    await ui.loadUpdateStatus();
    expect(fetcher).not.toHaveBeenCalled();
    expect(ui.dialogs).toHaveLength(0);
    await ui.checkReleaseUpdate();
    expect(ui.dialogs).toHaveLength(1);
    expect(ui.renderUpdateCard()).toContain("Update secret is missing or incorrect.");
    expect(fetcher.mock.calls.some(([url]) => url === "/update/start")).toBe(false);
    await ui.checkReleaseUpdate();
    expect(ui.dialogs).toHaveLength(2);
    expect(ui.dialogs[0].innerHTML).toContain('type="password"');
    expect(ui.dialogs[0].innerHTML).toContain('autocomplete="off"');
    expect(ui.dialogs[0].input.value).toBe("");
    expect(ui.dialogs[0].removed).toBe(true);
    expect(viewer).not.toContain("window.prompt(");
    expect(viewer).not.toContain("localStorage.setItem('updateSecret'");
  });

  it("never checks GitHub from the health view load path", () => {
    const loadHealth = viewer.slice(viewer.indexOf("async function loadHealth()"), viewer.indexOf("function healthRow("));
    expect(loadHealth).toContain("loadUpdateStatus()");
    expect(loadHealth).not.toContain("checkReleaseUpdate()");
    expect(viewer).toContain("if (action === 'check-update')");
  });
});
