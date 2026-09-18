import { describe, expect, it, vi } from "vitest";
import type { TypeSafeConfig } from "../src/config.js";
import {
  TYPESAFE_API_ENDPOINT,
  TypeSafeDecisionProvider,
  type TypeSafeQuestion,
} from "../src/providers/typesafe.js";

const config: TypeSafeConfig = {
  enabled: true,
  apiKey: "test-key",
  timeoutMs: 1_000,
  maxStateChars: 16_000,
  features: {
    compaction: true,
    admission: true,
    pipelineGates: true,
    scoring: true,
  },
};

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });
}

function apiResponse(answers: Record<string, unknown>): Record<string, unknown> {
  return {
    model: "jev-latest",
    answers,
    usage: { input_tokens: 20, output_tokens: 2 },
  };
}

function makeFetch(body: unknown, status = 200): typeof fetch {
  return vi.fn<typeof fetch>(async () => jsonResponse(body, status));
}

describe("TypeSafeDecisionProvider", () => {
  it("sends a bounded typed Noul request to the fixed API endpoint", async () => {
    const fetcher = makeFetch(apiResponse({ result: { type: "noul", noul: 0.91 } }));
    const onEvent = vi.fn();
    const provider = new TypeSafeDecisionProvider({ config, fetcher, onEvent });

    await expect(provider.evaluateNoul("admission", { text: "keep this" }, "Should this observation be retained?")).resolves.toBe(0.91);

    expect(fetcher).toHaveBeenCalledOnce();
    const [url, init] = fetcher.mock.calls[0]!;
    expect(url).toBe(TYPESAFE_API_ENDPOINT);
    expect(new Headers(init?.headers).get("authorization")).toBe("Bearer test-key");
    const payload = JSON.parse(String(init?.body)) as Record<string, unknown>;
    expect(payload).toMatchObject({
      model: "jev-latest",
      state: { text: "keep this" },
      questions: {
        result: { type: "noul", instructions: "Should this observation be retained?" },
      },
    });
    expect(onEvent).toHaveBeenCalledWith(expect.objectContaining({
      feature: "admission",
      outcome: "success",
      questionCount: 1,
    }));
  });

  it("validates Choice and Score answers against their requested criteria", async () => {
    const questions: Record<string, TypeSafeQuestion> = {
      route: {
        type: "choice",
        instructions: "Which memory operation applies?",
        criteria: { keep: null, drop: null },
      },
      importance: {
        type: "score",
        instructions: "How durable is this observation?",
        criteria: ["temporary", "useful", "durable"],
      },
    };
    const fetcher = makeFetch(apiResponse({
      route: {
        type: "choice",
        choice: "keep",
        probabilities: { keep: 0.9, drop: 0.1 },
        confidence: 0.8,
      },
      importance: {
        type: "score",
        score: 1.6,
        legend: { "0": "temporary", "1": "useful", "2": "durable" },
        probabilities: { "0": 0.05, "1": 0.3, "2": 0.65 },
        confidence: 0.78,
      },
    }));
    const provider = new TypeSafeDecisionProvider({ config, fetcher });

    await expect(provider.evaluate("pipelineGates", "observation", questions)).resolves.toEqual({
      route: {
        type: "choice",
        choice: "keep",
        probabilities: { keep: 0.9, drop: 0.1 },
        confidence: 0.8,
      },
      importance: {
        type: "score",
        score: 1.6,
        legend: { "0": "temporary", "1": "useful", "2": "durable" },
        probabilities: { "0": 0.05, "1": 0.3, "2": 0.65 },
        confidence: 0.78,
      },
    });
  });

  it("accepts minimal decision responses and derives score legend from the request", async () => {
    const fetcher = makeFetch({
      answers: {
        keep: { noul: 0.9 },
        importance: { score: 1.4, probabilities: { "0": 0.1, "1": 0.5, "2": 0.4 }, confidence: 0.8 },
      },
    });
    const provider = new TypeSafeDecisionProvider({ config, fetcher });

    await expect(provider.evaluate("scoring", "state", {
      keep: { type: "noul", instructions: "Should it be kept?" },
      importance: { type: "score", instructions: "Rate durability", criteria: ["temporary", "useful", "durable"] },
    })).resolves.toEqual({
      keep: { type: "noul", noul: 0.9 },
      importance: {
        type: "score",
        score: 1.4,
        legend: { "0": "temporary", "1": "useful", "2": "durable" },
        probabilities: { "0": 0.1, "1": 0.5, "2": 0.4 },
        confidence: 0.8,
      },
    });
  });

  it.each([
    ["master-disabled", { ...config, enabled: false }],
    ["feature-disabled", { ...config, features: { ...config.features, admission: false } }],
    ["missing-key", { ...config, apiKey: "" }],
  ])("fails open without an API call when %s", async (_name, providerConfig) => {
    const fetcher = makeFetch(apiResponse({ result: { type: "noul", noul: 1 } }));
    const provider = new TypeSafeDecisionProvider({ config: providerConfig, fetcher });

    await expect(provider.evaluateNoul("admission", "state", "Should it be kept?")).resolves.toBeUndefined();
    expect(fetcher).not.toHaveBeenCalled();
  });

  it.each([
    ["Noul outside probability bounds", { keep: { type: "noul", noul: 1.1 } }, "noul"],
    ["Noul type mismatch", { keep: { type: "choice", noul: 0.8 } }, "noul"],
    ["Choice missing probability options", {
      result: { type: "choice", choice: "keep", probabilities: { keep: 1 }, confidence: 1 },
    }, "choice"],
    ["Score legend mismatch", {
      result: {
        type: "score",
        score: 1,
        legend: { "0": "wrong", "1": "right" },
        probabilities: { "0": 0, "1": 1 },
        confidence: 1,
      },
    }, "score"],
  ])("fails open on malformed %s", async (_name, answers, kind) => {
    const fetcher = makeFetch(apiResponse(answers));
    const provider = new TypeSafeDecisionProvider({ config, fetcher });
    const questions: Readonly<Record<string, TypeSafeQuestion>> = kind === "noul"
      ? { keep: { type: "noul", instructions: "Should it be kept?" } }
      : kind === "choice"
        ? { result: { type: "choice", instructions: "Select", criteria: { keep: null, drop: null } } }
        : { result: { type: "score", instructions: "Rate", criteria: ["low", "high"] } };

    await expect(provider.evaluate("scoring", "state", questions)).resolves.toBeUndefined();
  });

  it.each([
    ["HTTP error", async () => jsonResponse({ error: "invalid" }, 401)],
    ["invalid JSON", async () => new Response("not-json", { status: 200 })],
    ["network failure", async () => { throw new Error("offline"); }],
  ])("fails open on %s", async (_name, implementation) => {
    const fetcher = vi.fn<typeof fetch>(implementation);
    const provider = new TypeSafeDecisionProvider({ config, fetcher });

    await expect(provider.evaluateNoul("admission", "state", "Should it be kept?")).resolves.toBeUndefined();
  });

  it("fails open when the request times out", async () => {
    const fetcher: typeof fetch = (_input, init) => new Promise((_resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new Error("aborted")), { once: true });
    });
    const onEvent = vi.fn();
    const provider = new TypeSafeDecisionProvider({
      config: { ...config, timeoutMs: 5 },
      fetcher,
      onEvent,
    });

    await expect(provider.evaluateNoul("admission", "state", "Should it be kept?")).resolves.toBeUndefined();
    expect(onEvent).toHaveBeenCalledWith(expect.objectContaining({
      feature: "admission",
      outcome: "timeout",
      questionCount: 1,
    }));
  });

  it("truncates oversized state before sending it", async () => {
    const fetcher = makeFetch(apiResponse({ result: { type: "noul", noul: 0.8 } }));
    const provider = new TypeSafeDecisionProvider({
      config: { ...config, maxStateChars: 256 },
      fetcher,
    });

    await expect(provider.evaluateNoul("compaction", "x".repeat(5_000), "Can this be dropped?")).resolves.toBe(0.8);

    const [, init] = fetcher.mock.calls[0]!;
    const payload = JSON.parse(String(init?.body)) as { state: string };
    expect(payload.state.length).toBeLessThanOrEqual(256);
    expect(payload.state).toContain("[TypeSafe state truncated]");
    expect(new TextEncoder().encode(String(init?.body)).byteLength).toBeLessThan(96 * 1024);
  });

  it("fails open for questions and responses above their configured bounds", async () => {
    const fetcher = makeFetch(apiResponse({ result: { type: "noul", noul: 1 } }));
    const provider = new TypeSafeDecisionProvider({ config, fetcher });

    await expect(provider.evaluateNoul("admission", "state", "x".repeat(2_001))).resolves.toBeUndefined();
    expect(fetcher).not.toHaveBeenCalled();

    const oversized = new TypeSafeDecisionProvider({
      config,
      fetcher: vi.fn<typeof fetch>(async () => new Response("x".repeat(70 * 1024), { status: 200 })),
    });
    await expect(oversized.evaluateNoul("admission", "state", "Should it be kept?")).resolves.toBeUndefined();
  });
});
