import { execFileSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const pythonProbe = String.raw`
import json, runpy, sys
Scorer = runpy.run_path(sys.argv[1])["Scorer"]

class FakeLibrary:
    def __init__(self, required):
        self.required = required
        self.calls = []
        self.received = None
        self.flags = None
    def llama_tokenize(self, vocab, encoded, length, tokens, capacity, add_special, parse_special):
        self.calls.append(capacity)
        self.received = encoded[:length]
        self.flags = [add_special, parse_special]
        if length != len(encoded):
            raise AssertionError("input was truncated before tokenization")
        if capacity < self.required:
            return -self.required
        for index in range(self.required):
            tokens[index] = index
        return self.required

def tokenize(text, required, add_special=False, parse_special=True):
    scorer = Scorer.__new__(Scorer)
    scorer.vocab = object()
    scorer.lib = FakeLibrary(required)
    tokens = Scorer._tokenize(scorer, text, add_special, parse_special)
    return {
        "count": len(tokens),
        "calls": scorer.lib.calls,
        "bytes": len(text.encode("utf-8")),
        "inputPreserved": scorer.lib.received == text.encode("utf-8"),
        "flags": scorer.lib.flags,
    }

ascii_result = tokenize("a" * 8190, 1400, True, False)
utf8_result = tokenize("é" * 5000, 1800, False, True)
retry_result = tokenize("short", 100)

oversized = Scorer.__new__(Scorer)
oversized.vocab = object()
oversized.lib = FakeLibrary(8193)
try:
    Scorer._tokenize(oversized, "x" * 8193)
    oversized_rejected = False
except RuntimeError as error:
    oversized_rejected = "bounded tokenizer capacity" in str(error)

context = Scorer.__new__(Scorer)
context._tokenize = lambda *args, **kwargs: [0] * 4097
try:
    context.score("query", "document")
    context_rejected = False
except ValueError as error:
    context_rejected = "above context 4096" in str(error)

print(json.dumps({
    "ascii": ascii_result,
    "utf8": utf8_result,
    "retry": retry_result,
    "oversizedRejected": oversized_rejected,
    "oversizedCalls": oversized.lib.calls,
    "contextRejected": context_rejected,
}))
`;

describe("Qwen tokenizer buffer bounds", () => {
  it("keeps complete ASCII and UTF-8 inputs within the 8192-token allocation cap", () => {
    const python = process.env.AGENTMEMORY_QWEN_PYTHON ?? "python";
    const script = fileURLToPath(new URL("../scripts/qwen-reranker.py", import.meta.url));
    const output = execFileSync(python, ["-c", pythonProbe, script], {
      encoding: "utf8",
      timeout: 10_000,
      windowsHide: true,
    });
    const result = JSON.parse(output.trim());

    expect(result.ascii).toEqual({
      count: 1400,
      calls: [8192],
      bytes: 8190,
      inputPreserved: true,
      flags: [true, false],
    });
    expect(result.utf8).toEqual({
      count: 1800,
      calls: [8192],
      bytes: 10_000,
      inputPreserved: true,
      flags: [false, true],
    });
    expect(result.retry).toEqual({
      count: 100,
      calls: [64, 100],
      bytes: 5,
      inputPreserved: true,
      flags: [false, true],
    });
    expect(result.oversizedRejected).toBe(true);
    expect(result.oversizedCalls).toEqual([8192]);
    expect(result.contextRejected).toBe(true);
  });
});
