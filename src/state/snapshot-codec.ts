import { createReadStream } from "node:fs";
import { open } from "node:fs/promises";
import { createGunzip } from "node:zlib";
import { Readable } from "node:stream";
import { KV } from "./schema.js";
import { SNAPSHOT_ROW_BYTES, SNAPSHOT_PUBLICATION_BYTES, SNAPSHOT_MAX_ROWS, SNAPSHOT_EXPORT_MS } from "./snapshot-read-view.js";

export const SNAPSHOT_CODEC = "agentmemory-records-v1";
export interface SnapshotRecord { scope: string; key: string; value: Record<string, unknown> }
const MAX_RECORD_BYTES = SNAPSHOT_ROW_BYTES + 16_384;
function object(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
function validRecord(value: unknown): asserts value is SnapshotRecord {
  if (!object(value) || typeof value.scope !== "string" || typeof value.key !== "string" || !value.key
    || Buffer.byteLength(value.key) > 4096 || !object(value.value)) throw new Error("SNAPSHOT_INVALID_RECORD");
  const base = [KV.sessions, KV.memories, KV.graphNodes, KV.accessLog] as string[];
  if (!base.includes(value.scope) && !/^mem:obs:[^\r\n]{1,4096}$/.test(value.scope)) throw new Error("SNAPSHOT_INVALID_SCOPE");
  const keyField = value.scope === KV.accessLog ? "memoryId" : "id";
  if (value.value[keyField] !== value.key || Buffer.byteLength(JSON.stringify(value.value)) > SNAPSHOT_ROW_BYTES) {
    throw new Error("SNAPSHOT_INVALID_RECORD");
  }
}
export async function* encodeSnapshot(records: AsyncIterable<SnapshotRecord>, version: string, timestamp: string): AsyncGenerator<string> {
  yield JSON.stringify({ codec: SNAPSHOT_CODEC, version, timestamp }) + "\n";
  let count = 0;
  for await (const record of records) {
    validRecord(record);
    if (++count > SNAPSHOT_MAX_ROWS) throw new Error("SNAPSHOT_RESOURCE_LIMIT");
    const line = JSON.stringify(record);
    if (Buffer.byteLength(line) > MAX_RECORD_BYTES) throw new Error("SNAPSHOT_RECORD_LIMIT");
    yield line + "\n";
  }
  yield JSON.stringify({ end: true, records: count }) + "\n";
}

class JsonInput {
  private readonly chunks: AsyncIterator<Buffer | string>;
  private buffer = "";
  private offset = 0;
  private bytes = 0;
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });
  private done = false;
  private readonly deadline = Date.now() + SNAPSHOT_EXPORT_MS;
  constructor(stream: Readable) { this.chunks = stream[Symbol.asyncIterator](); }
  async char(): Promise<string> {
    while (this.offset >= this.buffer.length && !this.done) {
      if (Date.now() >= this.deadline) throw new Error("SNAPSHOT_EXPIRED");
      const next = await this.chunks.next();
      this.offset = 0;
      if (next.done) { this.done = true; this.buffer = this.decoder.decode(); }
      else {
        const chunk = typeof next.value === "string" ? Buffer.from(next.value) : next.value;
        this.bytes += chunk.length;
        if (this.bytes > SNAPSHOT_PUBLICATION_BYTES) throw new Error("SNAPSHOT_RESOURCE_LIMIT");
        this.buffer = this.decoder.decode(chunk, { stream: true });
      }
    }
    return this.buffer[this.offset] ?? "";
  }
  async take(): Promise<string> { const char = await this.char(); this.offset++; return char; }
  async space(): Promise<string> {
    let char = await this.char();
    while (char && /\s/.test(char)) { await this.take(); char = await this.char(); }
    return char;
  }
  async expect(expected: string): Promise<void> {
    if (await this.space() !== expected) throw new Error("SNAPSHOT_INVALID_JSON");
    await this.take();
  }
  async value(): Promise<unknown> {
    let char = await this.space();
    if (!char) throw new Error("SNAPSHOT_UNEXPECTED_EOF");
    let text = "", depth = 0, quoted = false, escape = false;
    const composite = char === "{" || char === "[";
    const string = char === '"';
    for (;;) {
      char = await this.char();
      if (!char) break;
      if (!quoted && depth === 0 && text && (!composite && !string) && /[\s,}\]]/.test(char)) break;
      text += await this.take();
      if (text.length > MAX_RECORD_BYTES) throw new Error("SNAPSHOT_RECORD_LIMIT");
      if (quoted) {
        if (escape) escape = false;
        else if (char === "\\") escape = true;
        else if (char === '"') { quoted = false; if (string && depth === 0) break; }
      } else if (char === '"') quoted = true;
      else if (char === "{" || char === "[") { if (++depth > 128) throw new Error("SNAPSHOT_DEPTH_LIMIT"); }
      else if (char === "}" || char === "]") { depth--; if (depth === 0 && composite) break; }
    }
    if (Buffer.byteLength(text) > MAX_RECORD_BYTES) throw new Error("SNAPSHOT_RECORD_LIMIT");
    return JSON.parse(text) as unknown;
  }
  async separator(end: string): Promise<boolean> {
    const char = await this.space();
    if (char === end) { await this.take(); return false; }
    await this.expect(",");
    if (await this.space() === end) throw new Error("SNAPSHOT_INVALID_JSON");
    return true;
  }
}

async function* legacyRecords(input: JsonInput): AsyncGenerator<SnapshotRecord> {
  await input.expect("{");
  const seen = new Set<string>();
  if (await input.space() === "}") throw new Error("SNAPSHOT_EMPTY_LEGACY");
  do {
    const field = await input.value();
    if (typeof field !== "string" || seen.has(field)) throw new Error("SNAPSHOT_INVALID_FIELD");
    seen.add(field);
    await input.expect(":");
    const scope = ({ sessions: KV.sessions, memories: KV.memories, graphNodes: KV.graphNodes, accessLogs: KV.accessLog } as Record<string, string>)[field];
    if (scope) yield* legacyArray(input, scope);
    else if (field === "observations") {
      await input.expect("{");
      if (await input.space() === "}") await input.take();
      else do {
        const sessionId = await input.value();
        if (typeof sessionId !== "string" || !sessionId || Buffer.byteLength(sessionId) > 4096) throw new Error("SNAPSHOT_INVALID_SESSION");
        await input.expect(":");
        yield* legacyArray(input, KV.observations(sessionId));
      } while (await input.separator("}"));
    } else if (field === "version" || field === "timestamp") {
      if (typeof await input.value() !== "string") throw new Error("SNAPSHOT_INVALID_HEADER");
    } else throw new Error("SNAPSHOT_UNSUPPORTED_FIELD");
  } while (await input.separator("}"));
  if (!seen.has("version")) throw new Error("SNAPSHOT_INVALID_HEADER");
}
async function* legacyArray(input: JsonInput, scope: string): AsyncGenerator<SnapshotRecord> {
  await input.expect("[");
  if (await input.space() === "]") { await input.take(); return; }
  do {
    const value = await input.value();
    if (!object(value)) throw new Error("SNAPSHOT_INVALID_RECORD");
    const record = { scope, key: value[scope === KV.accessLog ? "memoryId" : "id"], value };
    validRecord(record);
    yield record;
  } while (await input.separator("]"));
}
export async function* readSnapshot(path: string): AsyncGenerator<SnapshotRecord> {
  const file = await open(path, "r");
  let gzip = false;
  try { const head = Buffer.alloc(2); await file.read(head, 0, 2, 0); gzip = head[0] === 0x1f && head[1] === 0x8b; }
  finally { await file.close(); }
  const source = createReadStream(path, { highWaterMark: 64 * 1024 });
  const stream = gzip ? source.pipe(createGunzip()) : source;
  const propagate = (error: Error) => stream.destroy(error);
  if (gzip) source.on("error", propagate);
  let count = 0;
  try {
    const input = new JsonInput(stream);
    if (!gzip) {
      for await (const record of legacyRecords(input)) {
        if (++count > SNAPSHOT_MAX_ROWS) throw new Error("SNAPSHOT_RESOURCE_LIMIT");
        yield record;
      }
    } else {
      const header = await input.value();
      if (!object(header) || header.codec !== SNAPSHOT_CODEC || typeof header.version !== "string"
        || typeof header.timestamp !== "string" || !Number.isFinite(Date.parse(header.timestamp))) throw new Error("SNAPSHOT_INVALID_HEADER");
      for (;;) {
        const value = await input.value();
        if (object(value) && value.end === true) {
          if (value.records !== count) throw new Error("SNAPSHOT_COUNT_MISMATCH");
          break;
        }
        validRecord(value);
        if (++count > SNAPSHOT_MAX_ROWS) throw new Error("SNAPSHOT_RESOURCE_LIMIT");
        yield value;
      }
    }
    if (await input.space() !== "") throw new Error("SNAPSHOT_TRAILING_DATA");
  } finally {
    stream.destroy(); source.destroy(); source.off("error", propagate);
  }
}
