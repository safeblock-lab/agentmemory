import { Buffer } from "node:buffer";

export const TOOL_INPUT_BYTES = 16_384;
export const TOOL_OUTPUT_BYTES = 8_192;

export function utf8Prefix(text: string, maxBytes: number): string {
  const bytes = Buffer.from(text, "utf8");
  if (bytes.length <= maxBytes) return text;
  let end = Math.max(0, maxBytes);
  while (end > 0 && (bytes[end] & 0xc0) === 0x80) end--;
  return bytes.subarray(0, end).toString("utf8");
}

export function boundCapture(value: unknown, maxBytes: number): unknown {
  if (value === undefined) return undefined;
  const text = typeof value === "string" ? value : JSON.stringify(value) ?? "";
  const bytes = Buffer.byteLength(text, "utf8");
  if (bytes <= maxBytes) return value;
  const marker = `\n[agentmemory truncated: original=${bytes} UTF-8 bytes; cap=${maxBytes}; uncaptured suffix unavailable]`;
  return utf8Prefix(text, Math.max(0, maxBytes - Buffer.byteLength(marker))) + marker;
}
