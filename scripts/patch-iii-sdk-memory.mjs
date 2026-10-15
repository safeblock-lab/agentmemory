import { createRequire } from 'node:module';
import { readFile, writeFile } from 'node:fs/promises';
import { dirname, join } from 'node:path';

const require = createRequire(import.meta.url);
const entry = require.resolve('iii-sdk');
const root = dirname(dirname(entry));
const metadata = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
if (metadata.version !== '0.22.1') throw new Error('AgentMemory transport patch requires iii-sdk 0.22.1');
const target = join(root, 'dist/index.mjs');
let source = (await readFile(target, 'utf8')).replaceAll('\r\n', '\n');
const marker = 'agentmemory-bounded-transport-v2';
if (source.includes('agentmemory-bounded-transport-v1')) {
  const before = 'const wireMessage = this.toWireFormat(messageType, message);\n    let data;';
  if (source.split(before).length !== 2) throw new Error('iii-sdk v1 patch upgrade source mismatch');
  source = source.replace(before, 'const invocationId = messageType === MessageType.InvokeFunction ? message.invocation_id : undefined;\n    const wireMessage = this.toWireFormat(messageType, message);\n    let data;');
  const start = source.indexOf('\tsendMessage(messageType, message, skipIfClosed = false) {');
  const end = source.indexOf('\tlogError(', start);
  if (start < 0 || end < 0) throw new Error('iii-sdk v1 patch upgrade method mismatch');
  const method = source.slice(start, end)
    .replaceAll('this.rejectTransportMessage(messageType, message.invocation_id,', 'this.rejectTransportMessage(messageType, messageType === MessageType.InvokeFunction ? message.invocation_id : undefined,')
    .replaceAll('this.sendMessageRaw(data, message.invocation_id)', 'this.sendMessageRaw(data, invocationId)')
    .replace('invocationId: message.invocation_id,', 'invocationId,')
    .replace('expiresAt: message.invocation_id ? this.invocations.get(message.invocation_id).expiresAt', 'expiresAt: invocationId ? this.invocations.get(invocationId).expiresAt');
  source = (source.slice(0, start) + method + source.slice(end)).replace('agentmemory-bounded-transport-v1', marker);
  await writeFile(target, source);
  console.log('[agentmemory] Upgraded bounded iii-sdk transport patch to v2');
}
if (!source.includes(marker)) {
  const replace = (before, after) => {
    if (source.split(before).length !== 2) throw new Error('iii-sdk transport patch source mismatch');
    source = source.replace(before, after);
  };
  replace('this.triggerTypes = /* @__PURE__ */ new Map();\n\t\tthis.messagesToSend = [];', `this.triggerTypes = /* @__PURE__ */ new Map();
    this.messagesToSend = [];
    // ${marker}: serialized queue bytes exclude private payloads from diagnostics.
    this.queuedBytes = 0;
    this.pendingInvocationBytes = 0;
    this.transportRejected = 0;
    this.transportLastWarning = 0;
    this.queueSweep = setInterval(() => this.pruneTransportQueue(), 1000);
    this.queueSweep.unref();`);
  replace('const invocation_id = crypto.randomUUID();', `const invocationBytes = Buffer.byteLength(JSON.stringify(payload) ?? "null");
    if (this.isShuttingDown || this.invocations.size >= 1024 || this.pendingInvocationBytes + invocationBytes > 64 * 1024 * 1024) {
      this.transportRejected++;
      throw new InvocationError({ code: "TRANSPORT_OVERLOADED", message: "SDK pending invocation limit reached or shutting down", function_id });
    }
    const invocation_id = crypto.randomUUID();`);
  replace('this.invocations.delete(invocation_id);\n\t\t\t\t\t\treject(', 'this.releaseTransportInvocation(invocation_id);\n            this.pruneTransportQueue();\n\t\t\t\t\t\treject(');
  replace('this.invocations.set(invocation_id, {', `this.pendingInvocationBytes += invocationBytes;
        this.invocations.set(invocation_id, {
          bytes: invocationBytes,
          expiresAt: Date.now() + effectiveTimeout,`);
  replace('\t\tthis.invocations.delete(invocation_id);\n\t}', '\t\tthis.releaseTransportInvocation(invocation_id);\n\t}');
  replace('this.invocations.clear();', `this.invocations.clear();
    clearInterval(this.queueSweep);
    this.messagesToSend = [];
    this.queuedBytes = 0;`);
  replace('this.invocations.clear();\n    clearInterval', 'this.invocations.clear();\n    this.pendingInvocationBytes = 0;\n    clearInterval');
  replace(`const pending = this.messagesToSend;
\t\tthis.messagesToSend = [];
\t\tfor (const message of pending) {
\t\t\tif (message.type === MessageType.InvokeFunction && typeof message.invocation_id === "string" && !this.invocations.has(message.invocation_id)) continue;
\t\t\tthis.sendMessageRaw(JSON.stringify(message));
\t\t}`, `this.pruneTransportQueue();
    const pending = this.messagesToSend;
    this.messagesToSend = [];
    this.queuedBytes = 0;
    for (const message of pending) {
      if (!this.sendMessageRaw(message.data, message.invocationId)) {
        this.rejectTransportMessage(message.type, message.invocationId, "TRANSPORT_OVERLOADED", false);
      }
    }`);
  const rawStart = source.indexOf('\tsendMessageRaw(data) {');
  const rawEnd = source.indexOf('\ttoWireFormat(', rawStart);
  if (rawStart < 0 || rawEnd < 0) throw new Error('iii-sdk raw transport patch source mismatch');
  source = source.slice(0, rawStart) + `
  __agentmemoryTransportDiagnostics() {
    return { connection: this.connectionState, queuedMessages: this.messagesToSend.length,
      queuedBytes: this.queuedBytes, pendingInvocations: this.invocations.size, pendingInvocationBytes: this.pendingInvocationBytes,
      bufferedBytes: this.ws?.bufferedAmount ?? 0, rejected: this.transportRejected };
  }
  releaseTransportInvocation(invocationId) {
    const invocation = this.invocations.get(invocationId);
    if (invocation) this.pendingInvocationBytes -= invocation.bytes ?? 0;
    this.invocations.delete(invocationId);
  }
  pruneTransportQueue() {
    const now = Date.now();
    this.messagesToSend = this.messagesToSend.filter((message) => {
      const keep = message.expiresAt > now && (!message.invocationId || this.invocations.has(message.invocationId));
      if (!keep) {
        this.queuedBytes -= message.bytes;
        if (message.invocationId) this.rejectTransportMessage(message.type, message.invocationId, "TIMEOUT", false);
      }
      return keep;
    });
  }
  rejectTransportMessage(type, invocationId, code, throwVoid = true) {
    this.transportRejected++;
    const error = new InvocationError({ code, message: "SDK transport capacity or connection unavailable" });
    const invocation = invocationId && this.invocations.get(invocationId);
    if (invocation) {
      this.releaseTransportInvocation(invocationId);
      invocation.reject(error);
    } else if (type === MessageType.InvokeFunction && !invocationId && throwVoid) throw error;
    if (Date.now() - this.transportLastWarning >= 60000) {
      this.transportLastWarning = Date.now();
      console.warn("[iii] Transport admission rejected", code);
    }
  }
  sendMessageRaw(data, invocationId) {
    const bytes = Buffer.byteLength(data);
    if (!this.ws || !this.isOpen() || bytes > 32 * 1024 * 1024 || this.ws.bufferedAmount + bytes > 64 * 1024 * 1024) return false;
    try {
      this.ws.send(data, (error) => {
        if (error) this.rejectTransportMessage(undefined, invocationId, "TRANSPORT_SEND_FAILED", false);
      });
      return true;
    } catch {
      return false;
    }
  }
` + source.slice(rawEnd);
  replace(`const wireMessage = this.toWireFormat(messageType, message);
\t\tif (this.isOpen()) this.sendMessageRaw(JSON.stringify(wireMessage));
\t\telse if (!skipIfClosed) this.messagesToSend.push(wireMessage);`, `if (this.isShuttingDown) {
      this.rejectTransportMessage(messageType, messageType === MessageType.InvokeFunction ? message.invocation_id : undefined, "TRANSPORT_CLOSED");
      return;
    }
    if (!this.isOpen() && skipIfClosed) return;
    const invocationId = messageType === MessageType.InvokeFunction ? message.invocation_id : undefined;
    const wireMessage = this.toWireFormat(messageType, message);
    let data;
    try { data = JSON.stringify(wireMessage); }
    catch {
      this.rejectTransportMessage(messageType, invocationId, "TRANSPORT_SERIALIZATION_FAILED");
      return;
    }
    if (this.isOpen()) {
      if (!this.sendMessageRaw(data, invocationId)) this.rejectTransportMessage(messageType, invocationId, "TRANSPORT_OVERLOADED");
      return;
    }
    this.pruneTransportQueue();
    const bytes = Buffer.byteLength(data);
    if (this.messagesToSend.length >= 1024 || this.queuedBytes + bytes > 32 * 1024 * 1024) {
      this.rejectTransportMessage(messageType, invocationId, "TRANSPORT_OVERLOADED");
      return;
    }
    this.messagesToSend.push({ data, bytes, type: messageType, invocationId,
      expiresAt: invocationId ? this.invocations.get(invocationId).expiresAt : Date.now() + 30000 });
    this.queuedBytes += bytes;`);
  await writeFile(target, source);
  console.log('[agentmemory] Applied bounded iii-sdk 0.22.1 ESM transport patch');
}
