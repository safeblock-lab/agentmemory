#!/usr/bin/env node
import { closeSync, constants, fstatSync, openSync, readSync } from "node:fs";
import { createHash } from "node:crypto";
import { execSync } from "node:child_process";
import { basename } from "node:path";
//#region src/hooks/_project.ts
function resolveProject(cwd) {
	const explicit = process.env["AGENTMEMORY_PROJECT_NAME"];
	if (explicit && explicit.trim()) return explicit.trim();
	const dir = cwd && cwd.trim() ? cwd : process.cwd();
	try {
		const top = execSync("git rev-parse --show-toplevel", {
			cwd: dir,
			stdio: [
				"ignore",
				"pipe",
				"ignore"
			],
			timeout: 500
		}).toString().trim();
		if (top) return basename(top);
	} catch {}
	return basename(dir);
}
function hookCwd(data) {
	if (!data || typeof data !== "object") return void 0;
	if (typeof data.cwd === "string" && data.cwd.trim()) return data.cwd;
	const roots = data.workspace_roots;
	if (Array.isArray(roots)) {
		for (const root of roots) if (typeof root === "string" && root.trim()) return root;
	}
	const projectDir = process.env["DEVIN_PROJECT_DIR"] || process.env["CLAUDE_PROJECT_DIR"];
	if (projectDir && projectDir.trim()) return projectDir;
}
//#endregion
//#region src/hooks/session-end.ts
function isSdkChildContext(payload) {
	if (process.env["AGENTMEMORY_SDK_CHILD"] === "1") return true;
	if (!payload || typeof payload !== "object") return false;
	return payload.entrypoint === "sdk-ts";
}
const REST_URL = process.env["AGENTMEMORY_URL"] || "http://localhost:3111";
const SECRET = process.env["AGENTMEMORY_SECRET"] || "";
function authHeaders() {
	const h = { "Content-Type": "application/json" };
	if (SECRET) h["Authorization"] = `Bearer ${SECRET}`;
	return h;
}
function transcriptCapture(data) {
	const path = data.transcript_path;
	if (typeof path !== "string" || !path.endsWith(".jsonl")) return void 0;
	let descriptor;
	try {
		descriptor = openSync(path, constants.O_RDONLY | constants.O_NONBLOCK);
		const info = fstatSync(descriptor);
		if (!info.isFile()) return void 0;
		const limit = 256 * 1024;
		const start = Math.max(0, info.size - limit);
		const buffer = Buffer.alloc(Math.min(info.size, limit));
		const bytesRead = readSync(descriptor, buffer, 0, buffer.length, start);
		let text = buffer.toString("utf8", 0, bytesRead);
		let windowStart = start;
		if (start > 0) {
			const boundary = buffer.subarray(0, bytesRead).indexOf(10);
			if (boundary < 0) return void 0;
			windowStart += boundary + 1;
			text = buffer.subarray(boundary + 1, bytesRead).toString("utf8");
		}
		const complete = text.lastIndexOf("\n");
		if (complete < 0) return void 0;
		text = text.slice(0, complete + 1);
		return {
			event: "transcript_capture_v1",
			producer: "session-end-transcript-v1",
			sourceKey: createHash("sha256").update(path).digest("hex"),
			revision: `${info.dev}:${info.ino}:${info.birthtimeMs}`,
			windowStart,
			windowEnd: windowStart + Buffer.byteLength(text),
			text,
			...windowStart > 0 ? { omission: "[transcript prefix outside bounded capture window; unavailable in this delivery]" } : {}
		};
	} catch {
		return;
	} finally {
		if (descriptor !== void 0) closeSync(descriptor);
	}
}
async function main() {
	let input = "";
	for await (const chunk of process.stdin) input += chunk;
	let data;
	try {
		data = JSON.parse(input);
	} catch {
		return;
	}
	if (!data || typeof data !== "object") return;
	if (isSdkChildContext(data)) return;
	const sessionId = data.session_id || data.sessionId || data.conversation_id || "unknown";
	const capture = transcriptCapture(data);
	if (capture) {
		const cwd = hookCwd(data) || process.cwd();
		fetch(`${REST_URL}/agentmemory/observe`, {
			method: "POST",
			headers: authHeaders(),
			body: JSON.stringify({
				hookType: "prompt_submit",
				sessionId,
				project: resolveProject(cwd),
				cwd,
				timestamp: (/* @__PURE__ */ new Date()).toISOString(),
				data: capture
			}),
			signal: AbortSignal.timeout(3e3)
		}).catch(() => {});
	}
	fetch(`${REST_URL}/agentmemory/session/end`, {
		method: "POST",
		headers: authHeaders(),
		body: JSON.stringify({ sessionId }),
		signal: AbortSignal.timeout(3e4)
	}).catch(() => {});
	if (process.env["CLAUDE_MEMORY_BRIDGE"] === "true") fetch(`${REST_URL}/agentmemory/claude-bridge/sync`, {
		method: "POST",
		headers: authHeaders(),
		signal: AbortSignal.timeout(3e4)
	}).catch(() => {});
	setTimeout(() => process.exit(0), 1500).unref();
}
main().catch(() => process.exit(0));
//#endregion
export {};
