import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { describe, it } from "node:test";

const extensionHandoffScript = String.raw`
	import assert from "node:assert/strict";
	import * as fs from "node:fs";
	import * as path from "node:path";
	import { createEventBus } from "@earendil-works/pi-coding-agent";
	import registerSubagentExtension from "./src/extension/index.ts";
	import { SESSION_LIVENESS_CHANGED_EVENT, querySessionLiveness } from "./src/api/session-liveness.ts";
	import { currentCompletionOwnerId } from "./src/shared/completion-owner.ts";
	import { DIRS, SUBAGENT_ASYNC_STARTED_EVENT, SUBAGENT_PROCESS_TERMINAL_EVENT } from "./src/shared/types.ts";
	import { writeAtomicJson } from "./src/shared/atomic-json.ts";
	import { resultFilePath, writeAsyncResultFile } from "./src/runs/background/result-files.ts";
	import { updateActiveRunIndex } from "./src/runs/background/active-run-index.ts";
	import { stopRequestsDir } from "./src/runs/background/control-channel.ts";

	// Exercise event-driven producer tracking on Darwin too; production uses fs polling there.
	Object.defineProperty(process, "platform", { value: "linux" });
	const originalSetTimeout = globalThis.setTimeout;
	const originalClearTimeout = globalThis.clearTimeout;
	const fakeTrackerTimers = new WeakSet();
	globalThis.setTimeout = ((handler, delay, ...args) => {
		const stack = new Error().stack ?? "";
		if ((delay === 0 || delay === 25) && stack.includes("/src/runs/background/async-job-tracker.ts")) {
			const timer = { active: true, unref() {} };
			fakeTrackerTimers.add(timer);
			queueMicrotask(() => {
				if (!timer.active) return;
				timer.active = false;
				handler(...args);
			});
			return timer;
		}
		return originalSetTimeout(handler, delay, ...args);
	});
	globalThis.clearTimeout = ((timer) => {
		if (timer && typeof timer === "object" && fakeTrackerTimers.has(timer)) {
			timer.active = false;
			return;
		}
		return originalClearTimeout(timer);
	});

	const configDir = path.join(process.env.PI_CODING_AGENT_DIR, "extensions", "subagent");
	fs.mkdirSync(configDir, { recursive: true });
	fs.writeFileSync(path.join(configDir, "config.json"), JSON.stringify({ completionBatch: { enabled: false } }));
	const sessionId = "11111111-1111-4111-8111-111111111111";
	const completionOwnerId = currentCompletionOwnerId();
	const handlers = new Map();
	const nativeHandlers = new Map();
	const events = createEventBus();
	const sentNotifications = [];
	let subagentTool;
	let rejectFirstCompletion = true;
	let firstRejectedResolve;
	const firstRejected = new Promise((resolve) => { firstRejectedResolve = resolve; });
	let firstAcceptedResolve;
	const firstAccepted = new Promise((resolve) => { firstAcceptedResolve = resolve; });
	let secondAcceptedResolve;
	const secondAccepted = new Promise((resolve) => { secondAcceptedResolve = resolve; });
	const transitions = [];
	const pi = new Proxy({
		events,
		on(name, handler) {
			const list = handlers.get(name) ?? [];
			list.push(handler);
			handlers.set(name, list);
			const native = nativeHandlers.get(name) ?? [];
			native.push(handler);
			nativeHandlers.set(name, native);
			return () => {};
		},
		registerTool(tool) { if (tool.name === "subagent") subagentTool = tool; }, registerCommand() {}, registerShortcut() {}, registerMessageRenderer() {}, getSessionName() {},
		sendMessage(message, options) {
			if (message.customType !== "subagent-notify") return;
			const entry = { message, options };
			if (rejectFirstCompletion) {
				rejectFirstCompletion = false;
				firstRejectedResolve(entry);
				throw new Error("synthetic first completion send rejection");
			}
			sentNotifications.push(entry);
			if (sentNotifications.length === 1) firstAcceptedResolve(entry);
			if (sentNotifications.length === 2) secondAcceptedResolve(entry);
		},
	}, { get(target, property) { return property in target ? target[property] : () => undefined; } });
	const ctx = {
		cwd: process.cwd(), hasUI: false, model: undefined,
		ui: { setWidget() {}, requestRender() {}, theme: { fg(_name, text) { return text; }, bg(_name, text) { return text; }, bold(text) { return text; } } },
		sessionManager: { getSessionId() { return sessionId; }, getSessionFile() { return null; }, getEntries() { return []; } },
		modelRegistry: { getAvailable() { return []; } },
	};
	const busy = () => querySessionLiveness(events, sessionId)?.busy;
	const withTimeout = async (promise, label) => {
		let timer;
		try {
			return await Promise.race([promise, new Promise((_, reject) => { timer = originalSetTimeout(() => reject(new Error("Timed out waiting for " + label)), 10_000); })]);
		} finally {
			if (timer) originalClearTimeout(timer);
		}
	};
	const nextTurn = () => new Promise((resolve) => setImmediate(resolve));
	const waitForFileChange = (directory, name) => new Promise((resolve, reject) => {
		const watcher = fs.watch(directory, (_event, filename) => {
			if (filename?.toString() !== name) return;
			watcher.close();
			resolve();
		});
		watcher.on("error", reject);
	});
	const writeStatus = (runId, state, stepState) => {
		const asyncDir = path.join(DIRS.async, runId);
		fs.mkdirSync(asyncDir, { recursive: true });
		const runnerProcessInstanceId = runId + "-runner";
		const status = {
			runId, sessionId, completionOwnerId, mode: "single", state,
			startedAt: Date.now(), lastUpdate: Date.now(), cwd: process.cwd(), pid: process.pid,
			steps: [{ agent: "worker", status: stepState, index: 0 }],
			...(state === "running" ? {} : { processTerminal: { version: 1, runId, runnerProcessInstanceId, state: "pending" } }),
		};
		writeAtomicJson(path.join(asyncDir, "status.json"), status);
		updateActiveRunIndex(asyncDir, state);
		return asyncDir;
	};
	const startChild = (runId) => {
		const asyncDir = writeStatus(runId, "running", "running");
		events.emit(SUBAGENT_ASYNC_STARTED_EVENT, {
			id: runId, sessionId, completionOwnerId, mode: "single", agent: "worker", asyncDir,
		});
		return asyncDir;
	};
	const endChildBeforePublishing = async (runId, state, stepState) => {
		const asyncDir = path.join(DIRS.async, runId);
		const observed = waitForFileChange(asyncDir, "status.json");
		writeStatus(runId, state, stepState);
		events.emit(SUBAGENT_PROCESS_TERMINAL_EVENT, {
			version: 1, runId, runnerProcessInstanceId: runId + "-runner", state: "pending",
		});
		await withTimeout(observed, runId + " terminal status change");
		await nextTurn();
		assert.equal(fs.existsSync(resultFilePath(DIRS.results, runId)), false, "terminal status must precede public result discovery");
		assert.equal(busy(), true, runId + " stays live while terminal publication is pending");
	};
	const publishResult = (runId, state, success, stopped = false) => writeAsyncResultFile(resultFilePath(DIRS.results, runId), {
		id: runId, runId, agent: "worker", mode: "single", state, success,
		stopped, exitCode: success ? 0 : 1, summary: stopped ? "cancelled child" : runId + " complete",
		timestamp: Date.now(), sessionId, completionOwnerId,
	});
	const consumeHandoff = (entry) => {
		const message = entry.message;
		for (const handler of nativeHandlers.get("message_start") ?? []) {
			handler({ message: { role: "custom", customType: message.customType, details: message.details } });
		}
	};

	events.on(SESSION_LIVENESS_CHANGED_EVENT, () => transitions.push(busy()));
	registerSubagentExtension(pi);
	assert.equal(busy(), undefined, "liveness is unavailable before session_start");
	for (const handler of handlers.get("session_start") ?? []) await handler({ reason: "startup" }, ctx);
	assert.equal(busy(), false, "an idle installed extension reports idle");
	for (const handler of nativeHandlers.get("agent_start") ?? []) handler({});

	const runA = "liveness-child-a";
	const runB = "liveness-child-b";
	startChild(runA);
	startChild(runB);
	assert.equal(busy(), true, "independent started children are live through the extension producer event path");
	await nextTurn();
	const stopResult = await subagentTool.execute("cancel-child-b", { action: "stop", id: runB }, new AbortController().signal, undefined, ctx);
	assert.notEqual(stopResult.isError, true, stopResult.content[0]?.text);
	assert.equal(fs.readdirSync(stopRequestsDir(path.join(DIRS.async, runB))).length, 1, "the registered stop action persisted the producer cancellation request");
	assert.equal(busy(), true, "a requested cancellation remains live until its producer publishes the terminal result");

	await endChildBeforePublishing(runA, "complete", "complete");
	await endChildBeforePublishing(runB, "stopped", "stopped");

	const firstAttemptPromise = withTimeout(firstRejected, "first completion send rejection");
	publishResult(runA, "complete", true);
	await firstAttemptPromise;
	assert.equal(sentNotifications.length, 0, "a rejected send is not an accepted parent handoff");
	assert.equal(busy(), true, "failed delivery keeps producer-owned liveness across the terminal-to-result gap");

	const retryObserved = waitForFileChange(DIRS.results, runA + ".json");
	publishResult(runA, "complete", true);
	await withTimeout(retryObserved, "retry result filesystem notification");
	const firstHandoff = await withTimeout(firstAccepted, "accepted retried completion handoff");
	assert.equal(firstHandoff.message.customType, "subagent-notify");
	assert.equal(firstHandoff.options.triggerTurn, true);
	assert.equal(busy(), true, "accepted parent wake retains liveness until message_start consumes its handoff");
	consumeHandoff(firstHandoff);
	assert.equal(busy(), true, "consuming child A's handoff does not hide independent child B");

	const secondHandoffPromise = withTimeout(secondAccepted, "cancelled child completion handoff");
	publishResult(runB, "stopped", false, true);
	const secondHandoff = await secondHandoffPromise;
	assert.match(secondHandoff.message.content, /cancelled child/);
	assert.equal(busy(), true, "cancelled child's completion remains live through its parent handoff");
	consumeHandoff(secondHandoff);
	assert.equal(busy(), false, "all children, result publications, and parent handoffs are settled");
	assert.ok(transitions.length >= 2);
	assert.ok(transitions.slice(0, -1).every((value) => value === true), "no intermediate idle transition is allowed: " + JSON.stringify(transitions));

	for (const handler of handlers.get("session_shutdown") ?? []) await handler({ reason: "quit" }, ctx);
	process.stdout.write(JSON.stringify({ transitions, acceptedNotifications: sentNotifications.length, rejectedFirstSend: true }));
`;

const supervisorCleanupScript = String.raw`
	import assert from "node:assert/strict";
	import * as fs from "node:fs";
	import * as path from "node:path";
	import { createEventBus } from "@earendil-works/pi-coding-agent";
	import registerSubagentExtension from "./src/extension/index.ts";
	import { querySessionLiveness } from "./src/api/session-liveness.ts";
	import { ensureSupervisorChannelDir, resolveSupervisorChannelDir } from "./src/intercom/native-supervisor-channel.ts";
	import { writeAtomicJson } from "./src/shared/atomic-json.ts";

	Object.defineProperty(process, "platform", { value: "linux" });
	const sessionId = "22222222-2222-4222-8222-222222222222";
	const handlers = new Map();
	const events = createEventBus();
	let supervisorTool;
	const pi = new Proxy({
		events,
		on(name, handler) { handlers.set(name, [...(handlers.get(name) ?? []), handler]); return () => {}; },
		registerTool(tool) { if (tool.name === "subagent_supervisor") supervisorTool = tool; },
		registerCommand() {}, registerShortcut() {}, registerMessageRenderer() {}, getSessionName() {}, sendMessage() {},
	}, { get(target, property) { return property in target ? target[property] : () => undefined; } });
	const ctx = {
		cwd: process.cwd(), hasUI: false,
		ui: { setWidget() {}, requestRender() {}, theme: { fg(_name, text) { return text; }, bg(_name, text) { return text; }, bold(text) { return text; } } },
		sessionManager: { getSessionId() { return sessionId; }, getSessionFile() { return null; }, getEntries() { return []; } },
		modelRegistry: { getAvailable() { return []; } },
	};
	const busy = () => querySessionLiveness(events, sessionId)?.busy;
	registerSubagentExtension(pi);
	for (const handler of handlers.get("session_start") ?? []) await handler({ reason: "startup" }, ctx);
	assert.equal(busy(), false);
	const requestId = "supervisor-liveness-request";
	const runId = "supervisor-liveness-child";
	const channelDir = resolveSupervisorChannelDir(runId, "worker", 0);
	ensureSupervisorChannelDir(channelDir);
	const requestPath = path.join(channelDir, "requests", requestId + ".json");
	writeAtomicJson(requestPath, {
		type: "subagent.supervisor.request", id: requestId, createdAt: Date.now(), expiresAt: Date.now() + 60_000,
		reason: "need_decision", message: "Choose the safe path", expectsReply: true,
		orchestratorSessionId: sessionId, runId, agent: "worker", childIndex: 0,
	});
	const status = await supervisorTool.execute("supervisor-liveness", { action: "status" });
	assert.equal(status.details.pending, 1);
	assert.equal(busy(), true, "a discovered supervisor request independently owns session liveness");
	const reply = await supervisorTool.execute("supervisor-liveness", { action: "reply", replyTo: requestId, message: "Proceed" });
	assert.match(reply.content[0].text, /Replied to supervisor request/);
	assert.equal(fs.existsSync(path.join(channelDir, "replies", requestId + ".json")), true);
	assert.equal(busy(), false, "reply cleanup releases only the resolved supervisor liveness owner");
	for (const handler of handlers.get("session_shutdown") ?? []) await handler({ reason: "quit" }, ctx);
	process.stdout.write(JSON.stringify({ pendingBeforeReply: true, idleAfterReply: true }));
`;

function runIsolatedExtensionScript(root: string, script: string): string {
	fs.mkdirSync(path.join(root, "home"), { recursive: true });
	fs.mkdirSync(path.join(root, "agent"), { recursive: true });
	const env = { ...process.env };
	delete env.PI_SUBAGENT_CHILD;
	delete env.PI_SUBAGENT_PARENT_SESSION;
	Object.assign(env, {
		HOME: path.join(root, "home"),
		USERPROFILE: path.join(root, "home"),
		PI_CODING_AGENT_DIR: path.join(root, "agent"),
		PI_SUBAGENTS_TEMP_ROOT: path.join(root, "temp"),
		TMPDIR: root,
		TMP: root,
		TEMP: root,
	});
	const result = spawnSync(process.execPath, ["--experimental-strip-types", "--input-type=module", "--eval", script], {
		cwd: process.cwd(), encoding: "utf-8", env, timeout: 20_000,
	});
	assert.equal(result.status, 0, result.stderr || result.stdout);
	return result.stdout;
}

describe("registered extension session liveness producers", () => {
	it("stays continuously busy across independent children, terminal publication, failed send retry, cancellation, and parent handoff", () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-session-liveness-extension-"));
		try {
			const output = JSON.parse(runIsolatedExtensionScript(root, extensionHandoffScript));
			assert.equal(output.rejectedFirstSend, true);
			assert.equal(output.acceptedNotifications, 2);
			assert.equal(output.transitions.at(-1), false);
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});

	it("keeps pending supervisor requests live until the registered reply tool resolves and cleans them up", () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-session-liveness-supervisor-"));
		try {
			assert.deepEqual(JSON.parse(runIsolatedExtensionScript(root, supervisorCleanupScript)), {
				pendingBeforeReply: true, idleAfterReply: true,
			});
		} finally {
			fs.rmSync(root, { recursive: true, force: true });
		}
	});
});
