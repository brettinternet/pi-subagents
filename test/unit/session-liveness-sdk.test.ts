import assert from "node:assert/strict";
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { fauxAssistantMessage, fauxProvider } from "@earendil-works/pi-ai";
import { DefaultResourceLoader, ModelRuntime, SessionManager, SettingsManager, createAgentSession } from "@earendil-works/pi-coding-agent";
import { querySessionLiveness, registerSessionLivenessResponder } from "../../src/api/session-liveness.ts";
import registerSubagentNotify from "../../src/runs/background/notify.ts";
import registerSubagentExtension from "../../src/extension/index.ts";
import { currentCompletionOwnerId } from "../../src/shared/completion-owner.ts";
import { SESSION_LIVENESS_CHANGED_EVENT } from "../../src/api/session-liveness.ts";

test("native Pi preserves accepted wake ownership across agent_settled and consumes steer app messages at message_start", { timeout: 30_000 }, async () => {
	const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-session-liveness-sdk-"));
	const cwd = path.join(root, "project");
	const agentDir = path.join(root, "agent");
	fs.mkdirSync(cwd);
	fs.mkdirSync(agentDir);
	const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
	process.env.PI_CODING_AGENT_DIR = agentDir;
	const sessionManager = SessionManager.inMemory(cwd);
	const sessionId = sessionManager.getSessionId();
	const ownerId = randomUUID();
	let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
	let notifier: ReturnType<typeof registerSubagentNotify> | undefined;
	let sendWhileStreaming: Promise<boolean> | undefined;
	let sendDuringSettle: Promise<boolean> | undefined;
	let responder: ReturnType<typeof registerSessionLivenessResponder> | undefined;
	let parentTurnActive = false;
	let streamingAtSend: boolean | undefined;
	let busyAtStreamSend: boolean | undefined;
	let busyAtSettleSend: boolean | undefined;
	let busyAfterRuntimeReload: boolean | undefined;
	let activeAtSteerStart: boolean | undefined;
	let activeAtDeferredStart: boolean | undefined;
	let idleAtSteerStart: boolean | undefined;
	let idleAtDeferredStart: boolean | undefined;
	let busyAtFinalSettle: boolean | undefined;
	const lifecycle: string[] = [];
	const faux = fauxProvider({ provider: "session-liveness", models: [{ id: "local" }], tokensPerSecond: 100_000 });
	faux.setResponses([
		() => fauxAssistantMessage("Original turn completed."),
		() => fauxAssistantMessage("Steer message processed."),
		() => fauxAssistantMessage("Deferred settled message processed."),
	]);
	const settingsManager = SettingsManager.inMemory({});
	const resourceLoader = new DefaultResourceLoader({
		cwd,
		agentDir,
		settingsManager,
		noExtensions: true,
		noSkills: true,
		noPromptTemplates: true,
		noThemes: true,
		noContextFiles: true,
		extensionFactories: [
			(api) => {
				api.registerProvider(faux.provider);
				const notifierState = { currentSessionId: sessionId, completionOwnerId: ownerId };
				const notifierOptions = { batchConfig: { enabled: false }, nativeSessionId: () => sessionId };
				notifier = registerSubagentNotify(api, notifierState, notifierOptions);
				responder = registerSessionLivenessResponder(api.events, {
					sessionId,
					isBusy: () => notifier?.hasPendingDelivery() ?? false,
				});
				api.on("agent_start", () => {
					parentTurnActive = true;
					responder?.invalidate();
					lifecycle.push("agent_start");
					if (!sendWhileStreaming) {
						streamingAtSend = session?.isStreaming;
						sendWhileStreaming = notifier!.deliver({
							id: "steer-wake",
							runId: "steer-wake",
							source: "async",
							sessionId,
							completionOwnerId: ownerId,
							success: true,
							summary: "A completion queued while the parent turn was active.",
						});
						busyAtStreamSend = querySessionLiveness(api.events, sessionId)?.busy;
					}
				});
				api.on("message_start", (event) => {
					if (event.message.role !== "custom" || event.message.customType !== "subagent-notify") return;
					lifecycle.push("message_start");
					if (lifecycle.filter((entry) => entry === "message_start").length === 1) {
						activeAtSteerStart = parentTurnActive;
						idleAtSteerStart = session?.isIdle;
					} else {
						activeAtDeferredStart = parentTurnActive;
						idleAtDeferredStart = session?.isIdle;
					}
				});
				api.on("agent_settled", () => {
					parentTurnActive = false;
					responder?.invalidate();
					lifecycle.push("agent_settled");
					if (!sendDuringSettle) {
						sendDuringSettle = notifier!.deliver({
							id: "settled-wake",
							runId: "settled-wake",
							source: "async",
							sessionId,
							completionOwnerId: ownerId,
							success: true,
							summary: "A completion queued during agent_settled.",
						});
						busyAtSettleSend = querySessionLiveness(api.events, sessionId)?.busy;
						notifier!.dispose();
						notifier = registerSubagentNotify(api, notifierState, notifierOptions);
						busyAfterRuntimeReload = querySessionLiveness(api.events, sessionId)?.busy;
					} else {
						busyAtFinalSettle = querySessionLiveness(api.events, sessionId)?.busy;
					}
				});
			},
		],
	});
	try {
		await resourceLoader.reload();
		const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: path.join(agentDir, "models.json"), allowModelNetwork: false });
		({ session } = await createAgentSession({
			cwd,
			agentDir,
			settingsManager,
			resourceLoader,
			modelRuntime,
			model: faux.getModel("local"),
			sessionManager,
			noTools: "builtin",
		}));
		await session.bindExtensions({});
		await session.prompt("Complete the original turn.");
		assert.equal(await sendWhileStreaming, true);
		assert.equal(await sendDuringSettle, true);
		assert.equal(streamingAtSend, true, "the first send used Pi's streaming steer queue");
		assert.equal(busyAtStreamSend, true, "accepted steer wake owns liveness while queued in the active turn");
		assert.equal(busyAtSettleSend, true, "accepted settled wake owns liveness while Pi defers its new run");
		assert.equal(busyAfterRuntimeReload, true, "process-global handoff ownership survives notifier disposal and reload");
		assert.equal(activeAtSteerStart, true, "the active-turn appMessage starts while the parent turn is active");
		assert.equal(activeAtDeferredStart, true, "the deferred appMessage starts after its agent_start");
		assert.equal(idleAtSteerStart, false, "message_start for a steer appMessage remains inside the active turn");
		assert.equal(idleAtDeferredStart, false, "message_start for the settled appMessage follows the deferred agent_start");
		assert.equal(busyAtFinalSettle, false, "the matching message_start consumes the final wake and no work remains");
		const firstSettle = lifecycle.indexOf("agent_settled");
		const nextStart = lifecycle.indexOf("agent_start", firstSettle + 1);
		const nextMessageStart = lifecycle.indexOf("message_start", firstSettle + 1);
		assert.ok(firstSettle >= 0 && nextStart > firstSettle && nextMessageStart > nextStart, `expected agent_settled -> agent_start -> message_start, got ${lifecycle.join(" -> ")}`);
		assert.equal(lifecycle.filter((event) => event === "message_start").length, 2, "both the steer and agent_settled custom messages started");
		console.log(`Actual Pi SDK: accepted wakes stayed busy through queued steer and agent_settled delivery; lifecycle ${lifecycle.join(" -> ")}.`);
	} finally {
		if (session) await (session.extensionRunner as any).emit({ type: "session_shutdown", reason: "quit" });
		notifier?.dispose();
		responder?.dispose();
		session?.dispose();
		if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
		else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
		fs.rmSync(root, { recursive: true, force: true });
	}
});

for (const producerFirst of [true, false]) {
	test(`full producer never announces idle during SDK reload (${producerFirst ? "producer" : "consumer"} first)`, { timeout: 30_000 }, async () => {
		const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-session-liveness-reload-"));
		const agentDir = path.join(root, "agent");
		fs.mkdirSync(agentDir);
		const previousAgentDir = process.env.PI_CODING_AGENT_DIR;
		process.env.PI_CODING_AGENT_DIR = agentDir;
		const sessionManager = SessionManager.inMemory(root);
		const sessionId = sessionManager.getSessionId();
		let session: Awaited<ReturnType<typeof createAgentSession>>["session"] | undefined;
		let reloaded = false;
		let tearingDown = false;
		let prematureContinuations = 0;
		let busyBeforeReload: boolean | undefined;
		const faux = fauxProvider({ provider: "liveness-reload", models: [{ id: "local" }], tokensPerSecond: 100_000 });
		faux.setResponses([
			() => fauxAssistantMessage("Yielded original turn."),
			() => fauxAssistantMessage("Processed the completion after reload."),
		]);
		const settingsManager = SettingsManager.inMemory({});
		const observeBoundary = (api: Parameters<typeof registerSubagentExtension>[0]) => {
			let waiting = false;
			const unsubscribe = api.events.on(SESSION_LIVENESS_CHANGED_EVENT, () => {
				if (waiting && tearingDown && session?.isIdle && querySessionLiveness(api.events, sessionId)?.busy === false) {
					prematureContinuations += 1;
				}
			});
			api.on("agent_start", () => { waiting = false; });
			api.on("agent_settled", () => { waiting = true; });
			api.on("session_shutdown", () => { waiting = false; unsubscribe(); });
		};
		const resourceLoader = new DefaultResourceLoader({
			cwd: root, agentDir, settingsManager,
			noExtensions: true, noSkills: true, noPromptTemplates: true, noThemes: true, noContextFiles: true,
			extensionFactories: [
				(api) => { api.registerProvider(faux.provider); },
				...(producerFirst ? [registerSubagentExtension, observeBoundary] : [observeBoundary, registerSubagentExtension]),
				(api) => {
					api.on("agent_settled", async () => {
						if (reloaded) return;
						reloaded = true;
						api.events.emit("subagent:async-complete", {
							id: randomUUID(), sessionId, completionOwnerId: currentCompletionOwnerId(),
							success: false, summary: "Review failed; parent must process this result.",
						});
						busyBeforeReload = querySessionLiveness(api.events, sessionId)?.busy;
						tearingDown = true;
						await session!.reload();
						tearingDown = false;
					});
				},
			],
		});
		try {
			await resourceLoader.reload();
			const modelRuntime = await ModelRuntime.create({ authPath: path.join(agentDir, "auth.json"), modelsPath: path.join(agentDir, "models.json"), allowModelNetwork: false });
			({ session } = await createAgentSession({ cwd: root, agentDir, settingsManager, resourceLoader, modelRuntime, model: faux.getModel("local"), sessionManager, noTools: "builtin" }));
			await session.bindExtensions({});
			await session.prompt("Yield with the review still pending.");
			assert.equal(reloaded, true, "exercise actual SDK reload, not just notifier disposal");
			assert.equal(busyBeforeReload, true, "the accepted deferred completion was outstanding");
			assert.equal(prematureContinuations, 0, "teardown must not authorize rollover before consumer shutdown");
			assert.ok(session.messages.some((message) => message.role === "custom" && message.customType === "subagent-notify"), "the deferred review result survives actual reload");
			assert.match(JSON.stringify(session.messages.at(-1)), /Processed the completion after reload/, "the originating parent processes the result after reload");
		} finally {
			if (session) await (session.extensionRunner as any).emit({ type: "session_shutdown", reason: "quit" });
			session?.dispose();
			if (previousAgentDir === undefined) delete process.env.PI_CODING_AGENT_DIR;
			else process.env.PI_CODING_AGENT_DIR = previousAgentDir;
			fs.rmSync(root, { recursive: true, force: true });
		}
	});
}
