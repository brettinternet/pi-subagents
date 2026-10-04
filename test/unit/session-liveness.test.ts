import assert from "node:assert/strict";
import { describe, it } from "node:test";
import {
	SESSION_LIVENESS_CHANGED_EVENT,
	SESSION_LIVENESS_QUERY_EVENT,
	querySessionLiveness,
	registerSessionLivenessResponder,
} from "../../src/api/session-liveness.ts";
import registerSubagentNotify from "../../src/runs/background/notify.ts";

const SESSION_A = "11111111-1111-4111-8111-111111111111";
const SESSION_B = "22222222-2222-4222-8222-222222222222";

function createEventBus() {
	const listeners = new Map<string, Set<(payload: unknown) => void>>();
	return {
		on(event: string, handler: (payload: unknown) => void) {
			const handlers = listeners.get(event) ?? new Set();
			handlers.add(handler);
			listeners.set(event, handlers);
			return () => handlers.delete(handler);
		},
		emit(event: string, payload?: unknown) {
			for (const handler of listeners.get(event) ?? []) handler(payload);
		},
	};
}

describe("session liveness API", () => {
	it("answers exact-session queries and emits changed only when the value changes", () => {
		const events = createEventBus();
		let busy = false;
		const changed: unknown[] = [];
		events.on(SESSION_LIVENESS_CHANGED_EVENT, (payload) => changed.push(payload));
		assert.equal(querySessionLiveness(events, SESSION_A), undefined, "an unavailable provider is not idle");
		const responder = registerSessionLivenessResponder(events, { sessionId: SESSION_A, isBusy: () => busy });
		assert.ok(responder);
		assert.deepEqual(querySessionLiveness(events, SESSION_A), { version: 1, sessionId: SESSION_A, busy: false });
		assert.equal(querySessionLiveness(events, SESSION_B), undefined, "a provider never answers another session's query");
		responder.invalidate();
		assert.equal(changed.length, 0, "unchanged state is not announced");
		busy = true;
		responder.invalidate();
		assert.equal(changed.length, 1);
		assert.deepEqual(changed[0], { version: 1, sessionId: SESSION_A });
		assert.deepEqual(querySessionLiveness(events, SESSION_A), { version: 1, sessionId: SESSION_A, busy: true });
		responder.dispose();
		assert.equal(querySessionLiveness(events, SESSION_A), undefined, "dispose revokes the responder");
	});

	it("a query cannot consume the change notification owed to other subscribers", () => {
		const events = createEventBus();
		let busy = true;
		const changed: unknown[] = [];
		events.on(SESSION_LIVENESS_CHANGED_EVENT, (payload) => changed.push(payload));
		const responder = registerSessionLivenessResponder(events, { sessionId: SESSION_A, isBusy: () => busy })!;
		busy = false;
		assert.equal(querySessionLiveness(events, SESSION_A)?.busy, false);
		responder.invalidate();
		assert.equal(changed.length, 1, "querying does not acknowledge another consumer's pending wake");
		responder.dispose();
	});

	it("fails closed when the producer state accessor throws", () => {
		const events = createEventBus();
		const responder = registerSessionLivenessResponder(events, {
			sessionId: SESSION_A,
			isBusy: () => { throw new Error("stale runtime"); },
		});
		assert.ok(responder);
		assert.deepEqual(querySessionLiveness(events, SESSION_A), { version: 1, sessionId: SESSION_A, busy: true });
		responder.dispose();
	});

	it("cleans handoffs when the native session shuts down", () => {
		const events = createEventBus();
		const nativeEvents = createEventBus();
		let sent: Record<string, unknown> | undefined;
		const pi = {
			events,
			on: nativeEvents.on,
			sendMessage(message: Record<string, unknown>) { sent = message; },
		};
		const notifier = registerSubagentNotify(pi as never, { currentSessionId: SESSION_A }, {
			batchConfig: { enabled: false },
			nativeSessionId: () => SESSION_A,
		});
		try {
			assert.equal(notifier.sendWake({ customType: "test-wake", content: "wake", display: true }, SESSION_A), true);
			const details = sent?.details as { piSubagentsSessionLiveness?: { token?: unknown } } | undefined;
			assert.equal(typeof details?.piSubagentsSessionLiveness?.token, "string");
			nativeEvents.emit("agent_start");
			assert.equal(notifier.hasPendingDelivery(), true);
			nativeEvents.emit("session_shutdown");
			assert.equal(notifier.hasPendingDelivery(), false, "a closed session cannot retain an undeliverable native wake");
		} finally {
			notifier.dispose();
		}
	});
});
