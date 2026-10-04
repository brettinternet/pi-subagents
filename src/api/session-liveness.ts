import type { ExtensionAPI } from "@earendil-works/pi-coding-agent";

export const SESSION_LIVENESS_QUERY_EVENT = "pi-subagents:session-liveness:query:v1";
export const SESSION_LIVENESS_CHANGED_EVENT = "pi-subagents:session-liveness:changed:v1";
export const SESSION_LIVENESS_VERSION = 1;

export interface SessionLivenessResult {
	version: 1;
	sessionId: string;
	busy: boolean;
}

export interface SessionLivenessQueryRequest {
	version: 1;
	sessionId: string;
	result?: SessionLivenessResult;
}

export interface SessionLivenessChangedEvent {
	version: 1;
	sessionId: string;
}

export interface SessionLivenessEventBus {
	on(event: string, handler: (payload: unknown) => void): () => void;
	emit(event: string, payload?: unknown): unknown;
}

export interface SessionLivenessResponder {
	invalidate(): void;
	dispose(): void;
}

const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/iu;

export function isSessionLivenessUuid(value: unknown): value is string {
	return typeof value === "string" && UUID_PATTERN.test(value);
}

function validResult(value: unknown, sessionId: string): value is SessionLivenessResult {
	if (!value || typeof value !== "object" || Array.isArray(value)) return false;
	const result = value as Record<string, unknown>;
	return result.version === SESSION_LIVENESS_VERSION
		&& result.sessionId === sessionId
		&& typeof result.busy === "boolean";
}

/** Query the installed owner synchronously. No response means liveness is unavailable, not idle. */
export function querySessionLiveness(events: Pick<ExtensionAPI, "events">["events"], sessionId: string): SessionLivenessResult | undefined {
	if (!isSessionLivenessUuid(sessionId)) return undefined;
	const request: SessionLivenessQueryRequest = { version: SESSION_LIVENESS_VERSION, sessionId };
	events.emit(SESSION_LIVENESS_QUERY_EVENT, request);
	return validResult(request.result, sessionId) ? request.result : undefined;
}

/** Install one exact-session synchronous responder. Call dispose before changing or tearing down its session. */
export function registerSessionLivenessResponder(
	events: Pick<ExtensionAPI, "events">["events"],
	input: { sessionId: string; isBusy(): boolean },
): SessionLivenessResponder | undefined {
	if (!isSessionLivenessUuid(input.sessionId)) return undefined;
	let active = true;
	const busyNow = (): boolean => {
		try {
			return input.isBusy() === true;
		} catch {
			return true;
		}
	};
	let lastBusy = busyNow();
	const listener = events.on(SESSION_LIVENESS_QUERY_EVENT, (payload) => {
		if (!active || !payload || typeof payload !== "object" || Array.isArray(payload)) return;
		const request = payload as Record<string, unknown>;
		if (request.version !== SESSION_LIVENESS_VERSION || request.sessionId !== input.sessionId || request.result !== undefined) return;
		const busy = busyNow();
		request.result = { version: SESSION_LIVENESS_VERSION, sessionId: input.sessionId, busy } satisfies SessionLivenessResult;
	});
	return {
		invalidate() {
			if (!active) return;
			const busy = busyNow();
			if (busy === lastBusy) return;
			lastBusy = busy;
			try {
				events.emit(SESSION_LIVENESS_CHANGED_EVENT, { version: SESSION_LIVENESS_VERSION, sessionId: input.sessionId } satisfies SessionLivenessChangedEvent);
			} catch {
				// A consumer's event handler cannot interrupt producer lifecycle changes.
			}
		},
		dispose() {
			if (!active) return;
			active = false;
			listener();
		},
	};
}
