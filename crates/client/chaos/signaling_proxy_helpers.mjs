// Keep the transport fixture so TCP Chaos runs cannot silently use UDP.
import { test as base } from "../playwright/transport_fixtures.mjs";
import { spawn } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";
import { TEST_SFU_WS_URL } from "../playwright/live_server_helpers.mjs";
const PROXY_DIRECTORY = fileURLToPath(new URL("../../../tests/signaling-proxy/", import.meta.url));
const PROXY_TARGET_DIRECTORY = fileURLToPath(new URL("../../../target/", import.meta.url));
const PROXY_BINARY = join(PROXY_TARGET_DIRECTORY, "debug", "o-sfu-signaling-proxy");
const EVENT_TIMEOUT_MS = 10_000;
const STOP_TIMEOUT_MS = 5_000;
const MAX_STDERR_CHARS = 4_096;
/**
 * Starts a separate signaling and optional UDP proxy for one browser scenario.
 *
 * The proxy emits events without payloads. Signaling rules affect whole WebSocket
 * messages and fire once across all connections, so a recovering client can
 * reuse the listener. With `deferred: true`, call `arm` after establishing the
 * call. `arm` resolves when the rule is enabled. Prefer the `startProxy` fixture:
 * it stops the process during teardown and preserves the original assertion failure.
 * Direct callers must await `stop` when finished.
 *
 * UDP rewrites ICE candidates to its datagram listeners and removes TCP candidates.
 * UDP selectors count per source flow and effects compose in their listed order.
 * `stats` returns a request-scoped snapshot of actual relay work.
 * `release` awaits the matching hold acknowledgement, which does not confirm delivery.
 *
 * @param {{rule?: {direction: string, t: string, response?: boolean, action: "hold" | "disconnect" | "drop" | "corrupt" | {close: {code: number, reason?: string}}}, upstream?: string, deferred?: boolean, udp?: {rules: Array<{direction: string, packet: string, skip?: number, every?: number, limit?: number, effects: Array<string | {delay: {ms: number}} | {duplicate: {copies: number, spacing_ms?: number}}>}>}}} options
 * @returns {Promise<{url: string, events: Array<object>, waitForEvent: Function, release: Function, arm: Function, stats: Function, stop: Function}>}
 */
export async function startSignalingProxy({
    rule,
    upstream = TEST_SFU_WS_URL,
    deferred = false,
    udp
}) {
    const child = spawn(PROXY_BINARY, [], { stdio: ["pipe", "pipe", "pipe"] });
    const events = [];
    const waiters = new Set();
    let closed = null;
    let failure = null;
    let stderr = "";
    let nextRequestId = 0;
    const processClosed = new Promise((resolve) => {
        child.once("close", (code, signal) => {
            closed = { code, signal };
            rejectWaiters(
                new Error(`signaling proxy exited: ${code ?? signal}${stderr ? `\n${stderr}` : ""}`)
            );
            resolve(closed);
        });
    });
    child.once("error", (error) => {
        failure = error;
        rejectWaiters(error);
    });
    child.stdin.on("error", (error) => {
        if (!closed) {
            failure = error;
            rejectWaiters(error);
        }
    });
    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
        stderr = (stderr + chunk).slice(-MAX_STDERR_CHARS);
    });
    const lines = createInterface({ input: child.stdout });
    lines.on("line", (line) => {
        try {
            const event = JSON.parse(line);
            events.push(event);
            for (const waiter of [...waiters]) {
                if (waiter.name === event.event && waiter.predicate(event)) {
                    waiters.delete(waiter);
                    clearTimeout(waiter.timer);
                    waiter.resolve(event);
                }
            }
        } catch (error) {
            failure = new Error(`invalid signaling proxy event: ${error}`);
            rejectWaiters(failure);
        }
    });
    function rejectWaiters(error) {
        for (const waiter of waiters) {
            clearTimeout(waiter.timer);
            waiter.reject(error);
        }
        waiters.clear();
    }
    function waitForEvent(name, predicate = () => true, timeoutMs = EVENT_TIMEOUT_MS) {
        const existing = events.find((event) => event.event === name && predicate(event));
        if (existing) {
            return Promise.resolve(existing);
        }
        if (failure || closed) {
            return Promise.reject(
                failure ?? new Error(`signaling proxy exited: ${closed.code ?? closed.signal}`)
            );
        }
        return new Promise((resolve, reject) => {
            const waiter = { name, predicate, resolve, reject, timer: null };
            waiter.timer = setTimeout(() => {
                waiters.delete(waiter);
                reject(
                    new Error(
                        `timed out waiting for signaling proxy event ${name}${stderr ? `\n${stderr}` : ""}`
                    )
                );
            }, timeoutMs);
            waiters.add(waiter);
        });
    }
    function send(command) {
        if (failure || closed || child.stdin.destroyed) {
            throw failure ?? new Error("signaling proxy is not accepting commands");
        }
        child.stdin.write(`${JSON.stringify(command)}\n`);
    }
    async function stop() {
        if (!closed) {
            if (!child.stdin.destroyed) {
                child.stdin.write('{"command":"stop"}\n');
                child.stdin.end();
            }
            let timer;
            await Promise.race([
                processClosed,
                new Promise((resolve) => {
                    timer = setTimeout(resolve, STOP_TIMEOUT_MS);
                })
            ]);
            clearTimeout(timer);
            if (!closed) {
                child.kill("SIGTERM");
                await processClosed;
            }
        }
        if (closed.code !== 0) {
            throw new Error(
                `signaling proxy exited: ${closed.code ?? closed.signal}${stderr ? `\n${stderr}` : ""}`
            );
        }
    }
    try {
        send({ upstream, rule, deferred, udp });
        const ready = await waitForEvent("ready");
        return {
            url: ready.url,
            events,
            waitForEvent,
            arm: async () => {
                const nextEvent = events.length;
                send({ command: "arm" });
                await waitForEvent("armed", (event) => events.indexOf(event) >= nextEvent);
            },
            stats: async () => {
                const requestId = ++nextRequestId;
                send({ command: "stats", requestId });
                return waitForEvent("stats", (event) => event.requestId === requestId);
            },
            release: async (connectionId) => {
                const nextEvent = events.length;
                send({ command: "release", connectionId });
                await waitForEvent(
                    "released",
                    (event) =>
                        event.connectionId === connectionId && events.indexOf(event) >= nextEvent
                );
            },
            stop
        };
    } catch (error) {
        await stop().catch(() => null);
        throw error;
    }
}

/** Builds the proxy with its selected toolchain. Rejects on build or spawn failure. */
function buildSignalingProxy() {
    return new Promise((resolve, reject) => {
        const child = spawn(
            process.env.SIGNALING_PROXY_CARGO ?? "cargo",
            [
                "build",
                "--locked",
                "-p",
                "o-sfu-signaling-proxy",
                "--target-dir",
                PROXY_TARGET_DIRECTORY
            ],
            { cwd: PROXY_DIRECTORY, stdio: "inherit" }
        );
        child.once("error", reject);
        child.once("close", (code, signal) => {
            if (code === 0) {
                resolve();
            } else {
                reject(new Error(`signaling proxy build failed: ${code ?? signal}`));
            }
        });
    });
}

let proxyBuild;
/**
 * Builds the proxy on demand and stops each process during teardown.
 * Playwright records the test failure first, so a stop error doesn't replace
 * the failed assertion.
 */
export const test = base.extend({
    startProxy: [
        // eslint-disable-next-line no-empty-pattern -- Playwright parses destructured fixture dependencies.
        async ({}, use) => {
            await (proxyBuild ??= buildSignalingProxy());
            const proxies = [];
            try {
                await use(async (options) => {
                    const proxy = await startSignalingProxy(options);
                    proxies.push(proxy);
                    return proxy;
                });
            } finally {
                await Promise.all(proxies.map((proxy) => proxy.stop()));
            }
        },
        { timeout: 360_000 }
    ]
});
