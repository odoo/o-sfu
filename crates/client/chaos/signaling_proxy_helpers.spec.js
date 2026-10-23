import { expect } from "@playwright/test";
import { createServer } from "node:http";
import { test } from "./signaling_proxy_helpers.mjs";

test.skip(
    ({ browserName, transport }) => browserName !== "chromium" || transport !== "udp",
    "Node-only proxy helper coverage"
);

test("each arm waits for its own acknowledgement", async ({ startProxy }) => {
    const proxy = await startProxy({
        deferred: true,
        rule: { direction: "client_to_server", t: "broadcast", action: "hold" }
    });
    for (let count = 1; count <= 2; count++) {
        await proxy.arm();
        expect(proxy.events.filter((event) => event.event === "armed")).toHaveLength(count);
    }
});

test("upstream handshake failures appear in event timeout diagnostics", async ({ startProxy }) => {
    const server = createServer((_request, response) => {
        response.writeHead(503);
        response.end();
    });
    await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
    let socket;
    try {
        const proxy = await startProxy({
            upstream: `ws://127.0.0.1:${server.address().port}/`,
            rule: { direction: "client_to_server", t: "broadcast", action: "hold" }
        });
        socket = new WebSocket(proxy.url);
        socket.addEventListener("error", () => {});
        await expect(proxy.waitForEvent("connected", () => true, 1_000)).rejects.toThrow(
            /timed out waiting for signaling proxy event connected[\s\S]*upstream handshake failed/
        );
    } finally {
        socket?.close();
        await new Promise((resolve) => server.close(resolve));
    }
});
