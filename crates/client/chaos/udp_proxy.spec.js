import { expect } from "@playwright/test";
import {
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    decodedFrameCount,
    disconnectPeer,
    peerMediaDiagnostics,
    peerSnapshot,
    publishSyntheticVideo,
    setStreamDownload,
    spawnLiveServer,
    streamDiagnostics,
    waitForDecodedRemoteVideoFrame
} from "../playwright/live_server_helpers.mjs";
import { test } from "./signaling_proxy_helpers.mjs";
const PUBLISHER_SESSION_ID = 41;
const SUBSCRIBER_SESSION_ID = 42;
const MEDIA_TIMEOUT_MS = 15_000;
test.skip(({ transport }) => transport === "tcp", "UDP relay scenarios require UDP media");
async function selectedRemoteEndpoint(page) {
    return page.evaluate(() => {
        const peer = globalThis.__liveHarness.client._runtime._peerSession._activePeer;
        const transport = [...peer.getSenders(), ...peer.getReceivers()].find(
            (endpoint) => endpoint.transport
        )?.transport;
        const remote = transport?.iceTransport.getSelectedCandidatePair()?.remote;
        return remote
            ? { address: remote.address, port: remote.port, protocol: remote.protocol }
            : null;
    });
}
async function repairCounters(httpBaseUrl) {
    const response = await fetch(`${httpBaseUrl}/metrics`);
    expect(response.ok).toBe(true);
    const text = await response.text();
    const counter = (sample) => {
        const line = text.split("\n").find((line) => line.startsWith(`${sample} `));
        expect(line, `missing repair metric ${sample}`).toBeDefined();
        return Number(line.slice(sample.length + 1));
    };
    return {
        nacks: counter('osfu_rtc_nacks_total{direction="sent_to_publisher"}'),
        rtx: counter('osfu_rtc_rtx_packets_total{direction="received_from_publisher"}')
    };
}
async function withRelayedScreen(
    { browserName, context, startProxy },
    { sessionId, rules },
    exercise
) {
    const firefox = browserName === "firefox";
    const server = await spawnLiveServer({
        bindPort: firefox ? 18101 : 18100,
        // The helper also binds TCP at max + 1, outside the ephemeral source-port range.
        rtcMinPort: firefox ? 28632 : 28600,
        rtcMaxPort: firefox ? 28663 : 28631
    });
    const pages = [];
    let proxy;
    let channelUuid;
    try {
        proxy = await startProxy({ upstream: server.wsUrl, udp: { rules }, deferred: true });
        channelUuid = await createChannel({ httpBaseUrl: server.httpBaseUrl });
        const publisher = await createPeerPage(context);
        pages.push(publisher);
        const subscriber = await createPeerPage(context);
        pages.push(subscriber);
        for (const [page, peerSessionId] of [
            [publisher, PUBLISHER_SESSION_ID],
            [subscriber, SUBSCRIBER_SESSION_ID]
        ]) {
            await connectPeer(page, {
                channelUuid,
                jwt: createConnectToken(channelUuid, peerSessionId),
                url: peerSessionId === sessionId ? proxy.url : server.wsUrl
            });
            await expect
                .poll(async () => (await peerSnapshot(page)).state, { timeout: MEDIA_TIMEOUT_MS })
                .toBe("connected");
            await page.evaluate(() => {
                const harness = globalThis.__liveHarness;
                harness.udpOriginalPeer = harness.client._runtime._peerSession._activePeer;
            });
        }
        // Featured selects hi, which Chromium suppresses for small canvas captures.
        await publishSyntheticVideo(publisher, "screen", "udp-screen", {
            width: firefox ? 640 : 1280,
            height: firefox ? 360 : 720,
            frameRate: 30,
            movingPattern: true
        });
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", true, "featured");
        await expect
            .poll(
                async () =>
                    (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]?.screen
                        ?.readyState,
                { timeout: MEDIA_TIMEOUT_MS }
            )
            .toBe("live");
        await waitForDecodedRemoteVideoFrame(subscriber, PUBLISHER_SESSION_ID, "screen");
        const proxiedPage = sessionId === PUBLISHER_SESSION_ID ? publisher : subscriber;
        const remote = await selectedRemoteEndpoint(proxiedPage);
        expect(remote).toMatchObject({ address: "127.0.0.1", protocol: "udp" });
        await proxy.waitForEvent(
            "udp_bound",
            (event) => event.address === `${remote.address}:${remote.port}`
        );
        expect((await proxy.stats()).udp.forwarded).toBeGreaterThan(0);
        await exercise({ publisher, subscriber, proxy, server });
        // A new baseline after the fault work excludes frames decoded before injection.
        const framesBefore = await decodedFrameCount(subscriber, PUBLISHER_SESSION_ID, "screen");
        expect(framesBefore).toBeGreaterThan(0);
        await expect
            .poll(() => decodedFrameCount(subscriber, PUBLISHER_SESSION_ID, "screen"), {
                timeout: MEDIA_TIMEOUT_MS
            })
            .toBeGreaterThan(framesBefore);
        await waitForDecodedRemoteVideoFrame(subscriber, PUBLISHER_SESSION_ID, "screen");
        for (const page of pages) {
            const snapshot = await peerSnapshot(page);
            expect(snapshot.state).toBe("connected");
            expect(snapshot.stateChanges.some((change) => change.state === "recovering")).toBe(
                false
            );
            expect(snapshot.errors).toEqual([]);
            expect(
                await page.evaluate(() => {
                    const harness = globalThis.__liveHarness;
                    return (
                        harness.udpOriginalPeer === harness.client._runtime._peerSession._activePeer
                    );
                })
            ).toBe(true);
        }
        expect(proxy.events.filter((event) => event.event === "connected")).toHaveLength(1);
        expect(await selectedRemoteEndpoint(proxiedPage)).toEqual(remote);
    } catch (error) {
        const diagnostics = await Promise.allSettled([
            ...pages.map((page) => peerMediaDiagnostics(page)),
            proxy?.stats(),
            channelUuid
                ? streamDiagnostics({
                      httpBaseUrl: server.httpBaseUrl,
                      roomId: channelUuid,
                      producerSessionId: PUBLISHER_SESSION_ID,
                      consumerSessionId: SUBSCRIBER_SESSION_ID,
                      streamType: "screen"
                  })
                : null
        ]);
        await test.info().attach("udp-media-diagnostics", {
            body: JSON.stringify(diagnostics, null, 2),
            contentType: "application/json"
        });
        throw error;
    } finally {
        await Promise.allSettled(pages.map((page) => disconnectPeer(page)));
        await server.stop();
    }
}
test("publisher RTP loss requests and accepts repair before decoded media advances", async ({
    browserName,
    context,
    startProxy
}) => {
    test.setTimeout(75_000);
    await withRelayedScreen(
        { browserName, context, startProxy },
        {
            sessionId: PUBLISHER_SESSION_ID,
            rules: [{ direction: "client_to_server", packet: "rtp", limit: 3, effects: ["drop"] }]
        },
        async ({ proxy, server }) => {
            const before = await repairCounters(server.httpBaseUrl);
            await proxy.arm();
            await expect
                .poll(async () => (await proxy.stats()).udp, { timeout: MEDIA_TIMEOUT_MS })
                .toMatchObject({ dropped: 3, rules: [3], pending: 0, overflow: 0 });
            // Only this room uses the server, so both deltas belong to its damaged ingress.
            await expect
                .poll(
                    async () => {
                        const after = await repairCounters(server.httpBaseUrl);
                        return after.nacks > before.nacks && after.rtx > before.rtx;
                    },
                    { timeout: MEDIA_TIMEOUT_MS }
                )
                .toBe(true);
        }
    );
});
test("selectively delayed duplicate RTP keeps the original media session decoding", async ({
    browserName,
    context,
    startProxy
}) => {
    test.setTimeout(75_000);
    await withRelayedScreen(
        { browserName, context, startProxy },
        {
            sessionId: SUBSCRIBER_SESSION_ID,
            rules: [
                {
                    direction: "server_to_client",
                    packet: "rtp",
                    every: 2,
                    limit: 4,
                    effects: [{ delay: { ms: 150 } }, { duplicate: { copies: 1, spacing_ms: 10 } }]
                }
            ]
        },
        async ({ proxy }) => {
            const before = (await proxy.stats()).udp;
            await proxy.arm();
            // Matches precede queueing. Observe every delayed copy before accepting an empty queue.
            await expect
                .poll(async () => (await proxy.stats()).udp, { timeout: MEDIA_TIMEOUT_MS })
                .toMatchObject({
                    rules: [4],
                    delayed: 8,
                    duplicated: 4,
                    pending: 0,
                    dropped: 0,
                    overflow: 0
                });
            const stats = (await proxy.stats()).udp;
            expect(stats.forwarded - before.forwarded).toBeGreaterThanOrEqual(8);
        }
    );
});
