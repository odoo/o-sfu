import { expect } from "@playwright/test";
import { WS_CLOSE_CODE } from "../dist/protocol_contract.js";
import {
    broadcast,
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    decodedFrameCount,
    disconnectPeer,
    latestBroadcastUpdate,
    pauseStream,
    peerLocalDescriptionSdp,
    peerMediaDiagnostics,
    peerSnapshot,
    publishSyntheticCamera,
    publishSyntheticScreen,
    publishSyntheticVideo,
    roomUserInfo,
    setStreamDownload,
    streamDiagnostics,
    updateInfo,
    waitForDecodedRemoteVideoFrame
} from "../playwright/live_server_helpers.mjs";
import { test } from "./signaling_proxy_helpers.mjs";
const PUBLISHER_SESSION_ID = 41;
const SUBSCRIBER_SESSION_ID = 42;
const CONNECT_TIMEOUT_MS = 15_000;
const DIAGNOSTICS_TIMEOUT_MS = 1_500;
// Matches INITIAL_RECOVERY_DELAY_MS in crates/protocol/src/core.rs.
const RECOVERY_TIMER_MS = 1_000;
const RECOVERY_TIMER_MARGIN_MS = 300;
async function disconnectPages(pages) {
    await Promise.allSettled(pages.map((page) => disconnectPeer(page)));
}
async function collectDiagnostics(collectors) {
    const diagnostics = Object.fromEntries(
        Object.keys(collectors).map((name) => [name, { status: "timed_out" }])
    );
    let timer;
    try {
        // A stalled collector must preserve completed evidence and the original test failure.
        await Promise.race([
            Promise.all(
                Object.entries(collectors).map(async ([name, collect]) => {
                    try {
                        diagnostics[name] = { status: "fulfilled", value: await collect() };
                    } catch (error) {
                        diagnostics[name] = { status: "rejected", reason: String(error) };
                    }
                })
            ),
            new Promise((resolve) => {
                timer = setTimeout(resolve, DIAGNOSTICS_TIMEOUT_MS);
            })
        ]);
    } finally {
        clearTimeout(timer);
    }
    return { ...diagnostics };
}
async function connectPair(context, channelUuid, pages, proxy, proxiedSessionId) {
    const publisher = await createPeerPage(context);
    pages.push(publisher);
    const subscriber = await createPeerPage(context);
    pages.push(subscriber);
    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID),
        url: proxiedSessionId === PUBLISHER_SESSION_ID ? proxy.url : undefined
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID),
        url: proxiedSessionId === SUBSCRIBER_SESSION_ID ? proxy.url : undefined
    });
    await Promise.all(
        [publisher, subscriber].map(async (page) => {
            await expect
                .poll(async () => (await peerSnapshot(page)).state, {
                    timeout: CONNECT_TIMEOUT_MS
                })
                .toBe("connected");
        })
    );
    return { publisher, subscriber };
}
function mediaDiagnostics(
    channelUuid,
    consumerSessionId,
    producerSessionId,
    streamType = "camera"
) {
    return streamDiagnostics({
        consumerSessionId,
        producerSessionId,
        roomId: channelUuid,
        streamType
    });
}
async function waitForDecodedVideo(page, sessionId, streamType, options) {
    await expect
        .poll(
            async () =>
                (await peerSnapshot(page)).consumers[String(sessionId)]?.[streamType]?.readyState ??
                null,
            {
                timeout: CONNECT_TIMEOUT_MS
            }
        )
        .toBe("live");
    return waitForDecodedRemoteVideoFrame(page, sessionId, streamType, options);
}
function broadcastsFrom(updates, sessionId, kind) {
    return updates
        .filter(
            (update) =>
                update.name === "broadcast" &&
                String(update.payload.senderId) === String(sessionId) &&
                update.payload.message?.kind === kind
        )
        .map((update) => update.payload.message.sequence);
}
async function observeFaultSocket(page) {
    await page.evaluate(() => {
        const harness = globalThis.__liveHarness;
        harness.proxyOldPeer = harness.client._runtime._peerSession._activePeer;
        const socket = harness.client._runtime._socketSession._activeSocket;
        socket.addEventListener(
            "close",
            ({ code, reason }) => {
                harness.proxyClose = { code, reason };
            },
            { once: true }
        );
    });
}
for (const code of [WS_CLOSE_CODE.AUTH_FAILED, WS_CLOSE_CODE.OVERLOADED]) {
    const recovers = code === WS_CLOSE_CODE.OVERLOADED;
    test(`close ${code} ${recovers ? "recovers saved intent" : "ends the saved attempt"}`, async ({
        context,
        startProxy
    }) => {
        test.setTimeout(45_000);
        const reason = `proxy-close-${code}`;
        const proxy = await startProxy({
            rule: {
                direction: "client_to_server",
                t: "broadcast",
                action: { close: { code, reason } }
            }
        });
        const pages = [];
        try {
            const channelUuid = await createChannel();
            const { publisher, subscriber } = await connectPair(
                context,
                channelUuid,
                pages,
                proxy,
                PUBLISHER_SESSION_ID
            );
            await updateInfo(publisher, { isRaisingHand: true });
            await expect
                .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }))
                .toMatchObject({ isRaisingHand: true });
            await observeFaultSocket(publisher);
            await broadcast(publisher, { kind: "proxy-close", sequence: 1 });
            const fault = await proxy.waitForEvent("closed");
            expect(fault.code).toBe(code);
            expect(fault.connectionId).toBe((await proxy.waitForEvent("connected")).connectionId);
            await expect
                .poll(() => publisher.evaluate(() => globalThis.__liveHarness.proxyClose))
                .toEqual({ code, reason });
            await expect
                .poll(() =>
                    publisher.evaluate(() => globalThis.__liveHarness.proxyOldPeer.connectionState)
                )
                .toBe("closed");
            if (!recovers) {
                await expect
                    .poll(async () => (await peerSnapshot(publisher)).stateChanges.at(-1))
                    .toEqual({ state: "closed", cause: "auth_failed" });
                await expect
                    .poll(() =>
                        roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID })
                    )
                    .toBeNull();
                await new Promise((resolve) =>
                    setTimeout(resolve, RECOVERY_TIMER_MS + RECOVERY_TIMER_MARGIN_MS)
                );
                expect((await peerSnapshot(publisher)).state).toBe("closed");
                expect(
                    (await peerSnapshot(publisher)).stateChanges.some(
                        (change) => change.state === "recovering"
                    )
                ).toBe(false);
                expect(proxy.events.filter((event) => event.event === "connected")).toHaveLength(1);
                await publisher.evaluate(
                    ({ url, token, room }) => {
                        globalThis.__liveHarness.client.connect(url, token, { channelUUID: room });
                    },
                    {
                        url: proxy.url,
                        token: createConnectToken(channelUuid, PUBLISHER_SESSION_ID),
                        room: channelUuid
                    }
                );
            }
            await proxy.waitForEvent(
                "connected",
                (event) => event.connectionId !== fault.connectionId
            );
            await expect
                .poll(async () => (await peerSnapshot(publisher)).state, {
                    timeout: CONNECT_TIMEOUT_MS
                })
                .toBe("connected");
            expect(
                (await peerSnapshot(publisher)).stateChanges.some(
                    (change) => change.state === "recovering"
                )
            ).toBe(recovers);
            await broadcast(publisher, { kind: "proxy-close", sequence: 2 });
            await expect
                .poll(() => latestBroadcastUpdate(subscriber, PUBLISHER_SESSION_ID))
                .toMatchObject({ payload: { message: { kind: "proxy-close", sequence: 2 } } });
            // this barrier also follows any saved info replay on the new connection.
            await expect
                .poll(
                    () => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }),
                    {
                        timeout: CONNECT_TIMEOUT_MS
                    }
                )
                .toMatchObject({ isRaisingHand: recovers });
            expect(
                broadcastsFrom(
                    (await peerSnapshot(subscriber)).updates,
                    PUBLISHER_SESSION_ID,
                    "proxy-close"
                )
            ).toEqual([2]);
            expect(proxy.events.filter((event) => event.event === "connected")).toHaveLength(2);
            expect(proxy.events.filter((event) => event.event === "closed")).toHaveLength(1);
            expect((await peerSnapshot(publisher)).errors).toEqual([]);
        } finally {
            await disconnectPages(pages);
        }
    });
}
test("a dropped broadcast does not disconnect or block the next broadcast", async ({
    context,
    startProxy
}) => {
    const proxy = await startProxy({
        rule: { direction: "client_to_server", t: "broadcast", action: "drop" }
    });
    const pages = [];
    try {
        const channelUuid = await createChannel();
        const { publisher, subscriber } = await connectPair(
            context,
            channelUuid,
            pages,
            proxy,
            PUBLISHER_SESSION_ID
        );
        const connection = await proxy.waitForEvent("connected");
        await publisher.evaluate(() => {
            const harness = globalThis.__liveHarness;
            harness.proxyOriginalSocket = harness.client._runtime._socketSession._activeSocket;
        });
        await broadcast(publisher, { kind: "proxy-drop", sequence: 1 });
        const fault = await proxy.waitForEvent("dropped");
        expect(fault.connectionId).toBe(connection.connectionId);
        await broadcast(publisher, { kind: "proxy-drop", sequence: 2 });
        await expect
            .poll(() => latestBroadcastUpdate(subscriber, PUBLISHER_SESSION_ID))
            .toMatchObject({ payload: { message: { kind: "proxy-drop", sequence: 2 } } });
        // the second broadcast crosses the same ordered connection after the lost message.
        expect(
            broadcastsFrom(
                (await peerSnapshot(subscriber)).updates,
                PUBLISHER_SESSION_ID,
                "proxy-drop"
            )
        ).toEqual([2]);
        expect(
            await publisher.evaluate(() => {
                const harness = globalThis.__liveHarness;
                return (
                    harness.proxyOriginalSocket ===
                        harness.client._runtime._socketSession._activeSocket &&
                    harness.proxyOriginalSocket.readyState === WebSocket.OPEN
                );
            })
        ).toBe(true);
        const snapshot = await peerSnapshot(publisher);
        expect(snapshot.state).toBe("connected");
        expect(snapshot.stateChanges.some((change) => change.state === "recovering")).toBe(false);
        expect(snapshot.errors).toEqual([]);
        expect(proxy.events.filter((event) => event.event === "connected")).toHaveLength(1);
        expect(proxy.events.filter((event) => event.event === "dropped")).toHaveLength(1);
    } finally {
        await disconnectPages(pages);
    }
});
test("corrupt inbound JSON ends the browser attempt with protocol error", async ({
    context,
    startProxy
}) => {
    const proxy = await startProxy({
        rule: { direction: "server_to_client", t: "broadcast", action: "corrupt" }
    });
    const pages = [];
    try {
        const channelUuid = await createChannel();
        const { publisher, subscriber } = await connectPair(
            context,
            channelUuid,
            pages,
            proxy,
            PUBLISHER_SESSION_ID
        );
        await updateInfo(publisher, { isRaisingHand: true });
        await expect
            .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }))
            .toMatchObject({ isRaisingHand: true });
        await observeFaultSocket(publisher);
        await publisher.evaluate(() => {
            const harness = globalThis.__liveHarness;
            harness.proxySocketLogs = [];
            harness.client.addEventListener("log", ({ detail }) => {
                harness.proxySocketLogs.push(detail.message);
            });
        });
        await broadcast(subscriber, { kind: "proxy-corrupt", sequence: 1 });
        const fault = await proxy.waitForEvent("corrupted");
        expect(fault.connectionId).toBe((await proxy.waitForEvent("connected")).connectionId);
        await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("closed");
        // the client maps 1002 to 4002 because WebSocket.close rejects wire code 1002.
        await expect
            .poll(() => publisher.evaluate(() => globalThis.__liveHarness.proxyClose?.code))
            .toBe(4002);
        await expect
            .poll(() => publisher.evaluate(() => globalThis.__liveHarness.proxySocketLogs))
            .toContain(`websocket closed with code ${WS_CLOSE_CODE.PROTOCOL_ERROR}`);
        expect(
            await publisher.evaluate(() => globalThis.__liveHarness.proxyOldPeer.connectionState)
        ).toBe("closed");
        await expect
            .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }))
            .toBeNull();
        await new Promise((resolve) =>
            setTimeout(resolve, RECOVERY_TIMER_MS + RECOVERY_TIMER_MARGIN_MS)
        );
        const snapshot = await peerSnapshot(publisher);
        expect(snapshot.state).toBe("closed");
        expect(snapshot.stateChanges.some((change) => change.state === "recovering")).toBe(false);
        expect(broadcastsFrom(snapshot.updates, SUBSCRIBER_SESSION_ID, "proxy-corrupt")).toEqual(
            []
        );
        expect(proxy.events.filter((event) => event.event === "connected")).toHaveLength(1);
        expect(proxy.events.filter((event) => event.event === "corrupted")).toHaveLength(1);
    } finally {
        await disconnectPages(pages);
    }
});
test("recovery applies latest intent and never replays a dropped broadcast", async ({
    context,
    startProxy
}) => {
    test.setTimeout(75_000);
    const proxy = await startProxy({
        rule: { direction: "client_to_server", t: "broadcast", action: "disconnect" }
    });
    const pages = [];
    let channelUuid;
    try {
        channelUuid = await createChannel();
        const { publisher, subscriber } = await connectPair(
            context,
            channelUuid,
            pages,
            proxy,
            PUBLISHER_SESSION_ID
        );
        await publishSyntheticCamera(publisher, "replay-publisher-camera");
        await publishSyntheticCamera(subscriber, "replay-subscriber-camera");
        await setStreamDownload(publisher, SUBSCRIBER_SESSION_ID, "camera", true);
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true);
        await Promise.all([
            waitForDecodedVideo(publisher, SUBSCRIBER_SESSION_ID, "camera"),
            waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "camera")
        ]);
        await updateInfo(publisher, { isRaisingHand: false });
        await expect
            .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toMatchObject({ isRaisingHand: false });
        await publisher.evaluate((subscriberId) => {
            const harness = globalThis.__liveHarness;
            const client = harness.client;
            const socket = client._runtime._socketSession._activeSocket;
            harness.proxyOldPeer = client._runtime._peerSession._activePeer;
            socket.addEventListener(
                "close",
                ({ code }) => {
                    harness.proxyCloseCode = code;
                },
                { once: true }
            );
            client.addEventListener("stateChange", ({ detail }) => {
                if (detail.state === "recovering" && !harness.proxyRecoveryIntentApplied) {
                    client.updateUpload("camera", null);
                    client.updateDownload(subscriberId, { camera: false });
                    client.updateInfo({ isRaisingHand: true });
                    harness.proxyRecoveryIntentApplied = true;
                }
            });
        }, SUBSCRIBER_SESSION_ID);
        await broadcast(publisher, { kind: "proxy-replay", sequence: 1 });
        const fault = await proxy.waitForEvent("disconnected");
        await expect
            .poll(
                () => publisher.evaluate(() => globalThis.__liveHarness.proxyRecoveryIntentApplied),
                {
                    timeout: CONNECT_TIMEOUT_MS
                }
            )
            .toBe(true);
        await expect
            .poll(() => publisher.evaluate(() => globalThis.__liveHarness.proxyCloseCode), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toBe(1006);
        await expect
            .poll(
                () =>
                    publisher.evaluate(() => globalThis.__liveHarness.proxyOldPeer.connectionState),
                {
                    timeout: CONNECT_TIMEOUT_MS
                }
            )
            .toBe("closed");
        await proxy.waitForEvent("connected", (event) => event.connectionId !== fault.connectionId);
        await expect
            .poll(async () => (await peerSnapshot(publisher)).state, {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toBe("connected");
        await broadcast(publisher, { kind: "proxy-replay", sequence: 2 });
        await expect
            .poll(() => latestBroadcastUpdate(subscriber, PUBLISHER_SESSION_ID), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toMatchObject({ payload: { message: { kind: "proxy-replay", sequence: 2 } } });
        expect(
            (await mediaDiagnostics(channelUuid, SUBSCRIBER_SESSION_ID, PUBLISHER_SESSION_ID))
                .publication
        ).toBeNull();
        await expect
            .poll(
                async () =>
                    (
                        await mediaDiagnostics(
                            channelUuid,
                            PUBLISHER_SESSION_ID,
                            SUBSCRIBER_SESSION_ID
                        )
                    ).subscription?.state ?? "inactive",
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toBe("inactive");
        await expect
            .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toMatchObject({ isRaisingHand: true });
        expect(
            broadcastsFrom(
                (await peerSnapshot(subscriber)).updates,
                PUBLISHER_SESSION_ID,
                "proxy-replay"
            )
        ).toEqual([2]);
        expect((await peerSnapshot(publisher)).errors).toEqual([]);
    } catch (error) {
        // Baseline decoding can fail before the fault. Capture media before teardown removes it.
        try {
            const diagnostics = await collectDiagnostics({
                publisherPeer: () => (pages[0] ? peerMediaDiagnostics(pages[0]) : null),
                subscriberPeer: () => (pages[1] ? peerMediaDiagnostics(pages[1]) : null),
                publisherCamera: () =>
                    channelUuid
                        ? mediaDiagnostics(channelUuid, SUBSCRIBER_SESSION_ID, PUBLISHER_SESSION_ID)
                        : null,
                subscriberCamera: () =>
                    channelUuid
                        ? mediaDiagnostics(channelUuid, PUBLISHER_SESSION_ID, SUBSCRIBER_SESSION_ID)
                        : null
            });
            await test.info().attach("recovery-media-diagnostics", {
                body: JSON.stringify({ ...diagnostics, proxyEvents: proxy.events }, null, 2),
                contentType: "application/json"
            });
        } catch {
            // Diagnostics must preserve the original scenario failure.
        }
        throw error;
    } finally {
        await disconnectPages(pages);
    }
});
test("lost renegotiation answer recovers the browser and publication", async ({
    context,
    startProxy
}) => {
    test.setTimeout(60_000);
    const proxy = await startProxy({
        rule: {
            direction: "client_to_server",
            t: "renegotiate",
            response: true,
            action: "disconnect"
        }
    });
    const pages = [];
    try {
        const channelUuid = await createChannel();
        const { publisher, subscriber } = await connectPair(
            context,
            channelUuid,
            pages,
            proxy,
            PUBLISHER_SESSION_ID
        );
        await publisher.evaluate(() => {
            globalThis.__liveHarness.proxyOldPeer =
                globalThis.__liveHarness.client._runtime._peerSession._activePeer;
        });
        await publishSyntheticCamera(publisher, "proxy-recovery-camera", {
            width: 640,
            height: 360
        });
        const fault = await proxy.waitForEvent("disconnected");
        await expect
            .poll(
                async () =>
                    (await peerSnapshot(publisher)).stateChanges.some(
                        (change) => change.state === "recovering"
                    ),
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toBe(true);
        await expect
            .poll(
                () =>
                    publisher.evaluate(() => globalThis.__liveHarness.proxyOldPeer.connectionState),
                {
                    timeout: CONNECT_TIMEOUT_MS
                }
            )
            .toBe("closed");
        await proxy.waitForEvent("connected", (event) => event.connectionId !== fault.connectionId);
        await expect
            .poll(async () => (await peerSnapshot(publisher)).state, {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toBe("connected");
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true);
        await expect
            .poll(
                () => mediaDiagnostics(channelUuid, SUBSCRIBER_SESSION_ID, PUBLISHER_SESSION_ID),
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toMatchObject({
                publication: { active: true },
                subscription: { state: "active" }
            });
        await waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "camera");
        expect((await peerSnapshot(publisher)).errors).toEqual([]);
    } finally {
        await disconnectPages(pages);
    }
});
test("canceling camera during a held offer preserves established screen media", async ({
    context,
    startProxy
}) => {
    test.setTimeout(75_000);
    const proxy = await startProxy({
        deferred: true,
        rule: { direction: "server_to_client", t: "renegotiate", response: false, action: "hold" }
    });
    const pages = [];
    try {
        const channelUuid = await createChannel();
        const { publisher, subscriber } = await connectPair(
            context,
            channelUuid,
            pages,
            proxy,
            PUBLISHER_SESSION_ID
        );
        const screen = await publishSyntheticScreen(publisher, "screen-before-camera-cancel");
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", true);
        await waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "screen", {
            expectedPixel: screen.fillPixel
        });
        const initialSdp = await peerLocalDescriptionSdp(publisher);
        expect(initialSdp).toBeTruthy();
        await proxy.arm();
        await publishSyntheticCamera(publisher, "camera-to-cancel");
        const blocked = await proxy.waitForEvent("blocked");
        const screenFramesBefore = await decodedFrameCount(
            subscriber,
            PUBLISHER_SESSION_ID,
            "screen"
        );
        expect(screenFramesBefore).not.toBeNull();
        await pauseStream(publisher, "camera");
        await updateInfo(publisher, { isRaisingHand: true });
        await expect
            .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toMatchObject({ isRaisingHand: true });
        await expect
            .poll(() => decodedFrameCount(subscriber, PUBLISHER_SESSION_ID, "screen"), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toBeGreaterThan(screenFramesBefore);
        await proxy.release(blocked.connectionId);
        await expect
            .poll(
                async () => {
                    const currentSdp = await peerLocalDescriptionSdp(publisher);
                    return Boolean(currentSdp && currentSdp !== initialSdp);
                },
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toBe(true);
        await updateInfo(publisher, { isRaisingHand: false });
        await expect
            .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }), {
                timeout: CONNECT_TIMEOUT_MS
            })
            .toMatchObject({ isRaisingHand: false });
        await expect
            .poll(
                async () =>
                    (
                        await mediaDiagnostics(
                            channelUuid,
                            SUBSCRIBER_SESSION_ID,
                            PUBLISHER_SESSION_ID
                        )
                    ).publication ?? null,
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toBeNull();
        await expect
            .poll(
                async () =>
                    (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]
                        ?.camera ?? null,
                {
                    timeout: CONNECT_TIMEOUT_MS
                }
            )
            .toBeNull();
        await expect
            .poll(
                () =>
                    mediaDiagnostics(
                        channelUuid,
                        SUBSCRIBER_SESSION_ID,
                        PUBLISHER_SESSION_ID,
                        "screen"
                    ),
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toMatchObject({ publication: { active: true } });
        await waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "screen", {
            expectedPixel: screen.fillPixel
        });
        expect(
            (await peerSnapshot(subscriber)).updates.filter(
                (update) =>
                    update.name === "track" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID &&
                    update.payload.type === "camera" &&
                    update.payload.active
            )
        ).toEqual([]);
        expect((await peerSnapshot(publisher)).state).toBe("connected");
        expect((await peerSnapshot(publisher)).errors).toEqual([]);
    } finally {
        await disconnectPages(pages);
    }
});
test("subscription and publication changes converge after held subscriber negotiation", async ({
    context,
    browserName,
    startProxy
}) => {
    test.setTimeout(90_000);
    const proxy = await startProxy({
        deferred: true,
        rule: { direction: "server_to_client", t: "renegotiate", response: false, action: "hold" }
    });
    const pages = [];
    try {
        const channelUuid = await createChannel();
        const { publisher, subscriber } = await connectPair(
            context,
            channelUuid,
            pages,
            proxy,
            SUBSCRIBER_SESSION_ID
        );
        await publishSyntheticCamera(publisher, "topology-first-camera");
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true);
        await waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "camera");
        await proxy.arm();
        const screen = await publishSyntheticVideo(publisher, "screen", "topology-screen", {
            width: browserName === "chromium" ? 1280 : 640,
            height: browserName === "chromium" ? 720 : 360,
            frameRate: 30,
            movingPattern: browserName === "chromium"
        });
        const blocked = await proxy.waitForEvent("blocked");
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", false);
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", false);
        await pauseStream(publisher, "camera");
        await updateInfo(subscriber, { isRaisingHand: true });
        await expect
            .poll(
                () =>
                    roomUserInfo({
                        roomId: channelUuid,
                        sessionId: SUBSCRIBER_SESSION_ID
                    }),
                {
                    timeout: CONNECT_TIMEOUT_MS
                }
            )
            .toMatchObject({ isRaisingHand: true });
        await expect
            .poll(
                async () =>
                    (
                        await mediaDiagnostics(
                            channelUuid,
                            SUBSCRIBER_SESSION_ID,
                            PUBLISHER_SESSION_ID,
                            "camera"
                        )
                    ).subscription?.state ?? "inactive",
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toBe("inactive");
        await expect
            .poll(
                async () =>
                    (
                        await mediaDiagnostics(
                            channelUuid,
                            SUBSCRIBER_SESSION_ID,
                            PUBLISHER_SESSION_ID,
                            "camera"
                        )
                    ).publication?.active ?? false,
                { timeout: CONNECT_TIMEOUT_MS }
            )
            .toBe(false);
        const camera = await publishSyntheticCamera(publisher, "topology-final-two-camera");
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true);
        await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", true);
        await updateInfo(subscriber, { isRaisingHand: false });
        await expect
            .poll(
                () =>
                    roomUserInfo({
                        roomId: channelUuid,
                        sessionId: SUBSCRIBER_SESSION_ID
                    }),
                {
                    timeout: CONNECT_TIMEOUT_MS
                }
            )
            .toMatchObject({ isRaisingHand: false });
        await proxy.release(blocked.connectionId);
        for (const streamType of ["camera", "screen"]) {
            await expect
                .poll(
                    () =>
                        mediaDiagnostics(
                            channelUuid,
                            SUBSCRIBER_SESSION_ID,
                            PUBLISHER_SESSION_ID,
                            streamType
                        ),
                    { timeout: CONNECT_TIMEOUT_MS }
                )
                .toMatchObject({
                    publication: { active: true },
                    subscription: { state: "active" }
                });
        }
        await Promise.all([
            waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "camera", {
                expectedPixel: camera.fillPixel
            }),
            waitForDecodedVideo(subscriber, PUBLISHER_SESSION_ID, "screen", {
                expectedPixel: browserName === "chromium" ? null : screen.fillPixel
            })
        ]);
        const final = await peerSnapshot(subscriber);
        expect(final.consumers[String(PUBLISHER_SESSION_ID)]?.camera?.readyState).toBe("live");
        expect(final.consumers[String(PUBLISHER_SESSION_ID)]?.screen?.readyState).toBe("live");
        expect(final.errors).toEqual([]);
    } finally {
        await disconnectPages(pages);
    }
});
for (const action of ["disconnect", "switch-room"]) {
    test(`${action} during recovery cancels the old connection attempt`, async ({
        context,
        startProxy
    }) => {
        test.setTimeout(60_000);
        const proxy = await startProxy({
            rule: { direction: "client_to_server", t: "broadcast", action: "disconnect" }
        });
        const pages = [];
        try {
            const oldRoom = await createChannel();
            const newRoom = action === "switch-room" ? await createChannel() : null;
            const publisher = await createPeerPage(context);
            pages.push(publisher);
            await connectPeer(publisher, {
                channelUuid: oldRoom,
                jwt: createConnectToken(oldRoom, PUBLISHER_SESSION_ID),
                url: proxy.url
            });
            await expect
                .poll(async () => (await peerSnapshot(publisher)).state, {
                    timeout: CONNECT_TIMEOUT_MS
                })
                .toBe("connected");
            await publishSyntheticCamera(publisher, "old-room-camera");
            await updateInfo(publisher, { isRaisingHand: true });
            await expect
                .poll(() => roomUserInfo({ roomId: oldRoom, sessionId: PUBLISHER_SESSION_ID }), {
                    timeout: CONNECT_TIMEOUT_MS
                })
                .toMatchObject({ isRaisingHand: true });
            await publisher.evaluate(
                ({ mode, room, token, url }) => {
                    const harness = globalThis.__liveHarness;
                    harness.client.addEventListener("stateChange", ({ detail }) => {
                        if (detail.state !== "recovering" || harness.proxyRecoveryActionDone) {
                            return;
                        }
                        harness.proxyRecoveryActionDone = true;
                        if (mode === "disconnect") {
                            harness.client.disconnect();
                        } else {
                            harness.client.connect(url, token, { channelUUID: room });
                        }
                    });
                },
                {
                    mode: action,
                    room: newRoom,
                    token: newRoom ? createConnectToken(newRoom, PUBLISHER_SESSION_ID) : null,
                    url: proxy.url
                }
            );
            await broadcast(publisher, { kind: "terminate-old-attempt", sequence: 1 });
            const fault = await proxy.waitForEvent("disconnected");
            await expect
                .poll(
                    () =>
                        publisher.evaluate(() => globalThis.__liveHarness.proxyRecoveryActionDone),
                    {
                        timeout: CONNECT_TIMEOUT_MS
                    }
                )
                .toBe(true);
            const expectedState = action === "disconnect" ? "disconnected" : "connected";
            await expect
                .poll(async () => (await peerSnapshot(publisher)).state, {
                    timeout: CONNECT_TIMEOUT_MS
                })
                .toBe(expectedState);
            await expect
                .poll(() => roomUserInfo({ roomId: oldRoom, sessionId: PUBLISHER_SESSION_ID }), {
                    timeout: CONNECT_TIMEOUT_MS
                })
                .toBeNull();
            if (newRoom) {
                await proxy.waitForEvent(
                    "connected",
                    (event) => event.connectionId !== fault.connectionId
                );
                await updateInfo(publisher, { isRaisingHand: false });
                await expect
                    .poll(
                        () => roomUserInfo({ roomId: newRoom, sessionId: PUBLISHER_SESSION_ID }),
                        {
                            timeout: CONNECT_TIMEOUT_MS
                        }
                    )
                    .toMatchObject({ isRaisingHand: false });
                await expect
                    .poll(
                        async () =>
                            (
                                await mediaDiagnostics(
                                    newRoom,
                                    SUBSCRIBER_SESSION_ID,
                                    PUBLISHER_SESSION_ID
                                )
                            ).publication ?? null,
                        { timeout: CONNECT_TIMEOUT_MS }
                    )
                    .toBeNull();
            }
            await new Promise((resolve) =>
                setTimeout(resolve, RECOVERY_TIMER_MS + RECOVERY_TIMER_MARGIN_MS)
            );
            expect((await peerSnapshot(publisher)).state).toBe(expectedState);
            expect(proxy.events.filter((event) => event.event === "connected").length).toBe(
                newRoom ? 2 : 1
            );
        } finally {
            await disconnectPages(pages);
        }
    });
}
