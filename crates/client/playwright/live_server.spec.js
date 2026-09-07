import { createSocket } from "node:dgram";
import { once } from "node:events";

import { expect, test } from "@playwright/test";

import {
    broadcast,
    cameraPublicationActive,
    cameraSubscriptionRid,
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage,
    disconnectPeer,
    forceRecoverableClose,
    latestBroadcastUpdate,
    latestInfoUpdate,
    latestTrackUpdate,
    localSenderEncodings,
    observeNegotiationNeeded,
    observeNegotiations,
    pauseStream,
    peerLocalDescriptionSdp,
    peerSnapshot,
    publishSyntheticAudio,
    publishSyntheticCamera,
    publishSyntheticScreen,
    roomUserInfo,
    setStreamDownload,
    spawnLiveServer,
    streamDiagnostics,
    updateInfo,
    waitForDecodedRemoteVideoFrame
} from "./live_server_helpers.mjs";

const PUBLISHER_SESSION_ID = 41;
const REPAIRED_RID_EXTENSION = "urn:ietf:params:rtp-hdrext:sdes:repaired-rtp-stream-id";
const SUBSCRIBER_SESSION_ID = 42;
const STUN_MAGIC_COOKIE = 0x2112a442;

test("unresponsive STUN does not block the initial answer", async ({ browserName, context }) => {
    test.skip(browserName !== "chromium", "Chromium-specific ICE gathering regression");
    test.setTimeout(15_000);
    const stun = createSocket("udp4");
    let listening = false;
    let stunRequests = 0;
    stun.on("message", (message) => {
        if (message.length >= 20 && message.readUInt32BE(4) === STUN_MAGIC_COOKIE) {
            stunRequests += 1;
        }
    });

    try {
        stun.bind(0, "127.0.0.1");
        await once(stun, "listening");
        listening = true;
        const channelUuid = await createChannel();
        const peer = await createPeerPage(context);

        await connectPeer(peer, {
            channelUuid,
            iceServers: [{ urls: `stun:127.0.0.1:${stun.address().port}` }],
            jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
        });

        await expect.poll(() => stunRequests).toBeGreaterThan(0);
        await expect
            .poll(async () => (await peerSnapshot(peer)).peerConnectionState, { timeout: 8_000 })
            .toBe("connected");
        const snapshot = await peerSnapshot(peer);
        expect(snapshot.state).toBe("connected");
    } finally {
        if (listening) {
            const closed = once(stun, "close");
            stun.close();
            await closed;
        }
    }
});

test("default VP8 camera pauses and resumes without renegotiation", async ({
    browserName,
    context
}) => {
    test.setTimeout(60_000);
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const negotiations = observeNegotiations(publisher);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await expectCommittedPauseResume({
        browserName,
        channelUuid,
        firstLabel: "camera-one",
        negotiations,
        publisher,
        resumedLabel: "camera-two",
        streamType: "camera",
        subscriber
    });
    await expect
        .poll(async () => localSenderEncodings(publisher, "camera"))
        .toEqual([
            {
                active: true,
                maxBitrate: 150000,
                rid: "lo",
                scaleResolutionDownBy: 4
            },
            {
                active: true,
                maxBitrate: 800000,
                rid: "mid",
                scaleResolutionDownBy: 2
            },
            {
                active: true,
                maxBitrate: 4000000,
                rid: "hi",
                scaleResolutionDownBy: 1
            }
        ]);
    await expect.poll(async () => peerLocalDescriptionSdp(publisher)).not.toBeNull();
    const sdp = await peerLocalDescriptionSdp(publisher);
    const video = parseVideoCodecAnswer(sdp);

    expect(video.vp8PayloadTypes.size).toBeGreaterThan(0);
    expect(video.hasSendRidLo).toBeTruthy();
    expect(video.hasSendRidHi).toBeTruthy();
    expect(video.hasSendSimulcastLoMidHi).toBeTruthy();
});

test("browser compatibility upload and download flows survive live-server replacement", async ({
    context
}) => {
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await publishSyntheticCamera(publisher, "initial-camera");

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", false);

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, false);

    const replacement = await createPeerPage(context);
    await connectPeer(replacement, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(replacement)).state).toBe("connected");
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(publisher);
            return snapshot.stateChanges.at(-1);
        })
        .toEqual({
            cause: "kicked",
            state: "closed"
        });
    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(subscriber);
            return snapshot.updates.filter(
                (update) =>
                    update.name === "disconnect" &&
                    update.payload.sessionId === PUBLISHER_SESSION_ID
            ).length;
        })
        .toBeGreaterThan(0);

    await publishSyntheticCamera(replacement, "replacement-camera");

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, false);

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true);

    await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);

    await disconnectPeer(replacement);

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.camera ?? null)
        .toBeNull();
});

test("late-joining subscriber receives the already-live publication", async ({ context }) => {
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await publishSyntheticCamera(publisher, "live-before-join");

    const subscriber = await createPeerPage(context);
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");
    await expect
        .poll(async () => latestTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "camera"))
        .toMatchObject(cameraTrackUpdateExpectation(PUBLISHER_SESSION_ID, true));
    await expect
        .poll(async () => {
            return (
                (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]?.camera ??
                null
            );
        })
        .toMatchObject({
            enabled: true,
            kind: "video",
            readyState: "live"
        });
});

test("audio and screen streams publish, pause and clean up independently", async ({
    browserName,
    context
}) => {
    test.setTimeout(60_000);
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const negotiations = observeNegotiations(publisher);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await publishSyntheticAudio(publisher, "synthetic-audio");

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "audio", true, "audio");
    await expect
        .poll(async () => {
            return (
                (await peerSnapshot(subscriber)).consumers[String(PUBLISHER_SESSION_ID)]?.audio ??
                null
            );
        })
        .toMatchObject({
            enabled: true,
            kind: "audio",
            readyState: "live"
        });

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "audio", false);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "audio", false, "audio");

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "audio", true);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "audio", true, "audio");

    await expectCommittedPauseResume({
        browserName,
        channelUuid,
        firstLabel: "screen-one",
        negotiations,
        publisher,
        resumedLabel: "screen-two",
        streamType: "screen",
        subscriber
    });
    await expect
        .poll(async () => localSenderEncodings(publisher, "screen"))
        .toEqual([
            {
                active: true,
                maxBitrate: 150000,
                rid: "lo",
                scaleResolutionDownBy: 4
            },
            {
                active: true,
                maxBitrate: 800000,
                rid: "mid",
                scaleResolutionDownBy: 2
            },
            {
                active: true,
                maxBitrate: 4000000,
                rid: "hi",
                scaleResolutionDownBy: 1
            }
        ]);

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", false);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", false, "video");

    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "screen", true);

    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, "screen", true, "video");

    await disconnectPeer(publisher);

    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.audio ?? null)
        .toBeNull();
    await expect
        .poll(async () => (await peerSnapshot(subscriber)).consumers["41"]?.screen ?? null)
        .toBeNull();
});

test("broadcast and info fanout through the browser bundle", async ({ context }) => {
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await broadcast(publisher, {
        kind: "sequence",
        sequence: 17
    });

    await expect
        .poll(async () => latestBroadcastUpdate(subscriber, PUBLISHER_SESSION_ID))
        .toMatchObject({
            name: "broadcast",
            payload: {
                message: {
                    kind: "sequence",
                    sequence: 17
                },
                senderId: PUBLISHER_SESSION_ID
            }
        });

    await updateInfo(
        publisher,
        {
            isRaisingHand: true,
            isTalking: true
        },
        { needRefresh: true }
    );

    await expect
        .poll(async () => latestInfoUpdate(subscriber, PUBLISHER_SESSION_ID))
        .toMatchObject({
            name: "info_change",
            payload: {
                [String(PUBLISHER_SESSION_ID)]: {
                    isRaisingHand: true,
                    isTalking: true
                }
            }
        });
});

test("live recovery replays sticky publish subscribe and info intents", async ({ context }) => {
    test.setTimeout(45_000);
    const channelUuid = await createChannel();
    const publisher = await createPeerPage(context);
    const subscriber = await createPeerPage(context);

    await connectPeer(publisher, {
        channelUuid,
        jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID)
    });
    await connectPeer(subscriber, {
        channelUuid,
        jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID)
    });

    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
    await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

    await publishSyntheticCamera(publisher, "recovery-publisher-camera");
    await publishSyntheticCamera(subscriber, "recovery-subscriber-camera");
    await setStreamDownload(publisher, SUBSCRIBER_SESSION_ID, "camera", true, "featured");
    await updateInfo(
        publisher,
        {
            isCameraOn: true,
            isRaisingHand: true
        },
        { needRefresh: true }
    );

    await expect
        .poll(() =>
            cameraPublicationActive({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID })
        )
        .toBeTruthy();
    await expect
        .poll(() =>
            cameraSubscriptionRid({
                consumerSessionId: PUBLISHER_SESSION_ID,
                producerSessionId: SUBSCRIBER_SESSION_ID,
                roomId: channelUuid
            })
        )
        .toBe("hi");
    await expect
        .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }))
        .toMatchObject({
            isCameraOn: true,
            isRaisingHand: true
        });

    await forceRecoverableClose(publisher);

    await expect
        .poll(async () => {
            const snapshot = await peerSnapshot(publisher);
            return snapshot.stateChanges.some((change) => change.state === "recovering");
        })
        .toBeTruthy();
    await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");

    await expect
        .poll(
            () =>
                cameraPublicationActive({
                    roomId: channelUuid,
                    sessionId: PUBLISHER_SESSION_ID
                }),
            { timeout: 15_000 }
        )
        .toBeTruthy();
    await setStreamDownload(subscriber, PUBLISHER_SESSION_ID, "camera", true, "featured");
    await expect
        .poll(
            () =>
                cameraSubscriptionRid({
                    consumerSessionId: SUBSCRIBER_SESSION_ID,
                    producerSessionId: PUBLISHER_SESSION_ID,
                    roomId: channelUuid
                }),
            { timeout: 15_000 }
        )
        .toBe("hi");
    await expect
        .poll(
            () =>
                cameraSubscriptionRid({
                    consumerSessionId: PUBLISHER_SESSION_ID,
                    producerSessionId: SUBSCRIBER_SESSION_ID,
                    roomId: channelUuid
                }),
            { timeout: 15_000 }
        )
        .toBe("hi");
    await expect
        .poll(() => roomUserInfo({ roomId: channelUuid, sessionId: PUBLISHER_SESSION_ID }), {
            timeout: 15_000
        })
        .toMatchObject({
            isCameraOn: true,
            isRaisingHand: true
        });
});

test("H264-only live publish applies RID simulcast and renders when supported", async ({
    browserName,
    context
}) => {
    test.skip(
        browserName === "firefox",
        "the bundled Playwright Firefox build does not render the current H264-only live flow"
    );
    const server = await spawnLiveServer({
        bindPort: 18084,
        rtcMinPort: 58264,
        rtcMaxPort: 58295,
        codecFlags: { h264: true, vp8: false }
    });
    try {
        const channelUuid = await createChannel({
            authKey: server.authKey,
            httpBaseUrl: server.httpBaseUrl
        });
        const publisher = await createPeerPage(context);
        const subscriber = await createPeerPage(context);

        await connectPeer(publisher, {
            channelUuid,
            jwt: createConnectToken(channelUuid, PUBLISHER_SESSION_ID),
            url: server.wsUrl
        });
        await connectPeer(subscriber, {
            channelUuid,
            jwt: createConnectToken(channelUuid, SUBSCRIBER_SESSION_ID),
            url: server.wsUrl
        });

        await expect.poll(async () => (await peerSnapshot(publisher)).state).toBe("connected");
        await expect.poll(async () => (await peerSnapshot(subscriber)).state).toBe("connected");

        await publishSyntheticCamera(publisher, "h264-simulcast");

        await expectCameraTrackUpdate(subscriber, PUBLISHER_SESSION_ID, true);
        await expect
            .poll(async () => localSenderEncodings(publisher, "camera"))
            .toEqual([
                {
                    active: true,
                    maxBitrate: 150000,
                    rid: "lo",
                    scaleResolutionDownBy: undefined
                },
                {
                    active: true,
                    maxBitrate: 800000,
                    rid: "mid",
                    scaleResolutionDownBy: undefined
                },
                {
                    active: true,
                    maxBitrate: 4000000,
                    rid: "hi",
                    scaleResolutionDownBy: undefined
                }
            ]);
        await expect.poll(async () => peerLocalDescriptionSdp(publisher)).not.toBeNull();
        const sdp = await peerLocalDescriptionSdp(publisher);
        const video = parseVideoCodecAnswer(sdp);

        expect(video.h264PayloadTypes.size).toBeGreaterThan(0);
        expect(video.vp8PayloadTypes.size).toBe(0);
        expect(video.hasSendRidLo).toBeTruthy();
        expect(video.hasSendRidHi).toBeTruthy();
        expect(video.hasSendSimulcastLoMidHi).toBeTruthy();
    } finally {
        await server.stop();
    }
});

test("live browser negotiation enables video recovery for optional codecs", async ({
    browserName,
    context
}) => {
    const liveServerPorts =
        browserName === "firefox"
            ? {
                  bindPort: 18083,
                  rtcMaxPort: 58263,
                  rtcMinPort: 58232
              }
            : {
                  bindPort: 18082,
                  rtcMaxPort: 58231,
                  rtcMinPort: 58200
              };
    const server = await spawnLiveServer({
        bindPort: liveServerPorts.bindPort,
        rtcMinPort: liveServerPorts.rtcMinPort,
        rtcMaxPort: liveServerPorts.rtcMaxPort,
        codecFlags: { h264: true, vp9: true }
    });
    try {
        const channelUuid = await createChannel({
            authKey: server.authKey,
            httpBaseUrl: server.httpBaseUrl
        });
        const peer = await createPeerPage(context);
        await connectPeer(peer, {
            channelUuid,
            jwt: createConnectToken(channelUuid, 77),
            url: server.wsUrl
        });

        await expect.poll(async () => (await peerSnapshot(peer)).state).toBe("connected");
        await expect.poll(async () => peerLocalDescriptionSdp(peer)).not.toBeNull();
        const sdp = await peerLocalDescriptionSdp(peer);
        const codecs = parseVideoCodecAnswer(sdp);

        expect(codecs.videoCodecPayloadTypes.size).toBeGreaterThan(0);
        if (codecs.h264PayloadTypes.size > 0) {
            expect(codecs.h264Variants).toEqual(
                new Set([
                    "packetization-mode=0;profile-level-id=42001f",
                    "packetization-mode=0;profile-level-id=42e01f",
                    "packetization-mode=0;profile-level-id=4d001f",
                    "packetization-mode=1;profile-level-id=42001f",
                    "packetization-mode=1;profile-level-id=42e01f",
                    "packetization-mode=1;profile-level-id=4d001f"
                ])
            );
        }
        if (codecs.vp9Profiles.size > 0) {
            expect(codecs.vp9Profiles).toEqual(new Set(["0", "2"]));
        }
        expect(codecs.genericNackPayloadTypes).toEqual(codecs.videoCodecPayloadTypes);
        expect(codecs.hasRepairedRidExtension).toBeTruthy();
        expect(codecs.rtxAssociations.size).toBe(codecs.videoCodecPayloadTypes.size);
        expect(new Set(codecs.rtxAssociations.keys())).toEqual(codecs.rtxPayloadTypes);
        expect(new Set(codecs.rtxAssociations.values())).toEqual(codecs.videoCodecPayloadTypes);
        expect(codecs.audioRepairAttributes).toEqual([]);
    } finally {
        await server.stop();
    }
});

async function expectCommittedPauseResume({
    browserName,
    channelUuid,
    firstLabel,
    httpBaseUrl,
    negotiations,
    publisher,
    resumedLabel,
    streamType,
    subscriber
}) {
    const firstTrack = await publishSyntheticStream(publisher, streamType, firstLabel);
    const activeDiagnostics = await expectStreamActivity(
        subscriber,
        channelUuid,
        streamType,
        true,
        httpBaseUrl
    );

    const negotiationNeeded = await observeNegotiationNeeded(publisher);
    const negotiationNeededCount = await negotiationNeeded();
    const negotiationCount = negotiations.count();
    const identity = streamIdentity(activeDiagnostics);

    await pauseStream(publisher, streamType);
    const pausedDiagnostics = await expectStreamActivity(
        subscriber,
        channelUuid,
        streamType,
        false,
        httpBaseUrl
    );

    expect(streamIdentity(pausedDiagnostics)).toEqual(identity);
    expect(await negotiationNeeded()).toBe(negotiationNeededCount);
    expect(negotiations.count()).toBe(negotiationCount);

    const resumedTrack = await publishSyntheticStream(publisher, streamType, resumedLabel);
    const resumedDiagnostics = await expectStreamActivity(
        subscriber,
        channelUuid,
        streamType,
        true,
        httpBaseUrl
    );

    expect(streamIdentity(resumedDiagnostics)).toEqual(identity);
    expect(await negotiationNeeded()).toBe(negotiationNeededCount);
    expect(negotiations.count()).toBe(negotiationCount);

    if (browserName === "chromium") {
        const frame = await waitForDecodedRemoteVideoFrame(
            subscriber,
            PUBLISHER_SESSION_ID,
            streamType,
            {
                expectedPixel: resumedTrack.fillPixel
            }
        );
        expect(frame.width).toBeGreaterThan(0);
        expect(frame.height).toBeGreaterThan(0);
        expect(frame.pixel.alpha).toBe(255);
        expect(pixelDistance(frame.pixel, firstTrack.fillPixel)).toBeGreaterThan(96);
    }
}

async function expectStreamActivity(subscriber, roomId, streamType, active, httpBaseUrl) {
    const state = active ? "active" : "inactive";
    await expect
        .poll(() => streamState(roomId, streamType, httpBaseUrl))
        .toMatchObject({
            publication: { active },
            source: {
                active,
                encodings: expect.arrayContaining([
                    expect.objectContaining({ encodingId: expect.any(Number) })
                ]),
                mid: expect.any(String),
                sourceId: expect.any(Number)
            },
            subscription: { state }
        });
    await expectTrackUpdate(subscriber, PUBLISHER_SESSION_ID, streamType, active, "video");

    const presenceField = streamType === "camera" ? "isCameraOn" : "isScreenSharingOn";
    await expect
        .poll(() => roomUserInfo({ httpBaseUrl, roomId, sessionId: PUBLISHER_SESSION_ID }))
        .toMatchObject({
            [presenceField]: active
        });
    await expect
        .poll(() => latestInfoUpdate(subscriber, PUBLISHER_SESSION_ID))
        .toMatchObject({
            payload: {
                [String(PUBLISHER_SESSION_ID)]: {
                    [presenceField]: active
                }
            }
        });

    return streamState(roomId, streamType, httpBaseUrl);
}

function streamState(roomId, streamType, httpBaseUrl) {
    return streamDiagnostics({
        consumerSessionId: SUBSCRIBER_SESSION_ID,
        httpBaseUrl,
        producerSessionId: PUBLISHER_SESSION_ID,
        roomId,
        streamType
    });
}

function publishSyntheticStream(page, streamType, label) {
    return streamType === "camera"
        ? publishSyntheticCamera(page, label)
        : publishSyntheticScreen(page, label);
}

function streamIdentity({ publication, source, subscription }) {
    return {
        consumerTransportMediaId: subscription.consumerTransportMediaId,
        publicationSourceId: publication.sourceId,
        publicationTransportMediaId: publication.transportMediaId,
        sourceId: source.sourceId,
        sourceMid: source.mid,
        sourceTransportMediaId: source.transportMediaId,
        subscriptionSourceId: subscription.sourceId,
        subscriptionSourceTransportMediaId: subscription.sourceTransportMediaId
    };
}

async function expectCameraTrackUpdate(page, sessionId, active) {
    await expectTrackUpdate(page, sessionId, "camera", active, "video");
}

async function expectTrackUpdate(page, sessionId, type, active, kind) {
    await expect
        .poll(async () => latestTrackUpdate(page, sessionId, type))
        .toMatchObject(trackUpdateExpectation(sessionId, type, active, kind));
}

function cameraTrackUpdateExpectation(sessionId, active) {
    return trackUpdateExpectation(sessionId, "camera", active, "video");
}

function trackUpdateExpectation(sessionId, type, active, kind) {
    return {
        name: "track",
        payload: {
            active,
            sessionId,
            track: {
                enabled: true,
                id: expect.any(String),
                kind,
                readyState: "live"
            },
            type
        }
    };
}

function pixelDistance(left, right) {
    return Math.hypot(left.red - right.red, left.green - right.green, left.blue - right.blue);
}

function parseVideoCodecAnswer(sdp) {
    const lines = sdp.split(/\r?\n/);
    const audioRepairAttributes = [];
    const genericNackPayloadTypes = new Set();
    const h264Variants = new Set();
    const h264PayloadTypes = new Set();
    const fmtpByPayloadType = new Map();
    const hasSendRidHi = lines.some((line) => /^a=rid:hi send(?: |$)/.test(line));
    const hasSendRidLo = lines.some((line) => /^a=rid:lo send(?: |$)/.test(line));
    const hasSendSimulcastLoMidHi = lines.some((line) => /^a=simulcast:send lo;mid;hi$/.test(line));
    const rtxAssociations = new Map();
    const rtxPayloadTypes = new Set();
    const videoCodecPayloadTypes = new Set();
    const videoPayloadTypes = new Map();
    const vp8PayloadTypes = new Set();
    const vp9PayloadTypes = new Set();
    const vp9Profiles = new Set();
    let currentMediaKind = null;
    let hasRepairedRidExtension = false;

    for (const line of lines) {
        const mediaDescriptionMatch = /^m=([^ ]+)/.exec(line);
        if (mediaDescriptionMatch) {
            [, currentMediaKind] = mediaDescriptionMatch;
            continue;
        }
        const repairedRidExtension =
            line.startsWith("a=extmap:") && line.includes(` ${REPAIRED_RID_EXTENSION}`);
        if (currentMediaKind === "audio") {
            const fmtpMatch = /^a=fmtp:\d+ (.+)$/.exec(line);
            if (
                /^a=rtpmap:\d+ rtx\//i.test(line) ||
                /^a=rtcp-fb:(?:\d+|\*) nack$/.test(line) ||
                repairedRidExtension ||
                (fmtpMatch && parseFmtpParameters(fmtpMatch[1]).apt)
            ) {
                audioRepairAttributes.push(line);
            }
            continue;
        }
        if (currentMediaKind !== "video") {
            continue;
        }
        if (repairedRidExtension) {
            hasRepairedRidExtension = true;
        }
        const genericNackMatch = /^a=rtcp-fb:(\d+|\*) nack$/.exec(line);
        if (genericNackMatch) {
            genericNackPayloadTypes.add(genericNackMatch[1]);
            continue;
        }
        const rtpmapMatch = /^a=rtpmap:(\d+) ([^/]+)\/\d+/.exec(line);
        if (rtpmapMatch) {
            const [, payloadType, codecName] = rtpmapMatch;
            videoPayloadTypes.set(payloadType, codecName);
            if (codecName === "rtx") {
                rtxPayloadTypes.add(payloadType);
            } else {
                videoCodecPayloadTypes.add(payloadType);
            }
            if (codecName === "H264") {
                h264PayloadTypes.add(payloadType);
            } else if (codecName === "VP8") {
                vp8PayloadTypes.add(payloadType);
            } else if (codecName === "VP9") {
                vp9PayloadTypes.add(payloadType);
            }
            continue;
        }
        const fmtpMatch = /^a=fmtp:(\d+) (.+)$/.exec(line);
        if (!fmtpMatch) {
            continue;
        }
        const [, payloadType, formatParams] = fmtpMatch;
        fmtpByPayloadType.set(payloadType, parseFmtpParameters(formatParams));
    }

    for (const [payloadType, codecName] of videoPayloadTypes) {
        const params = fmtpByPayloadType.get(payloadType);
        if (!params) {
            continue;
        }
        if (codecName === "H264") {
            h264Variants.add(
                `packetization-mode=${params["packetization-mode"]};profile-level-id=${params["profile-level-id"]}`
            );
            continue;
        }
        if (codecName === "VP9" && params["profile-id"]) {
            vp9Profiles.add(params["profile-id"]);
        }
    }

    for (const [payloadType, params] of fmtpByPayloadType) {
        const codecName = videoPayloadTypes.get(payloadType);
        if (codecName === "rtx") {
            if (params.apt) {
                rtxAssociations.set(payloadType, params.apt);
            }
        }
    }

    return {
        audioRepairAttributes,
        genericNackPayloadTypes,
        hasRepairedRidExtension,
        hasSendRidHi,
        hasSendRidLo,
        hasSendSimulcastLoMidHi,
        h264PayloadTypes,
        h264Variants,
        rtxAssociations,
        rtxPayloadTypes,
        videoCodecPayloadTypes,
        vp8PayloadTypes,
        vp9PayloadTypes,
        vp9Profiles
    };
}

function parseFmtpParameters(formatParams) {
    return Object.fromEntries(
        formatParams
            .split(";")
            .map((entry) => entry.trim())
            .filter(Boolean)
            .map((entry) => {
                const [key, value] = entry.split("=");
                return [key, value];
            })
    );
}

test("middle camera RID delivers half-resolution decoded video", async ({
    browserName,
    context
}) => {
    test.setTimeout(60_000);
    const maxVideoBitrate = 4_000_000;
    const middleBitrate = 800_000;
    const server = await spawnLiveServer({
        bindPort: browserName === "firefox" ? 18088 : 18087,
        rtcMinPort: browserName === "firefox" ? 58392 : 58360,
        rtcMaxPort: browserName === "firefox" ? 58423 : 58391,
        maxBitrateOut: middleBitrate,
        maxVideoBitrate
    });
    let diagnostics;
    let frame;
    try {
        const channelUuid = await createChannel({
            authKey: server.authKey,
            httpBaseUrl: server.httpBaseUrl
        });
        const publisher = await createPeerPage(context);
        const receiver = await createPeerPage(context);
        const participant = await createPeerPage(context);
        for (const [page, sessionId] of [
            [publisher, 41],
            [receiver, 42],
            [participant, 43]
        ]) {
            await connectPeer(page, {
                channelUuid,
                jwt: createConnectToken(channelUuid, sessionId),
                url: server.wsUrl
            });
            await expect.poll(async () => (await peerSnapshot(page)).state).toBe("connected");
        }
        await publishSyntheticCamera(publisher, "middle-camera", {
            width: 1280,
            height: 720,
            frameRate: 30,
            movingPattern: true
        });
        await setStreamDownload(receiver, 41, "camera", true, "pinned");
        await expect
            .poll(
                async () => {
                    diagnostics = await streamDiagnostics({
                        consumerSessionId: 42,
                        httpBaseUrl: server.httpBaseUrl,
                        producerSessionId: 41,
                        roomId: channelUuid,
                        streamType: "camera"
                    });
                    const selection = diagnostics.subscription?.selection;
                    const middle = diagnostics.source?.encodings.find(
                        (encoding) => encoding.rid === "mid"
                    );
                    return (
                        diagnostics.subscription?.state === "active" &&
                        selection?.selectedRid === "mid" &&
                        selection.latestReceiverBandwidthEstimateBps >= middleBitrate &&
                        selection.latestReceiverBandwidthEstimateBps < maxVideoBitrate &&
                        Number.isFinite(middle?.lastPacketAgeMs) &&
                        middle.lastPacketAgeMs < 1_000
                    );
                },
                { intervals: [20, 50, 100], timeout: 25_000 }
            )
            .toBeTruthy();
        // A selected RID can still await its strict gate. Decoded dimensions
        // prove that forwarding has left the incumbent or bootstrap encoding.
        await expect
            .poll(
                async () => {
                    frame = await waitForDecodedRemoteVideoFrame(receiver, 41, "camera");
                    return { width: frame.width, height: frame.height };
                },
                { intervals: [20, 50, 100], timeout: 15_000 }
            )
            .toEqual({ width: 640, height: 360 });
    } finally {
        await test.info().attach("middle-camera-diagnostics", {
            body: JSON.stringify({ diagnostics, frame }),
            contentType: "application/json"
        });
        await server.stop();
    }
});

test("startup pressure holds the thumbnail through the soft pause dwell", async ({
    browserName,
    context
}) => {
    test.setTimeout(60_000);
    // At this cap lo and mid both cost 150 kbps. Three camera floors
    // exceed the 400 kbps probe ceiling throughout the soft pause dwell.
    const maxVideoBitrate = 200_000;
    const server = await spawnLiveServer({
        bindPort: browserName === "firefox" ? 18086 : 18085,
        rtcMinPort: browserName === "firefox" ? 58328 : 58296,
        rtcMaxPort: browserName === "firefox" ? 58359 : 58327,
        maxBitrateOut: maxVideoBitrate,
        maxVideoBitrate
    });
    try {
        const channelUuid = await createChannel({
            authKey: server.authKey,
            httpBaseUrl: server.httpBaseUrl
        });
        const pinned = await createPeerPage(context);
        const receiver = await createPeerPage(context);
        const firstThumbnail = await createPeerPage(context);
        const thumbnail = await createPeerPage(context);
        const connect = async (page, sessionId) => {
            await connectPeer(page, {
                channelUuid,
                jwt: createConnectToken(channelUuid, sessionId),
                url: server.wsUrl
            });
            await expect.poll(async () => (await peerSnapshot(page)).state).toBe("connected");
        };
        const diagnostics = (producerSessionId) =>
            streamDiagnostics({
                consumerSessionId: 42,
                httpBaseUrl: server.httpBaseUrl,
                producerSessionId,
                roomId: channelUuid,
                streamType: "camera"
            });
        await connect(pinned, 41);
        await connect(receiver, 42);
        await connect(firstThumbnail, 44);
        await connect(thumbnail, 43);
        // Chromium limits tiny capture surfaces to one simulcast layer. Motion
        // keeps real throughput high enough for BWE to sustain the test band.
        await publishSyntheticCamera(pinned, "startup-pinned", {
            width: 480,
            height: 270,
            frameRate: 30,
            movingPattern: true
        });
        await setStreamDownload(receiver, 41, "camera", true, "pinned");
        // Establish high quality before adding pressure so recovery timing cannot
        // consume the thumbnail's grace before an over-budget sample is visible.
        let initial;
        try {
            await expect
                .poll(
                    async () => {
                        initial = await diagnostics(41);
                        const selection = initial.subscription?.selection;
                        return (
                            initial.source?.currentIncomingBitrateBps > 0 &&
                            selection?.latestReceiverBandwidthEstimateBps >= maxVideoBitrate &&
                            selection.selectedRid === "hi"
                        );
                    },
                    { intervals: [20, 50, 100], timeout: 15_000 }
                )
                .toBeTruthy();
        } finally {
            await test.info().attach("initial-camera-diagnostics", {
                body: JSON.stringify(initial),
                contentType: "application/json"
            });
        }
        // Probes can reach twice the outgoing target. Two thumbnail floors
        // keep the receiver over budget throughout that probe range.
        await publishSyntheticCamera(firstThumbnail, "startup-first-thumbnail");
        await setStreamDownload(receiver, 44, "camera", true, "visible_thumbnail");
        // Committed publication order breaks ties between thumbnail priorities.
        await expect.poll(async () => (await diagnostics(44)).publication?.active).toBe(true);
        await publishSyntheticCamera(thumbnail, "startup-thumbnail");
        await setStreamDownload(receiver, 43, "camera", true, "visible_thumbnail");
        let grace;
        await expect
            .poll(
                async () => {
                    const sample = await diagnostics(43);
                    const selection = sample.subscription?.selection;
                    if (
                        selection?.latestReceiverBandwidthEstimateBps >= maxVideoBitrate &&
                        selection.selectedVideoBitrateBps > selection.selectedVideoBudgetBps &&
                        sample.subscription.state === "active" &&
                        sample.transport?.videoSoftPauseRemainingMs > 0
                    ) {
                        grace = sample;
                    }
                    return Boolean(grace);
                },
                { intervals: [10, 20, 50], timeout: 15_000 }
            )
            .toBeTruthy();
        expect(grace.subscription.selection.selectedVideoBudgetBps).toBeGreaterThanOrEqual(
            maxVideoBitrate
        );
        expect(grace.subscription.selection.selectedVideoBudgetBps).toBeLessThan(3 * 150_000);
        await test.info().attach("soft-pause-grace", {
            body: JSON.stringify(grace),
            contentType: "application/json"
        });
        await expect
            .poll(async () => (await diagnostics(43)).subscription)
            .toMatchObject({
                state: "inactive",
                selection: { policyPauseReason: "budget_pressure" }
            });
        await expectCameraTrackUpdate(receiver, 41, true);
        if (browserName === "chromium") {
            await waitForDecodedRemoteVideoFrame(receiver, 41, "camera");
        }
        // A BWE change during grace can restart the high-layer upgrade dwell.
        try {
            await expect
                .poll(() =>
                    cameraSubscriptionRid({
                        consumerSessionId: 42,
                        httpBaseUrl: server.httpBaseUrl,
                        producerSessionId: 41,
                        roomId: channelUuid
                    })
                )
                .toBe("hi");
        } finally {
            await test.info().attach("final-camera-diagnostics", {
                body: JSON.stringify(await diagnostics(41)),
                contentType: "application/json"
            });
        }
    } finally {
        await server.stop();
    }
});
