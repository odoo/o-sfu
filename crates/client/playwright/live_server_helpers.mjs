import { spawn } from "node:child_process";
import { createHmac, randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const TEST_AUTH_KEY = "u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng=";
const TEST_ROOM_KEY = "6me1UEeLIMqcygWGz1icMuVMVcPbZyVOF2LlZZPCCOw=";
const TEST_SFU_HTTP_BASE_URL = "http://127.0.0.1:18080";
export const TEST_SFU_WS_URL = "ws://127.0.0.1:18080/";
const DIAGNOSTICS_ROOM_PATH = "/internal/diagnostics/rooms";
const AUDIO_OPERATION_TIMEOUT_MS = 250;
const RECOVERABLE_BROWSER_CLOSE_CODE = 4000;
const HARNESS_URL = "/playwright/fixtures/harness.html";
const STREAM_TYPES = new Set(["audio", "camera", "screen"]);

export async function createChannel({
    authKey = TEST_AUTH_KEY,
    roomKey = TEST_ROOM_KEY,
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL
} = {}) {
    const response = await fetch(`${httpBaseUrl}/v1/channel`, {
        headers: {
            Authorization: `Bearer ${signJwt(
                {
                    iss: `playwright-${randomUUID()}`,
                    key: roomKey
                },
                authKey
            )}`
        }
    });
    if (!response.ok) {
        throw new Error(`expected room creation to succeed, got HTTP ${response.status}`);
    }
    const payload = await response.json();
    return payload.uuid;
}

export function createConnectToken(channelUuid, sessionId, roomKey = TEST_ROOM_KEY) {
    return signJwt(
        {
            sfu_channel_uuid: channelUuid,
            session_id: sessionId
        },
        roomKey
    );
}

export async function createPeerPage(context) {
    const page = await context.newPage();
    await page.goto(HARNESS_URL);
    await page.evaluate((audioOperationTimeoutMs) => {
        globalThis.__liveHarnessSettleAudio = (operation) =>
            Promise.race([
                operation.catch(() => null),
                new Promise((resolve) => window.setTimeout(resolve, audioOperationTimeoutMs))
            ]);
        globalThis.__liveHarnessResumeAudio = (audioContext) => {
            try {
                void globalThis.__liveHarnessSettleAudio(audioContext.resume());
            } catch (_error) {
                void _error;
            }
        };
        globalThis.__liveHarnessStopLocalMedia = async (harness, streamType) => {
            const media = harness.localMedia?.[streamType] ?? null;
            if (media?.ticker !== undefined) {
                clearInterval(media.ticker);
            }
            if (media?.oscillator) {
                try {
                    media.oscillator.stop();
                } catch (_error) {
                    void _error;
                }
                media.oscillator.disconnect();
            }
            if (media?.audioContext && media.audioContext.state !== "closed") {
                await globalThis.__liveHarnessSettleAudio(media.audioContext.close());
            }
            media?.track?.stop();
            delete harness.localMedia[streamType];
        };
        globalThis.__liveHarness = {
            client: null,
            errors: [],
            localMedia: {},
            negotiationNeededByPeer: new WeakMap(),
            stateChanges: [],
            updates: []
        };
    }, AUDIO_OPERATION_TIMEOUT_MS);
    return page;
}

export function observeNegotiations(page) {
    let requestCount = 0;
    page.on("websocket", (socket) => {
        socket.on("framereceived", ({ payload }) => {
            const text = typeof payload === "string" ? payload : payload.toString();
            let batch;
            try {
                batch = JSON.parse(text);
            } catch {
                return;
            }
            if (!Array.isArray(batch)) {
                return;
            }
            for (const envelope of batch) {
                if (
                    (envelope?.t === "offer" || envelope?.t === "renegotiate") &&
                    envelope.q !== undefined
                ) {
                    requestCount += 1;
                }
            }
        });
    });
    return {
        count: () => requestCount
    };
}

export async function observeNegotiationNeeded(page) {
    await page.evaluate(() => {
        const harness = globalThis.__liveHarness;
        const peer = harness.client?._runtime?._peerSession?._activePeer;
        if (!peer || harness.negotiationNeededByPeer.has(peer)) {
            return;
        }
        const observation = { count: 0 };
        harness.negotiationNeededByPeer.set(peer, observation);
        peer.addEventListener("negotiationneeded", () => {
            observation.count += 1;
        });
    });
    return () =>
        page.evaluate(() => {
            const harness = globalThis.__liveHarness;
            const peer = harness.client?._runtime?._peerSession?._activePeer;
            return harness.negotiationNeededByPeer.get(peer)?.count ?? null;
        });
}

export async function connectPeer(page, { channelUuid, iceServers, jwt, url = TEST_SFU_WS_URL }) {
    await page.evaluate(
        async ({ channelUuid, iceServers, jwt, url }) => {
            const serializeTrack = (track) =>
                track
                    ? {
                          enabled: track.enabled,
                          id: track.id,
                          kind: track.kind,
                          muted: track.muted,
                          readyState: track.readyState
                      }
                    : null;
            const serializeUpdate = (detail) => {
                if (detail.name === "track") {
                    return {
                        name: detail.name,
                        payload: {
                            active: detail.payload.active,
                            sessionId: detail.payload.sessionId,
                            track: serializeTrack(detail.payload.track),
                            type: detail.payload.type
                        }
                    };
                }
                return structuredClone(detail);
            };

            const harness = globalThis.__liveHarness;
            const { SfuClient } = await import("/dist/odoo_sfu.js");
            const client = new SfuClient();
            harness.client = client;
            harness.errors = [];
            harness.stateChanges = [];
            harness.updates = [];
            client.addEventListener("handledError", (event) => {
                harness.errors.push(String(event.detail.error));
            });
            client.addEventListener("stateChange", (event) => {
                harness.stateChanges.push(structuredClone(event.detail));
            });
            client.addEventListener("update", (event) => {
                harness.updates.push(serializeUpdate(event.detail));
            });
            client.connect(url, jwt, { channelUUID: channelUuid, iceServers });
        },
        { channelUuid, iceServers, jwt, url }
    );
}

export async function publishSyntheticCamera(page, label, options) {
    return publishSyntheticVideo(page, "camera", label, options);
}

export async function publishSyntheticScreen(page, label) {
    return publishSyntheticVideo(page, "screen", label);
}

export async function publishSyntheticAudio(page, label) {
    await page.evaluate(async (nextLabel) => {
        const harness = globalThis.__liveHarness;
        if (!harness.client) {
            throw new Error("browser harness client is not connected");
        }
        await globalThis.__liveHarnessStopLocalMedia(harness, "audio");

        const audioContext = new AudioContext();
        const destination = audioContext.createMediaStreamDestination();
        const gain = audioContext.createGain();
        gain.gain.value = 0.02;
        const oscillator = audioContext.createOscillator();
        oscillator.frequency.value = 220 + (nextLabel.length % 12) * 20;
        oscillator.connect(gain).connect(destination);
        oscillator.start();
        globalThis.__liveHarnessResumeAudio(audioContext);
        const [track] = destination.stream.getAudioTracks();
        if (!track) {
            throw new Error("expected synthetic audio graph to expose an audio track");
        }
        harness.localMedia.audio = {
            audioContext,
            oscillator,
            track
        };
        harness.client.updateUpload("audio", track);
    }, label);
}

export async function publishSyntheticVideo(
    page,
    streamType,
    label,
    { width = 96, height = 96, frameRate = 10, movingPattern = false } = {}
) {
    assertStreamType(streamType);
    const colors = syntheticVideoColors(streamType, label);
    const fillPixel = pixelFromHex(colors[0]);
    return page.evaluate(
        async ({
            colors,
            fillPixel,
            label,
            streamType,
            width,
            height,
            frameRate,
            movingPattern
        }) => {
            const harness = globalThis.__liveHarness;
            if (!harness.client) {
                throw new Error("browser harness client is not connected");
            }
            await globalThis.__liveHarnessStopLocalMedia(harness, streamType);

            const canvas = document.createElement("canvas");
            canvas.width = width;
            canvas.height = height;
            const context = canvas.getContext("2d");
            if (!context) {
                throw new Error("expected 2D canvas context for synthetic video track");
            }
            let frame = 0;
            const draw = () => {
                context.fillStyle = colors[frame % colors.length];
                context.fillRect(0, 0, canvas.width, canvas.height);
                context.fillStyle = "#f3f4f6";
                context.font = "14px sans-serif";
                context.fillText(label, 8, 28);
                context.fillText(streamType, 8, 42);
                context.fillText(String(frame), 8, 56);
                if (movingPattern) {
                    const palette = [...colors, "#f3f4f6"];
                    for (let y = 0; y < canvas.height; y += 12) {
                        for (let x = 0; x < canvas.width; x += 12) {
                            context.fillStyle = palette[(x / 12 + y / 12 + frame) % palette.length];
                            context.fillRect(x, y, 10, 10);
                        }
                    }
                }
                frame += 1;
            };
            draw();
            const ticker = window.setInterval(draw, 1000 / frameRate);
            const stream = canvas.captureStream(frameRate);
            const [track] = stream.getVideoTracks();
            if (!track) {
                throw new Error("expected synthetic canvas capture to expose a video track");
            }
            harness.localMedia[streamType] = {
                ticker,
                track
            };
            harness.client.updateUpload(streamType, track);
            return {
                fillPixel,
                trackId: track.id
            };
        },
        { colors, fillPixel, label, streamType, width, height, frameRate, movingPattern }
    );
}

export async function setStreamDownload(
    page,
    targetSessionId,
    streamType,
    active,
    layout = undefined
) {
    assertStreamType(streamType);
    await page.evaluate(
        ({ active, layout, streamType, targetSessionId }) => {
            const harness = globalThis.__liveHarness;
            if (!harness.client) {
                throw new Error("browser harness client is not connected");
            }
            const states = {
                [streamType]: active
            };
            if (layout !== undefined) {
                states[`${streamType}Layout`] = layout;
            }
            harness.client.updateDownload(targetSessionId, states);
        },
        { active, layout, streamType, targetSessionId }
    );
}

export async function pauseStream(page, streamType) {
    assertStreamType(streamType);
    await page.evaluate((streamType) => {
        const harness = globalThis.__liveHarness;
        if (!harness.client) {
            throw new Error("browser harness client is not connected");
        }
        harness.client.updateUpload(streamType, null);
    }, streamType);
}

export async function disconnectPeer(page) {
    await page.evaluate(
        async (streamTypes) => {
            const harness = globalThis.__liveHarness;
            harness.client?.disconnect();
            await Promise.all(
                streamTypes.map((streamType) =>
                    globalThis.__liveHarnessStopLocalMedia(harness, streamType)
                )
            );
        },
        [...STREAM_TYPES]
    );
}

export async function broadcast(page, message) {
    await page.evaluate((nextMessage) => {
        const harness = globalThis.__liveHarness;
        if (!harness.client) {
            throw new Error("browser harness client is not connected");
        }
        harness.client.broadcast(nextMessage);
    }, message);
}

export async function updateInfo(page, info, options = { needRefresh: true }) {
    await page.evaluate(
        ({ nextInfo, nextOptions }) => {
            const harness = globalThis.__liveHarness;
            if (!harness.client) {
                throw new Error("browser harness client is not connected");
            }
            harness.client.updateInfo(nextInfo, nextOptions);
        },
        { nextInfo: info, nextOptions: options }
    );
}

export async function forceRecoverableClose(page) {
    await page.evaluate((closeCode) => {
        const websocket = globalThis.__liveHarness.client?._runtime?._socketSession?._activeSocket;
        if (!websocket || websocket.readyState >= WebSocket.CLOSING) {
            throw new Error("browser harness websocket is not open");
        }
        websocket.close(closeCode);
    }, RECOVERABLE_BROWSER_CLOSE_CODE);
}

export async function peerSnapshot(page) {
    return page.evaluate(() => {
        const serializeTrack = (track) =>
            track
                ? {
                      enabled: track.enabled,
                      id: track.id,
                      kind: track.kind,
                      muted: track.muted,
                      readyState: track.readyState
                  }
                : null;
        const serializeConsumers = (consumers) =>
            Object.fromEntries(
                [...consumers.entries()].map(([sessionId, entry]) => [
                    String(sessionId),
                    {
                        audio: serializeTrack(entry.audio?.track ?? null),
                        camera: serializeTrack(entry.camera?.track ?? null),
                        screen: serializeTrack(entry.screen?.track ?? null)
                    }
                ])
            );
        const harness = globalThis.__liveHarness;
        const client = harness.client;
        const peerConnection = client?._runtime?._peerSession?._activePeer;
        return {
            consumers: client ? serializeConsumers(client._consumers) : {},
            errors: [...harness.errors],
            peerConnectionState: peerConnection?.connectionState ?? null,
            state: client?.state ?? null,
            stateChanges: [...harness.stateChanges],
            updates: [...harness.updates]
        };
    });
}

export async function latestBroadcastUpdate(page, senderId) {
    return page.evaluate((expectedSenderId) => {
        const harness = globalThis.__liveHarness;
        return (
            harness.updates.findLast(
                (update) =>
                    update.name === "broadcast" &&
                    String(update.payload.senderId) === String(expectedSenderId)
            ) ?? null
        );
    }, senderId);
}

export async function latestInfoUpdate(page, sessionId) {
    return page.evaluate((targetSessionId) => {
        const harness = globalThis.__liveHarness;
        const targetKey = String(targetSessionId);
        return (
            harness.updates.findLast(
                (update) => update.name === "info_change" && update.payload[targetKey]
            ) ?? null
        );
    }, sessionId);
}

export async function latestTrackUpdate(page, targetSessionId, targetType) {
    return page.evaluate(
        ({ sessionId: nextSessionId, type: nextType }) => {
            const harness = globalThis.__liveHarness;
            return (
                harness.updates.findLast(
                    (update) =>
                        update.name === "track" &&
                        update.payload.sessionId === nextSessionId &&
                        update.payload.type === nextType
                ) ?? null
            );
        },
        { sessionId: targetSessionId, type: targetType }
    );
}

export async function cameraSubscriptionRid({
    consumerSessionId,
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL,
    producerSessionId,
    roomId
}) {
    const room = await fetchRoomDiagnostics(httpBaseUrl, roomId);
    const subscription = room
        ? cameraSubscription(room, consumerSessionId, producerSessionId)
        : null;
    if (!subscription || subscription.state !== "active") {
        return null;
    }
    return subscription.selection?.selectedRid ?? null;
}

export async function cameraPublicationActive({
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL,
    roomId,
    sessionId
}) {
    const room = await fetchRoomDiagnostics(httpBaseUrl, roomId);
    const user = room?.users.find((candidate) => userIdsMatch(candidate.userId, sessionId));
    return (
        user?.publications.some(
            (publication) => publication.streamId === "camera" && publication.active === true
        ) ?? false
    );
}

export async function roomUserInfo({ httpBaseUrl = TEST_SFU_HTTP_BASE_URL, roomId, sessionId }) {
    const room = await fetchRoomDiagnostics(httpBaseUrl, roomId);
    const user = room?.users.find((candidate) => userIdsMatch(candidate.userId, sessionId));
    return user?.userInfo ?? null;
}

export async function peerLocalDescriptionSdp(page) {
    return page.evaluate(() => {
        const peerConnection = globalThis.__liveHarness.client?._runtime?._peerSession?._activePeer;
        return peerConnection?.localDescription?.sdp ?? null;
    });
}

export async function localSenderEncodings(page, streamType) {
    assertStreamType(streamType);
    return page.evaluate((targetStreamType) => {
        const harness = globalThis.__liveHarness;
        const peerConnection = harness.client?._runtime?._peerSession?._activePeer;
        const localTrack = harness.localMedia?.[targetStreamType]?.track;
        if (!peerConnection || !localTrack) {
            return [];
        }
        const transceiver = peerConnection
            .getTransceivers()
            .find((candidate) => candidate.sender.track === localTrack);
        return (transceiver?.sender.getParameters().encodings ?? []).map((encoding) => ({
            active: encoding.active,
            maxBitrate: encoding.maxBitrate,
            rid: encoding.rid,
            scaleResolutionDownBy: encoding.scaleResolutionDownBy
        }));
    }, streamType);
}

export async function streamDiagnostics({
    consumerSessionId,
    httpBaseUrl = TEST_SFU_HTTP_BASE_URL,
    producerSessionId,
    roomId,
    streamType
}) {
    assertStreamType(streamType);
    const room = await fetchRoomDiagnostics(httpBaseUrl, roomId);
    const producer = room?.users.find((user) => userIdsMatch(user.userId, producerSessionId));
    const consumer = room?.users.find((user) => userIdsMatch(user.userId, consumerSessionId));
    const publication =
        producer?.publications.find((candidate) => candidate.streamId === streamType) ?? null;
    const source =
        room?.sources.find(
            (candidate) =>
                userIdsMatch(candidate.ownerUserId, producerSessionId) &&
                candidate.streamId === streamType
        ) ?? null;
    const subscription =
        consumer?.subscriptions.find(
            (candidate) =>
                userIdsMatch(candidate.producerUserId, producerSessionId) &&
                candidate.streamId === streamType
        ) ?? null;
    return {
        publication,
        source,
        subscription,
        transport: consumer?.transport ?? null
    };
}

export async function waitForDecodedRemoteVideoFrame(
    page,
    targetSessionId,
    streamType,
    { expectedPixel = null, maxPixelDistance = 96 } = {}
) {
    assertStreamType(streamType);
    return page.evaluate(
        async ({
            expectedPixel: nextExpectedPixel,
            maxPixelDistance: nextMaxPixelDistance,
            sessionId,
            streamType: targetStreamType
        }) => {
            const sleep = (ms) =>
                new Promise((resolve) => {
                    window.setTimeout(resolve, ms);
                });
            const nextVideoFrameMetadata = async (video, timeoutMs) => {
                if (typeof video.requestVideoFrameCallback !== "function") {
                    await sleep(Math.min(timeoutMs, 100));
                    return null;
                }
                return new Promise((resolve) => {
                    const timeout = window.setTimeout(
                        () => resolve(null),
                        Math.min(timeoutMs, 1_000)
                    );
                    video.requestVideoFrameCallback((_now, metadata) => {
                        window.clearTimeout(timeout);
                        resolve(metadata);
                    });
                });
            };
            const requestPlayback = (video) => {
                const playPromise = video.play();
                if (playPromise && typeof playPromise.catch === "function") {
                    playPromise.catch(() => null);
                }
            };
            const drawDecodedFrame = (video, usedVideoFrameCallback, decodedFrames) => {
                const canvas = document.createElement("canvas");
                canvas.width = 1;
                canvas.height = 1;
                const context = canvas.getContext("2d");
                if (!context) {
                    throw new Error("expected 2D canvas context for decoded frame check");
                }
                context.drawImage(video, 0, 0, 1, 1);
                const [red, green, blue, alpha] = context.getImageData(0, 0, 1, 1).data;
                return {
                    decodedFrames,
                    height: video.videoHeight,
                    pixel: { alpha, blue, green, red },
                    usedVideoFrameCallback,
                    width: video.videoWidth
                };
            };
            const pixelDistance = (left, right) =>
                Math.hypot(left.red - right.red, left.green - right.green, left.blue - right.blue);
            const harness = globalThis.__liveHarness;
            const client = harness.client;
            const track =
                client?._consumers?.get(sessionId)?.[targetStreamType]?.track ??
                client?._consumers?.get(String(sessionId))?.[targetStreamType]?.track ??
                null;
            if (!track) {
                throw new Error(
                    `remote ${targetStreamType} track for session ${String(sessionId)} is missing`
                );
            }

            const video = document.createElement("video");
            video.autoplay = true;
            video.muted = true;
            video.playsInline = true;
            video.srcObject = new MediaStream([track]);
            document.body.append(video);

            try {
                const startedAt = performance.now();
                const deadline = startedAt + 12_000;
                requestPlayback(video);
                const initialCurrentTime = video.currentTime;
                const initialDecodedFrames =
                    video.getVideoPlaybackQuality?.().totalVideoFrames ?? 0;
                let usedVideoFrameCallback = false;

                while (performance.now() < deadline) {
                    if (video.paused) {
                        requestPlayback(video);
                    }
                    const metadata = await nextVideoFrameMetadata(
                        video,
                        deadline - performance.now()
                    );
                    usedVideoFrameCallback ||= metadata !== null;
                    if (video.videoWidth > 0 && video.videoHeight > 0) {
                        const decodedFrames =
                            video.getVideoPlaybackQuality?.().totalVideoFrames ??
                            (metadata ? metadata.presentedFrames : 0);
                        const hasDecodedFrame =
                            decodedFrames > initialDecodedFrames ||
                            metadata !== null ||
                            video.currentTime > initialCurrentTime;
                        if (hasDecodedFrame) {
                            const frame = drawDecodedFrame(
                                video,
                                usedVideoFrameCallback,
                                decodedFrames
                            );
                            if (
                                !nextExpectedPixel ||
                                pixelDistance(frame.pixel, nextExpectedPixel) <=
                                    nextMaxPixelDistance
                            ) {
                                return frame;
                            }
                        }
                    }
                    await sleep(100);
                }

                throw new Error(
                    `remote ${targetStreamType} video did not decode a matching frame before timeout`
                );
            } finally {
                video.pause();
                video.srcObject = null;
                video.remove();
            }
        },
        {
            expectedPixel,
            maxPixelDistance,
            sessionId: targetSessionId,
            streamType
        }
    );
}

function assertStreamType(streamType) {
    if (!STREAM_TYPES.has(streamType)) {
        throw new Error(`unsupported stream type ${String(streamType)}`);
    }
}

function syntheticVideoColors(streamType, label) {
    if (streamType === "camera") {
        return label.includes("two") ? ["#d00068", "#f5a000"] : ["#14324a", "#5b2d1f"];
    }
    return label.includes("two") ? ["#d00068"] : ["#0060d4"];
}

function pixelFromHex(hex) {
    const value = Number.parseInt(hex.slice(1), 16);
    return {
        blue: value & 0xff,
        green: (value >> 8) & 0xff,
        red: (value >> 16) & 0xff
    };
}

export async function spawnLiveServer({
    authKey = TEST_AUTH_KEY,
    bindHost = "127.0.0.1",
    bindPort,
    host = "127.0.0.1",
    announcedIp = host,
    rtcMaxPort,
    rtcMinPort,
    maxBitrateOut,
    maxVideoBitrate,
    codecFlags = {}
}) {
    const env = {
        ...process.env,
        AUTH_KEY: authKey,
        BIND_ADDRESS: `${bindHost}:${bindPort}`,
        ANNOUNCED_IP: announcedIp,
        RTC_MAX_PORT: String(rtcMaxPort),
        RTC_MIN_PORT: String(rtcMinPort),
        CODEC_H264: String(Boolean(codecFlags.h264)),
        CODEC_VP9: String(Boolean(codecFlags.vp9))
    };
    if (Object.hasOwn(codecFlags, "vp8")) {
        env.CODEC_VP8 = String(Boolean(codecFlags.vp8));
    }
    if (maxBitrateOut !== undefined) {
        env.MAX_BITRATE_OUT = String(maxBitrateOut);
    }
    if (maxVideoBitrate !== undefined) {
        env.MAX_VIDEO_BITRATE = String(maxVideoBitrate);
    }
    const child = spawn(
        "cargo",
        ["run", "--quiet", "--manifest-path", "../../Cargo.toml", "-p", "o-sfu"],
        {
            cwd: fileURLToPath(new URL("../", import.meta.url)),
            env,
            stdio: "ignore"
        }
    );
    const httpBaseUrl = `http://${host}:${bindPort}`;
    for (let attempt = 0; attempt < 60; attempt += 1) {
        try {
            const response = await fetch(`${httpBaseUrl}/v1/noop`);
            if (response.ok) {
                return {
                    authKey,
                    child,
                    httpBaseUrl,
                    stop: async () => {
                        child.kill("SIGTERM");
                        await onceExit(child);
                    },
                    wsUrl: `ws://${host}:${bindPort}/`
                };
            }
        } catch (_error) {
            void _error;
        }
        await delay(500);
    }
    child.kill("SIGTERM");
    await onceExit(child);
    throw new Error(`o-sfu test server on port ${bindPort} did not become ready`);
}

async function fetchRoomDiagnostics(httpBaseUrl, roomId) {
    const response = await fetch(`${httpBaseUrl}${DIAGNOSTICS_ROOM_PATH}/${roomId}`);
    if (!response.ok) {
        return null;
    }
    return response.json();
}

function cameraSubscription(room, consumerSessionId, producerSessionId) {
    return room.users
        .find((user) => userIdsMatch(user.userId, consumerSessionId))
        ?.subscriptions.find(
            (subscription) =>
                userIdsMatch(subscription.producerUserId, producerSessionId) &&
                subscription.streamId === "camera"
        );
}

function userIdsMatch(actual, expected) {
    return String(actual) === String(expected);
}

function signJwt(payload, keyB64) {
    const encodedHeader = encodeJwtSegment({
        alg: "HS256",
        typ: "JWT"
    });
    const encodedPayload = encodeJwtSegment(payload);
    const signedData = `${encodedHeader}.${encodedPayload}`;
    const signature = createHmac("sha256", Buffer.from(keyB64, "base64"))
        // lgtm[js/insufficient-password-hash] HS256 signs a JWT, it does not store a password hash
        .update(signedData)
        .digest("base64url");
    return `${signedData}.${signature}`;
}

function encodeJwtSegment(value) {
    return Buffer.from(JSON.stringify(value)).toString("base64url");
}

async function onceExit(child) {
    if (child.exitCode !== null) {
        return;
    }
    await new Promise((resolve) => {
        child.once("exit", resolve);
    });
}
