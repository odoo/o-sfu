import assert from "node:assert/strict";
import test from "node:test";
import { CLIENT_UPDATE } from "../../dist/public_api.js";
import { FakeMediaTrack, FakePeerConnection } from "../support/browser_fakes.mjs";
import { decodeSentFrame, tick } from "../support/protocol_fakes.mjs";
import {
    connectRealWithWelcome,
    createCameraTrack,
    createRecoveryHarness,
    createSfuClientHarness,
    createScreenTrack,
    emitOfferWithBinding
} from "../support/sfu_client_harness.mjs";
import {
    audioMedia,
    audioUploadSlot,
    buildNegotiationFrame,
    buildVideoRenegotiationFrame,
    sdp,
    videoMedia,
    videoUploadSlot
} from "../support/negotiation_fixtures.mjs";

const EXPECTED_RID_ENCODINGS = [
    {
        active: true,
        maxBitrate: 150000,
        rid: "lo",
        scaleResolutionDownBy: 4
    },
    {
        active: true,
        maxBitrate: 900000,
        rid: "hi",
        scaleResolutionDownBy: 1
    }
];

test("negotiation creates a peer connection and emits lowercase track updates", async () => {
    const { client, core, emitMessage, peerConnections, updates, connectWithWelcome } =
        createSfuClientHarness();

    await connectWithWelcome({
        connectOptions: {
            iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
        }
    });

    await emitOfferWithBinding({ core, emitMessage });

    assert.equal(peerConnections.length, 1);
    assert.deepEqual(peerConnections[0].config, {
        iceServers: [{ urls: ["stun:one.example.test", "stun:two.example.test"] }]
    });
    assert.deepEqual(core.submittedAnswers, [
        {
            negotiationKind: "offer",
            requestId: "7",
            sdp: "answer-sdp"
        }
    ]);
    assert.equal(client.state, "connected");

    const track = createCameraTrack("track-1");
    peerConnections[0].emitTrack(track, "0");

    assert.deepEqual(updates, [
        {
            name: CLIENT_UPDATE.TRACK,
            payload: {
                active: true,
                sessionId: 42,
                track,
                type: "camera"
            }
        }
    ]);
    assert.equal(client._consumers.get(42).camera.track, track);
});

test("renegotiation attaches pending audio only to upload-eligible mids", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections, sockets } = harness;

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    const localAudioTrack = new FakeMediaTrack({
        id: "local-audio",
        kind: "audio"
    });
    client.publish("audio", localAudioTrack);
    await tick();

    await emitMessage(
        buildNegotiationFrame("renegotiate", "10", {
            sdp: sdp(audioMedia("consumer-audio", "sendonly"), audioMedia("producer-audio")),
            uploadSlots: [audioUploadSlot("producer-audio")]
        })
    );

    assertLastNegotiationResponse(sockets[0], "renegotiate", "10");

    const producerTransceiver = peerConnections[0].transceivers.find(
        (transceiver) => transceiver.mid === "producer-audio"
    );
    const consumerTransceiver = peerConnections[0].transceivers.find(
        (transceiver) => transceiver.mid === "consumer-audio"
    );
    assert.ok(producerTransceiver);
    assert.ok(consumerTransceiver);
    assert.equal(producerTransceiver.sender.track, localAudioTrack);
    assert.equal(consumerTransceiver.sender.track, null);
});

test("renegotiation binds a newly published local track before answering", async () => {
    const { peerConnections, track } = await renegotiateCamera(
        buildVideoRenegotiationFrame("9", { simulcastEncodings: [] }),
        "camera-track-1"
    );

    assert.equal(peerConnections[0].transceivers[2].sender.track, track);
    assert.equal(peerConnections[0].transceivers[2].direction, "sendonly");
    assert.equal(
        peerConnections[0].answerSnapshots.at(-1)[2].senderTrack,
        track,
        "the browser must bind the track before generating the renegotiation answer"
    );
});

test("renegotiation configures RID simulcast before answering supported video publishes", async () => {
    const { peerConnections, track, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("12", {
            rtpmap: "VP8/90000"
        }),
        "camera-track-simulcast"
    );

    assert.equal(transceiver.sender.track, track);
    assertSenderEncodings(peerConnections[0], transceiver, EXPECTED_RID_ENCODINGS);
});

test("renegotiation configures RID simulcast from server-defined upload slots", async () => {
    const { peerConnections, track, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("13", {
            codecs: ["H264"],
            payloadType: 102,
            rtpmap: "H264/90000"
        }),
        "camera-track-single"
    );

    assert.equal(transceiver.sender.track, track);
    assertSenderEncodings(peerConnections[0], transceiver, EXPECTED_RID_ENCODINGS);
});

test("renegotiation falls back to single encoding when the server ladder is invalid", async () => {
    const { peerConnections, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("14", {
            rtpmap: "VP8/90000",
            simulcastEncodings: [
                {
                    maxBitrate: 150000,
                    rid: "lo",
                    resolutionScale: 0
                },
                {
                    maxBitrate: 900000,
                    rid: "hi",
                    resolutionScale: 1
                }
            ]
        }),
        "camera-track-invalid-profile"
    );

    assertSenderEncodings(peerConnections[0], transceiver, []);
});

test("renegotiation falls back to single encoding when sender parameters are rejected", async () => {
    const { peerConnections, transceiver } = await renegotiateCamera(
        buildVideoRenegotiationFrame("12", {
            rtpmap: "VP8/90000"
        }),
        "camera-track-rejected-profile",
        {
            peerConnectionOptions: {
                senderOptionsByMid: {
                    2: { rejectSetParameters: true }
                }
            }
        }
    );

    assertSenderEncodings(peerConnections[0], transceiver, []);
});

test("initial offer binds a pending local track before answering", async () => {
    const { client, emitMessage, peerConnections, connectWithWelcome } = createSfuClientHarness({
        peerConnectionOptions: { autoConnect: false }
    });

    const track = createCameraTrack("camera-track-pending-offer");

    await connectWithWelcome();

    client.publish("camera", track);
    await tick();
    await emitMessage("offer");

    assert.equal(peerConnections[0].transceivers[1].sender.track, track);
    assert.equal(
        peerConnections[0].answerSnapshots.at(-1)[1].senderTrack,
        track,
        "the browser must bind the pending upload before generating the initial answer"
    );
});

test("offer submits its local description while ice gathering continues", async () => {
    class GatheringPeerConnection extends FakePeerConnection {
        async setLocalDescription(description) {
            await super.setLocalDescription(description);
            this.iceGatheringState = "gathering";
        }

        completeIceGathering() {
            this.iceGatheringState = "complete";
            this.onicegatheringstatechange?.();
        }
    }
    const { core, emitMessage, connectWithWelcome, peerConnections } = createSfuClientHarness({
        createPeerConnection: (config) =>
            new GatheringPeerConnection(config, {
                answerSdp: "answer-with-ice-credentials",
                autoConnect: false
            })
    });

    await connectWithWelcome();
    await emitMessage("offer");

    try {
        assert.deepEqual(core.submittedAnswers, [
            {
                negotiationKind: "offer",
                requestId: "7",
                sdp: "answer-with-ice-credentials"
            }
        ]);
    } finally {
        peerConnections[0].completeIceGathering();
        await tick();
    }
});

test("renegotiation binds pending camera and screen tracks to distinct offer-ordered mids", async () => {
    const harness = createRecoveryHarness();
    const { client, emitMessage, peerConnections } = harness;

    const cameraTrack = createCameraTrack("camera-track-distinct-mid");
    const screenTrack = createScreenTrack("screen-track-distinct-mid");

    await connectRealWithWelcome(harness);
    await emitMessage(buildNegotiationFrame("offer", "7", "1"));

    client.publish("camera", cameraTrack);
    await tick();
    client.publish("screen", screenTrack);
    await tick();
    await emitMessage(
        buildNegotiationFrame("renegotiate", "11", {
            sdp: sdp(videoMedia("2"), videoMedia("3")),
            uploadSlots: [
                videoUploadSlot("2", { simulcastEncodings: [] }),
                videoUploadSlot("3", { simulcastEncodings: [] })
            ]
        })
    );

    assert.equal(peerConnections[0].transceivers[2].sender.track, cameraTrack);
    assert.equal(peerConnections[0].transceivers[3].sender.track, screenTrack);
    assert.equal(peerConnections[0].answerSnapshots.at(-1)[2].senderTrack, cameraTrack);
    assert.equal(peerConnections[0].answerSnapshots.at(-1)[3].senderTrack, screenTrack);
});

function assertLastNegotiationResponse(socket, tag, responseTo) {
    assert.deepEqual(decodeSentFrame(socket, socket.sent.length - 1).at(-1), {
        t: tag,
        r: responseTo,
        p: {
            sdp: "answer-sdp"
        }
    });
}

async function renegotiateCamera(frame, trackId, harnessOptions = {}) {
    const harness = createRecoveryHarness(harnessOptions);
    const track = createCameraTrack(trackId);

    await connectRealWithWelcome(harness);
    await harness.emitMessage(buildNegotiationFrame("offer", "7", "1"));

    harness.client.publish("camera", track);
    await tick();
    await harness.emitMessage(frame);
    const transceiver = harness.peerConnections[0].transceivers.find(
        (candidate) => candidate.mid === "2"
    );
    assert.ok(transceiver);
    assertLastNegotiationResponse(harness.sockets[0], "renegotiate", JSON.parse(frame)[0].q);

    return {
        ...harness,
        track,
        transceiver
    };
}

function assertSenderEncodings(peerConnection, transceiver, expected) {
    const snapshot = peerConnection.answerSnapshots
        .at(-1)
        .find((candidate) => candidate.mid === transceiver.mid);
    assert.ok(snapshot);
    assert.deepEqual(snapshot.senderParameters, {
        encodings: expected
    });
}
