import { expect, test } from "./transport_fixtures.mjs";
import {
    connectPeer,
    createChannel,
    createConnectToken,
    createPeerPage
} from "./live_server_helpers.mjs";

test("transport observation survives immediate peer closure", async ({ context, transport }) => {
    const channelUuid = await createChannel();
    const peer = await createPeerPage(context);
    await peer.evaluate(() => {
        const ObservedPeerConnection = globalThis.RTCPeerConnection;
        globalThis.RTCPeerConnection = class extends ObservedPeerConnection {
            constructor(...args) {
                super(...args);
                this.addEventListener("connectionstatechange", () => {
                    if (this.connectionState === "connected") {
                        // NOTE: Close in the same event turn before pending stats can settle.
                        this.close();
                        globalThis.__closedConnectedPeer = true;
                    }
                });
            }
        };
    });
    await connectPeer(peer, {
        channelUuid,
        jwt: createConnectToken(channelUuid, 77)
    });
    await expect.poll(() => peer.evaluate(() => globalThis.__closedConnectedPeer)).toBe(true);
    expect(await peer.evaluate(() => globalThis.__selectedCandidateProtocols)).toEqual([transport]);
});
