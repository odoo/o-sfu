/**
 * Playwright `test` with a `transport` option, so live-server tests run once per
 * transport without changing their bodies.
 *
 * With `transport: "tcp"`, pages never see the SFU's UDP candidates, so the browser
 * must use RTC over TCP. In both modes every peer connection records the protocol of
 * its selected candidate pair, and the test fails if one used another transport.
 */
import { expect, test as base } from "@playwright/test";

export { expect };

export const test = base.extend({
    transport: ["udp", { option: true }],
    context: async ({ context, transport }, use) => {
        await context.addInitScript(recordSelectedProtocols);
        if (transport === "tcp") {
            await context.addInitScript(keepOnlyTcpCandidates);
        }
        await use(context);
        for (const page of context.pages()) {
            const protocols = await page.evaluate(
                () => globalThis.__selectedCandidateProtocols ?? []
            );
            for (const protocol of protocols) {
                expect(protocol, `a peer connection on ${page.url()} used ${protocol}`).toBe(
                    transport
                );
            }
        }
    }
});

function keepOnlyTcpCandidates() {
    const setRemoteDescription = RTCPeerConnection.prototype.setRemoteDescription;
    RTCPeerConnection.prototype.setRemoteDescription = function (description) {
        if (typeof description?.sdp !== "string") {
            return setRemoteDescription.call(this, description);
        }
        const sdp = description.sdp
            .split("\r\n")
            .filter((line) => !line.startsWith("a=candidate:") || line.includes(" tcp "))
            .join("\r\n");
        return setRemoteDescription.call(this, { type: description.type, sdp });
    };
}

function recordSelectedProtocols() {
    const protocols = [];
    globalThis.__selectedCandidateProtocols = protocols;
    const NativePeerConnection = globalThis.RTCPeerConnection;
    globalThis.RTCPeerConnection = class extends NativePeerConnection {
        constructor(...args) {
            super(...args);
            this.addEventListener("connectionstatechange", () => {
                if (this.connectionState === "connected") {
                    // NOTE: Firefox can leave getStats pending if the peer closes before
                    // it settles. Capture the selected transport before yielding to cleanup.
                    const transport = this.getSenders().find(
                        (sender) => sender.transport
                    )?.transport;
                    const protocol =
                        transport?.iceTransport.getSelectedCandidatePair()?.local.protocol;
                    protocols.push(protocol ?? "unknown");
                }
            });
        }
    };
}
