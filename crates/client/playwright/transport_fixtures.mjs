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
            const protocols = await page
                .evaluate(async () => {
                    await globalThis.__selectedCandidateProtocolsSettled?.();
                    return globalThis.__selectedCandidateProtocols ?? [];
                })
                .catch(() => []);
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

/**
 * Records, for each peer connection that connects into `globalThis.__selectedCandidateProtocols`.
 * `globalThis.__selectedCandidateProtocolsSettled()` waits for those records to be complete.
 */
function recordSelectedProtocols() {
    const protocols = [];
    const pending = [];
    globalThis.__selectedCandidateProtocols = protocols;
    // Resolves once every peer connection that connected so far has reported its protocol.
    globalThis.__selectedCandidateProtocolsSettled = () => Promise.all(pending);

    async function selectedProtocol(peerConnection) {
        const report = await peerConnection.getStats().catch(() => null);
        if (!report) {
            return null;
        }
        const stats = Array.from(report.values());
        // Chrome names the selected pair on its transport, Firefox flags the pair itself.
        const transport = stats.find((stat) => stat.type === "transport");
        const selectedPair = transport?.selectedCandidatePairId
            ? report.get(transport.selectedCandidatePairId)
            : stats.find((stat) => stat.type === "candidate-pair" && stat.selected);
        const localCandidate = selectedPair ? report.get(selectedPair.localCandidateId) : null;
        return localCandidate?.protocol ?? null;
    }

    async function recordProtocol(peerConnection) {
        for (let attempt = 0; attempt < 20; attempt += 1) {
            if (peerConnection.connectionState === "closed") {
                return;
            }
            const protocol = await selectedProtocol(peerConnection);
            if (protocol) {
                protocols.push(protocol);
                return;
            }
            await new Promise((resolve) => setTimeout(resolve, 100));
        }
        protocols.push("unknown");
    }

    const NativePeerConnection = globalThis.RTCPeerConnection;
    globalThis.RTCPeerConnection = class extends NativePeerConnection {
        constructor(...args) {
            super(...args);
            this.addEventListener("connectionstatechange", () => {
                if (this.connectionState === "connected") {
                    pending.push(recordProtocol(this));
                }
            });
        }
    };
}
