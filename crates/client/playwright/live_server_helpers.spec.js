import { once } from "node:events";
import { createServer } from "node:net";

import { expect, test } from "@playwright/test";
import { spawnLiveServer } from "./live_server_helpers.mjs";

test("dedicated server reports a TCP bind failure before the readiness timeout", async ({
    browserName
}) => {
    test.setTimeout(10_000);
    const rtcMinPort = browserName === "firefox" ? 28552 : 28520;
    const rtcMaxPort = rtcMinPort + 31;
    const portOffset = test.info().project.use.transport === "tcp" ? 1000 : 0;
    const blocker = createServer();
    blocker.listen(rtcMaxPort + 1 + portOffset, "127.0.0.1");
    await once(blocker, "listening");
    try {
        await expect(
            spawnLiveServer({
                bindPort: browserName === "firefox" ? 18092 : 18091,
                rtcMinPort,
                rtcMaxPort
            })
        ).rejects.toThrow(/exited with code 1[\s\S]*TCP acceptor: address in use/i);
    } finally {
        blocker.close();
        await once(blocker, "close");
    }
});
