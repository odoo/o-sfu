import { defineConfig } from "@playwright/test";

const CHROMIUM = { browserName: "chromium" };
const FIREFOX = {
    browserName: "firefox",
    launchOptions: {
        // Firefox needs camera permission to gather loopback candidates
        // for the synthetic canvas fixtures.
        firefoxUserPrefs: {
            "media.peerconnection.ice.loopback": true,
            "permissions.default.camera": 1
        }
    }
};

export default defineConfig({
    testDir: "./playwright",
    timeout: 30_000,
    outputDir: "./output/playwright/results",
    reporter: [["list"]],
    use: {
        baseURL: "http://127.0.0.1:4173",
        headless: true,
        screenshot: "only-on-failure",
        trace: "retain-on-failure",
        video: "retain-on-failure"
    },
    projects: [
        { name: "chromium", use: CHROMIUM },
        { name: "firefox", use: FIREFOX },
        {
            name: "chromium-tcp",
            testIgnore: "bundle.spec.js",
            use: { ...CHROMIUM, transport: "tcp" }
        },
        {
            name: "firefox-tcp",
            testIgnore: "bundle.spec.js",
            use: { ...FIREFOX, transport: "tcp" }
        }
    ],
    webServer: [
        {
            command: "node ./playwright/serve_client.mjs",
            reuseExistingServer: true,
            timeout: 10_000,
            url: "http://127.0.0.1:4173/playwright/fixtures/harness.html"
        },
        {
            command:
                "AUTH_KEY=u6bsUQEWrHdKIuYplirRnbBmLbrKV5PxKG7DtA71mng= TCP_BIND_ADDRESS=127.0.0.1:58100 HTTP_INTERFACE=127.0.0.1:18080 ANNOUNCED_IP=127.0.0.1 RTC_MIN_PORT=58000 RTC_MAX_PORT=58031 cargo run --quiet --manifest-path ../../Cargo.toml -p o-sfu",
            reuseExistingServer: true,
            timeout: 360_000,
            url: "http://127.0.0.1:18080/v1/noop"
        }
    ]
});
