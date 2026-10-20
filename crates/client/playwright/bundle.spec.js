import { expect, test } from "@playwright/test";

test("released bundle embeds Wasm and exposes only the public runtime API", async ({ page }) => {
    await page.goto("/playwright/fixtures/harness.html");

    const result = await page.evaluate(async () => {
        const fetchCalls = [];
        const originalFetch = globalThis.fetch;
        globalThis.fetch = (...args) => {
            fetchCalls.push(args.map(String));
            return Promise.reject(new Error("unexpected bundle fetch"));
        };
        try {
            const bundle = await import("/dist/odoo_sfu.js");
            const client = new bundle.SfuClient();
            return {
                exports: Object.keys(bundle).sort(),
                fetchCalls,
                initialState: {
                    availableFeatures: client.availableFeatures,
                    errors: client.errors,
                    recordingState: client.recordingState,
                    state: client.state
                }
            };
        } finally {
            globalThis.fetch = originalFetch;
        }
    });

    expect(result).toEqual({
        exports: ["CLIENT_UPDATE", "SFU_CLIENT_STATE", "SfuClient", "__info__"],
        fetchCalls: [],
        initialState: {
            availableFeatures: {
                rtc: false,
                recording: { audio: false, video: false, transcription: false }
            },
            errors: [],
            recordingState: { audio: false, video: false, transcription: false },
            state: "disconnected"
        }
    });
});
