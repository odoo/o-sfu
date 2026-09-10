import assert from "node:assert/strict";
import test from "node:test";
import { cameraSubscriptionRid } from "../playwright/live_server_helpers.mjs";

test("cameraSubscriptionRid reports selected encoding instead of layout intent", async (t) => {
    const subscription = {
        producerUserId: 1,
        streamId: "camera",
        state: "active",
        sourceId: "camera-source",
        layoutRole: "featured",
        selection: { selectedRid: null }
    };
    t.mock.method(globalThis, "fetch", async () =>
        Response.json({
            users: [{ userId: 2, subscriptions: [subscription] }],
            sources: [
                {
                    sourceId: "camera-source",
                    encodings: [{ policyRole: "featured", rid: "hi" }]
                }
            ]
        })
    );
    const query = { consumerSessionId: 2, producerSessionId: 1, roomId: "room" };
    assert.equal(await cameraSubscriptionRid(query), null);
    subscription.selection.selectedRid = "lo";
    assert.equal(await cameraSubscriptionRid(query), "lo");
    subscription.state = "inactive";
    assert.equal(await cameraSubscriptionRid(query), null);
});
