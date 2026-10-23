# Signaling and UDP proxy

Loopback-only Rama proxy. Examples use the Chaos
[`startProxy` fixture](../../crates/client/chaos/signaling_proxy_helpers.mjs),
which starts the process and handles teardown.

## Features

- Signaling: hold, drop, corrupt, disconnect or send a close code. Match
  `direction`, message type `t` and optional `response`. One fault per process.
- UDP: select `rtp`, `rtcp`, `stun` or `dtls` in either direction with
  per-source-flow `skip`, `every` and `limit` counters.
- Composition: combine signaling and UDP rules. UDP effects apply in order
  across matching rules: drop, delay and duplicate. Selective delay reorders packets.
- Timing: activate immediately or use `deferred: true` and `arm()`.
  Signaling holds wait for `release()`.
- Diagnostics: connection events, relay addresses, packet counters and queue
  gauges through `waitForEvent()` and `stats()`, without payload logging.

## Hold the initial offer

Install the rule before connecting. `blocked` confirms the offer has not reached
the browser. A hold stalls that WebSocket direction, including batched messages.

```js
const proxy = await startProxy({
    rule: { direction: "server_to_client", t: "offer", response: false, action: "hold" }
});
await connectPeer(page, { channelUuid, jwt, url: proxy.url });
const blocked = await proxy.waitForEvent("blocked");
// Assert or change browser state while the offer is held.
await proxy.release(blocked.connectionId);
```

`release()` acknowledges the hold ending. Assert browser state to confirm delivery.

## Combine UDP effects

Skip three server RTP packets, then select every second packet, up to four times
per source flow. Each selected packet is delivered after 150 ms and again after 160 ms.

```js
const proxy = await startProxy({
    deferred: true,
    udp: { rules: [{
        direction: "server_to_client", packet: "rtp",
        skip: 3, every: 2, limit: 4,
        effects: [{ delay: { ms: 150 } }, { duplicate: { copies: 1, spacing_ms: 10 } }]
    }] }
});
// Connect through proxy.url and start media before arming.
await proxy.arm();
await expect.poll(async () => (await proxy.stats()).udp.rules[0]).toBeGreaterThan(0);
```

Selectors start counting when armed. Omitted `skip` and `every` default to 0 and 1.
For handshake loss, use `packet: "stun"` or `"dtls"`, `effects: ["drop"]` and
omit `deferred` so the rule is active before connecting. `udp: { rules: [] }`
relays media without faults.

UDP accepts only `127.0.0.1` host candidates and removes TCP candidates.
Resource overflow closes the affected connection. See [policy limits](src/udp/policy.rs)
and [relay limits](src/udp/relay.rs).
