# Odoo SFU Dev Deployment Guide

This document explains how to set up the SFU server for Odoo development. It is not intended as a general deployment guide. For general deployment instructions, see [DEPLOYMENT](/DEPLOYMENT.md).

## Prerequisites

1. Install the [Rust toolchain](https://rust-lang.org/tools/install/):
    ```bash
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    ```

2. Install [`wasm-pack`](https://wasm-bindgen.github.io/wasm-pack/installer/) (required to build the SFU Client):
    ```bash
    cargo install wasm-pack
    ```

> [!TIP]
> * [**`cargo`**](https://doc.rust-lang.org/cargo/index.html) is the Rust package manager, which is downloaded automatically with the rest of the toolchain.
> * **`wasm`** is short for WebAssembly. Part of the SFU Client is written in Rust and compiled to `wasm` using `wasm-pack`.

---

## Build The SFU Server

1. Copy the repository URL from GitHub's Code menu and clone it:
    ```bash
    git clone <repository-url> o-sfu
    ```

2. Navigate to the root of the repository and run the build command:
    ```bash
    cd o-sfu
    cargo build -p o-sfu
    ```

---

## Build The SFU Client

1. Go to the root of the client crate:
    ```bash
    cd crates/client
    ```

2. Install dependencies using npm:
    ```bash
    npm ci
    ```
> [!TIP]
> `ci` stands for **c**lean **i**nstall, ensuring you get exactly what is in the lockfile.

3. Build the SFU Client:
    ```bash
    npm run build:odoo
    ```

4. Move the generated bundle (`crates/client/dist/odoo_sfu.js`) to the corresponding directory in your Odoo source code (`community/addons/mail/static/lib/odoo_sfu`).

> [!TIP]
> To enable full tooling capabilities, also move `crates/client/dist/odoo_sfu.d.ts` so your editor's LSP recognizes the SFU client's structure. You can then use JSDoc to enable typing:
> ```js
> /** @typedef {typeof import("./odoo_sfu")} SfuModule */
> /** @typedef {import("./odoo_sfu").SFU_CLIENT_STATE} SfuClientStateCatalog */
> ```

---

## Configure The Environment

The SFU uses meaningful defaults for almost everything. However, you must provide two specific configurations: `AUTH_KEY` and `ANNOUNCED_IP`. 

### 1. Generate your `AUTH_KEY`
This key authenticates client requests to the SFU server. It must be valid base64 data that decodes to at least 32 bytes. Generate it from cryptographically secure randomness with:
```bash
openssl rand -base64 32
```

### 2. Determine the `ANNOUNCED_IP`

This is the IP address where the SFU can be reached. It must be routable from the client's perspective and cannot be the loopback address (`127.0.0.1` aka localhost).

For a dev environment, you will likely use your machine's local network address. You can retrieve it using the following commands:

Linux:
```bash
ip route get 1.1.1.1 | awk '{for(i=1;i<=NF;i++) if($i=="src") print $(i+1); exit}'
```

macOS:
```bash
ipconfig getifaddr "$(route -n get default | awk '/interface:/{print $2; exit}')"
```

> [!IMPORTANT]
> Your machine's local address can change over time. You may want to avoid hardcoding it, otherwise you will have to update it manually.

### 3. Apply the Configurations

Now that you have your `AUTH_KEY` and `ANNOUNCED_IP`, you need to configure both the SFU server and Odoo:

1. For the SFU Server: Define `AUTH_KEY` and `ANNOUNCED_IP` as environment variables before running the server.

2. For Odoo:
    - Navigate to **Settings**, locate the Discuss section, and check both **Custom Call Servers** and **Custom SFU Server**.
    - Under **Custom SFU Server**, set the URL to `http://localhost:8070` (the default value of the `BIND_ADDRESS` environment variable).
    - Set the **Key** field to the value of your `AUTH_KEY`.

> [!TIP]
> Alternatively, you can provide these variables directly to Odoo via the environment variables `ODOO_SFU_KEY` and `ODOO_SFU_URL`.

> [!NOTE]
> By default, the SFU binds its HTTP and WebSocket server to `0.0.0.0:8070`. Override it with a full socket address via `BIND_ADDRESS` (for example, `BIND_ADDRESS=127.0.0.1:9000`).

---

## Run The Whole Thing

1. Start the SFU server (ensure your environment variables are set):
    ```bash
    cargo run -p o-sfu
    ```
2. Start Odoo

> [!TIP]
> Set the `RUST_LOG` environment variable for more detailed debug logs.
> ```bash
> RUST_LOG=debug cargo run -p o-sfu
> ```

### Testing the Setup

To test the deployment, start a call in Discuss with **at least three members**.

You can add participants by joining as guests through the invite link (found via the **Add People** button). In the channel's settings under the **Privacy** tab, ensure that the **Authorized Group** field is empty; otherwise, guests will not be able to join.

> [!IMPORTANT]
> With fewer than three participants, communication is peer-to-peer (P2P), and the SFU server is bypassed entirely.

#### How to verify Odoo is using the SFU:
Enable debug mode, start your call (with 3+ people), and hover over your profile picture in the call UI. Click the arrow that appears and check the connection type:
- If it says `server`, your setup is working.
- If it says `p2p`, something went wrong. (Double-check that you have at least three active participants).

> [!TIP]
> Press <kbd>Ctrl</kbd> + <kbd>K</kbd>, then type "debug" to quickly switch to debug mode

---

## Troubleshooting

If you have carefully followed the instructions above and the connection is still failing, check the following:
- [ ] Ensure your browser allows insecure connections (if you are running HTTP on local network IPs).
- [ ] Ensure you don't have a browser extension (e.g., Port Authority) blocking port scanning or WebRTC connections.
- [ ] Ensure you aren't behind a VPN.
