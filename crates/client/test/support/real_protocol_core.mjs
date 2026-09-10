import { readFile } from "node:fs/promises";

import init, { ProtocolCoreWasm } from "../../generated/o_sfu_protocol.js";
import {
    configureDefaultWasmProtocolCoreProvider,
    createProtocolCore
} from "../../dist/protocol_contract.js";

await init({
    module_or_path: await readFile(
        new URL("../../generated/o_sfu_protocol_bg.wasm", import.meta.url)
    )
});
configureDefaultWasmProtocolCoreProvider(() => new ProtocolCoreWasm());

export { createProtocolCore };
