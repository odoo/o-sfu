import { createServer } from "node:http";
import { readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const PORT = Number(process.env.O_SFU_CLIENT_TEST_PORT ?? 4173);
const CLIENT_ROOT = path.resolve(fileURLToPath(new URL("../", import.meta.url)));
const MIME_TYPE = {
    ".css": "text/css; charset=utf-8",
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".json": "application/json; charset=utf-8",
    ".mjs": "text/javascript; charset=utf-8",
    ".svg": "image/svg+xml",
    ".wasm": "application/wasm"
};

const server = createServer(async (request, response) => {
    try {
        const filePath = resolveRequestPath(request.url ?? "/");
        const body = await readFile(filePath);
        response.writeHead(200, {
            "Content-Type": MIME_TYPE[path.extname(filePath)] ?? "application/octet-stream",
            "Cache-Control": "no-store"
        });
        response.end(body);
    } catch (error) {
        const statusCode = isNotFoundError(error) ? 404 : 500;
        response.writeHead(statusCode, {
            "Content-Type": "text/plain; charset=utf-8"
        });
        response.end(statusCode === 404 ? "Not found" : "Internal server error");
    }
});

server.listen(PORT, "127.0.0.1");

function resolveRequestPath(requestUrl) {
    const pathname = new URL(requestUrl, `http://127.0.0.1:${PORT}`).pathname;
    const relativePath =
        pathname === "/" ? "playwright/fixtures/harness.html" : pathname.replace(/^\/+/, "");
    const filePath = path.resolve(CLIENT_ROOT, relativePath);
    const clientRootWithSeparator = `${CLIENT_ROOT}${path.sep}`;
    if (filePath !== CLIENT_ROOT && !filePath.startsWith(clientRootWithSeparator)) {
        const error = new Error("Path escapes client root");
        error.code = "ENOENT";
        throw error;
    }
    return filePath;
}

function isNotFoundError(error) {
    return (
        typeof error === "object" && error !== null && "code" in error && error.code === "ENOENT"
    );
}
