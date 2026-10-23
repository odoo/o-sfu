import { defineConfig } from "@playwright/test";
import browserConfig from "./playwright.config.mjs";

export default defineConfig({
    ...browserConfig,
    testDir: "./chaos",
    outputDir: "./output/chaos/results",
    reporter: [["list"], ["html", { outputFolder: "./output/chaos/report", open: "never" }]]
});
