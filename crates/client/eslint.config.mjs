import js from "@eslint/js";
import globals from "globals";
import n from "eslint-plugin-n";
import tseslint from "typescript-eslint";
import prettier from "eslint-plugin-prettier/recommended";
import { defineConfig } from "eslint/config";

export default defineConfig([
    js.configs.recommended,
    ...tseslint.configs.recommended,
    prettier,
    {
        files: ["src/**/*.ts"],
        languageOptions: {
            globals: {
                ...globals.browser
            }
        },
        plugins: {
            n
        },
        // Rules based on what is found in other odoo JS/TS codebases
        rules: {
            "n/no-unsupported-features/es-syntax": "off",
            "n/no-missing-import": "off",
            "comma-dangle": "off",
            "no-console": "error",
            "no-undef": "off",
            "no-restricted-globals": ["error", "event", "self"],
            "no-const-assign": ["error"],
            "no-debugger": ["error"],
            "no-dupe-class-members": ["error"],
            "no-dupe-keys": ["error"],
            "no-dupe-args": ["error"],
            "no-dupe-else-if": ["error"],
            "no-unsafe-negation": ["error"],
            "no-duplicate-imports": ["error"],
            "valid-typeof": ["error"],
            "@typescript-eslint/no-unused-vars": [
                "error",
                {
                    vars: "all",
                    args: "none",
                    ignoreRestSiblings: false,
                    caughtErrors: "all"
                }
            ],
            curly: ["error", "all"],
            "no-restricted-syntax": ["error", "PrivateIdentifier"],
            "prefer-const": [
                "error",
                {
                    destructuring: "all",
                    ignoreReadBeforeAssign: true
                }
            ]
        }
    },
    {
        files: [
            "scripts/**/*.mjs",
            "test/**/*.mjs",
            "playwright/**/*.js",
            "playwright/**/*.mjs",
            "eslint.config.mjs"
        ],
        languageOptions: {
            globals: {
                ...globals.browser,
                ...globals.node
            }
        }
    },
    {
        files: ["src/sfu_client.ts", "src/internals/peer_session.ts"],
        rules: {
            "no-restricted-imports": [
                "error",
                {
                    paths: [
                        {
                            name: "./protocol_contract.js",
                            message:
                                "SfuClient must use BrowserRuntime intents instead of protocol bindings"
                        },
                        {
                            allowImportNames: ["NegotiationKind"],
                            name: "../protocol_contract.js",
                            message:
                                "PeerSession must only share the negotiation result tag with the protocol contract"
                        }
                    ]
                }
            ]
        }
    },
    {
        files: ["src/sfu_client.ts"],
        rules: {
            // The interface adds typed EventTarget overloads to the same class symbol.
            "@typescript-eslint/no-unsafe-declaration-merging": "off"
        }
    }
]);
