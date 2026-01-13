import js from "@eslint/js";

export default [
  {
    ignores: ["node_modules/**", "dist/**", "coverage/**", "tmp/**", "eslint.config.mjs"],
  },
  js.configs.recommended,
  {
    languageOptions: {
      ecmaVersion: "latest",
      sourceType: "commonjs",
      globals: {
        console: "readonly",
        process: "readonly",
        module: "readonly",
        require: "readonly",
        exports: "readonly",
        __dirname: "readonly",
        __filename: "readonly",
        Buffer: "readonly",
        setInterval: "readonly",
        clearInterval: "readonly",
        setTimeout: "readonly",
        clearTimeout: "readonly",
        setImmediate: "readonly",
        clearImmediate: "readonly",
      },
    },
    rules: {
      "no-unused-vars": ["warn", { argsIgnorePattern: "^_", varsIgnorePattern: "^_", caughtErrorsIgnorePattern: "^_" }],
      semi: ["error", "never"],
      quotes: ["error", "single"],
      indent: ["error", 2],
    },
  },
];
