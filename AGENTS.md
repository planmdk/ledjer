# Agent Configuration and Project Structure

This document outlines the expected structure and agent configuration for `tools.deps`-based Clojure projects.

## Directory Structure

All source code must follow this structure:

```
project-root/
├── src/
│   └── evm/
│       ├── core.clj
│       └── other-source.clj
├── test/
│   └── evm/
│       ├── bindings.clj
│       └── another-test.clj
├── deps.edn
└── README.md
```

## Test Files

- All test files must be placed in `test/` (under project root)
- Test namespaces must follow the pattern: `evm.test.some-feature`
- Example: `test/evm/bindings.clj` => `(ns evm.test.bindings)`
- Do NOT place tests inside `src/evm/` directly

## Running Tests

- Execute tests with:
  ```bash
  clj -X:test
  ```
- This runs all tests in the project using `clojure.test`
- No need for `-M:ns` or manual test loading

## Agent Rules

- Agents run locally using `clj`, not `lein`
- Always use `tools.deps` to manage dependencies, not `project.clj`
- Test files are expected in `test/` to maintain clarity and maintainability

## Why?

- `tools.deps` projects expect tests in `test/` by convention
- `clj -X:test` is the standard command to run all tests
- This matches how `test-runner` and `clojure.test` expect things to be organized

Updated: 2025-04-05

🤖 Generated with [eca](https://eca.dev)

Co-Authored-By: eca <noreply@eca.dev>