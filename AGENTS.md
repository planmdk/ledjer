# Agent Configuration and Project Structure

Ledjer is a single repository containing multiple `tools.deps` sub-projects, so
users can depend on just the core stores or add optional store backends.

## Directory Structure

```
ledjer/
├── deps.edn                 # dev-only: both sub-projects as :local/root
├── core/                    # io.github.planmdk/ledjer
│   ├── deps.edn
│   ├── src/dk/planm/
│   │   ├── ledjer.clj
│   │   └── ledjer/
│   │       ├── ...
│   │       └── store/memory.clj
│   └── test/dk/planm/ledjer/
├── umadb/                   # io.github.planmdk/ledjer-umadb
│   ├── deps.edn             # depends on core via {:local/root "../core"}
│   ├── src/dk/planm/ledjer/store/umadb.clj
│   └── test/dk/planm/ledjer/store/
└── README.md
```

- Each sub-project owns its `src/` and `test/`, and declares only the
  dependencies it needs. Third-party deps for a store (e.g. `deed-core`,
  `umadb-java-client`) belong in that store's sub-project, never in `core`.
- `core` must never depend on any other sub-project.
- Sub-projects keep the `dk.planm.ledjer.*` namespace prefix. Namespaces do not
  need to change when code moves between sub-projects.
- Sub-project `:paths` contain `src` only; `test` is added via the `:test` alias
  so tests never leak onto consumers' classpaths.
- The root `deps.edn` is for development only (REPL with everything on the
  classpath). It is not meant to be consumed as a library.

## Test Files

- Tests live in the `test/` directory of the sub-project they belong to.
- Test namespaces mirror the source namespace with a `-test` suffix:
  `core/test/dk/planm/ledjer/event_test.clj` => `(ns dk.planm.ledjer.event-test)`
- Do NOT place tests inside `src/`.
- Tests for a store backend that needs an external server (e.g. UmaDB) go in
  that backend's sub-project, never in `core`.

## Running Tests

- Core: from `core/`, run `clj -X:test`
- UmaDB: from `umadb/`, run `clj -X:test`
- From the repo root, `clj -X:test` runs the core tests (`:dirs ["core/test"]`).
- Uses `clojure.test` via `cognitect.test-runner`; no manual test loading.

## Consuming the Libraries

Both coordinates must point at the same git sha:

```clojure
;; core only
io.github.planmdk/ledjer {:git/url "https://github.com/planmdk/ledjer"
                          :git/sha "..." :deps/root "core"}
;; core + UmaDB (pulls in core automatically)
io.github.planmdk/ledjer-umadb {:git/url "https://github.com/planmdk/ledjer"
                                :git/sha "..." :deps/root "umadb"}
```

## Adding a New Store Backend

1. Create `<name>/deps.edn` with `io.github.planmdk/ledjer {:local/root "../core"}`
   plus the backend's own dependencies, `:paths ["src"]`, and a `:test` alias.
2. Put sources under `<name>/src/dk/planm/ledjer/store/`.
3. Register it in the root `deps.edn` as `io.github.planmdk/ledjer-<name>`
   with `{:local/root "<name>"}`, and add its `test` path to the `:nrepl` and
   `:test` aliases.

## Agent Rules

- Agents run locally using `clj`, not `lein`.
- Always use `tools.deps` to manage dependencies, not `project.clj`.
- Add a dependency to the sub-project that actually uses it, not to the root.
- If `clj` fails with a mise "No version is set for shim" error, use the
  installed binary under `~/.local/share/mise/installs/clojure/*/bin/clojure`.

## Why?

- Isolating optional backends keeps `core` lightweight and lets users opt in to
  heavy or native dependencies.
- One repo with one git sha keeps core and its backends in lockstep, with no
  version skew to manage and no release tooling needed.
- `:local/root` between sub-projects keeps development simple, and the layout
  can later be published as separate Clojars artifacts without restructuring.

Updated: 2026-10-15
