# polaris-trade

Visibility: PUBLIC (OSS). Every crate is MIT OR Apache-2.0; no dependency on a private repository.

# Before starting work

- Run `lat locate` to find sections relevant to your task. Read them to understand the design intent before writing code.
- Run `lat expand` on user prompts to expand any `[[refs]]`: this resolves section names to file locations and provides context.

# Post-task checklist (REQUIRED, do not skip)

After EVERY task, before responding to the user:

- [ ] Update `lat.md/` if you added or changed any functionality, architecture, tests, or behavior
- [ ] Run `lat check`: all wiki links and code refs must pass
- [ ] Do not skip these steps. Do not consider your task done until both are complete.

---

# What is lat.md?

This project uses [lat.md](https://www.npmjs.com/package/lat.md) to maintain a structured knowledge graph of its architecture, design decisions, and test specs in the `lat.md/` directory. It is a set of cross-linked markdown files that describe **what** this project does and **why**: the domain concepts, key design decisions, business logic, and test specifications. Use it to ground your work in the actual architecture rather than guessing.

# Commands

```bash
lat locate "Section Name"      # find a section by name (exact, fuzzy)
lat refs "file#Section"        # find what references a section
lat expand "user prompt text"  # expand [[refs]] to resolved locations
lat check                      # validate all links and code refs
```

Run `lat --help` when in doubt about available commands or options.

# Syntax primer

- **Section ids**: `lat.md/path/to/file#Heading#SubHeading`: full form uses project-root-relative path (e.g. `lat.md/tests/search#RAG Replay Tests`). Short form uses bare file name when unique (e.g. `search#RAG Replay Tests`, `cli#search#Indexing`).
- **Wiki links**: `[[target]]` or `[[target|alias]]`: cross-references between sections. Can also reference source code: `[[src/foo.ts#myFunction]]`.
- **Source code links**: Wiki links in `lat.md/` files can reference functions, classes, constants, and methods in TypeScript/JavaScript/Python/Rust/Go/C files. Use the full path: `[[src/config.ts#getConfigDir]]`, `[[src/server.ts#App#listen]]` (class method), `[[lib/utils.py#parse_args]]`, `[[src/lib.rs#Greeter#greet]]` (Rust impl method), `[[src/app.go#Greeter#Greet]]` (Go method), `[[src/app.h#Greeter]]` (C struct). `lat check` validates these exist.
- **Code refs**: `// @lat: [[section-id]]` (JS/TS/Rust/Go/C) or `# @lat: [[section-id]]` (Python): ties source code to concepts

# Test specs

Key tests can be described as sections in `lat.md/` files (e.g. `tests.md`). Add frontmatter to require that every leaf section is referenced by a `// @lat:` comment in test code:

```markdown
---
lat:
  require-code-mention: true
---
# Tests

Authentication and authorization test specifications.

## User login

Verify credential validation and error handling for the login endpoint.

### Rejects expired tokens
Tokens past their expiry timestamp are rejected with 401, even if otherwise valid.

### Handles missing password
Login request without a password field returns 400 with a descriptive error.
```

Every section MUST have a description: at least one sentence explaining what the test verifies and why. Empty sections with just a heading are not acceptable. (This is a specific case of the general leading paragraph rule below.)

Each test in code should reference its spec with exactly one comment placed next to the relevant test, not at the top of the file:

```rust
// @lat: [[tests#User login#Rejects expired tokens]]
#[test]
fn rejects_expired_tokens() {
    // ...
}

// @lat: [[tests#User login#Handles missing password]]
#[test]
fn handles_missing_password() {
    // ...
}
```

Do not duplicate refs. One `@lat:` comment per spec section, placed at the test that covers it. `lat check` will flag any spec section not covered by a code reference, and any code reference pointing to a nonexistent section.

# Section structure

Every section in `lat.md/` **must** have a leading paragraph: at least one sentence immediately after the heading, before any child headings or other block content. The first paragraph must be ≤250 characters (excluding `[[wiki link]]` content). This paragraph serves as the section's overview and is used in search results, command output, and RAG context: keeping it concise guarantees the section's essence is always captured.

```markdown
# Good Section

Brief overview of what this section documents and why it matters.

More detail can go in subsequent paragraphs, code blocks, or lists.

## Child heading

Details about this child topic.
```

```markdown
# Bad Section

## Child heading

Details about this child topic.
```

The second example is invalid because `Bad Section` has no leading paragraph. `lat check` validates this rule and reports errors for missing or overly long leading paragraphs.

---

# Memory And Search Protocol (MANDATORY)

All agents (Conductor, subagents, standalone) MUST follow this order before planning, implementation, review, investigation, writing code, or delegating research:

1. Call `agentmemory/memory_recall` with task, file, and module keywords when available.
2. Use `lat locate` or `lat expand` for architecture and design context.
3. Use Semble for semantic code search: `uvx --from "semble[mcp]" semble search "query" .`.
4. Use exposed `fff-mcp` MCP tools (`fff-grep`, `fff-find_files`, `fff-multi_grep`) for exact/file search.
5. Use `rust-analyzer` for Rust definitions, references, hover, diagnostics.
6. Fall back to regular search/read tools if preferred tools are missing, fail, or lack needed capability. State fallback reason.

Fallback rule: if preferred tool is missing, fails, or lacks needed capability, use regular tools and state reason in response or handoff.

Subagents should try exposed `fff-mcp` tools before fallback. If unavailable, use `rg` or `find` and state reason.

Conductor prompts must repeat memory/search protocol and fallback behavior for subagents.

This duplicates Claude's own global `~/.claude/CLAUDE.md` protocol on purpose: Copilot (VS Code and CLI) has no reliable user-global config inheritance, so this file is the only place Copilot will ever see it.

# Code Comment Rules (MANDATORY: WRITING, NOT REVIEW)

**Every agent writing ANY code, doc comment, or inline comment MUST follow these rules. Violations block merge.**

## Banned In All Comments

NEVER write any of these in code comments, doc comments (`///`, `//!`), or inline comments (`//`):

- Spec artifact ids: requirement, task and acceptance-criteria ids, i.e. `REQ`, `TASK` or `AC` followed by a dash and a number (gate regex `(REQ|TASK|AC)-`)
- `Phase N` or `Phase X`: phase references
- `milestone Y`: milestone references
- `work unit N`: work unit references
- Em dash (U+2014)

## Allowed

- Cross-crate references: `// see transport_core::pool`
- Short annotations: `TODO`, `FIXME`, `HACK`, `NOTE`, `WARNING`, `PERF`, `SECURITY`, `BUG`
- `// SAFETY:` blocks with invariant justification
- Inline `//` runs up to 4 lines when stating a non-obvious invariant or contract (never to restate code). Reviewers must not flag length alone within that bound.

## Why

Spec ids leak process into permanent code. Git log and PR capture process history. Comments must stand alone post-merge.

## Subagent Relay (MANDATORY)

**Conductor MUST include the full "Banned In All Comments" list above in EVERY subagent handoff packet.** Subagents do not auto-load project instruction files. The handoff packet is their only source of truth for comment rules.

Implement-subagent packet must include:

```
CODE COMMENT RULES (MANDATORY: DO NOT VIOLATE):
NEVER write spec ids (REQ, TASK or AC followed by a dash and a number), Phase N, milestone Y, work unit N, or em dash (U+2014) in any code comment, doc comment, or inline comment.
Allowed: cross-crate refs (// see crate::module), TODO, FIXME, HACK, NOTE, WARNING, PERF, SECURITY, BUG, SAFETY.

CODE SIMPLICITY RULES (MANDATORY):
Smallest change that meets the ask; no speculative abstraction (trait for one impl, generic for one type, builder for two fields, config knob for one value).
No backwards compatibility unless the user asked: no #[deprecated] shims, _v2 twins, old-name re-exports, dual code paths, migration adapters. Change the signature and fix every call site.
No slop: dead code, unused params/imports, #[allow] to silence, .clone() to dodge a borrow, unwrap()/expect() on a lib path, bool-param flags, Box<dyn> where a generic fits, TODO placeholder instead of finishing, catch-all error swallow.
```

Code-review-subagent packet must include:

```
CODE COMMENT AUDIT (MANDATORY):
Flag every spec id (REQ, TASK or AC followed by a dash and a number), Phase N, milestone Y, work unit N, and em dash (U+2014) found in code comments. Any hit = NEEDS_REVISION.

CODE SIMPLICITY AUDIT (MANDATORY):
Flag speculative abstraction, unrequested backwards-compat shims (#[deprecated], _v2 twins, old-name re-exports, dual code paths), and slop (dead code, unused params, #[allow] to silence, borrow-dodging .clone(), lib-path unwrap(), bool-param flags, Box<dyn> where a generic fits, TODO placeholders, swallowed errors). Any hit = NEEDS_REVISION.
```

# Logging Rules (MANDATORY: libraries emit, binaries subscribe)

Every crate logs through the [`tracing`](https://docs.rs/tracing) crate. Libraries emit events; only binaries install a subscriber. No `println!`/`eprintln!` in library code.

## Level meanings

| Level | Use for |
| ----- | ------- |
| `error` | an operation failed and the caller loses data or a connection; a human should look |
| `warn`  | degraded but continuing: a recoverable fault, a fallback taken, a gap detected |
| `info`  | coarse lifecycle: session start/end, reconnect, config resolved. Not per message |
| `debug` | detailed flow for diagnosis: re-request ticks, retry cadence, state transitions |
| `trace` | firehose, per item; off in every normal build |

## Libraries

- Emit `tracing::{error,warn,info,debug,trace}!` events only. Never install a subscriber.
- No spans on the hot path: a span allocates and takes a dispatcher lock even when no subscriber is attached. Use plain events.
- No per-message events. Log state transitions (gap detected, reconnect, session end), never once per packet/row/message. A per-message event floods and defeats filtering.
- Prefer structured fields over interpolation: `tracing::warn!(stream, %err, "...")`, not a preformatted string. Fields are filterable and become OTLP attributes for free.
- Depend on `tracing` unconditionally when the crate has something to log. Pure-decode crates that never log add no dependency.

## Binaries

- Install exactly one subscriber, once, at startup, before any work begins.
- Honor `RUST_LOG`. A plain binary installs a `tracing_subscriber::fmt` subscriber on stderr with an env filter defaulting to `warn`.
- Consumers of this workspace's libraries install their own subscriber; the libraries stay silent until they do (standard Rust).

## Lint enforcement

Every library crate root (`lib.rs`) carries, as its first inner attribute:

```rust
#![cfg_attr(not(test), deny(clippy::print_stdout, clippy::print_stderr))]
```

The `cfg_attr(not(test), ...)` form leaves unit-test code free to print. Restriction lints are off by default, so this attribute is what enables the ban; lefthook `pre-commit` and CI `-D warnings` then enforce it. Binaries, examples, benches, and integration tests are separate targets and are unaffected.

## Sanctioned print exceptions

`println!`/`eprintln!` are allowed only in:

- binary CLI product output (`main.rs` and its bin-target modules), the program's actual stdout product;
- a binary's pre-subscriber-init usage or fatal-startup `eprintln!` (before any subscriber exists);
- `build.rs` `cargo:` directives.

# Post-Task Checklist (MANDATORY: ALL AGENTS, RUN BEFORE REPORTING DONE)

`<features>` below is the explicit feature list in `lefthook.yml` (clippy and nextest commands). The same list is the CI features job's `workspace-args`; the two must stay in step, and the list grows as crates join the workspace. Never enable every feature at once: the DPDK driver feature cannot build on macOS.

1. `cargo clippy --workspace --all-targets --features <features> -- -D warnings`: must pass
2. `cargo nextest run --workspace --features <features>`: must pass
3. `cargo test --doc --workspace`: must pass
4. `lat check`: must pass
5. `rg -n '(REQ|TASK|AC)-' -g '*.rs' crates`: must be empty
6. `rg -n '\x{2014}' -g '*.rs' crates`: must be empty
7. Update spec progress in `specs/<task-slug>/tasks.md` if any task changed state
8. Update `lat.md/` if any module/type/function was added, removed, or renamed

If any step fails: fix it. Do NOT skip. Do NOT report done until all pass.

# Commit Message Convention

Use Conventional Commits: `type(scope): subject`.

- Always include a scope for `feat`, `fix`, `refactor`, and `perf` commits.
- Valid types: `build`, `chore`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `revert`, `style`, `test`.
- Keep header length at 100 characters or less.
- Use lowercase subject style, not start-case, PascalCase, or upper-case.
- Do not suggest merge commits.

Two enforcement layers:

- **Local**: `lefthook.yml` runs `convco check` on `commit-msg` and enforces `<type>/<slug>` branch names on `pre-commit`. Install once per clone via `lefthook install` (needs `lefthook` + `convco` on PATH).
- **CI**: `pr-title` workflow (`amannn/action-semantic-pull-request`) blocks non-conforming PR titles at merge time.

Local hooks are the fast feedback loop; CI is the backstop for anyone who bypassed them.

# Unit Test Rules

**Unit tests MUST NOT connect to external services**: databases, APIs, or network resources beyond localhost.

- **No real service connections in unit tests**: no DB connections, HTTP clients, external APIs.
- **Use `#[ignore]` for integration tests**: tests requiring real external services or privileged kernel paths must be annotated with `#[ignore]` and only run via `cargo nextest run --run-ignored ignored-only`.
- **Use mockall for mocking**: prefer the [mockall](https://docs.rs/mockall/latest/mockall/) crate for mock implementations of traits and functions.
- **Localhost mock servers acceptable**: tests that bind to `127.0.0.1:0` with ephemeral ports and implement mock protocol servers in-process are acceptable.
- **E2E tests are exempt**: only apply these rules to unit/integration tests, not when the user explicitly asks for e2e tests.
- Run unit tests using `cargo nextest` for faster feedback loops.

When writing new tests:

1. Default to pure unit tests using test doubles/mocks.
2. Add mockall to `[workspace.dependencies]` in the root `Cargo.toml`, then to the crate's dev-dependencies: `mockall = { workspace = true }`.
3. Gate any external-service or privileged test with `#[ignore]`.
4. Document in test comments when the ignored run is required.
5. **Test behavior, not language features**: see the Useless Test Ban below; a test must be able to fail from a project bug.

---

# Code Growth Discipline (MANDATORY pre-write gate)

Apply before writing, not during review.

- New code: sketch module layout first. One concern per file; name the seam (trait impl, backend, config vs logic). Projected >600 non-test LOC or a second concern: start as mod dir, never "split later".
- Feature work: if the result would be too coupled or push a file past ~800 non-test LOC, land a split-first refactor commit (pure moves + `pub use` re-exports, gates green, zero behavior diff), then implement. Two commits, never one mixed.
- Cohesive single impl block: fine at any size. Trigger is mixed concerns, not LOC.
- Test mod >40% of file: sibling `tests.rs`.
- Split mechanics: inherent impls split across files freely; one trait impl per file; child modules see parent-private fields, siblings need `pub(super)`; move by exact line-range extraction, never retype, never uniform-dedent (raw-string fixtures corrupt silently); keep external paths via `pub use`.
- Hot-path inline: a new concrete (non-generic) fn on a per-message/per-burst path reachable across a crate boundary gets `#[inline]` at write time. Generic fns: nothing, MIR export covers them. `#[inline(always)]`: only with a measured delta cited.
- Validation honesty: a gate must compile the code it claims to validate. Feature-gated crates get `--features` in every validation command, spec, and CI caller.

# Useless Test Ban (MANDATORY)

A test must be able to fail from a project bug. Never write:

- Derive/stdlib restatement: thiserror `#[error]` string equality, `#[derive(Default)]` variant choice, `#[from]` conversion in isolation, derived Clone/Debug/Eq works, plain serde roundtrip with no custom impl, `Option::is_some` after `Some`.
- Field echo: constructor-stores-argument, struct-literal readback, a constant restated from the definition.
- Duplicate coverage: the branch is already covered; name the covering test and skip.
- Mock-call-count-only asserts with no observable output checked.
- False confidence: assertion weaker than the test name claims. Verify the actual contract via readback, or delete.
- Copy-pasted test doubles/encoders: they live once in `tests/support/`, per crate.
- Per-impl re-proof of a generic contract: one `assert_contract<T: Trait>` helper, called per implementation.

Keep (contract gates, not useless): wire/on-disk layout pins (size, alignment, discriminant, padding, format magic), codegen drift gates, determinism/replay gates, `Display` tests with a documented ops/log-matching rationale.

Review rule: any new test matching a banned class = NEEDS_REVISION. Relay packets for test-writing subagents must include this ban list.

# Code Simplicity Discipline (MANDATORY pre-write gate)

Apply before writing, not during review.

- No overcomplicated or overlong code. Smallest change that meets the ask. No speculative abstraction: no trait for one impl, generic for one type, builder for two fields, config knob for one value, indirection layer "for later". One concern per fn; if it needs section comments, split it. Plain `match`/`if`/iterator chain beats layered helpers.
- No backwards compatibility unless the user asks. Rename or change the signature and fix every call site. No `#[deprecated]` shims, `_v2` twins, old-name re-exports, dual code paths, "legacy" flags, or migration adapters. Breaking inside the workspace is fine; git history holds the old shape.
- No slop, no anti-patterns. Slop: dead code, unused params/imports, `#[allow]` to silence instead of fix, `.clone()` to dodge a borrow, `unwrap()`/`expect()` on a lib path, stringly-typed state, bool-param flags, `Box<dyn>` where a generic fits, newtype with no invariant, re-validating already-typed input, comment narrating code, TODO placeholder instead of finishing, catch-all error swallow. Delete on sight in touched code.

Review rule: any of the above in a diff = NEEDS_REVISION. Relay packets carry this section.

# Transports

Crates live under `crates/transports/` (`core`, `socket`, `io-uring`, `afxdp`, `dpdk`) and `crates/clients/` (`moldudp`, `soupbintcp`).

- Architecture docs go in `lat.md/transports/` and `lat.md/clients/`.
- Linux-only crates (io_uring, AF_XDP, DPDK) are checked through a Linux container built from `scripts/kernel-proof/Dockerfile`.
- Every crate containing `unsafe` carries `#![warn(clippy::undocumented_unsafe_blocks)]`; `-D warnings` makes it an error.
- Every library root carries the print-lint header from Logging Rules.
