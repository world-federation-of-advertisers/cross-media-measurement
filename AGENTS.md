# WFA Cross-Media Measurement System

Privacy-preserving system for cross-publisher, cross-media ads measurement through secure multiparty computation. Kotlin primary, C++ for crypto, Protocol Buffers for APIs, Bazel build, gRPC services, Kubernetes deployments.

## Commands

```shell
# Run all tests
bazel test //src/test/...

# Build everything
bazel build //src/main/...

# Containerized build (when host glibc > 2.36)
tools/bazel-container test //src/test/...

# Format Kotlin
ktfmt --google-style <file>

# Format BUILD/Starlark
buildifier <file>

# Format C++/Proto
clang-format --style=Google <file>

# Update Maven lockfile
REPIN=1 bazel run @maven//:pin

# Update Bazel module lockfile
bazel mod deps --lockfile_mode=update
```

Use `bazelisk` (not `bazel` directly) to respect `.bazelversion`.

## Project Structure

```
src/main/kotlin/org/wfanet/measurement/
  kingdom/          # Central coordinator — measurement lifecycle, entity registration
  duchy/            # MPC computation nodes — crypto protocols, mill workers
  edpaggregator/    # Event Data Provider aggregation
  reporting/        # Report generation and delivery
  securecomputation/# TEE-based secure computation
  common/           # Shared utilities (used by sibling packages only)
  api/              # Public API definitions (v2alpha)
  system/           # Internal system API (v1alpha)

src/main/proto/     # Protobuf service and config definitions
src/main/cc/        # C++ crypto library
src/test/           # All tests (mirrors src/main/ structure)
imports/java/       # Bazel aliases for external deps (mirrors JVM package paths)
build/              # Build system config, platforms, variables
docs/               # Architecture, operations, code style guides
```

`common/` directories contain code shared across sibling packages under the same parent. `//foo/common` is used by `//foo/bar` and `//foo/baz`, never by `//bar`.

Test infrastructure reusable across packages lives in `testing` subpackages under `src/main/` (marked `testonly`), not under `src/test/`.

## Code Style

Full guide: [docs/code-style.md](docs/code-style.md)

**Kotlin** — Google Android Kotlin style with **2-space indent** (not 4). Format with `ktfmt --google-style`.
- Use Truth (`assertThat`) for test assertions, ProtoTruth for protobuf subjects
- Use `assertFailsWith` from `kotlin.test` for expected exceptions
- Prefer Kotlin DSL builders for protobuf construction
- Specify types explicitly except when obvious (constructors, factory functions)
- Catch `StatusException` from RPCs at the call site — never let gRPC statuses propagate through servers that are also clients

**C++** — Google C++ style. Don't rely on transitive includes.

**BUILD/Starlark** — Bazel BUILD and .bzl style guides. Never rely on transitive deps.

**Protobuf** — Google Protobuf style guide. Serialized row messages end in `Details`.

Bazel BUILD file conventions: [docs/bazel-build-standards.md](docs/bazel-build-standards.md)
API and protobuf design conventions: [docs/api-standards.md](docs/api-standards.md)
Cryptography and Tink usage: [docs/security-standards.md](docs/security-standards.md)

## Testing

Full guide: [docs/testing-standards.md](docs/testing-standards.md)

- Test the public API contract, not the implementation
- Never expose internal functionality just to test it directly
- Prefer fakes over mocks; don't overuse mocks
- Bias toward more, smaller test cases
- Tests go in `src/test/` mirroring the `src/main/` path

```kotlin
// Good: testing public API
assertThat(service.getMeasurement(request))
  .isEqualTo(expected)

// Bad: testing internal state
assertThat(service.internalCache.size())
  .isEqualTo(3)
```

## Git Workflow

Full guide: [docs/dev-standards.md](docs/dev-standards.md)

**Conventional Commits** — required format:

```
<type>[optional scope][!]: <imperative sentence>

[optional body]

[optional footers]
```

Types: `build`, `ci`, `docs`, `feat`, `fix`, `perf`, `refactor`, `test`

The description must be a **complete imperative sentence** that stands alone:
- Good: `fix: Increase timeout for RecordIO client connections`
- Bad: `fix: bug with RecordIO client timeout`

Use `!` for breaking changes. Use `BREAKING-CHANGE` (hyphen, not space) as Git trailer.

Every significant PR must have an `Issue` trailer:
```
Issue: world-federation-of-advertisers/cross-media-measurement#123
```

**Code review:** Use [Reviewable](https://reviewable.io). Self-review before assigning reviewers. Review others' code promptly (< 48 hours).

## Boundaries

- **Never commit secrets** — no credentials, private keys, or sensitive config
- **Never expose database internal IDs** outside internal API servers
- **Never rely on transitive dependencies** — declare every dep explicitly in BUILD
- **Limit third-party dependencies** — privacy requirements mandate avoiding unaudited deps
- **Mark temporary changes** with `DO_NOT_SUBMIT` (automated check blocks merge)
- **No Java modules** — this project doesn't use `module-info.java`
- All public service APIs follow [AIPs](https://aip.dev/) unless explicitly noted
- TODOs must be actionable: `TODO(@username): <what to do and when>`

## Architecture

The system has three deployment types:

- **Kingdom** — central coordinator for measurement lifecycle, entity registration, certificate management
- **Duchies** (2+) — independent MPC computation nodes, each holds part of the decryption key
- **Data Providers** — supply encrypted measurement data via requisitions

Protocols: Liquid Legions V2, Reach-Only LLv2, Honest Majority Share Shuffle, TrusTEE.

Architecture details: [docs/architecture/](docs/architecture/)
Operations guides: [docs/operations/](docs/operations/)
