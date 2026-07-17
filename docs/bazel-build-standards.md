# Bazel Build Standards

## What Is This?

This guide covers conventions for Bazel BUILD files, MODULE.bazel, dependency
declarations, and related build infrastructure in WFA repositories. BUILD files
are treated as first-class code and should receive the same review scrutiny as
source files.

This supplements the general build guidance in [Code Style](code-style.md) and
[Dev Standards](dev-standards.md).

## Dependencies

### Explicit Dependencies

Do not rely on transitive dependencies. Every dependency used in a source file
must be explicitly declared in that target's `deps`.
([#3306](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/3306),
[PR #1945](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/1945#issuecomment-2513251533)) If your code imports
`com.google.common.collect.ImmutableList`, your target must directly depend on
the Guava target — even if another dependency already pulls it in transitively.

Conversely, do not include dependencies that are not directly used by the
target. A target that only uses generated gRPC code should not depend on the
message type library.

```bazel
kt_jvm_library(
    name = "my_lib",
    srcs = ["MyLib.kt"],
    deps = [
        "//src/main/kotlin/org/wfanet/measurement/common",
        "//imports/java/com/google/common/collect",
    ],
)
```

### Bills of Materials (BOMs)

For Maven dependencies, use BOMs when available rather than specifying versions
of individual artifacts. Leave out the version for anything covered by a BOM.
For example, artifacts
covered by `com.google.cloud:libraries-bom` (included by common-jvm) should not
have their versions specified individually.

### Third-Party Dependencies

Limit usage of unnecessary third-party dependencies.
([PR #701](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/701#issuecomment-1256431974)) Due to the privacy
requirements of this project, we prefer avoiding unaudited dependencies.

Before removing a dependency, search the entire GitHub org to confirm nothing
else uses it.

## Package & Target Structure

### The `//imports` Tree

The `//imports/java/` tree provides Bazel aliases for external Maven
dependencies. BUILD files outside of `//imports` should never reference
`@maven//` targets directly — instead, they depend on the alias targets in
`//imports/java/`.

The Bazel package under `//imports/java/` must mirror the Java/Kotlin package
structure exactly. The target name should reflect the library or artifact, not
the package.

For example, for the Java package `io.netty.channel` from the Netty transport
artifact, the Bazel package should be `//imports/java/io/netty` with a target
named `transport`.

### Default Target Naming

When a BUILD file has a single primary target, name it to match the Bazel
package directory so it becomes the default target.

For example, for package `//imports/java/com/rabbitmq/client`, name the target
`client`.

### Visibility

Use `package(default_visibility = ...)` instead of specifying visibility on
every target.
([PR #3574](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/3574#issuecomment-4026448303))

```bazel
package(default_visibility = ["//visibility:public"])
```

However, be specific about visibility when `//visibility:public` is too broad.
Targets should have the narrowest visibility that satisfies their actual
consumers.

### Target Granularity

If sources in a target are not always used together, consider splitting them
into separate library targets. This avoids pulling in unnecessary code as a
transitive dependency.

### Test Infrastructure

Test utilities in `testing` subpackages under `src/main/` must have
`testonly = True`.
([PR #3622](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/3622#issuecomment-4076151760)) See the
[Code Style](code-style.md) conventions for the `testing` subpackage pattern,
for example `src/main/kotlin/org/wfanet/measurement/testing` or
`src/main/kotlin/org/wfanet/measurement/duchy/testing`.

## Module & Lockfile Management

### Keeping Lockfiles in Sync

Generated lockfiles under version control must always be in sync with the
source they are generated from:

*   `MODULE.bazel.lock` — generated from `MODULE.bazel`. Update with:
    `bazel mod deps --lockfile_mode=update`
*   `maven_install.json` — generated from changes that affect Maven
    dependencies in `MODULE.bazel`. Update with:
    `REPIN=1 bazel run @maven//:pin`
*   `requirements_lock.txt` — generated from `pyproject.toml`

Not all changes to `MODULE.bazel` require updating `maven_install.json` — only
changes that affect Maven dependencies do.

### Temporary Overrides

When overriding a module dependency (e.g., `archive_override`,
`git_override`), always include a `DO_NOT_SUBMIT` comment following the same
format as TODO comments (see [Code Style: TODOs](code-style.md#todos)) to
provide context about the pending change.
([PR #2789](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/2789#discussion_r2288540263),
[PR #669](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/669#issuecomment-1222915827)) This tag is detected by an automated check and will block
the PR from being merged.

```bazel
# DO_NOT_SUBMIT(world-federation-of-advertisers/common-jvm#456)
archive_override(
    module_name = "common-jvm",
    urls = ["https://github.com/world-federation-of-advertisers/common-jvm/archive/df87054e5221ea82289ae6e9cfa68d1ef50563c2.tar.gz"],
    strip_prefix = "common-jvm-df87054e5221ea82289ae6e9cfa68d1ef50563c2",
)
```

## Python Import Paths

Import statements in Python files should not include source tree prefixes like
`src.main.python`. Use the `imports` attribute in BUILD files to put the
correct source root on the path so code imports `wfa.measurement...`, not
`src.main.python.wfa.measurement...`.

This project does not use Java modules. Do not add `module-info.java` files.

## Code Practices

### Constants

Extract magic strings as named constants. Ensure the constant name includes
anything needed for disambiguation, such as whether it is a format string or
what units it represents.

*   `EDP_TARGET_SERVICE_ACCOUNT` — ambiguous: is this a literal value or a
    format string?
*   `EDP_TARGET_SERVICE_ACCOUNT_FORMAT` — clearly a format string
*   `FOO_TIMEOUT_SECONDS` — units are explicit in the name

### Formatting

BUILD and Starlark files must be formatted with
[Buildifier](https://github.com/bazelbuild/buildtools/tree/master/buildifier).
([PR #21](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/21#discussion_r617052851))

## See Also

*   [Code Style](code-style.md) — language-specific style rules, transitive
    dependency policy, and formatter configuration
*   [Dev Standards](dev-standards.md) — commit message format, code review
    workflow, and PR requirements
