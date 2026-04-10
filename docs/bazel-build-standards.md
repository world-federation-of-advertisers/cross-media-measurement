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
must be explicitly declared in that target's `deps`. If your code imports
`com.google.common.collect.ImmutableList`, your target must directly depend on
the Guava target — even if another dependency already pulls it in transitively.

Conversely, do not include dependencies that are not directly used by the
target. A target that only uses generated gRPC code should not depend on the
message type library.

```python
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

Use BOMs when available rather than specifying versions of individual artifacts.
Leave out the version for anything covered by a BOM. For example, artifacts
covered by `com.google.cloud:libraries-bom` (included by common-jvm) should not
have their versions specified individually.

### Third-Party Dependencies

Limit usage of unnecessary third-party dependencies. Due to the privacy
requirements of this project, we prefer avoiding unaudited dependencies.

Before removing a dependency, search the entire GitHub org to confirm nothing
else uses it.

## Package & Target Structure

### Import Paths Must Match JVM Packages

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

```python
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

Test utilities under `src/main/testing` must have `testonly = True`. See the
[Code Style](code-style.md) conventions for the `testing` subpackage pattern.

## Module & Lockfile Management

### Keeping Lockfiles in Sync

Never change `maven_install.json` without corresponding changes to
`MODULE.bazel`, and vice versa. Lockfile update commands:

*   Bazel module lockfile: `bazel mod deps --lockfile_mode=update`
*   Maven lockfile: `REPIN=1 bazel run @maven//:pin`

### Bazel Version

Use [`bazelisk`](https://github.com/bazelbuild/bazelisk) to run Bazel so it
respects the `.bazelversion` file. Lockfile version mismatches are often caused
by running `bazel` directly instead of `bazelisk`.

### Temporary Overrides

When overriding a module dependency (e.g., `local_path_override`,
`archive_override`), always include a `# DO_NOT_SUBMIT` comment to prevent
accidental merging. This tag is detected by an automated check and will block
the PR from being merged.

```python
# DO_NOT_SUBMIT
local_path_override(
    module_name = "common-jvm",
    path = "../common-jvm",
)
```

## Import Paths

Import paths in BUILD files should not include source tree prefixes like
`src.main.python`. Use the `imports` attribute to put the correct directory on
the path.

This project does not use Java modules. Do not add `module-info.java` files.

## Code Practices

### Constants

Extract magic strings as named constants. When a constant is a format string
rather than a literal value, make this clear in the name.

*   `EDP_TARGET_SERVICE_ACCOUNT` — implies a literal value
*   `EDP_TARGET_SERVICE_ACCOUNT_FORMAT` — clearly a format string

### Formatting

BUILD and Starlark files must be formatted with
[Buildifier](https://github.com/bazelbuild/buildtools/tree/master/buildifier).

### GitHub Actions

Keep GitHub Actions versions up to date. Do not use deprecated versions.

## See Also

*   [Code Style](code-style.md) — language-specific style rules, transitive
    dependency policy, and formatter configuration
*   [Dev Standards](dev-standards.md) — commit message format, code review
    workflow, and PR requirements
