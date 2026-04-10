# Code Style

Rather than providing a complete style guide, this section outlines some
specific code style issues that tend to come up in code reviews. Besides
familiarizing yourself with these, please feel free to link to particular
sections of this when reviewing others’ code.

## General

*   Code should be thoroughly unit tested:
    *   Test the contract (public API), not the implementation.
        *   Corollary: Internal functionality that is not part of the public API
            should not be exposed just so it can be tested directly.
        *   See
            [Prefer Testing Public APIs Over Implementation-Detail Classes](https://testing.googleblog.com/2015/01/testing-on-toilet-prefer-testing-public.html)
    *   For dependencies in tests, carefully consider when to use fakes, mocks,
        stubs, or the real dependency. See
        [this article](https://testing.googleblog.com/2013/07/testing-on-toilet-know-your-test-doubles.html)
        for definitions and
        [this one](https://testing.googleblog.com/2013/05/testing-on-toilet-dont-overuse-mocks.html)
        for some guidance.
    *   Bias towards more, smaller test cases.
*   Code should be autoformatted and linted
*   Code should generally be free of compiler warnings

    Avoid introducing new compiler warnings. Steps for dealing with a warning:

    1.  Fix or work around.

        Make a strong attempt to actually address the warning or find another
        approach that avoids it.

    2.  If it's a false alarm, suppress the warning and document it.

        If you're convinced the code is safe and correct and that there's no
        other way to indicate this to the compiler, suppress the warning and
        leave a comment indicating why the warning is incorrect.

    3.  If all else fails, leave a TODO comment.

        In cases where the warning cannot yet be addressed (e.g. a deprecated
        symbol that we can't stop using yet), add a TODO. **Do not** suppress
        the warning in this case, as it should remain visible until it can be
        addressed properly.

*   Limit usage of unnecessary third party dependencies. Due to the privacy
    requirements of this project, we prefer avoiding unaudited dependencies.

## Conventions

*   Tests go into the `src/test/` tree.
*   Test infrastructure that either is complex enough to warrant being tested
    itself or is used by more than one package goes into a `testing` subpackage
    of the `src/main/` tree, with its Bazel targets marked as `testonly`.
*   All public service APIs (those called by a different component) follow the
    [AIPs](https://aip.dev/), with specific exceptions where noted.
*   Database access is restricted to servers hosting internal APIs.
*   Database internal IDs must not be exposed outside of internal API servers,
    i.e. they are not included in the internal API.
*   The term "reference ID" refers to an ID from an external system.
*   Names of protobuf messages for the serialized portion of a database row end
    in `Details`.

## Languages

### Kotlin

*   Follow the
    [Google Android Kotlin style guide](https://developer.android.com/kotlin/style-guide).
    *   Exception: Use two spaces instead of four for indentation.
*   See the
    [Kotlin coding conventions](https://kotlinlang.org/docs/coding-conventions.html)
    when neither this document nor the style guide offer an opinion.

#### Immutability & Declarations

*   Use `val` over `var` whenever the variable is never reassigned.
*   Use `const val` for compile-time constants (primitives and String literals
    in companion objects or at the top level).
*   Prefer computed properties via `get()` over backing fields when the value
    is directly derivable from another field.
*   Use the `Duration` type for durations. Never pass raw `Int` or `Long`
    values to represent time.

#### Protobuf & Builders

*   Prefer using Kotlin DSL builders for protocol buffers. See
    [Kotlin Generated Code](https://developers.google.com/protocol-buffers/docs/reference/kotlin-generated)
    in the Protocol Buffers Developer Guide. Use `copy {}` instead of
    `toBuilder()`.
*   For non-protobuf builders, use `Foo.newBuilder().apply { ... }.build()`.
*   Use `isNotEmpty()` over `isNotBlank()` for protobuf string fields, which
    are either a valid value or empty string (the default/unset value).

#### Type Safety & Expressions

*   Specify types explicitly except when they are obvious.
    *   For example, assignments from constructors or factory functions.
    *   "Superfluous" explicit types are fine except in cases where they clearly
        hurt readability or safety.
*   Use exhaustive `when` on enums (no `else` branch) so that adding a new
    enum value forces a compilation error.
*   Prefer `if` over `?.let` when not chaining or not using the result.
*   Extract a variable for repeated non-null assertions (`!!`) to avoid
    asserting multiple times.
*   Use `kotlin.text.Regex` instead of `java.util.regex.Pattern`.

#### Namespacing & Imports

*   Wildcard imports are not allowed.
*   Use (companion) objects for constants and static properties.
    *   This avoids polluting the global namespace.
*   Do not define top-level non-extension functions. Wrap them in a class or
    object instead.
*   Avoid defining extensions on common types when it may appear that they could
    work on all instances of that type.
    *   For example, `String.toFoo()` where the String must be a specific
        serialization of a `Foo` to work. This is a version of the "stringly
        typed" anti-pattern.

#### Error Handling

*   Either log or throw, never both. If you throw an exception, the caller
    decides whether and how to log.
*   Use the correct precondition function for the exception type:
    *   `require` / `requireNotNull` → `IllegalArgumentException` (argument
        validation)
    *   `check` / `checkNotNull` → `IllegalStateException` (state validation)
    *   `error` → `IllegalStateException` (illegal/impossible state)
*   Catch `StatusException` from RPCs immediately at the call site.
    *   This prevents gRPC statuses from being incorrectly propagated from gRPC
        servers that are also gRPC clients.
    *   If you need to re-throw the exception to be handled at a higher level,
        wrap it in another exception type.
*   When logging exceptions, use logging methods that accept the exception
    object so the full stack trace is captured.

#### Coroutines & Concurrency

*   `runBlocking` is restricted to tests and `main` entry points. It must not
    appear in library code, service implementations, or flow collectors.
*   Use `SendChannel.trySendBlocking` instead of wrapping `SendChannel.send`
    in `runBlocking`.
*   Use `Flow.produceIn` to get flow elements into a Channel.
*   Limit the scope of mutability using `.apply {}`.

#### Idioms

*   Use `getOrPut` for map caching patterns.
*   Specify argument names when passing literals for clarity.
*   Avoid unnecessary copies between `ByteString` and `ByteArray`.
*   Use trailing commas on multiline structures to reduce merge conflicts.
*   Use a regular for-each loop when not chaining a set of operations.

#### CLI Flags

*   For Picocli flags, use `lateinit var` with the `defaultValue` annotation
    parameter instead of assigning Kotlin default values. This lets the
    framework handle defaults.

#### Testing

*   Use the [Truth](https://truth.dev/) library for test assertions, importing
    `assertThat`.
    *   Make sure to import the [ProtoTruth](https://truth.dev/protobufs)
        version of `assertThat` as well if you have protobuf message subjects.
    *   Exception: Use the `kotlin.test` package for expected exceptions,
        e.g.
        [`assertFailsWith`](https://kotlinlang.org/api/latest/kotlin.test/kotlin.test/assert-fails-with.html).
*   See the [Testing Standards](testing-standards.md) for additional guidance
    on test structure and conventions.

### C++

*   Follow the
    [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)
*   Follow [Abseil’s Tips of the Week](http://abseil.io/tips)
*   Don’t rely on transitive includes.
    *   You may be able to use the
        [include-what-you-use](https://include-what-you-use.org) tool to help.

### Bazel BUILD and Starlark

*   Follow the Bazel
    [BUILD style guide](https://docs.bazel.build/versions/master/skylark/build-style.html)
    and
    [.bzl style guide](https://docs.bazel.build/versions/master/skylark/bzl-style.html)
*   Do not rely on transitive dependencies. Include all directly referenced
    dependencies in that target's `deps`.

### Protocol Buffers

Follow the
[Protocol Buffers style guide](https://developers.google.com/protocol-buffers/docs/style).

### Markdown

Follow the
[Google Markdown Style Guide](https://google.github.io/styleguide/docguide/style.html).

Note: We intend to keep our Markdown compatible with both the
[GitHub Flavored Markdown Spec](https://github.github.com/gfm/) and
[Gitiles](https://gerrit.googlesource.com/gitiles/+/HEAD/Documentation/markdown.md).

## Formatters/Linters

Automated formatters help keep code styling consistent, especially when there
are ambiguities in the style guide. You should run the appropriate formatters on
your code prior to submitting.

### Kotlin

Kotlin formatting is done by
[`ktfmt`](https://github.com/facebookincubator/ktfmt) with the `--google-style`
option. Please keep code `ktfmt`-formatted.

There is a
[`ktfmt` IntelliJ plugin](https://plugins.jetbrains.com/plugin/14912-ktfmt) that
you can install to replace the `Reformat Code` action in IntelliJ IDEs.

### BUILD and Starlark

Bazel includes a
[`Buildifier`](https://github.com/bazelbuild/buildtools/tree/master/buildifier)
tool.

### Other languages

[`clang-format`](https://clang.llvm.org/docs/ClangFormat.html) supports
formatting for multiple languages. Run it with `--style=Google`.

## TODOs

Use TODO comments to point out a desired improvement in the code. Each of these
should be actionable, explaining how it can be resolved and what conditions may
need to be met first.

### Format

TODO comments should be of the format `TODO(<context>): <comment>`, where the
context indicates the person or tracked issue with the most information about
the problem. This is not an assignee.

Prefer referencing an issue rather than a person whenever possible, as a
well-written issue provides permanent history and is usually linked to
additional context.

Here is how to reference people or issues in different systems:

*   Issue in a GitHub repository
    *   `TODO(<org>/<repo>#<number>)`
*   GitHub user
    *   `TODO(@<GitHub username>)`
*   Issue in internal [Google Issue Tracker](https://issuetracker.google.com)
    *   `TODO(b/<issue ID>)`
*   Googler
    *   `TODO(<@google.com username>)`

Note that references to Google systems are only included here to explain any
existing TODOs of this form in the code base. All new TODOs should reference
GitHub.

Example: `TODO(@SanjayVas): Switch Foo to Bar after the Baz dependency is
upgraded to v2.`

This means that if someone wants more information about this TODO, the best
person to ask would be GitHub user @SanjayVas.

## See Also

The [Dev Standards](dev-standards.md) guide has more useful information.
