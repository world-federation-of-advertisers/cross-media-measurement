# Testing Standards

## What Is This?

This guide covers testing practices for WFA repositories, including test
structure, assertion patterns, naming conventions, and what to test. It
supplements the general testing guidance in [Code Style](code-style.md).

The core principle from the code-style guide: **"Test the contract (public API),
not the implementation. Internal functionality that is not part of the public
API should not be exposed just so it can be tested directly."**

Additional guidance from the code-style guide:

*   "Bias towards more, smaller test cases."
*   "For dependencies in tests, carefully consider when to use fakes, mocks,
    stubs, or the real dependency."

## What to Test

### Test the Public API

For CLI tools, the public interface is the `main` function. Tests should invoke
`main` with arguments and assert on the output or side effects. This includes
user-visible flag handling such as defaults, validation, and error reporting.
([PR #2351](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/2351#discussion_r2111265367),
[PR #18](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/18#discussion_r616254253)) Tests should not
construct the command class directly or assert on Picocli internals.

For services, test the gRPC interface, not internal helpers. The constructor of
the implementation class should be inaccessible to the test.

```kotlin
@Test
fun `main writes output to specified file`() {
    val outputFile = tempDir.resolve("out.json")
    MyCliCommand.main(arrayOf("--output-file", outputFile.toString(), ...))
    assertThat(outputFile.readText()).contains(expectedContent)
}
```

Do not expose internal functionality just so it can be tested directly. If
something cannot be tested through the public API, consider whether it needs to
exist as a separate unit at all.

### Do Not Test Framework Internals

Assume Picocli, gRPC, protobuf, and other frameworks work as documented. Tests
should not exist solely to verify framework behavior in isolation. Black-box
tests through your public API are still appropriate when they verify your
command's observable behavior or your code's use of the framework.

### Bug Fixes Require Tests

If you are fixing a bug, write a test that reproduces the bug first. The test
should fail without the fix and pass with it.

## Assertions

### Truth Assertion Ordering

Use the [Truth](https://truth.dev/) library for test assertions.
([PR #18](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/18#discussion_r616177307)) Wrap the
Subject around the **actual** value. The expected value goes in the assertion
method.

```kotlin
// Correct
assertThat(actualResult).isEqualTo(expectedValue)

// Incorrect — arguments are inverted
assertThat(expectedValue).isEqualTo(actualResult)
```

For protobuf message subjects, import the
[ProtoTruth](https://truth.dev/protobufs) version of `assertThat` for better
diff output.

### Expected Exceptions

Use `assertFailsWith` from the `kotlin.test` package for expected exceptions.
([PR #1125](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/1125#issuecomment-1681075005))
Do not use try/catch or JUnit's `assertThrows`.

### Exception Message Assertions

Do not assert on entire exception messages. Exception messages are
human-readable and should not be considered part of the API contract. Assert
just enough to disambiguate exception types.

```kotlin
val exception = assertFailsWith<IllegalStateException> { doThing() }
assertThat(exception.message).contains("abc-123")
```

### Assertions Must Be Sufficient

If a test is supposed to verify that a field is set, the assertion must actually
check that field. A test that passes without verifying the behavior under test
provides false confidence.

## Test Doubles

Prefer purpose-built test implementations over mocking your own interfaces.
([#781](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/781#issuecomment-1341375697)) For
example, use `InMemoryStorageClient` (which exists specifically for testing) or
`FileSystemStorageClient` rather than mocking `StorageClient`.

Per the code-style guide, carefully consider when to use fakes, mocks, stubs,
or the real dependency. See
[Know Your Test Doubles](https://testing.googleblog.com/2013/07/testing-on-toilet-know-your-test-doubles.html)
for definitions and
[Don't Overuse Mocks](https://testing.googleblog.com/2013/05/testing-on-toilet-dont-overuse-mocks.html)
for guidance.

## Timing

Unit tests must not use real delays or rely on wall-clock timing.
([PR #2351](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/2351#discussion_r2111306698),
[#827](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/827#issuecomment-1404339766)) Tests that
depend on timing are flaky and slow. Use test dispatchers, fake clocks, or
restructure the code to be testable without timing.

## Test Setup

If a test resource is specific to one test method, create it inside that method.
([PR #2351](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/2351#discussion_r2111292495))
Do not create it in `@Before` or class-level setup where it runs for every test.

When automated testing is genuinely difficult, document what was manually tested
and why automation was impractical.

## Naming

Test method names should describe what the system under test does, not what the
test does. The general pattern is `<SUT method> does X [when Y]`.

```kotlin
@Test
fun `writeBlob publishes finalize event to subscribers`() { ... }

@Test
fun `writeBlob returns blob with correct size when data is empty`() { ... }
```

When testing multiple SUT methods in one class, prefix each test with the
method name being tested.

## Organization

*   Tests go in the `src/test/` tree, mirroring the `src/main/` path.
    ([PR #2351](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/2351#discussion_r2111258485))
*   Test infrastructure that is complex enough to warrant being tested itself or
    is used by more than one package goes into a `testing` subpackage of the
    `src/main/` tree, with its Bazel targets marked as `testonly`.
*   Favor numerous smaller test cases over fewer large ones.

## See Also

*   [Code Style](code-style.md) — general testing guidance, assertion library
    choices, and test double recommendations
*   [Dev Standards](dev-standards.md) — code review workflow and PR requirements
