# Code Style

## Languages

### Kotlin

We mostly adhere to the
[Google Android Kotlin style guide](https://developer.android.com/kotlin/style-guide).
The one exception is that we use two spaces instead of four for indentation.

Please keep Kotlin code `ktlint`-formatted. Note that our `.editorconfig` file
defines just a couple of overrides. This will produce code compliant with the
style guide.

In addition to the automatic linting and formatting, please observe these
guidelines:

1.  Write unit tests for all code.
    1.  Please only mock interfaces and abstract classes in tests.
    1.  Write test cases per behavior. Bias towards more, smaller test cases.
1.  Avoid third-party libraries where possible. Due to the privacy requirements
    of this project, we prefer avoiding unaudited dependencies.
1.  Avoid weirdness.
    1.  Don't unnecessarily define infix functions.
    1.  Avoid reflection.
    1.  Keep code predictable. If a code reviewer struggles to understand it,
        please add comments or refactor it to be clearer.

### Bazel BUILD and Starlark

Follow the Bazel
[BUILD style guide](https://docs.bazel.build/versions/master/skylark/build-style.html)
and
[.bzl style guide](https://docs.bazel.build/versions/master/skylark/bzl-style.html)

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

Kotlin formatting is done by [`ktlint`](https://ktlint.github.io/).

You can set up `ktlint`-compatible formatting in IntelliJ by following
[these instructions](https://github.com/pinterest/ktlint/blob/master/README.md#-with-intellij-idea).
Once this is set up, entire directories can be formatted at once by
right-clicking on the directory in the Project view and selecting "Reformat
Code".

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

*   Issue in this GitHub project
    *   `TODO(gh/<issue ID>)`
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
