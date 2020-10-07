# Contributing

Guidelines for contributing to the Measurement system.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement (CLA). You (or your employer) retain the copyright to your
contribution; this simply gives us permission to use and redistribute your
contributions as part of the project. Head over to
<https://cla.developers.google.com/> to see your current agreements on file or
to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Editing

While any editor should work, most contributors use IntelliJ IDEA Community
Edition. You can make an IntelliJ project through a `.bazelproject` file
containing:

```yaml
directories:
  .

derive_targets_from_directories: true

additional_languages:
  kotlin
```

## Code Style

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

### Bazel BUILD

Follow the Bazel
[BUILD Style Guide](https://docs.bazel.build/versions/master/skylark/build-style.html).

### Protocol Buffers

Follow the
[Google Protocol Buffers Style Guide](https://developers.google.com/protocol-buffers/docs/style).

### Markdown

Follow the
[Google Markdown Style Guide](https://google.github.io/styleguide/docguide/style.html).

Note: We intend to keep our Markdown compatible with both the
[GitHub Flavored Markdown Spec](https://github.github.com/gfm/) and
[Gitiles](https://gerrit.googlesource.com/gitiles/+/HEAD/Documentation/markdown.md).

## Formatters

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

### BUILD

Bazel includes a
[`buildifier`](https://github.com/bazelbuild/buildtools/tree/master/buildifier)
tool.

### Other languages

[`clang-format`](https://clang.llvm.org/docs/ClangFormat.html) supports
formatting for multiple languages. Run it with `--style=Google`.

## Code Reviews

All submissions, including submissions by project members, require review.
Ideally, add at least two reviewers for any non-trivial change. Reviewers should
aim for a **single business day turnaround**. If a review will take longer,
please add a comment informing the author that more time is necessary.

We use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.
