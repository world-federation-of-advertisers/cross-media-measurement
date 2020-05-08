# Contributing

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

We mostly adhere to the
[Google Android Kotlin style guide](https://developer.android.com/kotlin/style-guide).
The one exception is that we use two spaces instead of four for indentation.

Please keep Kotlin code `ktlint`-formatted. Note that our `.editorconfig` file
defines just a couple of overrides. This will produce code compliant with the
style guide.

In addition to the automatic linting and formatting, please observe these
guidelines:

1. Write unit tests for all code.
    1. Please only mock interfaces and abstract classes in tests.
    1. Write test cases per behavior. Bias towards more, smaller test cases.
1. Avoid third-party libraries where possible. Due to the privacy requirements
   of this project, we prefer avoiding unaudited dependencies.
1. Avoid weirdness.
    1. Don't unnecessarily define infix functions.
    1. Avoid reflection.
    1. Keep code predictable. If a code reviewer struggles to understand it,
       please add comments or refactor it to be clearer.

### IntelliJ Autoformatting
You can set up `ktlint`-compatible formatting in IntelliJ by following
[these instructions](https://github.com/pinterest/ktlint/blob/master/README.md#-with-intellij-idea).
Once this is set up, entire directories can be formatted at once by
right-clicking on the directory in the Project view and selecting "Reformat
Code".

## Code Reviews

All code undergoes a thorough code review. Ideally, add at least two reviewers
for any non-trivial change. Reviewers should aim for a **single business day
turnaround**. If a review will take longer, please add a comment informing the
author that more time is necessary.