# Contributing

Guidelines for contributing to the Measurement system.

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

See the [Code Style](docs/code-style.md) page.

## Building

See the [Building](docs/building.md) page.

## Code Reviews

All submissions, including submissions by project members, require review.
Ideally, add at least two reviewers for any non-trivial change. Reviewers should
aim for a **single business day turnaround**. If a review will take longer,
please add a comment informing the author that more time is necessary.

We use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.
