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

See the [code style](docs/code-style.md) page.

## Code Reviews

All submissions, including submissions by project members, require review.
Ideally, add at least two reviewers for any non-trivial change. Reviewers should
aim for a **single business day turnaround**. If a review will take longer,
please add a comment informing the author that more time is necessary.

We use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.
