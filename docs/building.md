# Building

The measurement system can be built using [Bazel](https://www.bazel.build/). The
resulting container images are intended to run in
[Kubernetes](https://kubernetes.io/) (K8s).

## Requirements

In order to build the primary system executables and run the corresponding
tests, your build environment must have the following:

*   Bazel
    *   See [`.bazelversion`](../.bazelversion)
*   GNU/Linux OS with x86-64 architecture
    *   Some image targets require building with glibc <= 2.36
    *   Known to work on Debian Bookworm and Ubuntu 22.04
*   [Clang](https://clang.llvm.org/)
*   [SWIG](http://swig.org/)
*   Bash
*   OpenSSL 1.1.1+

The executable dependencies must be in your `$PATH`.

The entire suite of tests can be run using the following command:

```shell
bazel test //src/test/...
```

### Specifying Host Platform

As stated above, some targets require building with glibc <= 2.36. Bazel cannot
detect the glibc version used by the local C++ toolchain, so we rely on the host
platform being explicitly specified using the `--host_platform` option. Known
compatible platforms are defined in the
[//build/platforms package](../build/platforms/BUILD.bazel).

For example, if your host machine is running Ubuntu 22.04, you would specify
`--host_platform=//build/platforms:ubuntu_22_04`.

## Make Variables

Some build targets rely on
["Make" variables](https://docs.bazel.build/versions/4.2.2/be/make-variables.html)
which can be defined on the command line or in `.bazelrc` using the `--define`
option. These are defined in [variables.bzl](../build/variables.bzl).

## IntelliJ Setup

IntelliJ IDEA is the recommended IDE for development work on this project. As of
2024-04-15, the project is known to be compatible with IntelliJ IDEA 2024.1
Community Edition.

You will need to set up your project using the
[Bazel plugin](https://plugins.jetbrains.com/plugin/8609-bazel) for features
such as code completion:

*   Select "Import Bazel Project", using the repository root as the workspace
    directory.
*   Ensure that `kotlin` is selected under the `additional_languages` section of
    the project view (`.bazelproject`) file. See below for an example.

    ```
    directories:
      .

    test_sources:
      src/test/*

    derive_targets_from_directories: true

    additional_languages:
      kotlin
    ```

## Containerized Builds

The `ghcr.io/world-federation-of-advertisers/bazel` container image provides a
build environment with the appropriate dependencies. This can be helpful when
your host machine does not meet the above requirements.

For convenience, the [`tools/bazel-container`](../tools/bazel-container) script
can be used in place of the `bazel` executable for building/testing. For
example:

```shell
tools/bazel-container test //src/test/...
```

Note: This script specifies the appropriate host platform to Bazel, so you
should not use the `--host_platform` option when using it.

Note that if you don't need to interact with the code on your host machine, it
may be simpler to use the Bash shell (`/bin/bash`) as your entry point and run
all of your commands from inside the container. You may even want to install
IntelliJ inside the container and then use X11 forwarding to use it from your
host machine.

### Running TestContainer-based Tests

There is some extra setup needed for running tests that use TestContainers
inside of a Docker container. The
[`tools/bazel-container`](../tools/bazel-container) script will take care of
these for you, but for reference:

*   Follow the instructions for sibling Docker containers in
    [Patterns for running tests inside a Docker container](https://www.testcontainers.org/supported_docker_environment/continuous_integration/dind_patterns/)
*   Set the `TESTCONTAINERS_RYUK_DISABLED` environment variable to `true` within
    the container. See
    https://github.com/opentable/otj-pg-embedded/issues/166#issuecomment-1020602855.

### Hybrid Development

If your host machine has too new a glibc version but meets all other
requirements, you can do most of your development on the host machine and just
use the container for building/deploying image targets using the
`tools/bazel-container` script. You can even run targets built inside the
container on your host machine.

When running on Linux, the script will write its output to the
`bazel-container-output` directory inside of your working directory. We can use
the `--script_path` option to the `run` subcommand to output a script that we
can run from the host machine. The `tools/bazel-container-run` script will do
this for you, so you can use it in place of `bazel run`.

## Local Kubernetes

You can bring up a minimal testing environment in a local Kubernetes cluster.
See [instructions](../src/main/k8s/local/README.md).
