# Building

The measurement system can be built using [Bazel](https://www.bazel.build/). The
resulting container images are intended to run in
[Kubernetes](https://kubernetes.io/) (K8s).

## Requirements

In order to build the primary system executables and run the corresponding
tests, your build environment must meet the following:

*   Bazel
    *   Known to work with Bazel 4.0.0
*   GNU/Linux OS with x86-64 architecture
    *   Known to work on Debian Bullseye and Ubuntu 18.04
*   [Clang](https://clang.llvm.org/)
*   [SWIG](http://swig.org/)
*   Bash
*   OpenSSL 1.1.1+

The executable dependencies must be in your `$PATH`.

The entire suite of tests can be run using the following command:

```shell
bazel test //src/test/...
```

### MacOS

To build on MacOS you need to run the bazel build/test in a docker container using
`tools/bazel-container build "//..."`

As of early June 2021, these steps work to get code completion working for Intellij on MacOS:
- use IntelliJ 2021.1.2 and the beta build of the Bazel Plugin
    - Add `https://plugins.jetbrains.com/plugins/list?channel=beta` to your list of plugin repos ([source](https://github.com/bazelbuild/intellij/issues/2406))
- Start a new project from scratch in IntelliJ by selecting "Import Bazel Project" and open the base dir of the cross-media-measurement repo
    - Uncomment "Kotlin" when creating the .bazelproject file
- Once the new projected is created, the Bazel plugin will kick off a sync and, after a while, IntelliJ should start detecting all dependencies and index them. Ignore the Bazel output errors, they happen because e.g. the Spanner Emulator can only be build on Linux.
- You can run most Kotlin tests (with some exceptions, e.g. TransportSecurityTest.kt doesn't work because of openssl incompatibilities) by directly invoking a particular test: `bazel test "//src/test/kotlin/org/wfanet/measurement/common/identity/..."` or similar.


### Local Kubernetes

You can bring up a minimal testing environment in a local Kubernetes environment
using either [Usernetes](https://github.com/rootless-containers/usernetes) (U7s)
with containerd or [kind](https://kind.sigs.k8s.io/). Use one of the following
commands:

```shell
bazel run //src/main/k8s:kingdom_and_three_duchies_u7s
```

```shell
bazel run //src/main/k8s:kingdom_and_three_duchies_kind
```

### Custom Docker Images

See [Docker Image Targets](../src/main/docker/README.md) for additional
requirements to build/deploy these.
