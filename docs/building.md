# Building

The measurement system can be built using [Bazel](https://www.bazel.build/). The
resulting container images are intended to run in
[Kubernetes](https://kubernetes.io/) (K8s).

## Requirements

In order to build the primary system executables and run the corresponding
tests, your build environment must meet the following:

*   Bazel
    *   Known to work with Bazel 3.7.0
*   GNU/Linux OS with x86-64 architecture
    *   Known to work on Debian Bullseye and Ubuntu 18.04
*   [Clang](https://clang.llvm.org/)
*   [SWIG](http://swig.org/)
*   Bash

The executable dependencies must be in your `$PATH`.

The entire suite of tests can be run using the following command:

```shell
bazel test //src/test/...
```

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
