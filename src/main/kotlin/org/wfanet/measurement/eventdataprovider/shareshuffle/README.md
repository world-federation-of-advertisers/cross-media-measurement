# Honest Majority Share Shuffle EDP Library

## Maven Artifact

This library is available as a Maven artifact from a GitHub Packages Maven
registry. See
[Installing a package](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#installing-a-package)
from the GitHub documentation. The primary artifact is
`org.wfanet.measurement.eventdataprovider:shareshuffle-v2alpha`.

## Native Dependency

This JVM library has a native dependency on the
`libsecret_share_generator_adapter.so` shared library. The simplest way to
include this is to depend on the additional `shareshuffle-native` Maven artifact
at runtime. This artifact includes a precompiled copy of the native library.

If using the precompiled artifact does not fit your use case, you can build the
library yourself using Bazel and add it to your Java library path. The build
target is
`@any_sketch_java//src/main/java/org/wfanet/frequencycount:secret_share_generator_adapter`.
See [Building](../../../../../../../../docs/building.md) for more information.
