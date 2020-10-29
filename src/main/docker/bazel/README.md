# Bazel Build Environment Image

The targets in this package are used to build and deploy a container image for
building this project with Bazel. You should only need to do this when changes
are needed to the build environment.

The image is deployed to `gcr.io/ads-open-measurement/bazel`.

## Customization

The image is based on `docker.io/library/ubuntu:18.04`, with customizations to
the build environment needed for this project.

In particular:

1.  Bazel
1.  JDK 11
1.  Python
1.  Git
1.  Timezone database (`tzdata`)
1.  [SWIG](http://swig.org/)

## Building in Container

Use the `bazel-docker` script in `tools/` the same way you would use the `bazel`
command. This will run Bazel in a Docker container, mounting the workspace as a
volume.

Note that workspace targets that require authentication, such as access to a
private container registry or Git repository, will fail. All such targets should
eventually be removed from the workspace.
