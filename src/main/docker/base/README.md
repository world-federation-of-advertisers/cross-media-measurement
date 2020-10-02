# Base Container Images

The targets in this package are used to build and deploy base container images
for container image targets. You should only need to do this when changes are
needed to a base image.

## Debian Bullseye Java Base

This image is deployed to `gcr.io/ads-open-measurement/java-base`. This project
has a WORKSPACE target to pull the image from that registry.

The image is used as the base for `java_image` targets that include JNI
dependencies. The reason for this is that the runtime environment needs to have
a version of glibc that is at least as recent as the one used to build the
shared libraries. Bullseye, being the current Testing version of Debian, has a
relatively recent version.

Note that the intent is to find an alternative solution such that the standard
distroless Java base image can be used for all of the `java_image` targets in
this project.
