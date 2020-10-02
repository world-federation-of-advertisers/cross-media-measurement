# Remote Build Execution (RBE) Image

The targets in this package are used to build and deploy a container image for
use with Bazel's Remote Build Execution (RBE) feature. You should only need to
do this when changes are needed to the remote build environment.

The image is deployed to `gcr.io/ads-open-measurement/rbe`. This project has a
WORKSPACE target to pull and use that image for RBE.

## Customization

The image is based on `marketplace.gcr.io/google/rbe-ubuntu18-04`, with
customizations to the build environment needed for this project.

In particular:

1.  JDK 11
1.  SWIG
1.  Timezone data (`tzdata`)
