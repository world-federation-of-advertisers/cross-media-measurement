# Docker Image Targets

This package contains targets for building and pushing Docker container images.

## Requirements

### Building from source

Building an image from source has no special requirements outside of what is
already required for this project, particularly the `rules_docker` container
rule requirements.

### Building with package management

Building images by installing packages using a distributions package manager
requires having Docker installed and in your `PATH`. Additionally, network
access is required to install packages from the package repositories.

### Pushing

Container registry write access is needed to push images.
