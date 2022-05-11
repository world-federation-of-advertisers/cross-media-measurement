# Machine Setup for GKE Deployment

## Bazel

Read through [Building](../building.md) and make sure that your build
environment meets the stated requirements. You may need to adjust some of the
`bazel` commands below depending on your machine configuration.

We recommend
[installing Bazel using Bazelisk](https://docs.bazel.build/versions/4.2.2/install-bazelisk.html),
where the `bazel` command in your path points to the Bazelisk executable.

## SDKs

Ensure the following additional software is installed on your machine.

*   [Kubectl](https://kubernetes.io/docs/tasks/tools/)
*   [Cloud SDK](https://cloud.google.com/sdk/docs/install)

If you are doing a GKE deployment, it is assumed that you have some familiarity
with using these.

### Configure the Google Cloud CLI

See the
[Before you begin](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-a-regional-cluster#before_you_begin)
section for basic configuration. You'll also want to configure the
[gcloud credential helper](https://cloud.google.com/container-registry/docs/advanced-authentication?hl=en#gcloud-helper).

## Clone the Repository

```shell
git clone https://github.com/world-federation-of-advertisers/cross-media-measurement.git
```

You may want to pick a known working revision and switch to that.

```shell
git checkout 7fab61049e425bb0edd5fa2802290bf1722254e7
```

You can run all of the tests as a sanity check to make sure everything passes.
Note that all of the tests must pass as a precondition for merging a pull
request, so if this fails it is most likely something specific to your
development environment.

```shell
bazel test //src/...
```
