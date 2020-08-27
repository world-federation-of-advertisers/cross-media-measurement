# Remote Building

This workspace supports Google Cloud's
[Remote Build Execution](https://cloud.google.com/remote-build-execution/docs/overview)
(RBE) service.

## Host Requirements

RBE is currently only supported from Linux hosts. It additionally requires
Docker to be installed on the host machine, as well as access to the
`gcr.io/ads-open-measurement/rbe-ubuntu16-04` image repository.

## Setting Up

### GCP Project

You will need a GCP project with an instance of the RBE service. GCP project
setup only needs to be done once for everyone using that project.

Follow the instructions for
[Enabling Remote Build Execution](https://cloud.google.com/remote-build-execution/docs/set-up/enable).

#### Results UI

Follow the instructions at
[Setting up the results UI](https://cloud.google.com/remote-build-execution/docs/results-ui/getting-started-results-ui).

### Bazel Configuration

Once you have set up the instance and permissions for remote caching, you'll
need to specify these with the `--project_id` and `--remote_instance_name` Bazel
command-line flags.

For convenience, you can configure these in a
[`.bazelrc` file](https://docs.bazel.build/versions/master/guide.html#bazelrc-the-bazel-configuration-file).
See below for a sample file.

### Container Registry Access

For now, the Docker image for the RBE container is in a private repository in
the GCP Container Registry. You will need to set up Docker authentication to
access it.

The simplest option is to use `gcloud` as a credential helper.

```shell
gcloud auth login
gcloud auth configure-docker
```

See
[Authentication methods](https://cloud.google.com/container-registry/docs/advanced-authentication)
for Container Registry.

## Running

Simply pass `--config=remote` to Bazel to use RBE, and/or `--config=results` to
upload results.

### Sample `.bazelrc`

The following is a sample `.bazelrc` for the `ads-open-measurement` GCP project
with `default_instance`.

```
# Personal GCP project/instance config for RBE.
build:_gcp --project_id=ads-open-measurement
build:_gcp --remote_instance_name=projects/ads-open-measurement/instances/default_instance

# Use personal _gcp config for RBE and results UI.
build:remote --config=_gcp
build:results --config=_gcp

# Always upload results for remote builds.
build:remote --config=results

# Always upload results for tests.
test --config=results
```
