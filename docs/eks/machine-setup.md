# Machine Setup for EKS Deployment

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
*   [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)
*   [EKS CLI](https://eksctl.io/)
*   [Terraform](https://developer.hashicorp.com/terraform/downloads)

If you are doing a EKS deployment, it is assumed that you have some familiarity
with using these.

### Configure the AWS CLI

See the
[configure](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/configure/index.html)
section for setting AWS Cli to with correct accesses.
After the EKS cluster is created, you'll also want to configure the
[kubeconfig](https://cloud.google.com/container-registry/docs/advanced-authentication?hl=en#gcloud-helper).

## Download the source code

You can download the source code for the
[latest release](https://github.com/world-federation-of-advertisers/cross-media-measurement/releases/latest)
from GitHub.

### Run tests (optional)

As a sanity check to ensure that you have your machine set up correctly, you can
run the automated tests.

```shell
bazel test //src/test/...
```

Note that these tests are run as part of the release process, so any failure is
most likely to be related to machine setup.
