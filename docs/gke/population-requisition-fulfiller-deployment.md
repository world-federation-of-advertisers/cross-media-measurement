# How to deploy a Halo Population Requisition Fulfiller on GKE

## Background

The [Halo "dev" (aka "cloud") Kubernetes configuration](../../src/main/k8s/dev)
can be used as the basis for deploying CMMS components using Google Kubernetes
Engine (GKE) on another Google Cloud project.

This guide assumes familiarity with Kubernetes (K8s) and GKE.

***Disclaimer***:

-   This guide is just one way of achieving the goal, not necessarily the best
    approach.
-   Almost all steps can be done via either the
    [Google Cloud Console](https://console.cloud.google.com/) UI or the
    [`gcloud` CLI](https://cloud.google.com/sdk/gcloud/reference). The doc picks
    the easier one for each step. But you are free to do it in an alternative
    way.
-   All names used in this doc can be replaced with something else. We use
    specific names in the doc for ease of reference.
-   All quotas and resource configs are just examples, adjust the quota and size
    based on the actual usage.
-   In the doc, we assume we are deploying to a single region, i.e. us-central1.
    If you are deploying to another region or multiple regions, just need to
    adjust each step mentioning "region" accordingly.

## Before You Start

See [Machine Setup](machine-setup.md).

The Population Requisition Fulfiller is designed to be deployed within the same
cluster as a Kingdom instance. See the
[Kingdom deployment guide](kingdom-deployment.md) to deploy a Kingdom instance
first. Ensure that `kubectl` is configured to point to that cluster.

## Create a Population `DataProvider`

You will first need to create a `DataProvider` resource within the Kingdom for
the Population Data Provider (PDP). See
[Creating Resources](../operations/creating-resources.md). Ensure that the
Kingdom has been restarted to allow the PDP to authenticate via mTLS.

## Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:population_requisition_fulfiller.tar \
  --define container_registry=ghcr.io \
  --define image_repo_prefix=world-federation-of-advertisers \
  --define image_tag=0.5.25 \
  --define pdp_name=dataProviders/RWrFFuS0FdM \
  --define pdp_cert_name=dataProviders/RWrFFuS0FdM/certificates/ZlbFzhtL6jw \
  --define 'event_message_type_url=type.googleapis.com/halo_cmm.origin.uk.eventtemplate.v1.EventMessage'
```

Extract the generated archive to some directory. It is recommended that you
extract it to a secure location, as you will be adding sensitive information to
it in the following step. It is also recommended that you persist this directory
so that you can use it to apply updates.

You can customize this generated object configuration with your own settings
such as the number of replicas per deployment, the memory and CPU requirements
of each container, and the JVM options of each container.

## Customize the K8s Secrets

We use a K8s Secrets to hold sensitive information, such as private keys.

### Secret files for testing

There are some [secret files](../../src/main/k8s/testing/secretfiles) within the
repository. These can be used for testing, but **must not** be used for
production environments as doing so would be highly insecure.

### `pdp-tls`

This is a
[TLS Secret](https://kubernetes.io/docs/concepts/configuration/secret/#tls-secrets)
containing the TLS certificate and private key for the PDP.

Place the following files into the `src/main/k8s/dev/pdp_tls/` path within the
Kustomization directory:

*   `tls.crt`

    PEM-encoded X.509 certificate.

*   `tls.key`

    PEM-encoded PKCS#8 private key.

You can use the insecure test certificate by building the
`//src/main/k8s/testing/secretfiles:pdp1_tls_files.tar` Bazel target to generate
an archive containing these files.

### `pdp-consent-signaling`

This Secret contains files needed for following the Consent Signaling protocol
within the CMMS API.

Place the following files into the `src/main/k8s/dev/pdp_consent_signaling/`
path within the Kustomization directory:

*   `pdp_cs_cert.der`

    DER-encoded X.509 certificate.

*   `pdp_cs_private.der`

    DER-encoded private key for the above certificate.

*   `pdp_enc_private.tink`

    Encryption private key in Tink binary keyset format.

You can use the insecure test certificate and keys by building the
`//src/main/k8s/testing/secretfiles:pdp1_consent_signaling_files.tar` Bazel
target to generate an archive containing these files.

## Customize the K8s ConfigMap

Place the following files into the `src/main/k8s/dev/pdp_config/` path within
the Kustomization directory:

*   `trusted_certs.pem`

    A collection of PEM-encoded X.509 certificates trusted by the PDP. This
    should include the Kingdom's root certificate as well as
    `MeasurementConsumer` (MC) root certificates for Consent Signaling.

*   `event_message_descriptor_set.pb`

    A serialized `FileDescriptorSet` protobuf message containing the event
    message type and all of its dependencies.

## Apply the K8s Kustomization

Use `kubectl` to apply the Kustomization. From the Kustomization directory run:

```shell
kubectl apply -k src/main/k8s/dev/population_requisition_fulfiller
```

You may want to use the `ApplySet` feature of `kubectl` to allow for safe
pruning on future applications.
