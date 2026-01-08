# Enabling TrusTEE in Kingdom and Duchy

This guide describes how to enable the TrusTEE protocol in both the Kingdom and
the Duchy.

## Overview

Enabling TrusTEE involves steps for both the Duchy (infrastructure and
configuration) and the Kingdom (configuration and feature flags).

## Duchy Setup

The following steps are required to enable TrusTEE in a Duchy.

### 1. Update GCP Resources

You need to update your infrastructure to include the Trustee Mill resources.
You can refer to the
[Duchy Terraform Example](../../src/main/terraform/gcloud/examples/duchy/README.md)
as a reference implementation, which has been tested in development
environments.

Key resources include: * Managed Instance Group (MIG) for running the mills. *
Service accounts and IAM roles. * The Trustee Mill service account requires
`roles/storage.objectAdmin` on the Duchy's storage bucket to read and write
computation blobs. * Networking components (Subnetwork, Cloud Router, Cloud
NAT). * Ensure the `trustee_mill_subnetwork_cidr_range` does not conflict with
existing GKE cluster subnets. A default of `10.127.0.0/24` is suggested to avoid
common conflicts. * Secret Manager secrets.

### 2. Run TrusTEE Mills

The TrusTEE mills run in a Managed Instance Group (MIG) using Confidential
Space. The Docker image for the mill must be signed, and the signature must be
available in the specified repository. It is signed by the
[Sign Images Workflow](../../.github/workflows/sign-images.yml) during the Halo
build process. The signature is stored along with the images in the GitHub image
repository.

#### MIG Configuration

The MIG is configured via the `trustee_config` variable in the Terraform module.
Key parameters include:

*   `docker_image`: The URL of the Docker image for the TrusTEE mill.
*   `signed_image_repo`: The repository where the image signature is stored.
*   `app_flags`: A list of command-line flags to pass to the mill application.
*   `machine_type`: The machine type for the instances. `c4d-standard-2` is
    recommended.
*   `replicas`: The number of instances. This can be the same or fewer than the
    number of Honest Majority Share Shuffle (HMSS) mills, as TrusTEE is more
    efficient.

#### Service Account and IAM

The MIG runs with a dedicated service account. The Terraform module
automatically creates this service account and grants it the necessary
permissions, including:

*   `roles/confidentialcomputing.workloadUser`: To use Confidential Space.
*   `roles/logging.logWriter`: To write logs to Cloud Logging.
*   `roles/monitoring.metricWriter`: To write metrics to Cloud Monitoring.
*   `roles/cloudtrace.agent`: To write traces to Cloud Trace.
*   `roles/secretmanager.secretAccessor`: To access the required secrets.
*   `roles/iam.serviceAccountUser`: To allow the VM to run as this service
    account.
*   `roles/storage.objectAdmin`: To read and write computation blobs in the
    Duchy's storage bucket.

#### Network Configuration

The MIG instances are deployed into a specific subnetwork
(`trustee_mill_subnetwork_network`). * **Subnetwork Range**: The CIDR range of
this subnetwork (defined by `trustee_mill_subnetwork_cidr_range`) must be added
to the allow list of the Duchy's internal API server to enable communication. *
**Cloud NAT**: A Cloud NAT is configured to allow the instances to access the
internet (e.g., to pull Docker images and access Google APIs) since they do not
have public IP addresses. * **Private Google Access**: Enabled on the subnetwork
to allow access to Google APIs (including Cloud Storage and Secret Manager)
without external IP addresses.

#### Secrets

The TrusTEE mill requires access to several secrets. These secrets are uploaded
to Google Secret Manager. The Mill application running in the MIG will access
and download them into the container. These secrets are defined in the
`trustee_config` and include:

*   `aggregator_tls_cert`: The TLS certificate for the aggregator.
*   `aggregator_tls_key`: The TLS private key for the aggregator.
*   `aggregator_cert_collection`: The collection of trusted root certificates.
*   `aggregator_cs_cert`: The consent signaling certificate.
*   `aggregator_cs_private`: The consent signaling private key.

Ensure that: * The Docker image for the TrusTEE mill is built and pushed to the
container registry. * The image is signed, and the signature is uploaded to the
repository. * The necessary secrets (TLS certificates and keys) are available in
Secret Manager.

The MIG will automatically start instances running the signed image.

### 3. Update Network Configuration

The TrusTEE mills need to communicate with the Duchy's internal API server. You
must update the network configuration to allow traffic from the TrusTEE mill's
subnetwork.

In your Kubernetes configuration, ensure that the `internal-api-server` service
allows ingress from the `trustee_mill_subnetwork_cidr_range`. Below is a example
using k8s YAML.

```yaml
  spec:
      selector:
        app: aggregator-internal-api-server-app
      ports:
        - port: 8443
          name: grpc-port
      type: LoadBalancer
      loadBalancerSourceRanges:
        - {your_trustee_cidr_range}
```

Additionally, ensure that the network policy for the `internal-api-server`
allows ingress from the `trustee_mill_subnetwork_cidr_range`. Again see the YAML
example below.

```yaml
spec:
  podSelector:
    matchLabels:
      app: aggregator-internal-api-server-app
  ingress:
    - from:
        - ipBlock:
            cidr: {your_trustee_cidr_range}
      ports:
        - port: 8443
```

Deploy the updated Kubernetes configuration to apply the network policies.

### 4. Update Configuration Files

The Duchy needs the `protocols_setup_config` to include the TrusTEE setup
configuration.

Ensure the
[`--protocols-setup-config`](../../src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald/HeraldDaemon.kt)
flag points to the protocols setup config file, and that the referenced file
includes the `trus_tee` configuration block.

The `protocols_setup_config` is defined by the
[`ProtocolsSetupConfig`](../../src/main/proto/wfa/measurement/internal/duchy/config/protocols_setup_config.proto)
message. An example configuration file can be found
[here](../../src/main/k8s/testing/secretfiles/aggregator_protocols_setup_config.textproto).

## Kingdom Setup

The following steps are required to enable TrusTEE in the Kingdom.

### 1. Update Configuration Files

The Kingdom needs the `trustee_protocol_config_config` to be set. This
configuration file defines the parameters for the TrusTEE protocol.

Ensure the
[`--trustee-protocol-config-config`](../../src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/TrusTeeProtocolConfig.kt)
flag is set and points to the correct file.

The `trustee_protocol_config_config` is defined by the
[`TrusTeeProtocolConfigConfig`](../../src/main/proto/wfa/measurement/internal/kingdom/protocol_config_config.proto)
message. An example configuration file can be found
[here](../../src/main/k8s/testing/secretfiles/trustee_protocol_config_config.textproto).

### 2. Enable Feature Flag

Finally, enable the TrusTEE protocol feature flag in the Kingdom. This is
controlled by the
[`--enable-trustee`](../../src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server/V2alphaPublicApiServer.kt)
flag.
