# Enabling TrusTEE in Duchy

This guide describes how to enable the TrusTEE protocol in a Duchy.

## Overview

Enabling TrusTEE involves the following steps:

1.  **Update GCP Resources**: Configure the necessary infrastructure.
2.  **Run TrusTEE Mills**: Deploy the TrusTEE mills in a Managed Instance Group (MIG) using Confidential Space with a signed image.
3.  **Update Network Configuration**: Allow access from the TrusTEE mill's network to the aggregator's internal service.
4.  **Update Configuration Files**: Update the Kingdom and Duchy configuration files to include TrusTEE parameters.
5.  **Enable Feature Flag**: Enable the TrusTEE feature flag in the Kingdom.

## 1. Update GCP Resources

You need to update your infrastructure to include the Trustee Mill resources. You can refer to the [Duchy Terraform Example](../../src/main/terraform/gcloud/examples/duchy/README.md) as a reference implementation, which has been tested in development environments.

Key resources include:
*   Managed Instance Group (MIG) for running the mills.
*   Service accounts and IAM roles.
    *   The Trustee Mill service account requires `roles/storage.objectAdmin` on the Duchy's storage bucket to read and write computation blobs.
*   Networking components (Subnetwork, Cloud Router, Cloud NAT).
    *   Ensure the `trustee_mill_subnetwork_cidr_range` does not conflict with existing GKE cluster subnets. A default of `10.127.0.0/24` is suggested to avoid common conflicts.
*   Secret Manager secrets.

## 2. Run TrusTEE Mills

The TrusTEE mills run in a Managed Instance Group (MIG) using Confidential Space. The Docker image for the mill must be signed, and the signature must be available in the specified repository.

### MIG Configuration

The MIG is configured via the `trustee_config` variable in the Terraform module. Key parameters include:

*   `docker_image`: The URL of the Docker image for the TrusTEE mill.
*   `signed_image_repo`: The repository where the image signature is stored.
*   `app_flags`: A list of command-line flags to pass to the mill application.

### Service Account and IAM

The MIG runs with a dedicated service account. The Terraform module automatically creates this service account and grants it the necessary permissions, including:

*   `roles/confidentialcomputing.workloadUser`: To use Confidential Space.
*   `roles/logging.logWriter`: To write logs to Cloud Logging.
*   `roles/monitoring.metricWriter`: To write metrics to Cloud Monitoring.
*   `roles/cloudtrace.agent`: To write traces to Cloud Trace.
*   `roles/secretmanager.secretAccessor`: To access the required secrets.
*   `roles/iam.serviceAccountUser`: To allow the VM to run as this service account.
*   `roles/storage.objectAdmin`: To read and write computation blobs in the Duchy's storage bucket.

### Network Configuration

The MIG instances are deployed into a specific subnetwork (`trustee_mill_subnetwork_network`).
*   **Subnetwork Range**: The CIDR range of this subnetwork (defined by `trustee_mill_subnetwork_cidr_range`) must be added to the allow list of the Duchy's internal API server to enable communication.
*   **Cloud NAT**: A Cloud NAT is configured to allow the instances to access the internet (e.g., to pull Docker images and access Google APIs) since they do not have public IP addresses.
*   **Private Google Access**: Enabled on the subnetwork to allow access to Google APIs (including Cloud Storage and Secret Manager) without external IP addresses.

### Secrets

The TrusTEE mill requires access to several secrets. These secrets are uploaded to Google Secret Manager. The Mill application running in the MIG will access and download them into the container. These secrets are defined in the `trustee_config` and include:

*   `aggregator_tls_cert`: The TLS certificate for the aggregator.
*   `aggregator_tls_key`: The TLS private key for the aggregator.
*   `aggregator_cert_collection`: The collection of trusted root certificates.
*   `aggregator_cs_cert`: The consent signaling certificate.
*   `aggregator_cs_private`: The consent signaling private key.

Ensure that:
*   The Docker image for the TrusTEE mill is built and pushed to the container registry.
*   The image is signed, and the signature is uploaded to the repository.
*   The necessary secrets (TLS certificates and keys) are available in Secret Manager.

The MIG will automatically start instances running the signed image.

## 3. Update Network Configuration

The TrusTEE mills need to communicate with the Duchy's internal API server. You must update the network configuration to allow traffic from the TrusTEE mill's subnetwork.

In your Kubernetes configuration (e.g., `duchy_gke.cue`), ensure that the `internal-api-server` service allows ingress from the `trustee_mill_subnetwork_cidr_range`.

Example configuration in `duchy_gke.cue`:

```cue
	services: {
		"internal-api-server": {
            // ...
			if _trusteeMillSubnetworkCidrRange != _|_ {
				spec: {
					type: "LoadBalancer"
					loadBalancerSourceRanges: [
						_trusteeMillSubnetworkCidrRange,
					]
				}
			}
		}
	}
```

Additionally, ensure that the network policy for the `internal-api-server` allows ingress from the `trustee_mill_subnetwork_cidr_range`.

```cue
	networkPolicies: {
		"internal-api-server": {
            // ...
			if _trusteeMillSubnetworkRange != _|_ {
				_ingresses: gRpc: {
					from: [{
						ipBlock: {
							cidr: _trusteeMillSubnetworkRange
						}
					}]
					ports: [{
						port: #GrpcPort
					}]
				}
			}
            // ...
		}
    }
```

Deploy the updated Kubernetes configuration to apply the network policies.

## 4. Update Configuration Files

Both the Kingdom and the Duchy require configuration updates to support the TrusTEE protocol.

### Kingdom Configuration

The Kingdom needs the `trustee_protocol_config_config` to be set. This configuration file defines the parameters for the TrusTEE protocol.

Ensure the [`--trustee-protocol-config-config`](../../src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/TrusTeeProtocolConfig.kt) flag is set and points to the correct file.

### Duchy Configuration

The Duchy needs the `protocols_setup_config` to include the TrusTEE setup configuration.

Ensure the [`--protocols-setup-config`](../../src/main/kotlin/org/wfanet/measurement/duchy/deploy/common/daemon/herald/HeraldDaemon.kt) flag points to the protocols setup config file, and that the referenced file includes the `trus_tee` configuration block.

## 5. Enable Feature Flag

Finally, enable the TrusTEE protocol feature flag in the Kingdom. This is controlled by the [`--enable-trustee`](../../src/main/kotlin/org/wfanet/measurement/kingdom/deploy/common/server/V2alphaPublicApiServer.kt) flag.
