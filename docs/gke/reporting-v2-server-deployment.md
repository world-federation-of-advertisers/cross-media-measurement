# Halo Reporting V2 Server Deployment on GKE

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

Many operations can be done either via the gcloud CLI or the Google Cloud web
console. This guide picks whichever is most convenient for that operation. Feel
free to use whichever you prefer.

### What are we creating/deploying?

-   1 Cloud SQL managed PostgreSQL database
-   2 Cloud Spanner databases
    -   `access`
    -   `reporting`
-   1 GKE cluster
    -   3 Kubernetes secret
        -   `certs-and-configs`
        -   `signing`
        -   `mc-config`
    -   1 Kubernetes configmap
        -   `config-files`
    -   5 Kubernetes services
        -   `postgres-internal-reporting-server` (Cluster IP)
        -   `reporting-v2alpha-public-api-server` (External load balancer)
        -   `reporting-grpc-gateway` (External load balancer)
        -   `access-internal-api-server` (Cluster IP)
        -   `access-public-api-server` (External load balancer)
    -   5 Kubernetes deployments
        -   `postgres-internal-reporting-server-deployment`
        -   `reporting-v2alpha-public-api-server-deployment`
        -   `reporting-grpc-gateway`
        -   `access-internal-api-server`
        -   `access-public-api-server`
    -   1 Kubernetes cron job
        -   `report-scheduling`
    -   7 Kubernetes network policies
        -   `postgres-internal-reporting-server-network-policy`
        -   `reporting-v2alpha-public-api-server-network-policy`
        -   `reporting-grpc-gateway-network-policy`
        -   `report-scheduling-network-policy`
        -   `access-internal-api-server-network-policy`
        -   `access-public-api-server-network-policy`
        -   `default-deny-ingress-and-egress`

## Before you start

See [Machine Setup](machine-setup.md).

## Provision Google Cloud Project infrastructure

This can be done using Terraform. See [the guide](terraform.md) to use the
example configuration for Reporting.

Applying the Terraform configuration will create a new cluster. You can use the
`gcloud` CLI to obtain credentials so that you can access the cluster via
`kubectl`. For example:

```shell
gcloud container clusters get-credentials reporting
```

Applying the Terraform configuration will also create external IP resources and
output the resource names. These will be needed in later steps.

## Add metrics to the cluster (optional)

See [Metrics Deployment](metrics-deployment.md).

## Build and push the container images (not recommended)

If you aren't using pre-built release images, you can build the images yourself
from source and push them to a container registry. For example, if you're using
the [Google Container Registry](https://cloud.google.com/container-registry),
you would specify `gcr.io` as your container registry and your Cloud project
name as your image repository prefix.

Assuming a project named `halo-cmm-dev` and an image tag `build-0001`, run the
following to build and push the images:

```shell
bazel run -c opt //src/main/docker:push_all_reporting_gke_images \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-dev --define image_tag=build-0001
```

Tip: If you're using [Hybrid Development](../building.md#hybrid-development) for
containerized builds, replace `bazel build` with `tools/bazel-container build`
and `bazel run` with `tools/bazel-container-run`.

## Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:reporting_v2.tar \
  --define reporting_public_api_address_name=reporting-v2alpha \
  --define google_cloud_project=halo-cmm-dev \
  --define postgres_instance=dev-postgres \
  --define postgres_region=us-central1 \
  --define kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-reporting-demo --define image_tag=build-0001 \
  --define basic_reports_enabled=true --define spanner_instance=instance
```

Note: The value of the `spanner_instance` variable is only used when
`basic_reports_enabled` is `true`. When `basic_reports` is `false`, you can use
a dummy value for `spanner_instance`.

Extract the generated archive to some directory.

You can customize this generated object configuration with your own settings
such as the number of replicas per deployment, the memory and CPU requirements
of each container, and the JVM options of each container.

## Customize the K8s secrets

We use K8s secrets to hold sensitive information, such as private keys.

### Certificates and signing keys

First, prepare all the files we want to include in the Kubernetes secret. The
`dev` configuration assumes the files have the following names:

1.  `all_root_certs.pem`

    This makes up the trusted root CA store. It's the concatenation of the root
    CA certificates for all the entities that the Reporting server interacts
    with, including:

    *   All Measurement Consumers
    *   Any entity which produces Measurement results (e.g. the Aggregator Duchy
        and Data Providers)
    *   The Kingdom
    *   The Reporting server itself (for internal traffic)

    Supposing your root certs are all in a single folder and end with
    `_root.pem`, you can concatenate them all with a simple shell command:

    ```shell
    cat *_root.pem > all_root_certs.pem
    ```

    Note: This assumes that all your root certificate PEM files end in newline.

1.  `reporting_root.pem`

    The Reporting server's root CA certificate.

1.  `reporting_tls.pem`

    The Reporting server's TLS certificate.

1.  `reporting_tls.key`

    The private key for the Reporting server's TLS certificate.

1.  `access_tls.pem`

    The Access server's TLS certificate.

1.  `access_tls.key`

    The private key for the Access server's TLS certificate.

In addition, you'll need to include the encryption and signing private keys for
the Measurement Consumers that this Reporting server instance needs to act on
behalf of. The encryption keys are assumed to be in Tink's binary keyset format.
The signing private keys are assumed to be DER-encoded unencrypted PKCS #8.

#### Testing keys

There are some [testing keys](../../src/main/k8s/testing/secretfiles) within the
repository. These can be used to create the above secret for testing, but **must
not** be used for production environments as doing so would be highly insecure.

Generate the archive:

```shell
bazel build //src/main/k8s/testing/secretfiles:archive
```

Extract the generated archive to the `src/main/k8s/dev/reporting_v2_secrets/`
path within the Kustomization directory.

### Measurement Consumer config

Contents:

1.  `measurement_consumer_config.textproto`

    [`MeasurementConsumerConfig`](../../src/main/proto/wfa/measurement/config/reporting/measurement_consumer_config.proto)
    protobuf message in text format.

### Generator

Place the above files into the `src/main/k8s/dev/reporting_v2_secrets/` path
within the Kustomization directory.

Create a `kustomization.yaml` file in that path with the following content,
substituting the names of your own keys:

```yaml
secretGenerator:
- name: signing
  files:
  - all_root_certs.pem
  - reporting_root.pem
  - reporting_tls.key
  - reporting_tls.pem
  - access_tls.pem
  - access_tls.key
  - mc_enc_public.tink
  - mc_enc_private.tink
  - mc_cs_private.der
- name: mc-config
  files:
  - measurement_consumer_config.textproto
```

## Customize the K8s ConfigMap

Configuration that may frequently change is stored in a K8s configMap. The `dev`
configuration uses one named `config-files`.

*   `authority_key_identifier_to_principal_map.textproto`
    *   [`AuthorityKeyToPrincipalMap`](../../src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto)
*   `open_id_providers_config.json`
    *   [`OpenIdProvidersConfig`](../../src/main/proto/wfa/measurement/config/access/open_id_providers_config.proto)
*   `encryption_key_pair_config.textproto`
    *   [`EncryptionKeyPairConfig`](../../src/main/proto/wfa/measurement/config/reporting/encryption_key_pair_config.proto)
*   `metric_spec_config.textproto`
    *   [`MetricSpecConfig`](../../src/main/proto/wfa/measurement/config/reporting/metric_spec_config.proto)
*   `known_event_group_metadata_type_set.pb`
    *   Protobuf `FileDescriptorSet` containing known `EventGroup` metadata
        types.
*   `impression_qualification_filter_config.textproto`
    *   [`ImpressionQualificationFilterConfig`](../../src/main/proto/wfa/measurement/config/reporting/impression_qualification_filter_config.proto)
        *   This is unused if the `basic_reports_enabled` variable was set to
            `false` when generating the Kustomization.

Place these files into the `src/main/k8s/dev/reporting_v2_config_files/` path
within the Kustomization directory.

## Apply the K8s Kustomization

Within the Kustomization directory, run

```shell
kubectl apply -k src/main/k8s/dev/reporting_v2
```

Now all components should be successfully deployed to your GKE cluster. You can
verify by running

```shell
kubectl get deployments
```

and

```shell
kubectl get services
```

and

```shell
kubectl get cronjobs
```

You should see something like the following:

```
NAME                                             READY   UP-TO-DATE   AVAILABLE   AGE
postgres-internal-reporting-server-deployment    1/1     1            1           254d
reporting-v2alpha-public-api-server-deployment   1/1     1            1           9m2s

```

```
NAME                                  TYPE           CLUSTER-IP     EXTERNAL-IP    PORT(S)          AGE
kubernetes                            ClusterIP      10.16.32.1     <none>         443/TCP          260d
postgres-internal-reporting-server    ClusterIP      10.16.39.47    <none>         8443/TCP         254d
reporting-v2alpha-public-api-server   LoadBalancer   10.16.32.255   34.135.79.68   8443:30104/TCP   8m45s
```

```
NAME                       SCHEDULE     SUSPEND   ACTIVE   LAST SCHEDULE   AGE
report-scheduling-cronjob  30 6 * * *   False     0        <none>          10m
```

## Appendix

### Configuring the Report Scheduling CronJob

See [Updating Retention Policies](../operations/updating-retention-policies.md)
for an example.

### Troubleshooting

*   `notAuthorized` error

    You see an error that looks something like this:

    ```
    {
      "code": 403,
      "errors": [
        {
          "domain": "global",
          "message": "The client is not authorized to make this request.",
          "reason": "notAuthorized"
        }
      ],
      "message": "The client is not authorized to make this request."
    }
    ```

    Make sure that your Cloud SQL instance has the `cloudsql.iam_authentication`
    flag set to `On`, and that you've followed all the steps for using Workload
    Identity and IAM Authentication. See the
    [Workload Identity](cluster-config.md#workload-identity) section in the
    Cluster Configuration doc.

    If you believe you have everything configured correctly, try deleting and
    recreating the IAM service account for DB access. Apparently there's a
    glitch with Cloud SQL that this sometimes resolves.

### Authentication

The default configuration supports both client certificates (mTLS) and RFC 9068
OAuth 2.0 access tokens. For testing purposes, you can use the
[OpenIdProvider tool](../../src/main/kotlin/org/wfanet/measurement/common/tools)
to generate access tokens.

### Manual testing via CLI

The
[`Reporting`](../../src/main/kotlin/org/wfanet/measurement/reporting/service/api/v2alpha/tools)
CLI tool can be used for manual testing as well as examples of how to call the
API.

### HTTP/REST

The public API is a set of gRPC services following the
[API Improvement Proposals](https://google.aip.dev/). The
`reporting-grpc-gateway` Service exposes a
[gRPC-Gateway](https://grpc-ecosystem.github.io/grpc-gateway/) instance
providing a RESTful HTTP interface. You can call this using any HTTP client
using an access token for authentication.
