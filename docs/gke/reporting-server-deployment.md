# Halo Reporting Server Deployment on GKE

## Important Note

This is the deployment guide for the old V1. For the new V2, see
[Reporting V2](reporting-v2-server-deployment.md).

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

Many operations can be done either via the gcloud CLI or the Google Cloud web
console. This guide picks whichever is most convenient for that operation. Feel
free to use whichever you prefer.

### What are we creating/deploying?

-   1 Cloud SQL managed PostgreSQL database
-   1 GKE cluster
    -   1 Kubernetes secret
        -   `certs-and-configs`
    -   1 Kubernetes configmap
        -   `config-files`
    -   2 Kubernetes services
        -   `postgres-reporting-data-server` (Cluster IP)
        -   `v1alpha-public-api-server` (External load balancer)
    -   2 Kubernetes deployments
        -   `postgres-reporting-data-server-deployment`
        -   `v1alpha-public-api-server-deployment`
    -   3 Kubernetes network policies
        -   `internal-data-server-network-policy`
        -   `public-api-server-network-policy`
        -   `default-deny-ingress-and-egress`

## Before you start

See [Machine Setup](machine-setup.md).

### Managed PostgreSQL Quick Start

If you don't have a managed PostgreSQL instance in your project, you can create
one in the
[Cloud Console](https://console.cloud.google.com/sql/instances/create;engine=PostgreSQL).
For the purposes of this guide, we assume the instance ID is `dev-postgres`.

Make sure that the instance has the `cloudsql.iam_authentication` flag set to
`On`. Set the machine type and storage based on your expected usage.

## Create the database

The Reporting server expects its own database within your PostgreSQL instance.
You can create one with the `gcloud` CLI. For example, a database named
`reporting` in the `dev-postgres` instance.

```shell
gcloud sql databases create reporting --instance=dev-postgres
```

## Build and push the container images

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

## Create resources for the cluster

See [GKE Cluster Configuration](cluster-config.md) for background.

### IAM Service Accounts

We'll want to
[create a least privilege service account](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa)
that our cluster will run under. Follow the steps in the linked guide to do
this.

We'll additionally want to create a service account that we'll use to allow the
internal API server to access the database. See
[Granting Cloud SQL database access](cluster-config.md#granting-cloud-sql-instance-access)
for how to make sure this service account has the appropriate role.

### KMS key for secret encryption

Follow the steps in
[Create a Cloud KMS key](https://cloud.google.com/kubernetes-engine/docs/how-to/encrypting-secrets#creating-key)
to create a KMS key and grant permission to the GKE service agent to use it.

Let's assume we've created a key named `k8s-secret` in a key ring named
`test-key-ring` in the `us-central1` region under the `halo-cmm-dev` project.
The resource name would be the following:
`projects/halo-cmm-dev/locations/us-central1/keyRings/test-key-ring/cryptoKeys/k8s-secret`.
We'll use this when creating the cluster.

Tip: For convenience, there is a "Copy resource name" action on the key in the
Cloud console.

## Create the cluster

See [GKE Cluster Configuration](cluster-config.md) for tips on cluster creation
parameters, or follow the quick start instructions below.

After creating the cluster, we can configure `kubectl` to be able to access it

```shell
gcloud container clusters get-credentials reporting
```

### Add Metrics to the cluster

See [Metrics Deployment](metrics-deployment.md).

### Quick start

Supposing you want to create a cluster named `reporting` for the Reporting
server, running under the `gke-cluster` service account in the `halo-cmm-dev`
project, the command would be

```shell
gcloud container clusters create reporting \
  --enable-network-policy --workload-pool=halo-cmm-dev.svc.id.goog \
  --service-account="gke-cluster@halo-cmm-dev.iam.gserviceaccount.com" \
  --database-encryption-key=projects/halo-cmm-dev/locations/us-central1/keyRings/test-key-ring/cryptoKeys/k8s-secret \
  --num-nodes=3 --enable-autoscaling --min-nodes=2 --max-nodes=4 \
  --machine-type=e2-small
```

Adjust the number of nodes and machine type according to your expected usage.
The cluster version should be no older than `1.24.0` in order to support
built-in gRPC health probe.

## Create the K8s ServiceAccount

In order to use the IAM service account that we created earlier from our
cluster, we need to create a K8s ServiceAccount and give it access to that IAM
service account.

For example, to create a K8s ServiceAccount named `internal-reporting-server`,
run

```shell
kubectl create serviceaccount internal-reporting-server
```

Supposing the IAM service account you created in a previous step is named
`reporting-internal` within the `halo-cmm-dev` project. You'll need to allow the
K8s service account to impersonate it

```shell
gcloud iam service-accounts add-iam-policy-binding \
  reporting-internal@halo-cmm-dev.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:halo-cmm-dev.svc.id.goog[default/internal-reporting-server]"
```

Finally, add an annotation to link the K8s service account to the IAM service
account:

```shell
kubectl annotate serviceaccount internal-reporting-server \
    iam.gke.io/gcp-service-account=reporting-internal@halo-cmm-dev.iam.gserviceaccount.com
```

## Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:reporting.tar \
  --define reporting_public_api_address_name=reporting-v1alpha \
  --define google_cloud_project=halo-cmm-dev \
  --define postgres_instance=dev-postgres \
  --define postgres_region=us-central1 \
  --define kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-kingdom-demo --define image_tag=build-0001
```

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

1.  `reporting_tls.pem`

    The Reporting server's TLS certificate.

1.  `reporting_tls.key`

    The private key for the Reporting server's TLS certificate.

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

Extract the generated archive to the `src/main/k8s/dev/reporting_secrets/` path
within the Kustomization directory.

### Measurement Consumer config

Contents:

1.  `measurement_consumer_config.textproto`

    [`MeasurementConsumerConfig`](../../src/main/proto/wfa/measurement/config/reporting/measurement_consumer_config.proto)
    protobuf message in text format.

### Generator

Place the above files into the `src/main/k8s/dev/reporting_secrets/` path within
the Kustomization directory.

Create a `kustomization.yaml` file in that path with the following content,
substituting the names of your own keys:

```yaml
secretGenerator:
- name: signing
  files:
  - all_root_certs.pem
  - reporting_tls.key
  - reporting_tls.pem
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
*   `encryption_key_pair_config.textproto`
    *   [`EncryptionKeyPairConfig`](../../src/main/proto/wfa/measurement/config/reporting/encryption_key_pair_config.proto)
*   `measurement_spec_config.textproto`
    *   [`MeasurementSpecConfig`](../../src/main/proto/wfa/measurement/config/reporting/measurement_spec_config.proto)
*   `known_event_group_metadata_type_set.pb`
    *   Protobuf `FileDescriptorSet` containing known `EventGroup` metadata
        types.

Place these files into the `src/main/k8s/dev/reporting_config_files/` path
within the Kustomization directory.

## Apply the K8s Kustomization

Within the Kustomization directory, run

```shell
kubectl apply -k src/main/k8s/dev/reporting
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

You should see something like the following:

```
NAME                                             READY   UP-TO-DATE   AVAILABLE   AGE
postgres-reporting-data-server-deployment        1/1     1            1           254d
reporting-public-api-v1alpha-server-deployment   1/1     1            1           9m2s
```

```
NAME                                  TYPE           CLUSTER-IP     EXTERNAL-IP    PORT(S)          AGE
kubernetes                            ClusterIP      10.16.32.1     <none>         443/TCP          260d
postgres-reporting-data-server        ClusterIP      10.16.39.47    <none>         8443/TCP         254d
reporting-public-api-v1alpha-server   LoadBalancer   10.16.32.255   34.135.79.68   8443:30104/TCP   8m45s
```

## Appendix

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

### Manual testing via CLI

The
[`Reporting`](../../src/main/kotlin/org/wfanet/measurement/reporting/service/api/v1alpha/tools)
CLI tool can be used for manual testing as well as examples of how to call the
API.

### HTTP/REST

The public API is a set of gRPC services following the
[API Improvement Proposals](https://google.aip.dev/). These can be exposed as an
HTTP REST API using the
[gRPC Gateway](https://github.com/grpc-ecosystem/grpc-gateway). The service
definitions include the appropriate protobuf annotations for this purpose.
