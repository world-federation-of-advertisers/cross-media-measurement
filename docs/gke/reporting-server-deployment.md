# Halo Reporting Server Deployment on GKE

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

The `dev` configuration uses the
[Container Registry](https://cloud.google.com/container-registry) to store our
docker images. Enable the Google Container Registry API in the console if you
haven't done it. If you use other repositories, adjust the commands accordingly.

Assuming a project named `halo-cmm-dev` and an image tag `build-0001`, run the
following to build and push the images:

```shell
bazel query 'filter("reporting", kind("container_push", //src/main/docker:all))' |
  xargs bazel build -c opt --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-dev --define image_tag=build-0001
```

and then push them:

```shell
bazel query 'filter("reporting", kind("container_push", //src/main/docker:all))' |
  xargs -n 1 bazel run -c opt --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-dev --define image_tag=build-0001
```

Tip: If you're using [Hybrid Development](../building.md#hybrid-development) for
containerized builds, replace `bazel build` with `tools/bazel-container build`
and `bazel run` with `tools/bazel-container-run`. You'll also want to pass the
`-o` option to `xargs`.

Note: You may want to add a specific tag for the images in your container
registry.

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

## Create the K8s Secrets

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

```shell
bazel run //src/main/k8s/testing/secretfiles:apply_kustomization
```

### Measurement Consumer config

Contents:

1.  `measurement_consumer_config.textproto`

    [`MeasurementConsumerConfig`](../../src/main/proto/wfa/measurement/config/reporting/measurement_consumer_config.proto)
    protobuf message in text format.

### Generator

To generate secrets, put the files in a directory and create a
`kustomization.yaml` file within it to specify the contents of each secret.

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

Apply the above to create the secrets:

```shell
kubectl apply -k <path-to-the-above-folder>
```

The generated secret names will be suffixed with a hash. We'll assume `abcdef`
is the hash for convenience. If you lose track of the secret names, you can find
them again using

```shell
kubectl get secrets
```

## Create the K8s ConfigMap

Configuration that may frequently change is stored in a K8s configMap. The `dev`
configuration uses one named `config-files`, which contains configuration files
in
[protobuf text format](https://developers.google.com/protocol-buffers/docs/text-format-spec).

*   `authority_key_identifier_to_principal_map.textproto` -
    [`AuthorityKeyToPrincipalMap`](../../src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto)
*   `encryption_key_pair_config.textproto` -
    [`EncryptionKeyPairConfig`](../../src/main/proto/wfa/measurement/config/reporting/encryption_key_pair_config.proto)

You can pass all of these files to the `kubectl create configmap` command:

```shell
kubectl create configmap config-files \
  --from-file=authority_key_identifier_to_principal_map.textproto \
  --from-file=encryption_key_pair_config.textproto
```

## Create the K8s manifest

Deploying to the cluster is generally done by applying a K8s manifest. You can
use the `dev` configuration as a base to get started. The `dev` manifest is a
YAML file that is generated from files written in [CUE](https://cuelang.org/)
using Bazel rules.

The main file for the `dev` Reporting server is
[`reporting_gke.cue`](../../src/main/k8s/dev/reporting_gke.cue). Some
configuration is in [`config.cue`](../../src/main/k8s/dev/config.cue) You can
modify these file to specify your own values for your own DB instance. **Do
not** push your modifications to the repository.

For example,

```cue
#GCloudProject: "foo-measurement"
#PostgresConfig: {
    project:  #GCloudProject
    instance: "psql"
    region:   "europe-west2"
}
```

You can also modify things such as the memory and CPU request/limit of each pod,
as well as the number of replicas per deployment.

To generate the YAML manifest from the CUE files, run the following
(substituting your own secret name):

```shell
bazel build //src/main/k8s/dev:reporting_gke \
  --define=k8s_reporting_secret_name=signing-abcdef
  --define=k8s_reporting_mc_config_secret_name=mc-config-abcdef \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-dev --define image_tag=build-0001
```

You can also do your customization to the generated YAML file rather than to the
CUE file.

Note: The `dev` configuration does not specify a tag or digest for the container
images. You likely want to change this for a production environment.

## Apply the K8s manifest

If you're using a manifest generated by the above Bazel target, the command to
apply that manifest is

```shell
kubectl apply -f bazel-bin/src/main/k8s/dev/reporting_gke.yaml
```

Substitute that path if you're using a different K8s manifest.

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
NAME                                        READY   UP-TO-DATE   AVAILABLE   AGE
postgres-reporting-data-server-deployment   1/1     1            1           16h
v1alpha-public-api-server-deployment        1/1     1            1           16h
```

```
NAME                             TYPE           CLUSTER-IP     EXTERNAL-IP    PORT(S)          AGE
kubernetes                       ClusterIP      10.16.32.1     <none>         443/TCP          6d21h
postgres-reporting-data-server   ClusterIP      10.16.39.47    <none>         8443/TCP         20h
v1alpha-public-api-server        LoadBalancer   10.16.46.241   34.135.79.68   8443:30290/TCP   20h
```

## Reserve an external IP

The `v1alpha-public-api-server` has an external load balancer IP so that it can
be accessed from outside the cluster. By default, the assigned IP address is
ephemeral. We can reserve a static IP to make it easier to access. See
[Reserving External IPs](cluster-config.md#reserving-external-ips).

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
