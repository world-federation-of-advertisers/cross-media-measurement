# How to deploy a Halo Duchy on GKE

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

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

## What are we creating/deploying?

For a Duchy named `worker1`:

-   1 Cloud Spanner database
-   1 Cloud Storage bucket
-   1 GKE cluster
-   1 Kubernetes secret
-   1 Kubernetes configmap
-   4 Kubernetes services
    -   worker1-async-computation-control-server (Cluster IP)
    -   worker1-computation-control-server (External load balancer)
    -   worker1-requisition-fulfillment-server (External load balancer)
    -   worker1-spanner-computations-server (Cluster IP)
-   6 Kubernetes deployments
    -   worker1-async-computation-control-server-deployment (gRPC service)
    -   worker1-computation-control-server-deployment (gRPC service)
    -   worker1-herald-daemon-deployment (Daemon Job)
    -   worker1-liquid-legions-v2-mill-daemon-deployment (Daemon Job)
    -   worker1-requisition-fulfillment-server-deployment (gRPC service)
    -   worker1-spanner-computations-server-deployment (gRPC service)
-   8 Kubernetes network policies
    -   worker1-async-computation-controls-server-network-policy
    -   worker1-computation-control-server-network-policy
    -   worker1-herald-daemon-network-policy
    -   worker1-liquid-legions-v2-mill-daemon-network-policy
    -   worker1-push-spanner-schema-job-network-policy
    -   worker1-requisition-fulfillment-server-network-policy
    -   worker1-spanner-computations-server-network-policy
    -   default-deny-ingress-and-egress

## Step 0. Before You Start

Follow Step 0 of the
[Kingdom deployment guide](kingdom-deployment.md#step-0-before-you-start).

## Step 1. Register your duchy with the kingdom (offline)

In order to join the Cross-Media Measurement System, the Duchy needs to first be
registered with the Kingdom. This will be done offline with the help from the
Kingdom operator.

The Duchy operator needs to share the following information with the Kingdom
operator:

-   The name (a string, used as an ID) of the Duchy (unique amongst all Duchies)
-   The CA ("root") certificate
-   A consent signaling ("leaf") certificate

The Kingdom operator will
[register all corresponding resources](../operations/creating-resources.md) for
the Duchy via internal admin tools. The resource names will be shared with the
Duchy operator.

## Step 2. Create the database

The Duchy expects its own database within your Spanner instance. The `dev`
configuration assumes that this is named `<duchy-name>_duchy_computations`.

You can create a database using the `gcloud` CLI. For example with a Duchy named
`worker1` in the `halo-cmms` instance:

```shell
gcloud spanner databases create worker1_duchy_computations \
  --instance=halo-cmms
```

## Step 3. Create the Cloud Storage Bucket

Each Duchy needs a storage bucket. One can be created from the
[Console](https://console.cloud.google.com/storage/browser). Note that bucket
names are public, globally unique, and cannot be changed once created. See
[Bucket naming guidelines](https://cloud.google.com/storage/docs/naming-buckets).

As the data in this bucket need not be exposed to the public internet, select
"Enforce public access prevention on this bucket".

## Step 4. Build and push the container images

If you aren't using pre-built release images, you can build the images yourself
from source and push them to a container registry. For example, if you're using
the [Google Container Registry](https://cloud.google.com/container-registry),
you would specify `gcr.io` as your container registry and your Cloud project
name as your image repository prefix.

Assuming a project named `halo-worker1-demo` and an image tag `build-0001`, run
the following to build and push the images:

```shell
bazel run -c opt //src/main/docker:push_all_duchy_gke_images \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-worker1-demo --define image_tag=build-0001
```

Tip: If you're using [Hybrid Development](../building.md#hybrid-development) for
containerized builds, replace `bazel build` with `tools/bazel-container build`
and `bazel run` with `tools/bazel-container-run`.

## Step 5. Create the Cluster

Follow the steps to
[create resources for the cluster](kingdom-deployment.md#step-3-create-resources-for-the-cluster)
from the Kingdom deployment guide.

Create an additional service account for storage, granting it the Storage Object
Admin role on the Storage bucket. See
[Granting Cloud Storage bucket access](cluster-config.md#granting-cloud-storage-bucket-access).

Enable the Kubernetes API in the console if your account hasn't done it. To
create a cluster named `worker1-duchy` in the `halo-worker1-demo` project, run
the following command:

```shell
gcloud container clusters create worker1-duchy \
  --enable-network-policy --workload-pool=halo-worker1-demo.svc.id.goog \
  --service-account="gke-cluster@halo-worker1-demo.iam.gserviceaccount.com" \
  --database-encryption-key=projects/halo-worker1-demo/locations/us-central1/keyRings/test-key-ring/cryptoKeys/k8s-secret \
  --num-nodes=2 --enable-autoscaling --min-nodes=2 --max-nodes=4 \
  --machine-type=e2-standard-2
```

Adjust the node pools based on your expected usage. You may wish to use GKE
features such as autoscaling or multiple node pools with different
machine/scheduling types. The default Mill and Herald configuration include a
toleration for running on
[Spot VMs](https://cloud.google.com/kubernetes-engine/docs/how-to/spot-vms#use_taints_and_tolerations_for).

The cluster version should be no older than `1.24.0` in order to support
built-in gRPC health probe.

To configure `kubectl` to access this cluster, run

```shell
gcloud container clusters get-credentials worker1-duchy
```

Now you can follow the steps for
[creating K8s service accounts](kingdom-deployment.md) from the Kingdom
deployment guide. Note that you'll need to follow the steps twice for the two
service accounts. The `dev` configuration assumes that they are named
`internal-server` and `storage`.

### Add Metrics to the cluster

See [Metrics Deployment](metrics-deployment.md).

## Step 6. Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:worker1_duchy.tar \
  --define kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  --define google_cloud_project=halo-kingdom-demo \
  --define spanner_instance=halo-cmms \
  --define duchy_cert_id=SVVse4xWHL0 \
  --define duchy_storage_bucket=worker1-duchy \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-worker1-demo --define image_tag=build-0001
```

Extract the generated archive to some directory.

You can customize this generated object configuration with your own settings
such as the number of replicas per deployment, the memory and CPU requirements
of each container, and the JVM options of each container.

## Step 7. Customize the K8s secret

We use a K8s secret to hold sensitive information, such as private keys.

The Duchy binaries are configured to read certificates and config files from a
mounted Kubernetes secret volume.

Prepare all the files we want to include in the Kubernetes secret. The following
files are required in a Duchy:

1.  `all_root_certs.pem`

    This makes up the TLS trusted CA store for the Duchy. It's the concatenation
    of the CA ("root") certificates for all the entites that connect to the
    Duchy, including:

    -   All other Duchies
    -   EDPs that select to fulfill requisitions at this Duchy
    -   This Duchy's own CA certificate (for Duchy internal traffic)

    Supposing your root certs are all in a single folder and end with
    `_root.pem`, you can concatenate them all with a simple shell command:

    ```shell
    cat *_root.pem > all_root_certs.pem
    ```

1.  `worker1_tls.pem`

    The `worker1` Duchy's TLS certificate in PEM format.

1.  `worker1_tls.key`

    The private key for the TLS certificate in PEM format.

1.  `worker1_cs_cert.der`

    The `worker1` Duchy's consent signaling certificate in DER format.

1.  `worker1_cs_private.der`

    The private key for the Duchy's consent signaling certificate in DER format.

1.  `duchy_cert_config.textproto`

    Configuration mapping Duchy root certificates to the corresponding Duchy ID.

    -   [Example](../../src/main/k8s/testing/secretfiles/duchy_cert_config.textproto)

1.  `xxx_protocols_setup_config.textproto` (replace xxx with the role)

    -   This contains information about the protocols run in the duchy
    -   Set the role (aggregator or non_aggregator) in the config appropriately
    -   [Example](../../src/main/k8s/testing/secretfiles/aggregator_protocols_setup_config.textproto)

Place these files into the `src/main/k8s/dev/worker1_duchy_secret/` path within
the Kustomization directory.

### Secret files for testing

There are some [secret files](../../src/main/k8s/testing/secretfiles) within the
repository. These can be used for testing, but **must not** be used for
production environments as doing so would be highly insecure.

Generate the archive:

```shell
bazel build //src/main/k8s/testing/secretfiles:archive
```

Extract the generated archive to the `src/main/k8s/dev/worker1_duchy_secret/`
path within the Kustomization directory.

## Step 8. Customize the K8s configMap

Configuration that may frequently change is stored in a K8s configMap. The `dev`
configuration uses one named `config-files` containing the file
`authority_key_identifier_to_principal_map.textproto`.

Place this file in the `src/main/k8s/dev/config_files/` path within the
Kustomization directory.

See [Creating Resources](../operations/creating-resources.md) for information on
this file format.

## Step 9. Apply the K8s Kustomization

Use `kubectl` to apply the Kustomization. From the Kustomization directory run:

```shell
kubectl apply -k src/main/k8s/dev/worker1_duchy
```

Now all Duchy components should be successfully deployed to your GKE cluster.
You can verify by running

```shell
kubectl get deployments
```

and

```shell
kubectl get services
```

You should see something like the following:

```
NAME                                                READY UP-TO-DATE AVAILABLE AGE
worker1-async-computation-control-server-deployment 1/1   1          1         1m
worker1-computation-control-server-deployment       1/1   1          1         1m
worker1-herald-daemon-deployment                    1/1   1          1         1m
worker1-liquid-legions-v2-mill-daemon-deployment    1/1   1          1         1m
worker1-requisition-fulfillment-server-deployment   1/1   1          1         1m
worker1-spanner-computations-server-deployment      1/1   1          1         1m
```

```
NAME                                     TYPE         CLUSTER-IP     EXTERNAL-IP    PORT(S)        AGE
worker1-async-computation-control-server ClusterIP    10.123.249.255 <none>         8443/TCP       1m
worker1-computation-control-server       LoadBalancer 10.123.250.81  34.134.198.198 8443:31962/TCP 1m
worker1-requisition-fulfillment-server   LoadBalancer 10.123.247.78  35.202.201.111 8443:30684/TCP 1m
worker1-spanner-computations-server      ClusterIP    10.123.244.10  <none>         8443/TCP       1m
kubernetes                               ClusterIP    10.123.240.1   <none>         443/TCP        1m
```

## Step 10. Make the Duchy accessible on the open internet

### Reserve the external IPs

There are two external APIs in the duchy. The
`worker1-requisition-fulfillment-server` (a.k.a. the public API) is called by
the EDPs to fulfill their requisitions. The `worker1-computation-control-server`
(a.k.a. the system API) is called by the other duchies to send computation
related data. As you can see from the result in the previous step. Only these
two services have external IPs. However, these external IPs are ephemeral. We
need to reserve them such that they are stable.

For example, in the halo dev instance, we have subdomains:

-   `public.worker1.dev.halo-cmm.org`
-   `system.worker1.dev.halo-cmm.org`

The domains/subdomains are what the EDPs and other duchies use to communicate
with the duchy.

See [Reserving External IPs](cluster-config.md#reserving-external-ips)

## Q/A

### Q1. How to generate certificates/key pairs?

You can use any certificate authority (CA) you wish, but the simplest if you're
deploying on GKE is the
[Cloud Certificate Authority Service](https://console.cloud.google.com/security/cas/caPools).

Certificate requirements:

*   Support both client and server TLS.
*   Include the following DNS hostnames in the subject alternative name (SAN)
    extension:
    *   The hostnames for any external IPs. For example, our dev Kingdom
        certificates have`*.kingdom.dev.halo-cmm.org` to cover both
        `public.kingdom.dev.halo-cmm.org` and `system.kingdom.dev.halo-cmm.org`
    *   `localhost` (some of our configurations assume this)

You can use the OpenSSL CLI to convert between formats as-needed (`openssl x509`
for certificates and `openssl pkcs8` for private keys).

Encryption keys can be generated using the
[Tinkey tool](https://github.com/google/tink/blob/master/docs/TINKEY.md).

### Q2. How to test if the duchy is working properly?

Follow the
["How to complete multi-cluster correctnessTest on GKE"](correctness-test.md)
doc and complete a correctness test using the duchy you have deployed.
