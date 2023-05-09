# How to deploy a Halo Kingdom on GKE

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

-   1 Cloud Spanner database
-   1 GKE cluster
    -   1 Kubernetes secret
    -   1 Kubernetes configmap
    -   3 Kubernetes services
        -   gcp-kingdom-data-server (Cluster IP)
        -   system-api-server (External load balancer)
        -   v2alpha-public-api-server (External load balancer)
    -   3 Kubernetes deployments
        -   gcp-kingdom-data-server-deployment
        -   system-api-server-deployment
        -   v2alpha-public-api-server-deployment
    -   4 Kubernetes network policies
        -   internal-data-server-network-policy
        -   system-api-server-network-policy
        -   public-api-server-network-policy
        -   default-deny-ingress-and-egress

## Step 0. Before You Start

See [Machine Setup](machine-setup.md).

### Google Cloud Project quick start

If you don't have an account with sufficient access to a Google Cloud project,
you can do the following:

1.  [Register](https://console.cloud.google.com/freetrial) a GCP account.
2.  In the [Google Cloud console](https://console.cloud.google.com/), create a
    new Google Cloud project.
3.  Set up Billing of the project in the Console -> Billing page.
4.  Update the project quotas in the Console -> IAM & Admin -> Quotas page If
    necessary. (The default quota might just work since the kingdom doesn't
    consume too many resources). You can skip this step for now and come back
    later if any future operation fails due to "out of quota" errors.

### Cloud Spanner quick start

If you don't have a Cloud Spanner instance in your project, you can do the
following:

1.  Visit the [Spanner](https://console.cloud.google.com/spanner/instances) page
    in Cloud Console.
2.  Enable the `Cloud Spanner API` if you have not done so yet.
3.  Click Create Instance

Note: 100 processing units is the current minimum value. This should be enough
to test things out, but you will likely want to adjust this depending on
expected load.

## Step 1. Create the database

The Kingdom expects its own database within your Spanner instance. The `dev`
configuration assumes that this is named `kingdom`.

You can create a database using the `gcloud` CLI. For example in the `halo-cmms`
instance:

```shell
gcloud spanner databases create kingdom --instance=halo-cmms
```

## Step 2. Build and push the container images

If you aren't using pre-built release images, you can build the images yourself
from source and push them to a container registry. For example, if you're using
the [Google Container Registry](https://cloud.google.com/container-registry),
you would specify `gcr.io` as your container registry and your Cloud project
name as your image repository prefix.

Assuming a project named `halo-kingdom-demo` and an image tag `build-0001`, run
the following to build and push the images:

```shell
bazel run -c opt //src/main/docker:push_all_kingdom_gke_images \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-kingdom-demo --define image_tag=build-0001
```

Tip: If you're using [Hybrid Development](../building.md#hybrid-development) for
containerized builds, replace `bazel build` with `tools/bazel-container build`
and `bazel run` with `tools/bazel-container-run`.

## Step 3. Create resources for the cluster

See [GKE Cluster Configuration](cluster-config.md) for background.

### IAM Service Accounts

We'll want to
[create a least privilege service account](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa)
that our cluster will run under. Follow the steps in the linked guide to do
this.

We'll additionally want to create a service account that we'll use to allow the
internal API server to access the Spanner database. See
[Granting Cloud Spanner database access](cluster-config.md#granting-cloud-spanner-database-access)
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

## Step 4. Create the cluster

Supposing you want to create a cluster named `halo-cmm-kingdom-demo-cluster` for
the Kingdom, running under the `gke-cluster` service account in the
`halo-kingdom-demo` project, the command would be

```shell
gcloud container clusters create halo-cmm-kingdom-demo-cluster \
  --enable-network-policy --workload-pool=halo-kingdom-demo.svc.id.goog \
  --service-account='gke-cluster@halo-kingdom-demo.iam.gserviceaccount.com' \
  --database-encryption-key=projects/halo-cmm-dev/locations/us-central1/keyRings/test-key-ring/cryptoKeys/k8s-secret \
  --num-nodes=3 --enable-autoscaling --min-nodes=3 --max-nodes=6 \
  --machine-type=e2-highcpu-2
```

Adjust the number of nodes and machine type according to your expected usage.
The cluster version should be no older than `1.24.0` in order to support
built-in gRPC health probe.

After creating the cluster, we can configure `kubectl` to be able to access it

```shell
gcloud container clusters get-credentials halo-cmm-kingdom-demo-cluster
```

### Add Metrics to the cluster

See [Metrics Deployment](metrics-deployment.md).

## Step 5. Create K8s service account

In order to use the IAM service account that we created earlier from our
cluster, we need to create a K8s service account and give it access to that IAM
service account.

For example, to create a K8s service account named `internal-server`, run

```shell
kubectl create serviceaccount internal-server
```

Supposing the IAM service account you created in a previous step is named
`kingdom-internal` within the `halo-kingdom-demo` project. You'll need to allow
the K8s service account to impersonate it

```shell
gcloud iam service-accounts add-iam-policy-binding \
  kingdom-internal@halo-kingdom-demo.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:halo-kingdom-demo.svc.id.goog[default/internal-server]"
```

Finally, add an annotation to link the K8s service account to the IAM service
account:

```shell
kubectl annotate serviceaccount internal-server \
    iam.gke.io/gcp-service-account=kingdom-internal@halo-kingdom-demo.iam.gserviceaccount.com
```

## Step 6. Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:kingdom.tar \
  --define google_cloud_project=halo-kingdom-demo \
  --define spanner_instance=halo-cmms \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-kingdom-demo --define image_tag=build-0001
```

Extract the generated archive to some directory.

You can customize this generated object configuration with your own settings
such as the number of replicas per deployment, the memory and CPU requirements
of each container, and the JVM options of each container.

## Step 7. Customize the K8s secret

We use a K8s secret to hold sensitive information, such as private keys.

First, prepare all the files we want to include in the Kubernetes secret. The
`dev` configuration assumes the files have the following names:

1.  `all_root_certs.pem`

    This makes up the TLS trusted root CA store for the Kingdom. It's the
    concatenation of the root CA certificates for all the entites that connect
    to the Kingdom, including:

    *   All Duchies
    *   All EDPs
    *   All MC reporting tools (frontends)
    *   The Kingdom's itself (for traffic between Kingdom servers)

    Supposing your root certs are all in a single folder and end with
    `_root.pem`, you can concatenate them all with a simple shell command:

    ```shell
    cat *_root.pem > all_root_certs.pem
    ```

    Note: This assumes that all your root certificate PEM files end in newline.

1.  `kingdom_root.pem`

    The root certificate of the Kingdom's CA.

1.  `kingdom_tls.pem`

    The Kingdom's TLS certificate.

1.  `kingdom_tls.key`

    The private key for the Kingdom's TLS certificate.

1.  `duchy_cert_config.textproto`

    Configuration mapping Duchy root certificates to the corresponding Duchy ID.

    -   [Example](../../src/main/k8s/testing/secretfiles/duchy_cert_config.textproto)

1.  `duchy_id_config.textproto`

    Configuration mapping external (public) Duchy IDs to internal Duchy IDs.

    -   [Example](../../src/main/k8s/testing/secretfiles/duchy_id_config.textproto)

1.  `llv2_protocol_config_config.textproto`

    Configuration for the Liquid Legions v2 protocol.

    -   [Example](../../src/main/k8s/testing/secretfiles/llv2_protocol_config_config.textproto)

***The private keys are confidential to the Kingdom, and are generated by the
Kingdom's certificate authority (CA).***

Place these files into the `src/main/k8s/dev/kingdom_secret/` path within the
Kustomization directory.

### Secret files for testing

There are some [secret files](../../src/main/k8s/testing/secretfiles) within the
repository. These can be used for testing, but **must not** be used for
production environments as doing so would be highly insecure.

Generate the archive:

```shell
bazel build //src/main/k8s/testing/secretfiles:archive
```

Extract the generated archive to the `src/main/k8s/dev/kingdom_secret/` path
within the Kustomization directory.

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
kubectl apply -k src/main/k8s/dev/kingdom
```

Now all Kingdom components should be successfully deployed to your GKE cluster.
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
NAME                                 READY UP-TO-DATE AVAILABLE AGE
gcp-kingdom-data-server-deployment   1/1   1          1         1m
system-api-server-deployment         1/1   1          1         1m
v2alpha-public-api-server-deployment 1/1   1          1         1m
```

```
NAME                      TYPE         CLUSTER-IP   EXTERNAL-IP  PORT(S)        AGE
gcp-kingdom-data-server   ClusterIP    10.3.245.210 <none>       8443/TCP       14d
kubernetes                ClusterIP    10.3.240.1   <none>       443/TCP        16d
system-api-server         LoadBalancer 10.3.248.13  34.67.15.39  8443:30347/TCP 14d
v2alpha-public-api-server LoadBalancer 10.3.255.191 34.132.87.22 8443:31300/TCP 14d
```

## Step 8. Make the Kingdom accessible on the open internet.

### Reserve the external IPs

There are two public APIs in the kingdom. The `v2alpha-public-api-server` is
called by the EDPs, MPs and MCs. The `system-api-server` is called by the
duchies. As you can see from the result in the previous step. Only these two
services have external IPs. However, these external IPs are ephemeral. We need
to reserve them such that they are stable.

See [Reserving External IPs](cluster-config.md#reserving-external-ips)

### Setup subdomain DNS A record

Update your domains or subdomains, one for the system API and one for the public
API, to point to the two corresponding external IPs.

For example, in the halo dev instance, we have subdomains:

-   `system.kingdom.dev.halo-cmm.org`
-   `public.kingdom.dev.halo-cmm.org`

The domains/subdomains are what the EDPs/MPs/MCs/Duchies use to communicate with
the kingdom.

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

Encryption keys can be generated using the
[Tinkey tool](https://github.com/google/tink/blob/master/docs/TINKEY.md).

### Q2. What if the secret or configuration files need to be updated?

Modify the Kustomization directory and re-apply it.

### Q3. How to test if the kingdom is working properly?

Follow the
["How to complete multi-cluster correctnessTest on GKE"](correctness-test.md)
doc and complete a correctness test using the Kingdom you have deployed.

If you don't want to deploy Duchies and simulators, you can just run
ResourceSetup to see if you can create the resources successfully. If yes, you
can consider the Kingdom is working properly.
