# How to deploy a Halo Duchy on EKS

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Amazon Elastic Kubernetes Service (EKS)
on another AWS Cloud project.

***Disclaimer***:

-   This guide is just one way of achieving the goal, not necessarily the best
    approach.
-   Almost all steps can be done via either the
    [AWS Cloud Console](https://console.aws.amazon.com/) UI or the
    [`aws` CLI](https://aws.amazon.com/cli/). The doc picks
    the easier one for each step. But you are free to do it in an alternative
    way.
-   All names used in this doc can be replaced with something else. We use
    specific names in the doc for ease of reference.
-   All quotas and resource configs are just examples, adjust the quota and size
    based on the actual usage.
-   In the doc, we assume we are deploying to a single region, i.e. us-west-2.
    If you are deploying to another region or multiple regions, just need to
    adjust each step mentioning "region" accordingly.

## What are we creating/deploying?

See the
[example Terraform configuration](../../src/main/terraform/aws/examples/duchy)
to see what resources are created.

For a Duchy named `worker2`, the cluster will be populated with the following:

-   Secret
    -   `certs-and-configs-<hash>`
-   ConfigMap
    -   `config-files-<hash>`
-   Deployment
    -   `worker2-async-computation-control-server-deployment`
    -   `worker2-computation-control-server-deployment`
    -   `worker2-herald-daemon-deployment`
    -   `worker2-liquid-legions-v2-mill-daemon-deployment`
    -   `worker2-requisition-fulfillment-server-deployment`
    -   `worker2-spanner-computations-server-deployment`
-   Service
    -   `worker2-async-computation-control-server` (Cluster IP)
    -   `worker2-spanner-computations-server`
    -   `worker2-requisition-fulfillment-server` (External load balancer)
    -   `v2alpha-public-api-server` (External load balancer)
-   CronJob
    -   `worker2-computations-cleaner-cronjob`
-   NetworkPolicy
    -   `default-deny-ingress-and-egress`
    -   `worker2-async-computation-controls-server-network-policy`
    -   `worker2-computation-control-server-network-policy`
    -   `worker2-computations-cleaner-network-policy`
    -   `worker2-herald-daemon-network-policy`
    -   `worker2-liquid-legions-v2-mill-daemon-network-policy`
    -   `worker2-requisition-fulfillment-server-network-policy`
    -   `worker2-spanner-computations-server-network-policy`

## Before you start

See [Machine Setup](machine-setup.md).

## Register your Duchy with the Kingdom (offline)

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

## Provision AWS cloud infrastructure

This can be done using Terraform. See [the guide](terraform.md) to use the
example configuration for the Duchy.

Applying the Terraform configuration will create a new cluster. You can use the
`aws` CLI to obtain credentials so that you can access the cluster via
`kubectl`. For example:

```shell
aws eks update-kubeconfig --region us-west-2 --name worker2-duchy
```

## Build and push the container images (optional)

If you aren't using
[pre-built release images](https://github.com/orgs/world-federation-of-advertisers/packages?repo_name=cross-media-measurement),
you can build the images yourself from source and push them to a container
registry. For example, if you're using the
[Amazon Elastic Container Registry](https://aws.amazon.com/ecr/), you
would specify `<account-id>.dkr.ecr.<account-region>.amazonaws.com` as your container registry and your Cloud project name as
your image repository prefix.

Assuming a project named `halo-worker2-demo`, an image tag `build-0001` and targeting aws account `010295286036` in `us-west-2`
region, run the following to build and push the images:

```shell
bazel run -c opt //src/main/docker:push_all_duchy_eks_images \
  --define container_registry=010295286036.dkr.ecr.us-west-2.amazonaws.com \
  --define image_repo_prefix=halo-worker2-demo \
  --define image_tag=build-0001
```

Tip: If you're using [Hybrid Development](../building.md#hybrid-development) for
containerized builds, replace `bazel build` with `tools/bazel-container build`
and `bazel run` with `tools/bazel-container-run`.

## Add metrics to the cluster (optional)

See [Metrics Deployment](metrics-deployment.md).

## Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:worker2_duchy.tar \
  --define kingdom_public_api_target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  --define google_cloud_project=halo-kingdom-demo \
  --define spanner_instance=halo-cmms \
  --define duchy_cert_id=SVVse4xWHL0 \
  --define duchy_storage_bucket=worker2-duchy \
  --define container_registry=ghcr.io \
  --define image_repo_prefix=world-federation-of-advertisers \
  --define image_tag=0.3.0
```

Extract the generated archive to some directory. It is recommended that you
extract it to a secure location, as you will be adding sensitive information to
it in the following step. It is also recommended that you persist this directory
so that you can use it to apply updates

You can customize this generated object configuration with your own settings
such as the number of replicas per deployment, the memory and CPU requirements
of each container, and the JVM options of each container.

## Customize the K8s secret

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

    Note: This assumes that all your root certificate PEM files end in newline.

1.  `worker2_tls.pem`

    The `worker2` Duchy's TLS certificate in PEM format.

1.  `worker2_tls.key`

    The private key for the TLS certificate in PEM format.

1.  `worker2_cs_cert.der`

    The `worker2` Duchy's consent signaling certificate in DER format.

1.  `worker2_cs_private.der`

    The private key for the Duchy's consent signaling certificate in DER format.

1.  `duchy_cert_config.textproto`

    Configuration mapping Duchy root certificates to the corresponding Duchy ID.

    -   [Example](../../src/main/k8s/testing/secretfiles/duchy_cert_config.textproto)

1.  `xxx_protocols_setup_config.textproto` (replace xxx with the role)

    -   This contains information about the protocols run in the duchy
    -   Set the role (aggregator or non_aggregator) in the config appropriately
    -   [Example](../../src/main/k8s/testing/secretfiles/aggregator_protocols_setup_config.textproto)

Place these files into the `src/main/k8s/dev/worker2_duchy_secret/` path within
the Kustomization directory.

### Secret files for testing

There are some [secret files](../../src/main/k8s/testing/secretfiles) within the
repository. These can be used for testing, but **must not** be used for
production environments as doing so would be highly insecure.

Generate the archive:

```shell
bazel build //src/main/k8s/testing/secretfiles:archive
```

Extract the generated archive to the `src/main/k8s/dev/worker2_duchy_secret/`
path within the Kustomization directory.

## Customize the K8s configMap

Configuration that may frequently change is stored in a K8s configMap. The `dev`
configuration uses one named `config-files` containing the file
`authority_key_identifier_to_principal_map.textproto`.

Place this file in the `src/main/k8s/dev/config_files/` path within the
Kustomization directory.

See [Creating Resources](../operations/creating-resources.md) for information on
this file format.

## Apply the K8s Kustomization

Use `kubectl` to apply the Kustomization. From the Kustomization directory run:

```shell
kubectl apply -k src/main/k8s/dev/worker2_duchy
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
worker2-async-computation-control-server-deployment 1/1   1          1         1m
worker2-computation-control-server-deployment       1/1   1          1         1m
worker2-herald-daemon-deployment                    1/1   1          1         1m
worker2-liquid-legions-v2-mill-daemon-deployment    1/1   1          1         1m
worker2-requisition-fulfillment-server-deployment   1/1   1          1         1m
worker2-spanner-computations-server-deployment      1/1   1          1         1m
```

```
NAME                                     TYPE         CLUSTER-IP     EXTERNAL-IP    PORT(S)        AGE
worker2-async-computation-control-server ClusterIP    10.123.249.255 <none>         8443/TCP       1m
worker2-computation-control-server       LoadBalancer 10.123.250.81  34.134.198.198 8443:31962/TCP 1m
worker2-requisition-fulfillment-server   LoadBalancer 10.123.247.78  35.202.201.111 8443:30684/TCP 1m
worker2-spanner-computations-server      ClusterIP    10.123.244.10  <none>         8443/TCP       1m
kubernetes                               ClusterIP    10.123.240.1   <none>         443/TCP        1m
```

## Make the Duchy accessible outside the cluster

### Reserve the external IPs

There are two external APIs in the duchy. The
`worker2-requisition-fulfillment-server` (a.k.a. the public API) is called by
the EDPs to fulfill their requisitions. The `worker2-computation-control-server`
(a.k.a. the system API) is called by the other duchies to send computation
related data. As you can see from the result in the previous step. Only these
two services have external IPs. However, these external IPs are ephemeral. We
need to reserve them such that they are stable.

For example, in the halo dev instance, we have subdomains:

-   `public.worker2.dev.halo-cmm.org`
-   `system.worker2.dev.halo-cmm.org`

The domains/subdomains are what the EDPs and other duchies use to communicate
with the duchy.

See [Reserving External IPs](cluster-config.md#reserving-external-ips)

## [Q/A](../gke/duchy-deployment.md#Q/A)

