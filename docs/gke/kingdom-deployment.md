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

See the
[example Terraform configuration](../../src/main/terraform/gcloud/examples/kingdom)
to see what resources are created.

The cluster will be populated with the following:

-   Secret
    -   `certs-and-configs-<hash>`
-   ConfigMap
    -   `config-files-<hash>`
-   Service
    -   `gcp-kingdom-data-server` (Cluster IP)
    -   `system-api-server` (External load balancer)
    -   `v2alpha-public-api-server` (External load balancer)
-   Deployment
    -   `gcp-kingdom-data-server-deployment`
    -   `system-api-server-deployment`
    -   `v2alpha-public-api-server-deployment`
-   CronJob
    -   `completed-measurements-deletion-cronjob`
    -   `pending-measurements-cancellation-cronjob`
    -   `exchanges-deletion-cronjob`
-   NetworkPolicy
    -   `default-deny-network-policy`
    -   `kube-dns-network-policy`
    -   `gke-network-policy`
    -   `internal-data-server-network-policy`
    -   `system-api-server-network-policy`
    -   `public-api-server-network-policy`
    -   `completed-measurements-deletion-network-policy`
    -   `pending-measurements-cancellation-network-policy`
    -   `exchanges-deletion-network-policy`

## Before You Start

See [Machine Setup](machine-setup.md).

## Provision Google Cloud Project infrastructure

This can be done using Terraform. See [the guide](terraform.md) to use the
example configuration for the Kingdom.

Applying the Terraform configuration will create a new cluster. You can use the
`gcloud` CLI to obtain credentials so that you can access the cluster via
`kubectl`. For example:

```shell
gcloud container clusters get-credentials kingdom
```

Applying the Terraform configuration will also create external IP resources and
output the resource names. These will be needed in later steps.

## Build and push the container images (optional)

If you aren't using
[pre-built release images](https://github.com/orgs/world-federation-of-advertisers/packages?repo_name=cross-media-measurement),
you can build the images yourself from source and push them to a container
registry. For example, if you're using the
[Google Container Registry](https://cloud.google.com/container-registry), you
would specify `gcr.io` as your container registry and your Cloud project name as
your image repository prefix.

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

## Add metrics to the cluster (optional)

See [Metrics Deployment](metrics-deployment.md).

## Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can
use the `dev` configuration as a base to get started. The Kustomization is
generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the `dev` Kustomization, run the following (substituting your own
values):

```shell
bazel build //src/main/k8s/dev:kingdom.tar \
  --define google_cloud_project=halo-kingdom-demo \
  --define spanner_instance=halo-cmms \
  --define kingdom_public_api_address_name=kingdom-v2alpha \
  --define kingdom_system_api_address_name=kingdom-system-v1alpha \
  --define container_registry=ghcr.io \
  --define image_repo_prefix=world-federation-of-advertisers \
  --define image_tag=0.5.2
```

Extract the generated archive to some directory. It is recommended that you
extract it to a secure location, as you will be adding sensitive information to
it in the following step. It is also recommended that you persist this directory
so that you can use it to apply updates.

You can customize this generated object configuration with your own settings
such as the number of replicas per deployment, the memory and CPU requirements
of each container, and the JVM options of each container.

## Customize the K8s secret

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

1.  `ro_llv2_protocol_config_config.textproto`

    Configuration for the Reach-Only Liquid Legions v2 protocol.

    -   [Example](../../src/main/k8s/testing/secretfiles/ro_llv2_protocol_config_config.textproto)

1.  `hmss_protocol_config_config.textproto`

    Configuration for the Honest Majority Share Shuffle protocol.

    -   [Example](../../src/main/k8s/testing/secretfiles/hmss_protocol_config_config.textproto)

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

## Customize the K8s configMap

Configuration that may frequently change is stored in a K8s configMap. The `dev`
configuration uses one named `config-files` containing the following files:

*   `authority_key_identifier_to_principal_map.textproto`
    *   See [Creating Resources](../operations/creating-resources.md)
*   `known_event_group_metadata_type_set.pb`
    *   Protobuf `FileDescriptorSet` containing known `EventGroup` metadata
        types.

Place these files in the `src/main/k8s/dev/config_files/` path within the
Kustomization directory.

## Apply the K8s Kustomization

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

## Make the Kingdom accessible outside the cluster

### Set up DNS records

In the DNS configuration for a domain you own, add `A` records to assign names
to the external IPs of the public and system API services.

For example in the Halo dev environment we use the following subdomains:

-   `v2alpha.kingdom.dev.halo-cmm.org`
-   `v1alpha.system.kingdom.dev.halo-cmm.org`

Other components (such as Duchies) and integrators (such as data providers and
model providers) can use these names to access Kingdom services.

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
