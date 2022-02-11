# How to deploy a Halo Duchy on GKE

Last updated: 2022-01-27

***The doc is updated to work with commit
[7fab61049e425bb0edd5fa2802290bf1722254e7](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/429/commits/0b0e10d952f1fbfe4aa665c722e859bdeed9128d).
Newer changes may require some commands to be updated. Stay tuned.***

***Disclaimer***:

-   This instruction is just one way of achieving the goal, not necessarily the
    best approach.
-   Almost all steps can be done via either the
    [Google Cloud Console](https://console.cloud.google.com/) UI or the GCloud
    CLI. The doc picks the easier one for each step. But you are free to do it
    in an alternative way.
-   All names used in this doc can be replaced with something else. We use
    specific names in the doc for ease of reference.
-   All quotas and resource configs are just examples, adjust the quota and size
    based on the actual usage.
-   In the doc, we assume we are deploying a duchy named `worker1` to a single
    region, i.e. `us-central1`. If you are deploying to another region or
    multiple regions, just need to adjust each step mentioning "region"
    accordingly.

## What are we creating/deploying?

-   1 GCP Project
    -   1 spanner instance
    -   1 Cloud Storage bucket
    -   1 GKE cluster
    -   1 Kubernetes secret
    -   1 Kubernetes configmap
    -   4 Kubernetes Service
        -   worker1-async-computation-control-server (Cluster IP)
        -   worker1-computation-control-server (External load balancer)
        -   worker1-requisition-fulfillment-server (External load balancer)
        -   worker1-spanner-computations-server (Cluster IP)
    -   6 Kubernetes deployment
        -   worker1-async-computation-control-server-deployment (gRPC service)
        -   worker1-computation-control-server-deployment (gRPC service)
        -   worker1-herald-daemon-deployment (Daemon Job)
        -   worker1-liquid-legions-v2-mill-daemon-deployment (Daemon Job)
        -   worker1-requisition-fulfillment-server-deployment (gRPC service)
        -   worker1-spanner-computations-server-deployment (gRPC service)
    -   1 Kubernetes Job
        -   worker1-push-spanner-schema-job
    -   8 Kubernetes NetworkPolicy
        -   worker1-async-computation-controls-server-network-policy
        -   worker1-computation-control-server-network-policy
        -   worker1-herald-daemon-network-policy
        -   worker1-liquid-legions-v2-mill-daemon-network-policy
        -   worker1-push-spanner-schema-job-network-policy
        -   worker1-requisition-fulfillment-server-network-policy
        -   worker1-spanner-computations-server-network-policy
        -   default-deny-ingress-and-egress

## Step 0. Before You Start

See [Machine Setup](machine-setup.md).

## Step 1. Register your duchy with the kingdom (offline)

In order to join the cross-media-measurement system, the duchy needs to first
register itself with the kingdom. This will be done offline with the help from
the kingdom operator.

The duchy operator needs to share the following information to the kingdom
operator.

-   The display name (a string, used as externalId) of the duchy (unique among
    all duchies)
-   public key of root certificate
-   public key of the TLS certificate
-   public key of the consent signaling certificate

The Kingdom operator will register all corresponding resources for the duchy via
internal admin tools. The resource ids will be shared with the duchy operator.

TODO: add a link to the doc about how to onboard a duchy to the system (doesn't
exist yet)

## Step 2. Create and set up the GCP Project

1.  [Register](https://console.cloud.google.com/freetrial) a GCP account.
2.  In the [Google Cloud console](https://console.cloud.google.com/), create a
    GCP project with project name `halo work1 demo`, and project id
    `halo-worker1-demo`. (The project name doesn't matter and can be changed
    later, but the project id will be used in all our configurations and is
    immutable, so it is better to choose a good human-readable name for the
    project id.)
3.  Set up Billing of the project in the Console -> Billing page.
4.  Update the project quotas in the Console -> IAM & Admin -> Quotas page If
    necessary. (The default quota might just work since the kingdom doesn't
    consume too many resources). You can skip this step for now and come back
    later if any future operation fails due to "out of quota" errors.

If you are using an existing project, make sure your account has the `writer`
role in that project.

## Step 3. Create the Spanner instance Visit the GCloud console spanner page.

Visit the GCloud console
[spanner](https://console.cloud.google.com/spanner/instances) page.

1.  Enable the `Cloud Spanner API` if you haven’t done it yet.
2.  Click Create Instance with
    -   instance name = `halo-worker1-instance` (You can use whatever name here,
        but usually we use "-instance" as the suffix)
    -   regional, us-central1
    -   Processing units = 100 (cost would be $0.09 per hour)

Notes:

-   100 processing units is the minimum value we can choose. It should be good
    enough before the project is fully launched and we get more traffic.
-   By default, the instance is accessible by all clusters created within this
    GCP project.

## Step 4. Create the Cloud Storage Bucket

Note: all settings here are just examples. Pick values that make sense in your
use case.

1.  Visit the GCloud console
    [Cloud Storage](https://console.cloud.google.com/storage/browser) page
2.  Click on `CREATE BUCKET`
3.  Name the bucket `halo-worker1-bucket` or something else.
4.  Choose Location type = `Region`, and Location = `us-central1`
5.  Use default settings in other options and click on `CREATE`

The created cloud storage bucket will be accessible by all pods/clusters in the
`halo-worker1-demo` GCP project.

## Step 5. Prepare the docker images

In this example, we use Google Cloud
[container-registry](https://cloud.google.com/container-registry) to store our
docker images. Enable the Google Container Registry API in the console if you
haven't done it. If you use other repositories, adjust the commands accordingly.

1.  Clone the halo cross-media-measurement git repository locally, and check out
    the targeted commit (likely the one mentioned in the beginning of the doc).
2.  Build and push the binaries to gcr.io by running the following commands

    ```shell
    bazel run src/main/docker/push_push_spanner_schema_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo

    bazel run src/main/docker/push_duchy_spanner_computations_server_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo

    bazel run src/main/docker/push_duchy_requisition_fulfillment_server_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo

    bazel run src/main/docker/push_duchy_computation_control_server_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo

    bazel run src/main/docker/push_duchy_async_computation_control_server_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo

    bazel run src/main/docker/push_duchy_herald_daemon_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo

    bazel run src/main/docker/push_duchy_liquid_legions_v2_mill_daemon_image \
      -c opt --define container_registry=gcr.io \
      --define image_repo_prefix=halo-worker1-demo
    ```

    You should see log like "Successfully pushed Docker image to
    gcr.io/halo-worker1- demo/setup/push-spanner-schema:latest".

    Or you can run

    ```shell
    bazel query 'kind("container_push", //src/main/docker:all)' | xargs -n 1 -o \
      bazel run -c opt --define container_registry=gcr.io --define \
      image_repo_prefix=halo-kingdom-demo
    ```

    Tip: If you're using [Hybrid Development](../building.md#hybrid-development)
    for containerized builds, replace `bazel run` with
    `tools/bazel-container-run`.

    The above command builds and pushes all halo binaries one by one. You should
    see logs like "Successfully pushed Docker image to gcr.io/halo-kingdom-
    demo/something" multiple times.

    If you see an `UNAUTHORIZED` error, run the following command and retry.

    ```shell
    gcloud auth configure-docker
    gcloud auth login
    ```

## Step 6. Create the Cluster

Read the "Before you begin" section at this
[page](https://cloud.google.com/kubernetes-engine/docs/how-to/creating-a-regional-cluster#before_you_begin)
and finish those steps if you have not done them yet.

We recommend using `gcloud init`, and choose default region as `us-central1-c`
(or any other region you prefer)

After it is done, verify it works by running

```shell
gcloud config get-value project
```

You should get result

```shell
halo-worker1-demo
```

Note: if you are using an existing project, you can run the following command to
set the current project to it.

```shell
gcloud projects list
gcloud config set project project-id
```

Enable the Kubernetes API in the console if your account hasn't done it. Run the
following command to create the cluster

```shell
gcloud container clusters create halo-cmm-worker1-demo-cluster --num-nodes 3
--machine-type=e2-medium --enable-autoscaling --min-nodes 2 --max-nodes 5
--enable-network-policy --scopes
"https://www.googleapis.com/auth/cloud-platform"
```

Note:

-   The duchy contains 4 API services and 2 daemon jobs. Those API services and
    the herald daemon don't require too many resources. However, the mill daemon
    performs CPU intensive computations, and may need plenty of replicas
    depending on how many active computations the system is expected to process
    per day. Select the appropriate machine-type and number of nodes based on
    the traffic and your budget. For the demo purpose, we choose `e2-medium`. In
    product, you may want to choose from the
    [compute-optimized](https://cloud.google.com/compute/docs/compute-optimized-machines)
    machine family, e.g., `c2-standard-4`, which are more expensive.
-   The "scopes" and "enable-network-policy" flags are critical for the kingdom.
    It is recommended to specify them during cluster creation time.

Run

```shell
gcloud container clusters list
```

You should get something like

```shell
NAME                          LOCATION      MASTER_VERSION   MASTER_IP      MACHINE_TYPE NODE_VERSION     NUM_NODES STATUS
halo-cmm-worker1-demo-cluster us-central1-c 1.20.10-gke.1600 34.122.232.195 e2-small     1.20.10-gke.1600 2         RUNNING
```

Finally, we connect to the above cluster in `kubectl` by running:

```shell
gcloud container clusters get-credentials halo-cmm-worker1-demo-cluster
```

## Step 7. Create Kubernetes secrets.

***(Note: this step does not use any halo code, and you don't need to do it
within the cross-media-measurement repo.)***

The kingdom binary is configured to read certificates and config files from the
mounted Kubernetes secret volume.

First, prepare all the files we want to include in the Kubernetes secret. The
following files are required in the kingdom

1.  all_root_certs.pem

    -   This is the concatenation of root certificates of all parties that this
        duchy communicates with, including

        -   all other duchies
        -   edps that select to fulfill requisitions at this duchy
        -   This duchy own root certificate (for duchy internal traffic)
        -   A certificate used for health check purpose

    -   One way to construct the all_root_certs file is put all root certs in
        the same folder, and add a BUILD.bazel similar to

        ```shell
        genrule(
          name = "all_root_certs",
          srcs = glob(["*_root.pem"]),
          outs = ["all_root_certs.pem"],
          cmd = "cat $(SRCS) > $@",
          visibility = ["//visibility:public"],
        )
        ```

        and then build with `bazel`.

2.  `worker1_tls.pem`

    -   This is the public key of this duchy's TLS certificate.

3.  `worker1_tls.key`

    -   This is the private key of this duchy's TLS certificate.

4.  `health_probe_tls.pem`

    -   This is the public key of the TLS certificate used to do health checks
        in the kingdom.

5.  `health_probe_tls.key`

    -   This is the private key of the TLS certificate used to do health checks
        in the kingdom.

6.  `worker1_cs_cert.der`

    -   This is the public key of this duchy's consent signaling certificate.

7.  `worker1_cs_private.der`

    -   This is the private key of this duchy's consent signaling certificate.

8.  `xxx_protocols_setup_config.textproto` (replace xxx with the role)

    -   This contains information about the protocols run in the duchy
    -   Set the role (aggregator or non_aggregator) in the config appropriately
    -   [Example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles/aggregator_protocols_setup_config.textproto)

Second, put all above files in the same folder (anywhere in your local machine),
and create a file with name `kustomization.yaml` and content as follows

```shell
secretGenerator:
- name: certs-and-configs
  files:
  - all_root_certs.pem
  - worker1_tls.pem
  - worker1_tls.key
  - health_probe_tls.pem
  - health_probe_tls.key
  - worker1_cs_cert.der
  - worker1_cs_private.der
  - protocols_setup_config.textproto
```

and run

```shell
kubectl apply -k path-to-the-above-folder
```

Now the secret is created in the `halo-cmm-worker1-demo-cluster`. You should be
able to see the secret by running

```shell
kubectl get secrets
```

We assume the name is `certs-and-configs-abcdedf` and will use it in the
following documents.

Note: there are
[test certificates and config files](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/k8s/testing/secretfiles)
checked in the cross-media-measurement repo. You can run the following command
to create a secret for testing.

```shell
kubectl apply -k src/main/k8s/testing/secretfiles
```

## Step 8. Create the configmap

Create a `authority_key_identifier_to_principal_map.textproto` file with the
following content. This file contains all EDPs that are allowed to call this
duchy to fulfill the requisitions. If there is no EDP registered yet. Just leave
the file empty.

```shell
# proto-file: src/main/proto/wfa/measurement/config/authority_key_to_principal_map.proto
# proto-message: AuthorityKeyToPrincipalMap
entries {
  authority_key_identifier: "\xD6\x65\x86\x86\xD8\x7E\xD2\xC4\xDA\xD8\xDF\x76\x39\x66\x21\x3A\xC2\x92\xCC\xE2"
  principal_resource_name: "dataProviders/HRL1wWehTSM"
}
entries {
  authority_key_identifier: "\x6F\x57\x36\x3D\x7C\x5A\x49\x7C\xD1\x68\x57\xCD\xA0\x44\xDF\x68\xBA\xD1\xBA\x86"
  principal_resource_name: "dataProviders/djQdz2ehSSE"
}
entries {
  authority_key_identifier: "\xEE\xB8\x30\x10\x0A\xDB\x8F\xEC\x33\x3B\x0A\x5B\x85\xDF\x4B\x2C\x06\x8F\x8E\x28"
  principal_resource_name: "dataProviders/SQ99TmehSA8"
}
entries {
  authority_key_identifier: "\x74\x72\x6D\xF6\xC0\x44\x42\x61\x7D\x9F\xF7\x3F\xF7\xB2\xAC\x0F\x9D\xB0\xCA\xCC"
  principal_resource_name: "dataProviders/TBZkB5heuL0"
}
entries {
  authority_key_identifier: "\xA6\xED\xBA\xEA\x3F\x9A\xE0\x72\x95\xBF\x1E\xD2\xCB\xC8\x6B\x1E\x0B\x39\x47\xE9"
  principal_resource_name: "dataProviders/HOCBxZheuS8"
}
entries {
  authority_key_identifier: "\xA7\x36\x39\x6B\xDC\xB4\x79\xC3\xFF\x08\xB6\x02\x60\x36\x59\x84\x3B\xDE\xDB\x93"
  principal_resource_name: "dataProviders/VGExFmehRhY"
}
```

Run

```shell
kubectl create configmap config-files \
--from-file=path_to_file/authority_key_identifier_to_principal_map.textproto
```

Whenever there is a new EDP onboarded to the system, you need to add an entry
for this EDP. Update this file and run the following command to replace the
ConfigMap in the kingdom cluster

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
--from-file=path_to_file/authority_key_identifier_to_principal_map.textproto \
| kubectl replace -f -
```

You can verify that the config file is successfully update by running

```shell
kubectldescribe configmaps config-files
```

## Step 9. Update cue files.

We recommend that you modify the
[duchy_gke.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/duchy_gke.cue)
file in your local branch and deploy using it directly. Or you can create
another version of it. But whatever way you do, you keep the changes locally, DO
NOT COMMIT it.

First, open the `/src/main/k8s/dev/duchy_gke.cue` file in your local branch.
Update the tag variables in the beginning to the following values.

```shell
# KingdomSystemApiTarget: "your kingdom's system API domain or subdomain:8443"
# GloudProject: "halo-worker1-demo"
# SpannerInstance: "halo-worker1-instance"
# CloudStorageBucket: "halo-worker1-bucket"
# ContainerRegistry: "gcr.io"
```

If you picked other names in the previous steps, update the value accordingly.

Then in the same file, update the following part to match with the domains you
plan to use.

```shell
_computation_control_targets: {
  "aggregator": "your aggregator's system API domain:8443"
  "worker1": "your worker1's system API domain:8443"
  "worker2": "your worker2's system API domain:8443"
}
```

You can also update the memory and cpu request/limit of each pod, as well as the
number of replicas per deployment.

## Step 10. Deploy the duchy to GKE

Replace the k8s_duchy_secret_name and duchy_cert_name (obtained from the kingdom
operator offline) in the command, and run

```shell
bazel run src/main/kotlin/org/wfanet/measurement/tools:deploy_duchy_to_gke \
  --define=k8s_duchy_secret_name=certs-and-configs-abcdedf \
  --define=duchy_name=aggregator \
  --define=duchy_cert_name=duchies/aggregator/certificates/something \
  --define=duchy_protocols_setup_config=aggregator_protocols_setup_config.textproto \
  -- \
  --yaml-file=duchy_gke.yaml --cluster-name=halo-cmm-aggregator-demo-cluster \
  --environment=dev
```

Now all duchy components will be successfully deployed to your GKE cluster. You
can verify by running

```shell
$ kubectl get deployments
NAME                                                READY UP-TO-DATE AVAILABLE AGE
worker1-async-computation-control-server-deployment 1/1   1          1         1m
worker1-computation-control-server-deployment       1/1   1          1         1m
worker1-herald-daemon-deployment                    1/1   1          1         1m
worker1-liquid-legions-v2-mill-daemon-deployment    1/1   1          1         1m
worker1-requisition-fulfillment-server-deployment   1/1   1          1         1m
worker1-spanner-computations-server-deployment      1/1   1          1         1m

$ kubectl get service
NAME                                     TYPE         CLUSTER-IP     EXTERNAL-IP    PORT(S)        AGE
worker1-async-computation-control-server ClusterIP    10.123.249.255 <none>         8443/TCP       1m
worker1-computation-control-server       LoadBalancer 10.123.250.81  34.134.198.198 8443:31962/TCP 1m
worker1-requisition-fulfillment-server   LoadBalancer 10.123.247.78  35.202.201.111 8443:30684/TCP 1m
worker1-spanner-computations-server      ClusterIP    10.123.244.10  <none>         8443/TCP       1m
kubernetes                               ClusterIP    10.123.240.1   <none>         443/TCP        1m
```

Note: If you are not modifying the existing `duchy_gke.cue` file, but are
creating another cue file in the previous step, you need to update the
`yaml-file` and `environment` args to make it work.

## Step 11. Make the Duchy accessible on the open internet.

### Reserve the external IPs

There are two external APIs in the duchy. The
`worker1-requisition-fulfillment-server` (a.k.a. the public API) is called by
the EDPs to fulfill their requisitions. The `worker1-computation-control-server`
(a.k.a. the system API) is called by the other duchies to send computation
related data. As you can see from the result in the previous step. Only these
two services have external IPs. However, these external IPs are ephemeral. We
need to reserve them such that they are stable.

Go to the Gcloud [Console](https://console.cloud.google.com/networking), under
VPC network -> External IP address, find the above two external IPs, and click
RESERVE on the right.

Follow this
[link](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address#gcloud)
if you want to reserve the IPs using Cloud CLI. Setup subdomain DNS A record
Update your domains or subdomains, one for the system API and one for the public
API, to point to the two corresponding external IPs.

For example, in the halo dev instance, we have subdomains:

-   `public.worker1.dev.halo-cmm.org`
-   `system.worker1.dev.halo-cmm.org`

The domains/subdomains are what the EDPs and other duchies use to communicate
with the duchy.

## Additional setting you may want to make

After finishing the above steps, we have

-   1 system API, 1 public API and 2 internal API running.
-   Only gRPC requests are allowed and connections are via mTLS.
-   All communications between pods within the cluster are also encrypted via
    mTLS.
-   Network policy is set such that
    -   only the system API and public API are accessible via the external IP
    -   only the Internal API is allowed to send requests outside (We plan to
        restrict the target to only Cloud Spanner, not down yet).

In this section, we list some additional settings/configurations you may want to
consider. They are mostly for enhancing security.

### 1. Application-layer secrets

encryption Those certifications and configurations we stored in Kubernetes
secret are encrypted on the storage layer, but not on the application layer. In
other works, whoever has access to the cluster resource can just call

```shell
kubectl get secrets secret_name -o json
```

to see the content of the files in the secret.

This may not be an issue if there are only a small number of people that have
access to the cluster resources. These people should already have access to
those secret files if they need to be able to create them.

However, if we want, we can enable Application-layer secrets encryption in the
cluster.

-   Go to Console -> Kubernetes Engine ->
    [Clusters](https://console.cloud.google.com/kubernetes/list)
-   Open the cluster you want to config Under Security,
-   edit the "Application-layer secrets encryption"

Note that you need to enable
[Cloud KMS](https://console.cloud.google.com/security/kms) in your GCP project
and create a private key for encrypting the secret. You also need to grant the
service account "cloudkms.cryptoKeyEncrypterDecrypter" role in the Console ->
[IAM & Admin](https://console.cloud.google.com/iam-admin) page. Check the
"include Google-provided role grants" to see the service account you are looking
for.

(Note: Whether this part works or not is not confirmed yet.)

### 2. Role Based Access Control

You can use both IAM and Kubernetes
[RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) to control
access to your GKE cluster. GCloud provides the "Google Groups for RBAC"
feature. Follow this
[instruction](https://cloud.google.com/kubernetes-engine/docs/how-to/role-based-access-control)
if you want to set it up.

## Q/A

### Q1. How to generate the duchy TLS Certificate?

A: You are free to use whatever tools. One option is to use the `openssl CLI`
following these steps.

1.  install the latest openssl. for example, 3.0.1 on MAC or 1.1.1l on linux.
2.  run the following commands

    ```shell
    openssl req -out test_root.pem -new
    -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -keyout test_root.key
    -x509 -days 3650 -subj '/O=Some Organization/CN=Some CA' -extensions v3_ca
    -addext subjectAltName=DNS:ca.someorg.example.com
    ```

    The above command will create two files.

    -   test_root.key: the private key of the root certificate
    -   test_root.pem: the public key of the root certificate
        -   You can run `openssl x509 -in test_root.pem -text -noout` to check
            the information within the certificate.

3.  Then create a file named `test_user.cnf` with the following content

    ```shell
    [usr_cert]
    basicConstraints=CA:FALSE
    authorityKeyIdentifier=keyid:always,issuer
    subjectKeyIdentifier=hash
    keyUsage=nonRepudiation,digitalSignature,keyEncipherment
    subjectAltName=DNS:server.someorg.example.com
    ```

4.  Then run two commands:

    ```shell
    openssl req -out test_user.csr -new -newkey ec -pkeyopt
    ec_paramgen_curve:prime256v1 -nodes -keyout test_user.key -subj '/O=Some
    Organization/CN=Some Server'

    openssl x509 -in test_user.csr -out test_user.pem
    -days 365 -req -CA test_root.pem -CAkey test_root.key -CAcreateserial -extfile
    test_user.cnf -extensions usr_cert
    ```

5.  The first command will generate a user key `test_user.key`, the second
    command will generate a user certificate with the above root certificate and
    user key `test_user.pem`. This test_user certificate can be either used in
    TLS connection or consent signature signing.

Or you can use the bazel tools the halo team created following these steps.

1.  checkout the
    [common-jvm](https://github.com/world-federation-of-advertisers/common-jvm)
    repo.
2.  open the
    [build/openssl/BUILD.bazel](https://github.com/world-federation-of-advertisers/common-jvm/blob/main/build/openssl/BUILD.bazel)
    file
3.  modify the attributes in the generate_root_certificate and
    generate_user_certificate targets accordingly.
4.  run `bazel build build/openssl/...`
5.  The cert files will be exported to the bazel-out directory, e.g.
    `bazel-out/k8-fastbuild/bin/build/openssl` or `bazel-bin/build/openssl`
    depending on your OS.

### Q2. How to test if the duchy is working properly?

Follow the
["How to complete multi-cluster correctnessTest on GKE"](correctness-test.md)
doc and complete a correctness test using the duchy you have deployed.
