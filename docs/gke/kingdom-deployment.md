# How to deploy a Halo Kingdom on GKE

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
-   In the doc, we assume we are deploying to a single region, i.e. us-central1.
    If you are deploying to another region or multiple regions, just need to
    adjust each step mentioning "region" accordingly.

## What are we creating/deploying?

-   1 GCP Project
    -   1 spanner instance
    -   1 GKE cluster
    -   1 Kubernetes secret
    -   1 Kubernetes configmap
    -   3 Kubernetes Service
        -   gcp-kingdom-data-server (Cluster IP)
        -   system-api-server (External load balancer)
        -   v2alpha-public-api-server (External load balancer)
    -   3 Kubernetes deployment
        -   gcp-kingdom-data-server-deployment
        -   system-api-server-deployment
        -   v2alpha-public-api-server-deployment
    -   1 Kubernetes Job
        -   kingdom-push-spanner-schema-job
    -   4 Kubernetes NetworkPolicy
        -   internal-data-server-network-policy
        -   system-api-server-network-policy
        -   public-api-server-network-policy
        -   default-deny-ingress-and-egress

## Step 0. Before You Start

Install the following software on your machine.

-   [Kubectl](https://kubernetes.io/docs/tasks/tools/)
-   [Bazel](https://docs.bazel.build/versions/main/install.html)
-   [Cloud SDK](https://cloud.google.com/sdk/docs/install)

Clone the halo
[cross-media-measurement](https://github.com/world-federation-of-advertisers/cross-media-measurement)
git repository locally, and check out the targeted commit (likely the one
mentioned in the beginning of the doc).

```shell
git clone https://github.com/world-federation-of-advertisers/cross-media-measurement.git
git checkout the-expected-commit-number
```

Run the following command in the root directory and make sure it passes

```shell
bazel test src/...
```

or

```shell
tools/bazel-container test src/...
```

depending on your machine OS.

Read this
[page](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/building.md)
for more details if you have trouble building/testing the codes.

## Step 1. Create and set up the GCP Project

1.  [Register](https://console.cloud.google.com/freetrial) a GCP account.
2.  In the [Google Cloud console](https://console.cloud.google.com/), create a
    GCP project with project name `halo kingdom demo`, and project id
    `halo-kingdom-demo`. (The project name doesn't matter and can be changed
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

## Step 2. Create Spanner instance

Visit the GCloud console
[spanner](https://console.cloud.google.com/spanner/instances) page.

1.  Enable the `Cloud Spanner API` if you haven’t done it yet.
2.  Click Create Instance with
    -   instance name = `halo-kingdom-demo-instance` (You can use whatever name
        here, but usually we use "-instance" as the suffix)
    -   regional, us-central1
    -   Processing units = 100 (cost would be $0.09 per hour)

Notes:

-   100 processing units is the minimum value we can choose. It should be good
    enough before the project is fully launched and we get more traffic.
-   By default, the instance is accessible by all clusters created within this
    GCP project.

## Step 3. Prepare the docker images

In this example, we use Google Cloud
[container-registry](https://cloud.google.com/container-registry) to store our
docker images. Enable the Google Container Registry API in the console if you
haven't done it. If you use other repositories, adjust the commands accordingly.

1.  Clone the halo
    [cross-media-measurement](https://github.com/world-federation-of-advertisers/cross-media-measurement)
    git repository locally, and check out the targeted commit (likely the one
    mentioned in the beginning of the doc).
2.  Build and push the binaries to gcr.io by running the following commands

    ```shell
    $tools/bazel-container-run src/main/docker/push_push_spanner_schema_image \
    -c opt --define container_registry=gcr.io \
    --define image_repo_prefix=halo-kingdom-demo

    $tools/bazel-container-run src/main/docker/push_kingdom_data_server_image \
    -c opt --define container_registry=gcr.io \
    --define image_repo_prefix=halo-kingdom-demo

    $tools/bazel-container-run src/main/docker/push_kingdom_v2alpha_public_api_server_image \
    -c opt --define container_registry=gcr.io \
    --define image_repo_prefix=halo-kingdom-demo

    $tools/bazel-container-run src/main/docker/push_kingdom_system_api_server_image \
    -c opt --define container_registry=gcr.io \
    --define image_repo_prefix=halo-kingdom-demo
    ```

    You should see log like "Successfully pushed Docker image to
    gcr.io/halo-kingdom- demo/setup/push-spanner-schema:latest"

    or you can run

    ```shell
    $bazel query 'kind("container_push", //src/main/docker:all)' | xargs -n 1 -o \
    tools/bazel-container-run -c opt --define container_registry=gcr.io --define \
    image_repo_prefix=halo-kingdom-demo
    ```

    The above command builds and pushes all halo binaries one by one. You should
    see logs like "Successfully pushed Docker image to gcr.io/halo-kingdom-
    demo/something" multiple times.

    If you see errors using `tools/bazel-contain-run`, check the
    [Hybrid Development approach](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/building.md#hybrid-development).

    If you see an `UNAUTHORIZED` error, run the following command and retry.

    ```shell
    gcloud auth configure-docker
    gcloud auth login
    ```

## Step 4. Create Cluster

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
halo-kingdom-demo
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
gcloud container clusters create halo-cmm-kingdom-demo-cluster --num-nodes 2
--machine-type=e2-small --enable-autoscaling --min-nodes 1 --max-nodes 5
--enable-network-policy --scopes
"https://www.googleapis.com/auth/cloud-platform"
```

Note:

-   The kingdom only contains 3 API servers (Public API, System API and Internal
    Data Server), the resource requirement is pretty low. Machine type
    `e2-small` should be good enough in the early stage.
-   The "scopes" and "enable-network-policy" flags are critical for the kingdom.
    It is recommended to specify them during cluster creation time.

Run

```shell
gcloud container clusters list
```

You should get something like

```shell
NAME                          LOCATION      MASTER_VERSION   MASTER_IP      MACHINE_TYPE NODE_VERSION     NUM_NODES STATUS
halo-cmm-kingdom-demo-cluster us-central1-c 1.20.10-gke.1600 34.122.232.195 e2-small     1.20.10-gke.1600 2         RUNNING
```

Finally, we connect to the above cluster in `kubectl` by running:

```shell
gcloud container clusters get-credentials halo-cmm-kingdom-demo-cluster
```

## Step 5. Create Kubernetes secrets.

***(Note: this step does not use any halo code, and you don't need to do it
within the cross-media-measurement repo.)***

The kingdom binary is configured to read certificates and config files from the
mounted Kubernetes secret volume.

First, prepare all the files we want to include in the Kubernetes secret. The
following files are required in the kingdom

1.  all_root_certs.pem

    -   This is the concatenation of root certificates of all parties that are
        allowed to send request to the kingdom, including
        -   all duchies
        -   all edps
        -   all MCs or frontends
        -   The kingdom's own root certificate (for kingdom internal traffic)
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

2.  `kingdom_tls.pem`

    -   This is the public key of the Kingdom's TLS certificate.

3.  `kingdom_tls.key`

    -   This is the private key of the Kingdom's TLS certificate.

4.  `health_probe_tls.pem`

    -   This is the public key of the TLS certificate used to do health checks
        in the kingdom.

5.  `health_probe_tls.key`

    -   This is the private key of the TLS certificate used to do health checks
        in the kingdom.

6.  `duchy_id_config.textproto`

    -   [Example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles/duchy_id_config.textproto)

7.  `llv2_protocol_config_config.textproto`

    -   [Example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles/llv2_protocol_config_config.textproto)

***health_probe_tls.pem, health_probe_tls.key, kingdom_tls.pem and
kingdom_tls.key are confidential to the kingdom and determined by the kingdom
itself.*** All other files are not sensitive and need to be worked out together
among the Kingdom and all duchy operators.

Second, put all above files in the same folder (anywhere in your local machine),
and create a file with name `kustomization.yaml` and content as follows

```shell
secretGenerator:
- name: certs-and-configs
  files:
  - all_root_certs.pem
  - kingdom_tls.key
  - kingdom_tls.pem
  - health_probe_tls.pem
  - health_probe_tls.key
  - duchy_id_config.textproto
  - llv2_protocol_config_config.textproto
  - authority_key_identifier_to_principal_map.textproto
```

and run

```shell
kubectl apply -k path-to-the-above-folder
```

Now the secret is created in the `halo-cmm-kingdom-demo-cluster`. You should be
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

## Step 6. Update cue files.

We recommend that you modify the
[kingdom_gke.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/kingdom_gke.cue)
file in your local branch and deploy using it directly. Or you can create
another version of it. But whatever way you do, you keep the changes locally, DO
NOT COMMIT it.

Update the tag variables in the beginning to match the names you choose in
earlier steps.

```shell
# GloudProject:      "halo-kingdom-demo"
# SpannerInstance:   "halo-kingdom-demo-instance"
# ContainerRegistry: "gcr.io"
```

You can also update the memory and cpu request/limit of each pod, as well as the
number of replicas per deployment.

## Step 7. Deploy the kingdom to GKE

Create an empty configmap since this is a branch new kingdom, there is no EDP
registered in the system yet. Run

```shell
touch /tmp/authority_key_identifier_to_principal_map.textproto
kubectl create configmap config-files \
--from-file=/tmp/authority_key_identifier_to_principal_map.textproto
```

Deploy the kingdom by running (update the parameters if you use different
values)

```shell
tools/bazel-container-run
src/main/kotlin/org/wfanet/measurement/tools:deploy_kingdom_to_gke \
--define=k8s_kingdom_secret_name=certs-and-configs-abcdedg \
-- --yaml-file=kingdom_gke.yaml --cluster-name=halo-cmm-kingdom-demo-cluster \
--environment=dev
```

Now all kingdom components will be successfully deployed to your GKE cluster.
You can verify by running

```shell
$ kubectl get deployments
NAME                                 READY UP-TO-DATE AVAILABLE AGE
gcp-kingdom-data-server-deployment   1/1   1          1         1m
system-api-server-deployment         1/1   1          1         1m
v2alpha-public-api-server-deployment 1/1   1          1         1m

$ kubectl get services
NAME                      TYPE         CLUSTER-IP   EXTERNAL-IP  PORT(S)        AGE
gcp-kingdom-data-server   ClusterIP    10.3.245.210 <none>       8443/TCP       14d
kubernetes                ClusterIP    10.3.240.1   <none>       443/TCP        16d
system-api-server         LoadBalancer 10.3.248.13  34.67.15.39  8443:30347/TCP 14d
v2alpha-public-api-server LoadBalancer 10.3.255.191 34.132.87.22 8443:31300/TCP 14d
```

Note: If you are not modifying the existing `kingdom_gke.cue` file, but are
creating another cue file in the previous step, you need to update the
`yaml-file` and `environment` args to make it work.

## Step 8. Make the Kingdom accessible on the open internet.

### Reserve the external IPs

There are two public APIs in the kingdom. The `v2alpha-public-api-server` is
called by the EDPs, MPs and MCs. The `system-api-server` is called by the
duchies. As you can see from the result in the previous step. Only these two
services have external IPs. However, these external IPs are ephemeral. We need
to reserve them such that they are stable.

Go to the Gcloud [Console](https://console.cloud.google.com/networking), under
VPC network -> External IP address, find the above two external IPs, and click
RESERVE on the right.

Follow this
[link](https://cloud.google.com/compute/docs/ip-addresses/reserve-static-external-ip-address#gcloud)
if you want to reserve the IPs using Cloud CLI.

### Setup subdomain DNS A record

Update your domains or subdomains, one for the system API and one for the public
API, to point to the two corresponding external IPs.

For example, in the halo dev instance, we have subdomains:

-   `system.kingdom.dev.halo-cmm.org`
-   `public.kingdom.dev.halo-cmm.org`

The domains/subdomains are what the EDPs/MPs/MCs/Duchies use to communicate with
the kingdom.

## Additional setting you may want to make

After finishing the above steps, we have

-   1 system API, 1 public API and 1 internal API running.
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

### Q1. How to generate the kingdom TLS Certificate?

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

### Q2. What if the secret files need to be updated?

If the kingdom needs to onboard a new EDP or MC, the current design requires the
kingdom to update the config files, e.g., `all_root_certs.pem`. Just redo step
5,6,7.

### Q3. How to test if the kingdom is working properly?

Follow the
["How to complete multi-cluster correctnessTest on GKE"](correctness-test.md)
doc and complete a correctness test using the Kingdom you have deployed.

If you don't want to deploy duchies and simulators, you can just deploy the
resourceSetupJob in the same kingdom cluster to see if you can create the
resources successfully. If yes, you can consider the Kingdom is working
properly.
