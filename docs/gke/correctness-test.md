# How to complete multi-cluster correctnessTest on GKE

Last updated: 2022-01-27

***The doc is updated to work with commit
[7fab61049e425bb0edd5fa2802290bf1722254e7](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/429/commits/0b0e10d952f1fbfe4aa665c722e859bdeed9128d).
Newer changes may require some commands to be updated. Stay tuned.***

This documentation provides step-by-step instructions on how to complete a GKE
multi-cluster correctnessTest for the cross media measurement system.

In the demo, we use the test certificates, configurations and data provided
[here](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/k8s/testing),
and cue files defined
[here](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/k8s/dev)
for all components. The reader is expected to do a very small amount of manual
update to the codebase during the demo.

Please refer to the following documentation if you need extra information on
deploying particular components in production.

-   [How to deploy a Halo Kingdom on GKE](kingdom-deployment.md)
-   [How to deploy a Halo Duchy on GKE](duchy-deployment.md)

However, you don't need to follow the above docs for kingdom/duchy deployments
in this demo. Since many steps can be simplified. For example, there is no need
to reserve static IPs for the public services during the test. In other words,
reading this doc is good enough for the reader to deploy everything required for
the correctnessTest on GKE.

## Overview

In the correctness test, we will create

-   1 GCP project, namely
    -   `halo-cmm-demo`
-   1 Spanner instance in the `halo-cmm-demo` GCP project
    -   `demo-instance`
        -   The kingdom and three duchies will share the same spanner instance,
            but they will use their own databases.
-   1 GCP BigQuery table inside the `halo-cmm-demo` GCP project
    -   `demo.labelled_events`: The table contains pre-generated Synthetic test
        data, and will be used by all EDP simulators.
-   5 GCP clusters inside the `halo-cmm-demo` GCP project, namely
    -   `halo-cmm-kingdom-demo-cluster`
        -   for running all kingdom components
    -   `halo-cmm-aggregator-demo-cluster`
        -   for running all components inside the aggregator duchy
    -   `halo-cmm-worker1-demo-cluster`
        -   for running all components inside the worker1 duchy
    -   `halo-cmm-worker2-demo-cluster`
        -   for running all components inside the worker2 duchy
    -   `halo-cmm-simulator-demo-cluster`
        -   for running all 6 EDP simulators and the Frontend simulator

Note: You are free to use other names for the above resources, e.g. GCP project,
Spanner instance, GCP Cluster. Just make sure you update the names in the cue
files and various commands accordingly. In this documentation, we assume the
above names are used.

***Since we are doing multi-cluster deployment, you will need your own domain to
manage the DNS records of the kingdom and duchies' public and system APIs. Make
sure you have a domain you can config.***

## Step 0. Before You Start

See [Machine Setup](machine-setup.md).

# Step 1. Create and setup the GCP Project

1.  [Register](https://console.cloud.google.com/freetrial) a GCP account.
2.  In the [Google Cloud console](https://console.cloud.google.com/), create a
    GCP project with project name `halo cmm demo`, and project id
    `halo-cmm-demo`. (The project name doesn't matter and can be changed later,
    but the project id will be used in all our configurations and is immutable,
    so it is better to choose a good human-readable name for the project id.)
3.  Set up Billing of the project in the Console -> Billing page.
4.  Update the project quotas in the Console -> IAM & Admin -> Quotas page If
    necessary. (The default quota might just work since the kingdom doesn't
    consume too many resources). You can skip this step for now and come back
    later if any future operation fails due to "out of quota" errors.

If you are using an existing project, make sure your account has the `writer`
role in that project.

# Step 2. Create the Spanner instance

Visit the GCloud console
[spanner](https://console.cloud.google.com/spanner/instances) page.

1.  Enable the `Cloud Spanner API` if you haven’t done it yet.
2.  Click Create Instance with
    -   instance name = `demo-instance` (You can use whatever name here, but
        usually we use "-instance" as the suffix)
    -   regional, us-central1
    -   Processing units = 100 (cost would be $0.09 per hour)

Notes:

-   100 processing units is the minimum value we can choose. It should be good
    enough before the project is fully launched and we get more traffic.
-   By default, the instance is accessible by all clusters created within this
    GCP project.

# Step 3. Create the Cloud Storage Bucket

1.  Visit the GCloud console
    [Cloud Storage](https://console.cloud.google.com/storage/browser) page.
2.  Click on `CREATE BUCKET`
3.  Name the bucket `halo-cmm-demo-bucket` or something else.
4.  Choose Location type = `Region`, and Location = `us-central1`
5.  Use default settings in other options and click on `CREATE`

The created cloud storage bucket will be accessible by all pods/clusters in the
`halo-cmm-demo` GCP project.

# Step 4. Prepare test data for the correctnessTest

We use a pre-generate synthetic test data in the correctnessTest. The data file
can be found in your local branch at
[src/main/k8s/testing/data/synthetic-labelled-events.csv](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/data/synthetic-labelled-events.csv).

We'll upload the data to a table in Google Cloud Bigquery. The EDP simulators
will query the events from the table according to the requisition
specifications.

***(Note that in M1B, the EDP simulators do a fixed query since the EventFilter
implementation is not done yet. In other words, all measurements will have the
same result but with random noise. In M2, we will be able to specify query
parameters in the Frontend simulator and create different measurements.)***

1.  Visit the GCloud console
    [Bigquery](https://console.cloud.google.com/bigquery) page. Enable the
    BiqQuery API in the project if you haven't done it yet.
2.  Create a dataset
    -   Click the three-dot button to the right of the `halo-cmm-demo` project
    -   Click `Create dataset`
    -   Set the "Dataset ID" to `demo`
    -   Set the "data location" to `us-central1`
    -   Click `CREATE DATASET`
3.  Create a table
    -   Click the three-dot button to the right of the `demo` data set.
    -   Click `create table`
    -   Set "create table from" to `upload`
    -   Select the local file `halo-cmm-test-labelled-events.csv` in your local
        branch
    -   Set Destination Table to `labelled-events`
    -   Check the Schema -> Auto detect box
    -   Click `CREATE TABLE`
    -   You should see a table named `labelled-events` being created. But the
        `create table` window might still be open, just click `CANCEL` to quit
        it.
4.  Check the table is valid
    -   Click on the `labelled_events` table. You should see something like this

![image-step-4-1](step-4-1.png)![image-step-4-1](step-4-2.png)

Now this synthetic test data is ready to use in the correctness test.

# Step 5. Prepare the docker images

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
    bazel query 'kind("container_push", //src/main/docker:all)' | xargs -n 1 -o \
      bazel run -c opt --define container_registry=gcr.io --define \
      image_repo_prefix=halo-cmm-demo
    ```

    Tip: If you're using [Hybrid Development](../building.md#hybrid-development)
    for containerized builds, replace `bazel run` with
    `tools/bazel-container-run`.

    The above command builds and pushes all halo binaries one by one. You should
    see logs like "Successfully pushed Docker image to gcr.io/halo-cmm-
    demo/something" multiple times.

    If you see an `UNAUTHORIZED` error, run the following command and retry.

    ```shell
    gcloud auth configure-docker
    gcloud auth login
    ```

# Step 6. Create Clusters

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
halo-cmm-demo
```

Note: if you are using an existing project, you can run the following command to
set the current project to it.

```shell
gcloud projects list
gcloud config set project project-id
```

Enable the Kubernetes API in the console if your account hasn't done it. Run the
following commands to create five clusters

```shell
$ gcloud container clusters create halo-cmm-kingdom-demo-cluster --num-nodes 2
--machine-type=e2-small --enable-autoscaling --min-nodes 1 --max-nodes 5
--enable-network-policy --scopes "https://www.googleapis.com/auth/cloud-platform"

$ gcloud container clusters create halo-cmm-aggregator-demo-cluster
--num-nodes 3 --machine-type=e2-medium --enable-autoscaling --min-nodes 2
--max-nodes 4 --enable-network-policy --scopes "https://www.googleapis.com/auth/cloud-platform"

$ gcloud container clusters create halo-cmm-worker1-demo-cluster --num-nodes
3 --machine-type=e2-medium --enable-autoscaling --min-nodes 2 --max-nodes 4
--enable-network-policy --scopes "https://www.googleapis.com/auth/cloud-platform"

$ gcloud container clusters create halo-cmm-worker2-demo-cluster --num-nodes
3 --machine-type=e2-medium --enable-autoscaling --min-nodes 2 --max-nodes 4
--enable-network-policy --scopes "https://www.googleapis.com/auth/cloud-platform"

$ gcloud container clusters create halo-cmm-simulator-demo-cluster
--num-nodes 3 --machine-type=e2-medium --enable-autoscaling --min-nodes 2
--max-nodes 4 --enable-network-policy --scopes "https://www.googleapis.com/auth/cloud-platform"
```

## Note:

-   The kingdom only contains 3 API servers (Public API, System API and Internal
    Data Server), the resource requirement is pretty low. Machine type
    `e2-small` should be good enough.
-   The "scopes" and "enable-network-policy" flags are critical. It is
    recommended to specify them during cluster creation time.

Run

```shell
gcloud container clusters list
```

You should get something like

```shell
NAME                               LOCATION       MASTER_VERSION   MASTER_IP       MACHINE_TYPE  NODE_VERSION    NUM_NODES STATUS
halo-cmm-aggregator-demo-cluster   us-central1-c  1.21.5-gke.1302  35.225.15.39    e2-medium     1.21.5-gke.1302 3         RUNNING
halo-cmm-kingdom-demo-cluster      us-central1-c  1.21.5-gke.1302  104.197.242.99  e2-small      1.21.5-gke.1302 3         RUNNING
halo-cmm-simulator-demo-cluster    us-central1-c  1.21.5-gke.1302  35.184.107.75   e2-medium     1.21.5-gke.1302 3         RUNNING
halo-cmm-worker1-demo-cluster      us-central1-c  1.21.5-gke.1302  34.132.207.127  e2-medium     1.21.5-gke.1302 3         RUNNING
halo-cmm-worker2-demo-cluster      us-central1-c  1.21.5-gke.1302  34.68.24.213    e2-medium     1.21.5-gke.1302 3         RUNNING
```

# Step 7. Deploy the kingdom

1.  Open the `/src/main/k8s/dev/kingdom_gke.cue` file in your local branch.
    Update the tag variables in the beginning to the following values.

    ```shell
    # GloudProject: "halo-cmm-demo"
    # SpannerInstance: "demo-instance"
    # ContainerRegistry: "gcr.io"
    ```

    If you picked other names in the previous steps, update the value
    accordingly.

2.  Connect to the kingdom cluster in `kubectl` by running:

    ```shell
    gcloud container clusters get-credentials halo-cmm-kingdom-demo-cluster
    ```

3.  Upload all certificates and config files to GKE as a k8s secretes by
    running:

    ```shell
    kubectl apply -k src/main/k8s/testing/secretfiles/
    ```

    You should see something like secret/certs-and-configs-gb46dm7468 configured

    If the name of the secret is different, use the actual name in the following
    section of this doc.

    If you forget the name, you can also run the following command to get it

    ```shell
    kubectl get secrets
    ```

4.  Create an empty configmap since this is a branch new kingdom, there is no
    EDP registered in the system yet. Run

    ```shell
    touch /tmp/authority_key_identifier_to_principal_map.textproto
    kubectl create configmap config-files \
       --from-file=/tmp/authority_key_identifier_to_principal_map.textproto
    ```

5.  Deploy the kingdom by running (update the parameters if you use different
    values)

    ```shell
    bazel run \
      //src/main/kotlin/org/wfanet/measurement/tools:deploy_kingdom_to_gke \
      --define=k8s_kingdom_secret_name=certs-and-configs-gb46dm7468 \
      -- \
      --yaml-file=kingdom_gke.yaml \
      --cluster-name=halo-cmm-kingdom-demo-cluster \
      --environment=dev
    ```

6.  Verify everything is fine by run:

    ```shell
    $ kubectl get pods
    NAME                                                  READY STATUS    RESTARTS AGE
    gcp-kingdom-data-server-deployment-6c4d678876-44qzk   1/1   Running   2        1m
    kingdom-push-spanner-schema-job-g85qh                 0/1   Completed 0        1m
    system-api-server-deployment-755877996c-2s7w7         1/1   Running   0        1m
    v2alpha-public-api-server-deployment-7d6f998f6b-wv2rn 1/1   Running   0        1m

    $ kubectl get services
    NAME                      TYPE         CLUSTER-IP  EXTERNAL-IP    PORT(S)        AGE
    gcp-kingdom-data-server   ClusterIP    10.76.7.164 <none>         8443/TCP       158m
    kubernetes                ClusterIP    10.76.0.1   <none>         443/TCP        38h
    system-api-server         LoadBalancer 10.76.5.150 34.69.224.217  8443:31393/TCP 1m
    v2alpha-public-api-server LoadBalancer 10.76.8.37  34.123.211.251 8443:31883/TCP 1m
    ```

7.  Update the kingdom DNS records.

    The kingdom has two publicly accessible services, i.e, the system-api-server
    and the v2alpha-public-api-server in the above logs. Go to your domain
    management UI and create two Type A DNS records for these two external IPs.

    In our case, we point `public.kingdom.dev.halo-cmm.org` to `34.123.211.251`,
    and `system.kingdom.dev.halo-cmm.org` to `34.69.224.217`.

# Step 8. Deploy the resourceSetupJob

(You only need to finish this step if you are using a clean Spanner database.
The resources created are persisted in the Spanner Database, and can be used
again and again by different clusters and different deployments)

Now the kingdom is running, we will deploy a one time resourceSetup Job to the
same kingdom cluster. This job well create

-   1 account
-   1 MeasurementConsumer owned by the above account
-   1 API authentication key owned by the MeasurementConsumer
-   6 EventDataProviders with display names equal to edp1~6, respectively.
-   3 duchy certificates for each of the three duchies.

1.  Run the following command to deploy the resource-setup-job.

    ```shell
    bazel run src/main/kotlin/org/wfanet/measurement/tools:deploy_kingdom_to_gke \
        --define=k8s_kingdom_secret_name=certs-and-configs-gb46dm7468 \
        -- --yaml-file=resource_setup_gke.yaml \
        --cluster-name=halo-cmm-kingdom-demo-cluster --environment=dev
    ```

2.  See the log by running

    ```shell
    kubectl logs -f job.batch/resource-setup-job
    ```

    You should get something like.

    ```shell
    $ kubectl logs -f job.batch/resource-setup-job
    Jan 26, 2022 9:50:43 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Starting with RunID: 2022-01-2621-50-31-988 ...
    Jan 26, 2022 9:50:51 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created data provider: dataProviders/HRL1wWehTSM
    Jan 26, 2022 9:50:51 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created data provider: dataProviders/djQdz2ehSSE
    Jan 26, 2022 9:50:51 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created data provider: dataProviders/SQ99TmehSA8
    Jan 26, 2022 9:50:51 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created data provider: dataProviders/TBZkB5heuL0
    Jan 26, 2022 9:50:51 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created data provider: dataProviders/HOCBxZheuS8
    Jan 26, 2022 9:50:51 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created data provider: dataProviders/VGExFmehRhY
    Jan 26, 2022 9:50:58 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created measurement consumer: measurementConsumers/TGWOaWehLQ8
    Jan 26, 2022 9:50:58 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: API key for measurement consumer measurementConsumers/TGWOaWehLQ8: ZEhkVZhe1Q0
    Jan 26, 2022 9:50:59 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created certificate duchies/aggregator/certificates/DTDmi5he1do
    Jan 26, 2022 9:50:59 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created certificate duchies/worker1/certificates/Vr9cWmehKZM
    Jan 26, 2022 9:50:59 PM org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup process
    INFO: Successfully created certificate duchies/worker2/certificates/QBC5Lphe1p0
    ```

    ***All resourceNames in the logs are important, and will be used when
    deploying the corresponding components. keep a record of the log.*** We will
    use the values in the above log in the following commands, please update the
    parameters using the actual values you get in your log.

# Step 9. Restart the kingdom Public API

In order for the EDP simulator to talk to the kingdom, we need to register them
in the kingdom config file called `AuthorityKeyToPrincipalMap`.

Recall that we've created an empty configMap in step 7 earlier. Now we update
the file with some meaningful content shown below. Remember to replace these
principal_resource_names with the values obtained in the logs in Step 8. The
AKIDs come from the EDP certificates in
[secretfiles](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles).

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

run the following command to update the file with the above content.

```shell
vi /tmp/authority_key_identifier_to_principal_map.textproto
```

Then, run the following command to replace the ConfigMap in the kingdom cluster

```shell
kubectl create configmap config-files --output=yaml --dry-run=client \
--from-file=/tmp/authority_key_identifier_to_principal_map.textproto \
| kubectl replace -f -
```

You can verify that the config file is successfully update by running

```shell
kubectl describe configmaps config-files
```

Finally, restart the Kingdom deployments that depend on config-files. At the
moment, this is just the public API server.

```shell
kubectl rollout restart deployments/v2alpha-public-api-server-deployment
```

# Step 10. Deploy the Duchies

In this step, we create three duchies. The commands are almost the same except
for the parameters in the commands.

First, open the `/src/main/k8s/dev/duchy_gke.cue` file in your local branch.
Update the tag variables in the beginning to the following values.

```shell
# KingdomSystemApiTarget: "your kingdom's system API domain or subdomain:8443"
# GloudProject: "halo-cmm-demo"
# SpannerInstance: "demo-instance"
# CloudStorageBucket: "halo-cmm-demo-bucket"
# ContainerRegistry: "gcr.io"
```

If you picked other names in the previous steps, update the value accordingly.

Then in the same file, update the following part to match with the domains you
plan to use

```shell
_computation_control_targets: {
  "aggregator": "your aggregator's system API domain:8443"
  "worker1": "your worker1's system API domain:8443"
  "worker2": "your worker2's system API domain:8443"
}
```

## Aggregator

1.  Connect to the aggregator cluster in `kubectl` by running:

    ```shell
    gcloud container clusters get-credentials halo-cmm-aggregator-demo-cluster
    ```

2.  Create the k8s secret and configmap.

    ```shell
    kubectl apply -k src/main/k8s/testing/secretfiles/
    kubectl create configmap config-files \
    --from-file=/tmp/authority_key_identifier_to_principal_map.textproto
    ```

3.  Deploy all components in the aggregator (replace the duchy_cert_name in the
    command)

    ```shell
    bazel run \
      //src/main/kotlin/org/wfanet/measurement/tools:deploy_duchy_to_gke \
      --define=k8s_duchy_secret_name=certs-and-configs-gb46dm7468 \
      --define=duchy_name=aggregator \
      --define=duchy_cert_name=duchies/aggregator/certificates/DTDmi5he1do \
      --define=duchy_protocols_setup_config=aggregator_protocols_setup_config.textproto \
      -- \
      --yaml-file=duchy_gke.yaml \
      --cluster-name=halo-cmm-aggregator-demo-cluster \
      --environment=dev
    ```

4.  Update the DNS records for the aggregator's public and system APIs

    ```shell
    $ kubectl get services
    NAME                                        TYPE         CLUSTER-IP  EXTERNAL-IP   PORT(S)        AGE
    aggregator-async-computation-control-server ClusterIP    10.16.12.88 <none>        8443/TCP       97m
    aggregator-computation-control-server       LoadBalancer 10.16.4.6   34.133.4.73   8443:30898/TCP 97m
    aggregator-requisition-fulfillment-server   LoadBalancer 10.16.1.214 35.224.63.238 8443:31523/TCP 97m
    aggregator-spanner-computations-server      ClusterIP    10.16.2.183 <none>        8443/TCP       97m
    kubernetes                                  ClusterIP    10.16.0.1   <none>        443/TCP        32h
    ```

    The public API is `aggregator-requisition-fulfillment-server`, and we point
    `public.aggregator.dev.halo-cmm.org` to `35.224.63.238` The system API is
    `aggregator-computation-control-server`, and we point
    `system.aggregator.dev.halo-cmm.org` to `34.133.4.73`

## Worker1

1.  Connect to the worker1 cluster in kubectl by running:

    ```shell
    gcloud container clusters get-credentials halo-cmm-worker1-demo-cluster
    ```

2.  Create the k8s secret and configmap.

    ```shell
    kubectl apply -k src/main/k8s/testing/secretfiles/
    kubectl create configmap config-files \
        --from-file=/tmp/authority_key_identifier_to_principal_map.textproto
    ```

3.  Deploy all components in the worker1 duchy (replace the duchy_cert_name in
    the command)

    ```shell
    bazel run src/main/kotlin/org/wfanet/measurement/tools:deploy_duchy_to_gke \
      --define=k8s_duchy_secret_name=certs-and-configs-gb46dm7468 \
      --define=duchy_name=worker1 \
      --define=duchy_cert_name=duchies/worker1/certificates/Vr9cWmehKZM \
      --define=duchy_protocols_setup_config=non_aggregator_protocols_setup_config.textproto \
      -- \
      --yaml-file=duchy_gke.yaml --cluster-name=halo-cmm-worker1-demo-cluster \
      --environment=dev
    ```

4.  Update the DNS records for the worker1's public and system APIs

    ```shell
    $ kubectl get services
    NAME                                     TYPE         CLUSTER-IP   EXTERNAL-IP   PORT(S)        AGE
    kubernetes                               ClusterIP    10.16.16.1   <none>        443/TCP        32h
    worker1-async-computation-control-server ClusterIP    10.16.19.25  <none>        8443/TCP       89m
    worker1-computation-control-server       LoadBalancer 10.16.22.156 34.66.77.253  8443:32245/TCP 89m
    worker1-requisition-fulfillment-server   LoadBalancer 10.16.17.136 35.192.125.41 8443:32645/TCP 89m
    worker1-spanner-computations-server      ClusterIP    10.16.22.127 <none>        8443/TCP       89m
    ```

    The public API is worker1-requisition-fulfillment-server, and we point
    public.worker1.dev.halo-cmm.org to 35.192.125.41 The system API is
    worker1-computation-control-server, and we point
    system.worker1.dev.halo-cmm.org to 34.66.77.253

## Worker2

1.  Connect to the worker2 cluster in kubectl by running:

    ```shell
    gcloud container clusters get-credentials halo-cmm-worker2-demo-cluster
    ```

2.  Create the k8s secret and configmap.

    ```shell
    kubectl apply -k src/main/k8s/testing/secretfiles/
    kubectl create configmap config-files \
    --from-file=/tmp/authority_key_identifier_to_principal_map.textproto
    ```

3.  Deploy all components in the worker2 duchy (replace the duchy_cert_name in
    the command)

    ```shell
    bazel run \
      //src/main/kotlin/org/wfanet/measurement/tools:deploy_duchy_to_gke \
      --define=k8s_duchy_secret_name=certs-and-configs-gb46dm7468 \
      --define=duchy_name=worker2 \
      --define=duchy_cert_name=duchies/worker2/certificates/QBC5Lphe1p0 \
      --define=duchy_protocols_setup_config=non_aggregator_protocols_setup_config.textproto \
      -- \
      --yaml-file=duchy_gke.yaml --cluster-name=halo-cmm-worker2-demo-cluster \
      --environment=dev
    ```

4.  Update the DNS records for the worker2's public and system APIs

    ```shell
    $ kubectl get services
    NAME                                     TYPE         CLUSTER-IP  EXTERNAL-IP    PORT(S)        AGE
    kubernetes                               ClusterIP    10.28.0.1   <none>         443/TCP        31h
    worker2-async-computation-control-server ClusterIP    10.28.10.33 <none>         8443/TCP       90m
    worker2-computation-control-server       LoadBalancer 10.28.14.71 35.239.245.108 8443:32339/TCP 90m
    worker2-requisition-fulfillment-server   LoadBalancer 10.28.8.159 35.222.74.184  8443:32504/TCP 90m
    worker2-spanner-computations-server      ClusterIP    10.28.13.40 <none>         8443/TCP       90m
    ```

    The public API is worker2-requisition-fulfillment-server, and we point
    public.worker2.dev.halo-cmm.org to 35.222.74.184 The system API is
    worker2-computation-control-server, and we point
    system.worker2.dev.halo-cmm.org to 35.239.245.108

You can now verify the duchy works. Take duchy2 as an example. You should see
something like this

```shell
$ kubectl get pods
NAME                                                            READY STATUS    RESTARTS AGE
worker2-async-computation-control-server-deployment-5f6b7dc6gt4 1/1   Running   0        107s
worker2-computation-control-server-deployment-6574d489db-4nlvd  1/1   Running   0        107s
worker2-herald-daemon-deployment-f4464fdbb-lwrmm                1/1   Running   0        107s
worker2-liquid-legions-v2-mill-daemon-deployment-55cdf8f78d7gxf 1/1   Running   0        107s
worker2-push-spanner-schema-job-cd2xk                           0/1   Completed 0        107s
worker2-requisition-fulfillment-server-deployment-64b4d6bbfnz94 1/1   Running   0        107s
worker2-spanner-computations-server-deployment-f57b576bc-wvvhk  1/1   Running   1        107s
```

View the logs of the mill, it should show that it is
`pollAndProcessNextComputation`, but none is available yet. (if there are errors
in the beginning, it is fine. That is because the dependent service is not ready
yet)

```shell
$ kubectl logs deployment/worker2-liquid-legions-v2-mill-daemon-deployment
...
Jan 26, 2022 11:35:49 PM org.wfanet.measurement.duchy.daemon.mill.MillBase pollAndProcessNextComputation
INFO: @Mill worker2-liquid-legions-v2-mill-daemon-deployment-55cdf8f78d7gxf:
No computation available, waiting for the next poll...
Jan 26, 2022 11:35:50 PM org.wfanet.measurement.duchy.daemon.mill.MillBase pollAndProcessNextComputation
INFO: @Mill worker2-liquid-legions-v2-mill-daemon-deployment-55cdf8f78d7gxf:
Polling available computations...
Jan 26, 2022 11:35:50 PM org.wfanet.measurement.duchy.daemon.mill.MillBase pollAndProcessNextComputation
INFO: @Mill worker2-liquid-legions-v2-mill-daemon-deployment-55cdf8f78d7gxf:
No computation available, waiting for the next poll... ...
```

# Step 11. Deploy the EDP Simulators

In this step, we deploy 6 EDP simulators in the same GCP clusters. Each of them
acts as one of the 6 different EDPs.

1.  Open the `/src/main/k8s/dev/edp_simulator_gke.cue` file in your local
    branch. Update the tag variables in the beginning to the following values.

    ```shell
    #KingdomPublicApiTarget: "your kingdom public API domain/subdomain:8443"
    #DuchyPublicApiTarget: "your kingdom system API domain/subdomain:8443"
    #GloudProject: "halo-cmm-demo"
    #CloudStorageBucket:"halo-cmm-demo-bucket"
    #ContainerRegistry: "gcr.io"
    ```

    If you picked other names in the previous steps, update the value
    accordingly.

2.  connect to the simulator cluster in `kubectl` by running:

    ```shell
    gcloud container clusters get-credentials halo-cmm-simulator-demo-cluster
    ```

3.  Create the k8s secret which contains the certificates and config files used
    by the EDP simulators.

    ```shell
    kubectl apply -k src/main/k8s/testing/secretfiles/
    ```

    Since we are using the same files to create the secret, the name of the k8s
    secret should be the same as the one in the kingdom cluster. And in our
    case, it is certs-and-configs-gb46dm7468

4.  Deploy all 6 EDP simulators by running

    ```shell
    bazel run \
      //src/main/kotlin/org/wfanet/measurement/tools:deploy_edp_simulator_to_gke \
      --define=k8s_simulator_secret_name=certs-and-configs-gb46dm7468 \
      --define=mc_name=measurementConsumers/TGWOaWehLQ8 \
      --define=edp1_name=dataProviders/HRL1wWehTSM \
      --define=edp2_name=dataProviders/djQdz2ehSSE \
      --define=edp3_name=dataProviders/SQ99TmehSA8 \
      --define=edp4_name=dataProviders/TBZkB5heuL0 \
      --define=edp5_name=dataProviders/HOCBxZheuS8 \
      --define=edp6_name=dataProviders/VGExFmehRhY \
      -- \
      --yaml-file=edp_simulator_gke.yaml \
      --cluster-name=halo-cmm-simulator-demo-cluster --environment=dev
    ```

5.  Verify everything is fine.

    ```shell
    $ kubectl get pods NAME READY STATUS RESTARTS AGE
    edp1-simulator-deployment-b76f8b667-2zvcp 1/1 Running 0 64s
    edp2-simulator-deployment-7d9844f554-mx7hc 1/1 Running 0 64s
    edp3-simulator-deployment-76dd8699cc-dxtc4 1/1 Running 0 64s
    edp4-simulator-deployment-6664db67b7-jz2gf 1/1 Running 0 64s
    edp5-simulator-deployment-57989ff79d-2zblf 1/1 Running 0 64s
    edp6-simulator-deployment-65cd9f4c6b-ck6jm 1/1 Running 0 63s
    ```

    Check the log of any one of them, you should see something like this

    ```shell
    $ kubectl logs -f edp1-simulator-deployment-b76f8b667-2zvcp
    Picked up JAVA_TOOL_OPTIONS:
    Jan 26, 2022 10:55:16 PM org.wfanet.measurement.loadtest.dataprovider.EdpSimulator createEventGroup
    INFO: Successfully created eventGroup dataProviders/HRL1wWehTSM/eventGroups/NeQ2xZiZsN0...
    Jan 26, 2022 10:55:16 PM org.wfanet.measurement.loadtest.dataprovider.EdpSimulator executeRequisitionFulfillingWorkflow
    INFO: Executing requisitionFulfillingWorkflow...
    Jan 26, 2022 10:55:18 PM org.wfanet.measurement.loadtest.dataprovider.EdpSimulator executeRequisitionFulfillingWorkflow
    INFO: No unfulfilled requisition. Polling again later...
    Jan 26, 2022 10:55:18 PM org.wfanet.measurement.loadtest.dataprovider.EdpSimulator executeRequisitionFulfillingWorkflow
    INFO: Executing requisitionFulfillingWorkflow...
    Jan 26, 2022 10:55:18 PM org.wfanet.measurement.loadtest.dataprovider.EdpSimulator executeRequisitionFulfillingWorkflow
    INFO: No unfulfilled requisition. Polling again later... ...
    ```

# Step 12 (Final one). Deploy the Frontend simulator and complete the correctness test

Now the kingdom + 3 duchies + 6 EDP simulators are all deployed and running. We
can deploy the Frontend simulator to act as the measurement consumer and create
a measurement. Then, the frontendSimulator will

-   periodically (time interval of 30s) polling from the kingdom to get the
    result of the measurement.
-   then read the raw data provided by the EDP simulators and compute the
    expected measurement result.
-   compare the measured result from the kingdom to the expected result and make
    sure they pass.

1.  Connect to the simulator cluster in `kubectl` by running:

    ```shell
    gcloud container clusters get-credentials halo-cmm-simulator-demo-cluster
    ```

2.  Create the k8s secret

    ```shell
    kubectl apply -k src/main/k8s/testing/secretfiles/
    ```

3.  Deploy the frontend simulator job ( replace the mc_name and mc_api_key
    first)

    ```shell
    bazel run \
      //src/main/kotlin/org/wfanet/measurement/tools:deploy_frontend_simulator_to_gke \
      --define=k8s_simulator_secret_name=certs-and-configs-gb46dm7468 \
      --define=mc_name=measurementConsumers/TGWOaWehLQ8 \
      --define=mc_api_key=ZEhkVZhe1Q0 \
      -- \
      --yaml-file=frontend_simulator_gke.yaml \
      --cluster-name=halo-cmm-simulator-demo-cluster --environment=dev
    ```

The frontend simulator job takes about 6 minutes to complete, since that is how
long the MPC protocol takes to finish. Eventually, you should see logs like this

```shell
$ kubectl logs -f job.batch/frontend-simulator-job
Jan 27, 2022 12:47:01 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Created measurement measurementConsumers/TGWOaWehLQ8/measurements/Y6gTFpj__3g.
Jan 27, 2022 12:47:02 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Computation not done yet, wait for another 30 seconds.
Jan 27, 2022 12:47:32 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
...
...
Jan 27, 2022 12:52:33 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Got computed result from Kingdom: reach {
  value: 11542
}
frequency {
  relative_frequency_distribution {
    key: 1
    value: 0.2601439790575916
  }
  relative_frequency_distribution {
    key: 2
    value: 0.17981020942408377
  }
  ...
}
Jan 27, 2022 12:52:39 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Expected result: reach {
  value: 11570
}
frequency {
  relative_frequency_distribution {
    key: 1
    value: 0.25174145472217724
  }
  relative_frequency_distribution {
    key: 2
    value: 0.18078729953021222
  }
  ...
}
Jan 27, 2022 12:52:40 AM org.wfanet.measurement.loadtest.frontend.FrontendSimulator process
INFO: Computed result is equal to the expected result. Correctness Test passes.
```

# How to monitor the computation after a measurement is created?

There are two places you can monitor the process of a measurement. The log of
various pods and the Kingdom Spanner table.

For monitoring purposes, we will mainly use the GCloud Spanner UI to query the
databases. If something is wrong, we will see logs to debug.

1.  Visit the GCloud console
    [spanner](https://console.cloud.google.com/spanner/instances) page.
2.  Select demo-instance
3.  Select kingdom
4.  Click Query on the left

## Query the measurement status

```shell
SELECT
  MeasurementId,
  CASE State
    WHEN 1 THEN "PENDING_REQUISITION_PARAMS"
    WHEN 2 THEN "PENDING_REQUISITION_FULFILLMENT"
    WHEN 3 THEN "PENDING_PARTICIPANT_CONFIRMATION"
    WHEN 4 THEN "PENDING_COMPUTATION"
    WHEN 5 THEN "SUCCEEDED"
    WHEN 6 THEN "FAILED"
    WHEN 7 THEN "CANCELLED"
    ELSE "MEASUREMENT_STATE_UNKNOWN"
  END AS State,
  CAST(JSON_VALUE(MeasurementDetailsJson, '$.encryptedResult') AS STRING) AS Result,
FROM
  Measurements
```

Example result

![query-1](query-1.png)

Note that the final result is encrypted, so you won't be able to see the reach
and frequency result in plaintext.

## Query the requisition status

```shell
SELECT
  MeasurementId,
  RequisitionId,
  CASE State
    WHEN 1 THEN "UNFULFILLED"
    WHEN 2 THEN "FULFILLED"
    WHEN 3 THEN "REFUSED"
    ELSE "STATE_UNKNOWN"
  END AS State,
FROM
  Requisitions
```

Example result ![query-2](query-2.png)

If all requisitions are stuck at `UNFULFILLED` state for more than 1 minute,
then something is wrong.

## Query the MPC protocol progress

```shell
SELECT
  MeasurementId,
  FORMAT_TIMESTAMP("%Y-%m-%d %H:%M:%E2S", CreateTime) as CreateTime,
  CASE DuchyId
    WHEN 1234 THEN "Aggregator"
    WHEN 2345 THEN "Worker 1"
    WHEN 3456 THEN "Worker 2"
    ELSE "UNKNOWN"
  END as MpcWoker,
  JSON_VALUE(DuchyMeasurementLogDetailsJson,'$.stageAttempt.stageName') AS StageName,
  JSON_VALUE(DuchyMeasurementLogDetailsJson,'$.stageAttempt.attemptNumber') AS Attempt,
FROM DuchyMeasurementLogEntries
ORDER BY CreateTime DESC
```

Example result

![query-3](query-3.png)

# Cleanup

It costs money to keep all the kingdom, duchies and simulators running, even if
there is no measurement being computed. With the setting of the clusters in this
docs, it costs several tens of dollars per 24 hours. Most cost comes from these
Kubernetes clusters.

If you are just curious about the project and want to try running the
correctnessTest once and don't plan to run it again in the near future.

-   Just delete the GCP project. Everything will be gone.

If you plan to run the correctnessTest once in a while, but not frequently, for
example once a week.

-   Delete all GKE clusters in the GCP console
-   No need to touch the GCloud storage bucket and BigQuery tables

If you plan to run the correctnessTest lots of time, but want an empty database
before a certain run.

-   Delete all tables in the Spanner instance.
-   or simply delete the Spanner instance and create it again.

Note: In Step 8, the resources created are persisted in the Spanner database.
You can use the same resourceNames to complete as many correctnessTest as you
like.

You only need to run step 8 if you have reset the kingdom Spanner database. In
other words, delete/recreate the clusters or redeploy the kingdom or any other
components, doesn't have any impact on the resources, thus the parameters in
those commands don't change. As a result, you should keep a copy of those result
names in the log of the ResourceSetupJob, if you want to reuse the same
resources (EDP, MC, etc.) for different measurements.

# Troubleshooting

If anything is wrong, first check

1.  if the resource name in the commands is correct.
2.  if you have created the secret in all clusters and configmap in all clusters
    but the simulator cluster
3.  if you have set the DNS record for all kingdom and duchies public and system
    APIs. (In total, there are 8 of them).

## Requisition can not be fulfilled

Check the log of any EDP simulator, if the FulfillRequisition RPC fails, it is
highly likely that the IP address the simulator sends traffic to is not correct.
If you are reusing the same subdomain and are updating its IP address, the
update may not be effective for a long time. So the best practice is to create a
new Type A record instead of updating an existing one.
