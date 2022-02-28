# How to deploy Panel Match Resource Setup on GKE

Last updated: 2022-02-14

***The doc is updated to work with commit
a1c9ab2860efbf2ee8f70d74aab64c5b6aa13b6a. Newer changes may require some
commands to be updated. Stay tuned.***

## What are we creating/deploying?

-   Under already Kingdom deployed GKE cluster
    -   1 Kubernetes secret with Exchange Workflow
    -   1 Kubernetes Job
        -   resource-setup-job

## Step 0. Before You Start

Follow [Machine Setup](machine-setup.md).

Deploy the Kingdom by simply following these [Instructions](kingdom-deployment.md). 
You should see all the services are up-to-date and ready.

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
bazel test //src/...
```

Read this
[page](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/building.md)
for more details if you have trouble building/testing the codes.

## Step 1. Prepare the docker image.

In this example, we use Google Cloud
[container-registry](https://cloud.google.com/container-registry) to store our
docker images. Enable the Google Container Registry API in the console if you
haven't done it. If you use other repositories, adjust the commands accordingly.

1. Clone the halo
    [cross-media-measurement](https://github.com/world-federation-of-advertisers/cross-media-measurement)
    git repository locally, and check out the targeted commit (likely the one
    mentioned in the beginning of the doc).
2. Build and push the binaries to gcr.io by running the following commands

    ```shell
    $bazel run src/main/docker/push_panel_match_resource_setup_runner_image \
    -c opt --define container_registry=gcr.io \
    --define image_repo_prefix=halo-kingdom-demo
    ```

    You should see log like "Successfully pushed Docker image to 
    gcr.io/ads-open-measurement/loadtest/panel-match-resource-setup:latest"

    If you see an `UNAUTHORIZED` error, run the following command and retry.

    ```shell
    gcloud auth configure-docker
    gcloud auth login
    ```

## Step 2. Update Kubernetes secrets.

***(Note: If you have already exchange_workflow.textproto in your remote secrets, simply skip this step. This file is critical configuration for Panel Match daemon and same configuration will be used in daemons as well.)***

The kingdom and resource setup binaries are configured to read certificates and config files from the
mounted Kubernetes secret volume.

At this point all the secrets and files should be in our GKE cluster, 
but if you don't have exchange_workflow.textproto simply run the following command.

7.  `exchange_workflow.textproto`

    -   [Example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles/exchange_workflow.textproto)
    
Note: there are
[test certificates and config files](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/k8s/testing/secretfiles)
checked in the cross-media-measurement repo. You can run the following command
to create a secret for testing.

and run

```shell
kubectl apply -k src/main/k8s/testing/secretfiles
```

Now the secret is created in the `halo-cmm-kingdom-demo-cluster`. You should be
able to see the secret by running

```shell
kubectl get secrets
```

We assume the name is `certs-and-configs-abcdedf` and will use it in the
following documents.

## Step 3. Update cue files.

We recommend that you modify the
[panel_match_resource_setup_gke.cue](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/panel_match_resource_setup_gke.cue)
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

## Step 4. Deploy the resource setup job to GKE

Create an empty configmap since this is a branch new kingdom, there is no EDP
registered in the system yet. Run

```shell
touch /tmp/authority_key_identifier_to_principal_map.textproto
kubectl create configmap config-files \
--from-file=/tmp/authority_key_identifier_to_principal_map.textproto
```

Deploy the Panel Match Resource Setup by running (update the parameters if you use different
values)

```shell
tools/bazel-container-run
src/main/kotlin/org/wfanet/measurement/tools:deploy_kingdom_to_gke \
--define=k8s_kingdom_secret_name=certs-and-configs-abcdedg \
-- --yaml-file=oanel_match_resource_setup_gke.yaml --cluster-name=halo-cmm-kingdom-demo-cluster \
--environment=dev
```

Now all components will be successfully deployed to your GKE cluster.
You can verify by running

```shell
$ kubectl get deployments
NAME                                 READY UP-TO-DATE AVAILABLE AGE
resource-setup-job-6p74z             1/1   1          1         1s
gcp-kingdom-data-server-deployment   1/1   1          1         1m
system-api-server-deployment         1/1   1          1         1m
v2alpha-public-api-server-deployment 1/1   1          1         1m
```

Note: If you are not modifying the existing `panel_match_resource_setup_gke.cue` file, but are
creating another cue file in the previous step, you need to update the
`yaml-file` and `environment` args to make it work.

## Step 5. Retrieve Resource Keys from logs

Once the Resource Setup Job completed we need to see logs and retrieve 
all the resource keys and ids necessary to trigger Panel Match daemons.

```shell
NAME                                                    READY   STATUS
resource-setup-job-6p74z                                0/1     Completed
gcp-kingdom-data-server-deployment-777784487f-9jvw7     1/1     Running
kingdom-push-spanner-schema-job-5pvgq                   0/1     Completed
system-api-server-deployment-6764c4c6db-qs6gp           1/1     Running
v2alpha-public-api-server-deployment-65cddd995f-6rz4k   1/1     Running
```

***(Note: See resource-setup-job-6p74z status is Completed. 6p74z here is arbitrary number, you should see different numbers instead.)***

Now, check the logs of completed job.

```shell
$ kubectl logs -f resource-setup-job-6p74z 
```

We should see something like below;

```shell
INFO: Starting with RunID: 2022-02-1323-45-32-427 ...
Feb 13, 2022 11:45:55 PM org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup process
INFO: Successfully created model provider: modelProviders/Fo1dngqFfCg
Feb 13, 2022 11:45:56 PM org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup process
INFO: Successfully created data provider: dataProviders/DQkfCAqFdeg
Feb 13, 2022 11:45:57 PM org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup process
INFO: Successfully created Recurring Exchange: recurringExchanges/GrbSFAqFcSQ
```

Here, please note down the resource keys; modelProviders/Fo1dngqFfCg, dataProviders/DQkfCAqFdeg, and recurringExchanges/GrbSFAqFcSQ.

We are going to plug these in Panel Match daemon deployment later.
