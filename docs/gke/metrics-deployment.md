# Halo Metrics Deployment on GKE

How to add visibility into CMMS component metrics to a cluster.

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

Many operations can be done either via the gcloud CLI or the Google Cloud web
console. This guide picks whichever is most convenient for that operation. Feel
free to use whichever you prefer.

### What are we creating/deploying?

-   1 GMP ClusterPodMonitoring
    -   opentelemetry-collector-pod-monitor
-   1 GMP PodMonitoring
    -   collector-pod-monitor
-   1 GMP Rules
    -   recording-rules
-   1 OpenTelemetry Operator OpenTelemetryCollector
    -   default
-   1 OpenTelemetry Operator Instrumentation
    -   open-telemetry-java-agent
-   3 Kubernetes ConfigMaps
    -   default-collector
    -   grafana-config
    -   grafana-datasource-and-dashboard-provider
-   1 Kubernetes Secret
    -   grafana-auth
-   3 Kubernetes Deployments
    -   default-collector
    -   grafana-deployment
    -   prometheus-frontend-deployment
-   5 Kubernetes Services
    - default-collector
    - default-collector-headless  
    - default-collector-monitoring
    - grafana
    - prometheus-frontend
-   3 Kubernetes NetworkPolicies
    -   opentelemetry-collector-network-policy
    -   grafana-network-policy
    -   prometheus-frontend-network-policy

## Before you start

Deploy a Halo component. See the related guides:
[Create Kingdom Cluster](kingdom-deployment.md),
[Create Duchy Cluster](duchy-deployment.md), or
[Create Reporting Cluster](reporting-server-deployment.md).

## Enable Managed Service for Prometheus on the Cluster

This can be done via the Google Cloud Console under "Features", or using the
gcloud CLI. For example, assuming a cluster named "kingdom":

```shell
gcloud beta container clusters update kingdom --enable-managed-prometheus
```

## Service Accounts

Make sure that the least-privilege service account you created for the cluster
has permissions to access the Cloud Monitoring API. See
[Cluster Configuration](cluster-config.md#cluster-service-account).

The Prometheus frontend will need a K8s service account that has access to Cloud
Monitoring (e.g. the `roles/iam.workloadIdentityUser` role). See
[Configure a service account for Workload Identity](https://cloud.google.com/stackdriver/docs/managed-prometheus/query#gmp-wli-svcacct).
For the `dev` configuration, the K8s service account is named `gmp-monitoring`.

## Create the K8s Object Configurations

Deploying to the cluster is generally done by applying a K8s object
configuration file. You can use the `dev` configurations as a base to get
started. The `dev` configurations are YAML files that are generated from files
written in [CUE](https://cuelang.org/) using Bazel rules.

You do any customization in to the CUE files, or in the generated YAML file.

### OpenTelemetry Collectors and Instrumentation

The default `dev` configuration for OpenTelemetry collection is in
[`open_telemetry_gke.cue`](../../src/main/k8s/dev/open_telemetry_gke.cue), which
depends on [`open_telemetry.cue`](../../src/main/k8s/open_telemetry.cue).

The default build target is `//src/main/k8s/dev:open_telemetry_gke`.

### Prometheus Monitoring and Rules

The `dev` configuration is in
[`prometheus_gke.cue`](../../src/main/k8s/dev/prometheus_gke.cue). The build
target is `//src/main/k8s/dev:prometheus_gke`.

### Grafana

The `dev` configuration is in
[`grafana_gke.cue`](../../src/main/k8s/dev/grafana_gke.cue). The build target is
`//src/main/k8s/dev:grafana_gke`.

In order to build the target, you will need to create a `grafana-auth` secret.
You can use `kubectl` to create this. To reduce the likelihood of leaking your
password, we read it in from STDIN.

Tip: Ctrl+D is the usual key combination for closing the input stream.

Assuming the database username is `user`, run:

```shell
kubectl create secret generic grafana-auth --type='kubernetes.io/basic/auth' \
  --append-hash --from-file=password=/dev/stdin --from-literal=user=user
```

Then use the secret name when building the target:

```shell
bazel build //src/main/k8s/dev:grafana_gke \
  --define=k8s_grafana_secret_name=grafana-auth-dmg429kb29
```

## Apply the K8s Object Configurations

### Install cert-manager

You must use a [cert-manager](https://github.com/cert-manager/cert-manager/),
[OpenTelemetry Operator](https://github.com/open-telemetry/opentelemetry-operator/),
and collector image that are compatible with each other. See the
[Compatibility matrix](https://github.com/open-telemetry/opentelemetry-operator#compatibility-matrix)
and the collector image specified in
[`open_telemetry.cue`](../../src/main/k8s/open_telemetry.cue).

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.10.0/cert-manager.yaml
```

### Install OpenTelemetry Operator

```shell
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.62.1/opentelemetry-operator.yaml
```

### Apply OpenTelemetry and Prometheus Configurations

You can just use `kubectl apply`, specifying the configuration files you created
in the previous step.

### Apply Grafana Configurations

Before applying the Grafana object configuration, you'll need to create a
`grafana-config` ConfigMap which houses the core Grafana settings in a file
named `grafana.ini`. See
[Grafana documentation](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana).

To create the ConfigMap using our sample
[`grafana.ini`](../../src/main/k8s/testing/grafana/grafana.ini), run

```shell
kubectl create configmap grafana-config \
  --from-file=src/main/k8s/testing/grafana/grafana.ini
```

After this, you can apply the object configuration file using `kubectl apply`.

### Verification

Now all components should be successfully deployed to your GKE cluster. You can
verify by running

```shell
kubectl get clusterpodmonitorings
```

```shell
kubectl get -n gmp-system podmonitorings
```

```shell
kubectl get rules
```

```shell
kubectl get opentelemetrycollectors
```

```shell
kubectl get instrumentations
```

```shell
kubectl get configmaps
```

```shell
kubectl get secrets
```

```shell
kubectl get deployments
```

```shell
kubectl get services
```

```shell
kubectl get networkpolicy
```

You should see something like the following:

```
NAME                                  AGE
opentelemetry-collector-pod-monitor   4h7m
prometheus-pod-monitor                4h7m
```

```
NAME                    AGE
collector-pod-monitor   5m12s
```

```
NAME              AGE
recording-rules   3m4s
```

```
NAME          MODE         VERSION   AGE
default       deployment   0.60.0    131m
```

```
NAME                        AGE   ENDPOINT   SAMPLER   SAMPLER ARG
open-telemetry-java-agent   68s
```

```
NAME                                         DATA   AGE
default-collector                         1      60s
grafana-config                               1      43s
grafana-datasource-and-dashboard-provider    1      43s
```

```
NAME                           TYPE                       DATA   AGE
grafana-auth-dmg429kb29        kubernetes.io/basic/auth   2      42s
```

```
NAME                                   READY   UP-TO-DATE   AVAILABLE   AGE
default-collector                   1/1     1            1           7m4s
grafana-deployment                     1/1     1            1           6m46s
prometheus-frontend-deployment         1/1     1            1           7m21s
```

```
NAME                              TYPE           CLUSTER-IP     EXTERNAL-IP   PORT(S)          AGE
default-collector              ClusterIP      10.96.119.25   <none>        4317/TCP         5m13s
default-collector-headless     ClusterIP      None           <none>        4317/TCP         5m13s
default-collector-monitoring   ClusterIP      10.108.6.18    <none>        8888/TCP         3m18s
grafana                           ClusterIP      10.108.8.178   <none>        3000/TCP         3m
prometheus-frontend               ClusterIP      10.108.3.88    <none>        9090/TCP         3m35s
```

```
NAME                                     POD-SELECTOR                                          AGE
grafana-network-policy                   app=grafana-app                                       5m56s
opentelemetry-collector-network-policy   app.kubernetes.io/component=opentelemetry-collector   51m
prometheus-frontend-network-policy       app=prometheus-frontend-app                           6m29s
```

## Restart Deployments to Start Collecting Metrics

Once the above configurations have been applied, the cluster deployments should
start exporting metrics upon their next revision.

Verify the pods have the label `scrape=true` with

```shell
kubectl describe pod <NAME-OF-POD>
```

If the label is missing, recreate the k8s manifest from the latest cue files
that add the label then apply the updated manifests before restarting.

## Verify Managed Prometheus can Scrape Metrics

Visit the
[Managed Prometheus](https://console.cloud.google.com/monitoring/prometheus)
page in Cloud Console. Query `up` and `scrape_samples_scraped`.

The first one tells you which targets it can find and whether they are up, and
the latter is a good way to check that scraping is occurring. If it hasn't been
long enough, the latter might show all 0's, but after a couple of minutes you
should be seeing results for every target that is up.

## Adding Additional Metrics

The above adds OpenTelemetry JVM and RPC metrics, as well as self-monitoring of 
the Managed Prometheus collectors. With the above as a base, it is possible to 
add other metrics that can be scraped.

### kubelet and cAdvisor

See
[kubelet](https://cloud.google.com/stackdriver/docs/managed-prometheus/setup-managed#kubelet-metrics)

## List of OpenTelemetry Metrics on Prometheus Dashboard

### OpenTelemetry Auto Instrumented RPC and JVM Metrics

-   rpc_client_duration_bucket
-   rpc_client_duration_count
-   rpc_client_duration_sum
-   rpc_server_duration_bucket
-   rpc_server_duration_count
-   rpc_server_duration_sum
-   process_runtime_jvm_buffer_count
-   process_runtime_jvm_buffer_limit
-   process_runtime_jvm_buffer_usage
-   process_runtime_jvm_classes_current_loaded
-   process_runtime_jvm_classes_loaded
-   process_runtime_jvm_classes_unloaded
-   process_runtime_jvm_cpu_utilization
-   process_runtime_jvm_memory_committed
-   process_runtime_jvm_memory_init
-   process_runtime_jvm_memory_limit
-   process_runtime_jvm_memory_usage
-   process_runtime_jvm_system_cpu_load_1m
-   process_runtime_jvm_system_cpu_utilization
-   process_runtime_jvm_threads_count

### Mill Metrics

-   active_non_daemon_thread_count
-   jni_wall_clock_duration_millis
-   stage_wall_clock_duration_millis
-   stage_cpu_time_duration_millis
-   initialization_phase_crypto_cpu_time_duration_millis
-   setup_phase_crypto_cpu_time_duration_millis
-   execution_phase_one_crypto_cpu_time_duration_millis
-   execution_phase_two_crypto_cpu_time_duration_millis
-   execution_phase_three_crypto_cpu_time_duration_millis

### Additional Metrics Created using Existing Metrics

-   rpc_client_request_rate_per_second
-   rpc_client_request_error_rate_per_second
