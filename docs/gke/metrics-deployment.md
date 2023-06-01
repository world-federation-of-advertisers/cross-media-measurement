# Halo Metrics Deployment on GKE

Getting visibility into CMMS metrics using Google Cloud Monitoring.

## Background

We can use Google Managed Prometheus (GMP) on GKE clusters to get metrics into
Google Cloud Monitoring. Using OpenTelemetry we can also collect more detailed
metrics from CMMS component pods.

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

Many operations can be done either via the gcloud CLI or the Google Cloud web
console. This guide picks whichever is most convenient for that operation. Feel
free to use whichever you prefer.

### What are we creating/deploying?

*   OpenTelemetryCollector
    *   `default`
*   OpenTelemetry Instrumentation
    *   `open-telemetry-java-agent`
*   GMP ClusterPodMonitoring
    *   `prometheus-pod-monitor`
    *   `opentelemetry-collector-pod-monitor`
*   GMP PodMonitoring
    *   `collector-pod-monitor`
*   NetworkPolicy
    *   `opentelemetry-collector-network-policy`

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

You can customize the generated object configuration as-needed.

### OpenTelemetry Collectors and Instrumentation

The default `dev` configuration for OpenTelemetry collection is in
[`open_telemetry_gke.cue`](../../src/main/k8s/dev/open_telemetry_gke.cue), which
depends on [`open_telemetry.cue`](../../src/main/k8s/open_telemetry.cue).

The default build target is `//src/main/k8s/dev:open_telemetry_gke`.

### Prometheus Monitoring

The `dev` configuration is in
[`prometheus_gke.cue`](../../src/main/k8s/dev/prometheus_gke.cue). The build
target is `//src/main/k8s/dev:prometheus_gke`.

## Apply the K8s Object Configurations

### Install cert-manager

You must use a [cert-manager](https://github.com/cert-manager/cert-manager/),
[OpenTelemetry Operator](https://github.com/open-telemetry/opentelemetry-operator/),
and collector image that are compatible with each other. See the
[Compatibility matrix](https://github.com/open-telemetry/opentelemetry-operator#compatibility-matrix)
and the collector image specified in
[`open_telemetry.cue`](../../src/main/k8s/open_telemetry.cue).

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.11.2/cert-manager.yaml
```

### Install OpenTelemetry Operator

```shell
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.77.0/opentelemetry-operator.yaml
```

### Apply OpenTelemetry and Prometheus Configurations

You can just use `kubectl apply`, specifying the configuration files you created
in the previous step.

## Restart Deployments to Start Collecting Metrics

You will need to restart all of the Deployments to pick up the Java agent
instrumentation.

```shell
for deployment in $(kubectl get deployments -o name); do kubectl rollout restart $deployment; done
```

Verify the pods have the label `scrape=true` with

```shell
kubectl describe pod <NAME-OF-POD>
```

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
