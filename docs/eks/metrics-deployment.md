# Halo Metrics Deployment on EKS

Getting visibility into CMMS metrics using Amazon Managed Service for Prometheus
and Grafana.

## Background

We can use [Amazon Managed Prometheus (AMP)](https://aws.amazon.com/prometheus/)
to collect metrics from EKS cluster and then viewing them in
[Amazon Managed Grafana](https://aws.amazon.com/grafana/) dashboards.
Using OpenTelemetry we can also collect more detailed metrics from CMMS component pods.

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Amazon Elastic Kubernetes Service (EKS)
on another AWS Cloud project.

Many operations can be done either via the `aws` CLI or the AWS web console.
This guide picks whichever is most convenient for that operation. Feel
free to use whichever you prefer.

### What are we creating/deploying?

*   OpenTelemetryCollector
    *   `default`
*   OpenTelemetry Instrumentation
    *   `open-telemetry-java-agent`
*   NetworkPolicy
    *   `opentelemetry-collector-network-policy`

## Before you start

Deploy a Halo component. See the related guides (only Duchy supports AWS at the moment):
[Create Duchy Cluster](duchy-deployment.md)

## Create a workspace in Amazon Managed Prometheus

This can be done via the AWS Console under `Amazon Managed Service for Prometheus` portal,
or using the [`aws` CLI](https://docs.aws.amazon.com/prometheus/latest/userguide/AMP-manage-ingest-query.html#AMP-create-workspace).
For example:

```shell
aws amp create-workspace --alias halo-cmm-dev
```

Then retrieve the remote write URL endpoint of this workspace from the AWS web console which will be used in later
steps. It will be a URL like `https://aps-workspaces.<region>.amazonaws.com/workspaces/<id>/api/v1/remote_write`

## Service Accounts

Make sure that the least-privilege service account you created for the cluster
has permissions to access the Cloud Monitoring API. See
[Cluster Configuration](cluster-config.md#cluster-service-account).

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

You will need to restart all the Deployments to pick up the Java agent
instrumentation.

```shell
for deployment in $(kubectl get deployments -o name); do kubectl rollout restart $deployment; done
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

The above adds OpenTelemetry JVM and RPC metrics. With the above as a base, it 
is possible to add other metrics that can be scraped.

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
