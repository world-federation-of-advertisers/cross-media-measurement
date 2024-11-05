# Halo Metrics Deployment on GKE

Getting visibility into CMMS metrics using Google Cloud Monitoring.

## Background

We can use the OpenTelemetry Collector export OpenTelemetry metrics into Google
Cloud Monitoring and OpenTelemetry traces into Google Cloud Trace.

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
*   NetworkPolicy
    *   `opentelemetry-collector-network-policy`
    *   `to-opentelemetry-collector-network-policy`

## Before you start

Deploy a Halo component. See the related guides:
[Create Kingdom Cluster](kingdom-deployment.md),
[Create Duchy Cluster](duchy-deployment.md), or
[Create Reporting Cluster](reporting-server-deployment.md).

## Google Cloud APIs

Ensure that the Cloud Monitoring and Cloud Trace APIs are enabled. You can do
this from the [APIs and Services](https://console.cloud.google.com/apis) page in
the Google Cloud Console.

## Service Accounts

The `dev` configuration expects an IAM service account named `open-telemetry`
that is enabled for Workload Identity.

It must have at least the following roles:

*   `roles/monitoring.metricWriter`
*   `roles/cloudtrace.agent`

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

## Apply the K8s Object Configurations

### Install cert-manager

You must use a [cert-manager](https://github.com/cert-manager/cert-manager/),
[OpenTelemetry Operator](https://github.com/open-telemetry/opentelemetry-operator/),
and collector image that are compatible with each other. See the
[Compatibility matrix](https://github.com/open-telemetry/opentelemetry-operator#compatibility-matrix)
and the collector image specified in
[`open_telemetry.cue`](../../src/main/k8s/open_telemetry.cue).

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.14.5/cert-manager.yaml
```

### Install OpenTelemetry Operator

```shell
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.99.0/opentelemetry-operator.yaml
```

### Apply OpenTelemetry Configurations

You can just use `kubectl apply`, specifying the configuration file you created
in the previous step.

## Restart Deployments to Start Collecting Metrics

You will need to restart all the Deployments to pick up the Java agent
instrumentation.

```shell
for deployment in $(kubectl get deployments -o name); do kubectl rollout restart $deployment; done
```

## Exported Metrics

These will be visible in the "Workload" domain.

### Automatic Java instrumentation

*   jvm.class.count
*   jvm.class.loaded
*   jvm.class.unloaded
*   jvm.cpu.count
*   jvm.cpu.recent_utilization
*   jvm.cpu.time
*   jvm.gc.duration
*   jvm.memory.committed
*   jvm.memory.limit
*   jvm.memory.used
*   jvm.memory.used_after_last_gc
*   jvm.thread.count
*   rpc.client.duration
*   rpc.server.duration

### Halo instrumentation

*   halo_cmm.thread_pool.size
*   halo_cmm.thread_pool.active_count
*   halo_cmm.computation.stage.crypto.cpu.time
*   halo_cmm.computation.stage.crypto.time
*   halo_cmm.computation.stage.time
*   halo_cmm.retention.deleted_measurements
*   halo_cmm.retention.deleted_exchanges
*   halo_cmm.retention.cancelled_measurements
