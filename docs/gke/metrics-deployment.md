# Halo Metrics Deployment on GKE

## Background

The configuration for the [`dev` environment](../../src/main/k8s/dev) can be
used as the basis for deploying CMMS components using Google Kubernetes Engine
(GKE) on another Google Cloud project.

Many operations can be done either via the gcloud CLI or the Google Cloud web
console. This guide picks whichever is most convenient for that operation. Feel
free to use whichever you prefer.

### What are we creating/deploying?
- 1 GKE cluster
    - 1 Managed Service for Prometheus
    - 1 cert-manager
    - 1 OpenTelemetry Operator
    - 2 GMP ClusterPodMonitoring
        - prometheus-pod-monitor
        - opentelemetry-collector-pod-monitor
    - 1 GMP PodMonitoring
      - collector-pod-monitor
    - 1 GMP Rules
      - recording-rules
    - 2 OpenTelemetry Operator OpenTelemetryCollector
      - default-sidecar
      - deployment
    - 1 OpenTelemetry Operator Instrumentation
      - open-telemetry-java-agent
    - 5 Kubernetes ConfigMaps
      - default-side-collector
      - deployment-collector
      - grafana-config
      - grafana-datasource-and-dashboard-provider
      - grafana-provisioning
    - 1 Kubernetes Secret
      - grafana-auth
    - 3 Kubernetes Deployments
      - deployment-collector
      - grafana-deployment
      - prometheus-frontend-deployment
    - 3 Kubernetes Services
      - deployment-collector-monitoring 
      - grafana
      - prometheus-frontend
    - 3 Kubernetes NetworkPolicies
      - opentelemetry-collector-network-policy
      - grafana-network-policy
      - prometheus-frontend-network-policy

## Before you start

Create a cluster for a Halo component. Complete a guide up until the "create
the cluster" step. See 
[Create Kingdom Cluster](kingdom-deployment.md#step-4-create-the-cluster),
[Create Duchy Cluster](duchy-deployment.md#step-5-create-the-cluster), or
[Create Reporting Cluster](reporting-server-deployment.md#create-the-cluster).

### Quick start

Adjust the number of nodes and machine type according to your expected usage.

## Deploy Managed Service for Prometheus

Enable Managed Service for Prometheus on the cluster. It is a quick toggle on
Google Cloud web console and everything will be handled automatically.

## Install cert-manager

The cert-manager version corresponds with the OpenTelemetry Operator version.
Only the versions below have been tested.

```shell
kubectl apply -f https://github.com/cert-manager/cert-manager/releases/download/v1.9.1/cert-manager.yaml
```

## Install OpenTelemetry Operator

```shell
kubectl apply -f https://github.com/open-telemetry/opentelemetry-operator/releases/download/v0.60.0/opentelemetry-operator.yaml
```

## IAM Service Accounts

We'll want to
[create a least privilege service account](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa)
that our cluster will run under. Follow the steps in the linked guide to do
this.

We'll additionally want to create a service account that we'll use to allow the
OpenTelemetry Collector in 
[`open_telemetry_gke.cue`](../../src/main/k8s/dev/open_telemetry_gke.cue) to 
have read access to the Spanner database. See
[Granting Cloud Spanner database access](cluster-config.md#granting-cloud-spanner-database-access)
for how to make sure this service account has the appropriate role.

[`prometheus_gke.cue`](../../src/main/k8s/dev/prometheus_gke.cue) also requires 
a service account, but with the role `roles/monitoring.viewer` instead.

## Create the K8s manifest

Deploying to the cluster is generally done by applying a K8s manifest. You can
use the `dev` configuration as a base to get started. The `dev` manifest is a
YAML file that is generated from files written in [CUE](https://cuelang.org/)
using Bazel rules.

The main files for the `dev` Metrics are
[`prometheus_gke.cue`](../../src/main/k8s/dev/prometheus_gke.cue), 
[`open_telemetry_gke.cue`](../../src/main/k8s/dev/open_telemetry_gke.cue), and
[`grafana_gke.cue`](../../src/main/k8s/dev/grafana_gke.cue).

[`config.cue`](../../src/main/k8s/dev/config.cue) needs to be modified to 
work with the right Spanner database. For example, if this is for a kingdom 
cluster, the files need to be modified to work with the Spanner instance and 
database the kingdom cluster is using. A reporting server cluster doesn't use 
Spanner, so nothing here would be used. The contents of
[`open_telemetry_gke.cue`](../../src/main/k8s/dev/open_telemetry_gke.cue) should 
be swapped with the local version:
[`open_telemetry.cue`](../../src/main/k8s/local/open_telemetry.cue).

If desired, you can modify the filtering of the OpenTelemetry metrics. The 
config is found in the base version:
[`open_telemetry.cue`](../../src/main/k8s/open_telemetry.cue).

For Grafana config, see [grafana config](metrics-deployment.md#Grafana)

To generate the YAML manifests from the CUE files:

You can use `kubectl` to create the `db-auth` secret. To reduce the likelihood
of leaking your password, we read it in from STDIN.

Tip: Ctrl+D is the usual key combination for closing the input stream.

Assuming the database username is `user`, run:

```shell
kubectl create secret generic grafana-auth --type='kubernetes.io/basic/auth' \
  --append-hash --from-file=password=/dev/stdin --from-literal=user=user
```

Then use the secret name here:

```shell
bazel build //src/main/k8s/dev:prometheus_gke
bazel build //src/main/k8s/dev:open_telemetry_gke
bazel build //src/main/k8s/dev:grafana_gke \
  --define=grafana_secret_name=grafana-auth-dmg429kb29
```

You can also do your customization to the generated YAML file rather than to the
CUE file.

## Apply the K8s manifest

Run the following first for the Grafana examples, or replace the contents first
for customization.

```shell
kubectl create configmap grafana-provisioning \
  --from-file=src/main/k8s/testing/grafana/alerting.json \
  --from-file=src/main/k8s/testing/grafana/dashboard.json \
  --from-file=src/main/k8s/testing/grafana/contact_points.yaml \
  --from-file=src/main/k8s/testing/grafana/notification_policies.yaml

kubectl create configmap grafana-config \
  --from-file=src/main/k8s/testing/grafana/grafana.ini
```

If you're using manifests generated by the above Bazel targets, the commands to
apply the manifests is

```shell
kubectl apply -f bazel-bin/src/main/k8s/dev/prometheus_gke.yaml
kubectl apply -f bazel-bin/src/main/k8s/dev/open_telemetry_gke.yaml
kubectl apply -f bazel-bin/src/main/k8s/dev/grafana_gke.yaml
```

Substitute the paths if you're using different K8s manifests.

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
NAME              MODE         VERSION   AGE
default-sidecar   sidecar      0.60.0    4h7m
deployment        deployment   0.60.0    131m
```

```
NAME                        AGE   ENDPOINT   SAMPLER   SAMPLER ARG
open-telemetry-java-agent   68s  
```

```
NAME                                         DATA   AGE
default-sidecar-collector                    1      60s
deployment-collector                         1      60s
grafana-config                               1      43s
grafana-datasource-and-dashboard-provider    1      43s
grafana-provisioning                         1      43s
```

```
NAME                           TYPE                       DATA   AGE
grafana-auth-dmg429kb29        kubernetes.io/basic/auth   2      42s
```

```
NAME                                   READY   UP-TO-DATE   AVAILABLE   AGE
deployment-collector                   1/1     1            1           7m4s
grafana-deployment                     1/1     1            1           6m46s
prometheus-frontend-deployment         1/1     1            1           7m21s
```

```
NAME                              TYPE           CLUSTER-IP     EXTERNAL-IP   PORT(S)          AGE
deployment-collector-monitoring   ClusterIP      10.108.6.18    <none>        8888/TCP         3m18s
grafana                           ClusterIP      10.108.8.178   <none>        3000/TCP         3m
prometheus-frontend               ClusterIP      10.108.3.88    <none>        9090/TCP         3m35s
```

```
NAME                                     POD-SELECTOR                                  AGE
grafana-network-policy                   app=grafana-app                               5m56s
opentelemetry-collector-network-policy   app.kubernetes.io/name=deployment-collector   51m
prometheus-frontend-network-policy       app=prometheus-frontend-app                   6m29s
```

## Restart Deployments to Start Collecting Metrics

If deployments were started before these steps, they have to be restarted.
Verify the pods have the label `scrape=true` with

```shell
kubectl describe pod <NAME-OF-POD>
```

If the label is missing, recreate the k8s manifest from the latest cue files
that add the label then apply the updated manifests before restarting.

## Verify Managed Prometheus can Scrape Metrics

Visit the [Managed Prometheus](https://console.cloud.google.com/monitoring/prometheus) 
page in Cloud Console. Query `up` and `scrape_samples_scraped`. 

The first one tells you which targets it can find and whether they are up, and 
the latter is a good way to check that scraping is occurring. If it 
hasn't been long enough, the latter might show all 0's, but after a couple of
minutes you should be seeing results for every target that is up.

## Grafana

The Grafana server deployed using the cue files only has some examples. The
dashboards and alerting rules can be configured using the browser, which can be
accessed through port-forwarding or exposed externally. It is recommended to 
export the dashboards and the alerting rules. The exported configuration files 
can be used to recreate the dashboards and alerting rules on startup as well as
be version controlled for future changes.

### Exporting Dashboards

The dashboard can be exported in the top left corner. There are two buttons to
the right of the dashboard name. One of them is a share button, which can be
used to export. Note: if exporting a new dashboard for the first time, the
datasource variable inside {} needs to be replaced with `prometheus` in the
JSON file.

### Exporting Alerting Rules

Exporting alerting rules requires a few extra steps. In the bottom left corner,
there is a settings button that has an API Keys option. Use that to create an
API key with admin permissions that is short-lived (minutes). There is an
example curl call that is shown:

```shell
curl \
  -H "Authorization: Bearer eyJrIjoiNXJwd3c2T3h4aEtjcUhjRUprM3N2TmFzc0cyRE5nS0QiLCJuIjoiQWRtaW4iLCJpZCI6MX0=" \
  http://localhost:31112/api/dashboards/home
```

When viewing the alerting rule to export, get the ID from the URL in the browser
address bar and replace the curl URL using that ID.

```shell
curl \
  -H "Authorization: Bearer eyJrIjoiNXJwd3c2T3h4aEtjcUhjRUprM3N2TmFzc0cyRE5nS0QiLCJuIjoiQWRtaW4iLCJpZCI6MX0=" \
   http://localhost:31112/api/v1/provisioning/alert-rules/K42WlaD4z
```

This returns a JSON response that contains the alerting rule, but a few things
need to be done to make it work. 
[`alerting.json`](../../src/main/k8s/testing/grafana/alerting.json) has the
format that needs to be used. Each alerting rule is an element in the rules 
array. The data, condition, title, and for fields in the JSON response 
corresponds to the same fields in the example. The ruleGroup field corresponds 
to the name field in the elements of the group array. The folderUID field isn't 
used, instead use the folder name for the folder field. The interval field in
the example is the evaluation interval. It doesn't show up in the JSON response
so the evaluation interval will have to be retrieved from the browser.

### Provisioning Notification Policies and Contact Points

Notification policies and contact points for managing alerts can be found in
[`notification_policies.yaml`](../../src/main/k8s/testing/grafana/notification_policies.yaml) 
and 
[`contact_points.yaml`](../../src/main/k8s/testing/grafana/contact_points.yaml).
Documentation can be found 
[here](https://grafana.com/docs/grafana/latest/administration/provisioning/#contact-points).

### Grafana Configuration

Grafana also has some core configuration settings. The example can be found in
[`grafana.ini`](../../src/main/k8s/testing/grafana/grafana.ini). This needs
to be configured as well. Documentation can be found
[here](https://grafana.com/docs/grafana/latest/setup-grafana/configure-grafana).

## Adding Additional Metrics

The above adds OpenTelemetry JVM and RPC metrics, and Cloud Spanner metrics, as 
well as self-monitoring of the Managed Prometheus collectors. With the above as 
a base, it is possible to add other metrics that can be scraped.

### kubelet and cAdvisor

See [kubelet](https://cloud.google.com/stackdriver/docs/managed-prometheus/setup-managed#kubelet-metrics)

## List of OpenTelemetry Metrics on Prometheus Dashboard

### OpenTelemetry Auto Instrumented RPC and JVM Metrics

- rpc_client_duration_bucket
- rpc_client_duration_count
- rpc_client_duration_sum
- rpc_server_duration_bucket
- rpc_server_duration_count
- rpc_server_duration_sum
- process_runtime_jvm_buffer_count
- process_runtime_jvm_buffer_limit
- process_runtime_jvm_buffer_usage
- process_runtime_jvm_classes_current_loaded
- process_runtime_jvm_classes_loaded
- process_runtime_jvm_classes_unloaded
- process_runtime_jvm_cpu_utilization
- process_runtime_jvm_memory_committed
- process_runtime_jvm_memory_init
- process_runtime_jvm_memory_limit
- process_runtime_jvm_memory_usage
- process_runtime_jvm_system_cpu_load_1m
- process_runtime_jvm_system_cpu_utilization
- process_runtime_jvm_threads_count

### Mill Metrics
- active_non_daemon_thread_count
- jni_wall_clock_duration_millis
- stage_wall_clock_duration_millis
- stage_cpu_time_duration_millis
- initialization_phase_crypto_cpu_time_duration_millis
- setup_phase_crypto_cpu_time_duration_millis
- execution_phase_one_crypto_cpu_time_duration_millis
- execution_phase_two_crypto_cpu_time_duration_millis
- execution_phase_three_crypto_cpu_time_duration_millis

### Additional Metrics Created using Existing Metrics

- rpc_client_request_rate_per_second
- rpc_client_request_error_rate_per_second

### Cloud Spanner Metrics Exported using OpenTelemetry Receiver

- database_spanner_active_queries_summary_active_count
- database_spanner_active_queries_summary_count_older_than_100s
- database_spanner_active_queries_summary_count_older_than_10s
- database_spanner_active_queries_summary_count_older_than_1s
- database_spanner_lock_stats_total_total_lock_wait_seconds
- database_spanner_query_stats_top_all_failed_avg_latency_seconds
- database_spanner_query_stats_top_all_failed_execution_count
- database_spanner_query_stats_top_avg_bytes
- database_spanner_query_stats_top_avg_cpu_seconds
- database_spanner_query_stats_top_avg_latency_seconds
- database_spanner_query_stats_top_avg_rows
- database_spanner_query_stats_top_avg_rows_scanned
- database_spanner_query_stats_top_cancelled_or_disconnected_execution_count
- database_spanner_query_stats_top_execution_count
- database_spanner_query_stats_top_timed_out_execution_count
- database_spanner_query_stats_total_all_failed_avg_latency_seconds
- database_spanner_query_stats_total_all_failed_execution_count
- database_spanner_query_stats_total_avg_bytes
- database_spanner_query_stats_total_avg_cpu_seconds
- database_spanner_query_stats_total_avg_latency_seconds
- database_spanner_query_stats_total_avg_rows
- database_spanner_query_stats_total_avg_rows_scanned
- database_spanner_query_stats_total_cancelled_or_disconnected_execution_count
- database_spanner_query_stats_total_execution_count
- database_spanner_query_stats_total_timed_out_execution_count
- database_spanner_txn_stats_top_avg_bytes
- database_spanner_txn_stats_top_avg_commit_latency_seconds
- database_spanner_txn_stats_top_avg_participants
- database_spanner_txn_stats_top_avg_total_latency_seconds
- database_spanner_txn_stats_total_commit_abort_count
- database_spanner_txn_stats_top_commit_attempt_count
- database_spanner_txn_stats_top_commit_failed_precondition_count
- database_spanner_txn_stats_top_commit_retry_count
- database_spanner_txn_stats_total_avg_bytes
- database_spanner_txn_stats_total_avg_commit_latency_seconds
- database_spanner_txn_stats_total_avg_participants
- database_spanner_txn_stats_total_avg_total_latency_seconds
- database_spanner_txn_stats_total_commit_abort_count
- database_spanner_txn_stats_total_commit_attempt_count
- database_spanner_txn_stats_total_commit_failed_precondition_count
- database_spanner_txn_stats_total_commit_retry_count
