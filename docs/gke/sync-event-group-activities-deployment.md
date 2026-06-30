# How to deploy the EventGroupActivity Sync CronJob on GKE

## Background

The EventGroupActivity Sync CronJob (`sync-event-group-activities-<edp>`) runs
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
on a schedule to reconcile a JSON "spot-data" input file in GCS against the
`EventGroupActivity` resources for a single `DataProvider` in the Kingdom. It
runs in the existing EDP Aggregator GKE cluster.

Per-environment values (DataProvider, spot-data blob URI, Kingdom target)
come from a textproto blob stored in a GitHub environment variable, written
into the `edp-aggregator-config` ConfigMap at deploy time and mounted into
the CronJob pod. The same Kustomization deploys cleanly across dev, head,
and qa.

For background on the underlying sync semantics (idempotency, per-EventGroup
isolation, retries, max-delete safety guard, dry-run mode), see the KDoc on
`EventGroupActivitySync`.

## Before You Start

1.  An EDP Aggregator deployment exists in the target environment per the
    [deployment guide](../edpaggregator/deployment-guide.md). The
    `edp-aggregator` K8s Secret containing the EDP's TLS cert/key
    (`edp7_tls.pem`, `edp7_tls.key`) and the Kingdom's root cert
    (`kingdom_root.pem`) must already be present.
2.  The EDP has a registered `DataProvider` in the Kingdom and at least one
    `EventGroup`.
3.  A spot-data JSON file is produced by an upstream pipeline and written to a
    known GCS object. Schema is a JSON array of records, each:

    ```json
    {"parent": "dataProviders/<dp>/eventGroups/<eg>", "event_group_activity_date": "2026-06-30T00:00:00Z"}
    ```
4.  The cluster's
    [Workload Identity service account](../edpaggregator/deployment-guide.md)
    used by the EDP Aggregator pods has `roles/storage.objectViewer` on the
    spot-data bucket.

## Configure the per-EDP GitHub environment variable

For each EDP being synced, set a textproto-valued variable in
**GitHub → Settings → Environments → `<env>` → Variables**. Variable name:

```
EVENT_GROUP_ACTIVITY_SYNC_<EDP>_CONFIG_CONTENT
```

(e.g. `EVENT_GROUP_ACTIVITY_SYNC_EDP7_CONFIG_CONTENT`).

Value is an `EventGroupActivitySyncConfig` textproto. For dev/edp7:

```textproto
# proto-file: wfa/measurement/config/edpaggregator/event_group_activity_sync_config.proto
# proto-message: wfa.measurement.config.edpaggregator.EventGroupActivitySyncConfig
data_provider: "dataProviders/T5RryPMNong"
spot_data_blob_uri: "gs://secure-computation-storage-dev-bucket/edp/edp7/spot-data.json"
gcs_project: "halo-cmm-dev"
kingdom_public_api_target: "v2alpha.kingdom.dev.halo-cmm.org:8443"
kingdom_public_api_cert_host: "localhost"
```

If the variable is unset on an environment, the CronJob will fail on deploy
(the ConfigMap generator can't find the file). Either set it before deploying
or remove the corresponding entry from `_syncEventGroupActivitiesArgs` in
[`edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue) for
that environment.

## Deploy

The CronJob ships through the standard EDP Aggregator deploy workflows:

```shell
gh workflow run configure-edp-aggregator.yml \
  -f environment=<env> \
  -f image-tag=<tag> \
  -f apply=true
```

Or via the full pipeline:

```shell
gh workflow run update-cmms.yml \
  -f environment=<env> \
  -f apply=true
```

## Validate the First Deploy with Dry-Run

The initial CUE has `--dry-run` baked into the args. The first deployed
CronJob lists the existing activities, computes the diff against the input
file, and logs what it *would* do without issuing any batch create/delete
RPCs. This validates the wiring (image, secrets, ConfigMap, GCS read,
Kingdom mTLS, throttling) without any risk of mutating activity state.

To trigger a run immediately instead of waiting for the schedule:

```shell
kubectl create job --from=cronjob/sync-event-group-activities-edp7-cronjob \
  sync-event-group-activities-edp7-manual-$(date +%s)
```

Inspect the pod logs:

```shell
kubectl logs -l app=sync-event-group-activities-edp7-app --tail=200
```

You should see a structured `Sync result:` block with non-zero
`totalInputRecords`, the would-be `activitiesCreated` / `activitiesDeleted`
counts, and zero `eventGroupsFailed`. If `eventGroupsGuardSkipped > 0`, the
max-delete-fraction safety guard tripped — confirm the input file is complete
before removing `--dry-run`.

## Switch to Non-Dry-Run

Once the dry-run results look right, remove `--dry-run` from
[`edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue) and
re-deploy. The next scheduled run will create / delete activities.

## Monitoring

OpenTelemetry metrics published by `EventGroupActivitySyncMetrics`:

-   `edpa.event_group_activity.activities_created`
-   `edpa.event_group_activity.activities_deleted`
-   `edpa.event_group_activity.activities_unchanged`
-   `edpa.event_group_activity.event_groups_processed{outcome=success|guard_skipped|failed}`
-   `edpa.event_group_activity.sync_errors{error_type=...}`
-   `edpa.event_group_activity.deletes_skipped_guard`
-   `edpa.event_group_activity.sync_latency`

All metrics carry a `data_provider_name` attribute, so dashboards/alerts split
cleanly per EDP across multiple CronJob entries.

A run exits non-zero only when `eventGroupsFailed > 0` (an RPC failure during
list/batch). Guard-skipped runs exit zero — alert on the
`deletes_skipped_guard` counter, not the K8s Job failure count.

## Adding More EDPs

1.  Add a new entry to `_syncEventGroupActivitiesArgs` in the dev overlay,
    keyed by a short identifier and referencing
    `event-group-activity-sync-config-<edp>.textproto` as the `--config-file`.
2.  Add that filename to
    [`edp_aggregator_config_kustomization.yaml`](../../src/main/k8s/dev/edp_aggregator_config_kustomization.yaml).
3.  Add a step to
    [`configure-edp-aggregator.yml`](../../.github/workflows/configure-edp-aggregator.yml)
    that writes the new env var into the same config dir.
4.  Define `EVENT_GROUP_ACTIVITY_SYNC_<EDP>_CONFIG_CONTENT` on each env.

## Troubleshooting

-   **Pod CrashLoopBackOff with TLS errors:** the per-EDP `tls-cert-file` /
    `tls-key-file` paths must reference files that exist in the
    `edp-aggregator` Secret. List with
    `kubectl get secret edp-aggregator -o jsonpath='{.data}' | jq 'keys'`.
-   **`Blob not found for URI:`** the spot-data file does not exist at the
    URI configured in the textproto. Check the upstream pipeline. The
    CronJob does not create a placeholder.
-   **`Record parent ... is not an EventGroup under DataProvider`:** the
    input file contains records for a different DataProvider, or has a
    malformed parent. This aborts the entire sync; fix the input upstream.
-   **`eventGroupsGuardSkipped > 0`:** for one or more EventGroups, the
    deletion fraction exceeded `--max-delete-fraction` (default 1.0,
    disabled). Tune the flag if your input is expected to fluctuate, or fix
    the input.
-   **`unable to find file: event-group-activity-sync-config-<edp>.textproto`
    during `kubectl apply`:** the `EVENT_GROUP_ACTIVITY_SYNC_<EDP>_CONFIG_CONTENT`
    GitHub variable is unset on this environment. Set it (Settings →
    Environments → `<env>` → Variables) and re-trigger the workflow.
