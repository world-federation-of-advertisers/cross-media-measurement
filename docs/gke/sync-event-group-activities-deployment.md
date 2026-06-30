# How to deploy the EventGroupActivity Sync CronJob on GKE

## Background

The EventGroupActivity Sync CronJob (`sync-event-group-activities-<edp>`) runs
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
on a schedule to reconcile a JSON "spot-data" input file in GCS against the
`EventGroupActivity` resources for a single `DataProvider` in the Kingdom. It
runs in the existing EDP Aggregator GKE cluster.

The same Kustomization deploys cleanly across dev, head, and qa. Per-EDP
values (DataProvider, spot-data blob URI, GCS project) come from GitHub
environment variables — set them in **Settings → Environments → `<env>` →
Variables**. If the variables are unset on an environment, no CronJob is
generated for that EDP on that environment.

For background on the underlying sync semantics (idempotency, per-EventGroup
isolation, retries, max-delete safety guard, dry-run mode), see the KDoc on
`EventGroupActivitySync`.

This guide assumes familiarity with the existing
[EDP Aggregator deployment](../edpaggregator/deployment-guide.md) on GKE — the
sync CronJob ships inside the same Kustomization and reuses the same
`edp-aggregator` Secret.

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

    Per-record validation rules (concrete EventGroup parent under the configured
    DataProvider, ISO-8601 instant) live in
    [`SpotDataParser`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/eventgroupactivities/SpotDataParser.kt)
    and `EventGroupActivitySync.sync()`.
4.  The cluster's
    [Workload Identity service account](../edpaggregator/deployment-guide.md)
    used by the EDP Aggregator pods has `roles/storage.objectViewer` on the
    spot-data bucket.

## Configure GitHub environment variables

In **GitHub → Settings → Environments → `<env>` → Variables**, set:

| Variable | Purpose | Example |
|---|---|---|
| `KINGDOM_PUBLIC_API_TARGET` | gRPC target of the Kingdom public API | `public.kingdom.dev.halo-cmm.org:8443` |
| `SYNC_EVENT_GROUP_ACTIVITIES_EDP7_DATA_PROVIDER` | DataProvider resource name for edp7 | `dataProviders/T5RryPMNong` |
| `SYNC_EVENT_GROUP_ACTIVITIES_EDP7_BLOB_URI` | GCS URI of the edp7 spot-data JSON | `gs://secure-computation-storage-dev-bucket/edp/edp7/spot-data.json` |
| `SYNC_EVENT_GROUP_ACTIVITIES_GCS_PROJECT` | GCP project to read the blob from | `halo-cmm-dev` |

`KINGDOM_PUBLIC_API_TARGET` is already used by other EDPA components; reuse it.
Leave any of the per-EDP variables unset to skip the CronJob on that
environment.

To add more EDPs, define analogous `SYNC_EVENT_GROUP_ACTIVITIES_<edp>_*`
variables, extend
[`build/variables.bzl`](../../build/variables.bzl)'s
`EDP_AGGREGATOR_K8S_SETTINGS` struct, add the entries to
[`src/main/k8s/dev/BUILD.bazel`](../../src/main/k8s/dev/BUILD.bazel)'s
`cue_dump` call, and add a matching `if` block in
[`src/main/k8s/dev/edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue).

## Deploy

The CronJob is part of the standard EDP Aggregator Kustomization, so it ships
through the normal deploy workflows:

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
RPCs. This validates the wiring (image, secrets, GCS read, Kingdom mTLS,
throttling) without any risk of mutating activity state.

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

## Troubleshooting

-   **Pod CrashLoopBackOff with TLS errors:** the per-EDP `tls-cert-file` /
    `tls-key-file` paths must reference files that exist in the
    `edp-aggregator` Secret. List with
    `kubectl get secret edp-aggregator -o jsonpath='{.data}' | jq 'keys'`.
-   **`Blob not found for URI:`** the spot-data file does not exist at the
    configured `SYNC_EVENT_GROUP_ACTIVITIES_EDP7_BLOB_URI`. Check the upstream
    pipeline. The CronJob does not create a placeholder.
-   **`Record parent ... is not an EventGroup under DataProvider`:** the input
    file contains records for a different DataProvider, or has a malformed
    parent. This aborts the entire sync; fix the input upstream.
-   **`eventGroupsGuardSkipped > 0`:** for one or more EventGroups, the
    deletion fraction exceeded `--max-delete-fraction` (default 1.0, disabled).
    Tune the flag if your input is expected to fluctuate, or fix the input.
-   **No CronJob present after deploy:** the
    `SYNC_EVENT_GROUP_ACTIVITIES_<edp>_DATA_PROVIDER` environment variable is
    unset for this environment. Setting it (and the matching `_BLOB_URI` /
    `_GCS_PROJECT` variables) and re-deploying generates the CronJob.
