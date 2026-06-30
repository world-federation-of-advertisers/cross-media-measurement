# How to deploy the EventGroupActivity Sync CronJob on GKE

## Background

The EventGroupActivity Sync CronJob (`sync-event-group-activities-<edp>`) runs
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
on a schedule to reconcile a JSON "spot-data" input file in GCS against the
`EventGroupActivity` resources for a single `DataProvider` in the Kingdom. It
runs in the existing EDP Aggregator GKE cluster.

One CronJob is generated per EDP entry in
[`_syncEventGroupActivitiesArgs`](../../src/main/k8s/dev/edp_aggregator_gke.cue),
so a DataProvider that does not need this reconciliation just has no entry.

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
    `edp-aggregator` K8s Secret containing the EDP's TLS cert/key and the
    Kingdom's root cert must already be present.
2.  The EDP has a registered `DataProvider` in the Kingdom and at least one
    `EventGroup`.
3.  A spot-data JSON file is produced by an upstream pipeline and written to a
    known GCS object — e.g.
    `gs://secure-computation-storage-<env>-bucket/edp/<edp>/spot-data.json`.
    Schema is a JSON array of records, each:

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

## Configure the per-EDP CronJob

Edit
[`src/main/k8s/dev/edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue)
to add or remove entries in `_syncEventGroupActivitiesArgs`. One entry per EDP,
keyed by a short identifier (the generated CronJob name is
`sync-event-group-activities-<key>`).

```cue
_syncEventGroupActivitiesArgs: {
    "edp7": [
        "--data-provider=dataProviders/T5RryPMNong",
        "--blob-uri=gs://secure-computation-storage-dev-bucket/edp/edp7/spot-data.json",
        "--gcs-project-id=halo-cmm-dev",
        "--kingdom-public-api-target=public.kingdom.dev.halo-cmm.org:8443",
        "--kingdom-public-api-cert-host=localhost",
        "--tls-cert-file=/var/run/secrets/files/edp7_tls.pem",
        "--tls-key-file=/var/run/secrets/files/edp7_tls.key",
        "--cert-collection-file=/var/run/secrets/files/kingdom_root.pem",
        "--list-page-size=1000",
        "--throttler-minimum-interval=100ms",
        "--dry-run",
    ]
}
```

The cert paths must reference files already present in the `edp-aggregator`
Secret. The TLS cert/key are the per-EDP credentials the Kingdom recognizes for
that DataProvider; the cert-collection-file is the Kingdom's root.

The schedule is shared across all per-EDP entries and defaults to daily at
06:00 UTC. Override per-environment with:

```cue
_syncEventGroupActivitiesCronSchedule: "0 6 * * *"
```

## First Deploy: Run in Dry-Run

For the first deploy, leave `--dry-run` in the args. The CronJob will list the
existing activities, compute the diff against the input file, and log what it
*would* do without issuing any batch create/delete RPCs. This validates the
wiring (image, secrets, GCS read, Kingdom mTLS, throttling) without any risk
of mutating activity state.

Build and apply the updated Kustomization following the existing
[EDP Aggregator deployment](../edpaggregator/deployment-guide.md) flow:

```shell
bazel build //src/main/k8s/dev:edp_aggregator_gke.tar \
  --define container_registry=ghcr.io \
  --define image_repo_prefix=world-federation-of-advertisers \
  --define image_tag=<your-tag> \
  --define google_cloud_project=<gcp-project> \
  --define spanner_instance=<spanner-instance>
```

Extract the archive and `kubectl apply -k` it the same way you do for the rest
of the EDP Aggregator.

After the next scheduled run completes, inspect the pod logs:

```shell
kubectl logs -l app=sync-event-group-activities-edp7-app --tail=200
```

You should see a structured `Sync result:` block with non-zero
`totalInputRecords`, the would-be `activitiesCreated` / `activitiesDeleted`
counts, and zero `eventGroupsFailed`. If `eventGroupsGuardSkipped > 0`, the
max-delete-fraction safety guard tripped — confirm the input file is complete
before removing `--dry-run`.

To trigger a run immediately instead of waiting for the schedule:

```shell
kubectl create job --from=cronjob/sync-event-group-activities-edp7-cronjob \
  sync-event-group-activities-edp7-manual-$(date +%s)
```

## Switch to Non-Dry-Run

Once the dry-run results look right, remove `--dry-run` from the args, rebuild
the Kustomization, and re-apply. The next scheduled run will create / delete
activities.

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

Add another entry to `_syncEventGroupActivitiesArgs` with a distinct key and
distinct `--data-provider` / `--tls-cert-file` / `--tls-key-file` / `--blob-uri`
values. Re-apply. K8s `concurrencyPolicy: Forbid` is per-CronJob, so the EDPs
do not interfere with each other.

## Troubleshooting

-   **Pod CrashLoopBackOff with TLS errors:** the per-EDP `tls-cert-file` /
    `tls-key-file` paths must reference files that exist in the
    `edp-aggregator` Secret. List with
    `kubectl get secret edp-aggregator -o jsonpath='{.data}' | jq 'keys'`.
-   **`Blob not found for URI:`** the spot-data file does not exist at the
    configured `--blob-uri`. Check the upstream pipeline. The CronJob does not
    create a placeholder.
-   **`Record parent ... is not an EventGroup under DataProvider`:** the input
    file contains records for a different DataProvider, or has a malformed
    parent. This aborts the entire sync; fix the input upstream.
-   **`eventGroupsGuardSkipped > 0`:** for one or more EventGroups, the
    deletion fraction exceeded `--max-delete-fraction` (default 1.0, disabled).
    Tune the flag if your input is expected to fluctuate, or fix the input.
