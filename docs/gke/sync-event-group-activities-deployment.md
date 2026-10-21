# How to deploy the EventGroupActivity Sync CronJob on GKE

## Background

The EventGroupActivity Sync CronJob (`sync-event-group-activities-<edp>`) runs
[`SyncEventGroupActivities`](../../src/main/kotlin/org/wfanet/measurement/edpaggregator/tools/SyncEventGroupActivities.kt)
on a schedule to reconcile a JSON "spot-data" input file in GCS against the
`EventGroupActivity` resources for a single `DataProvider` in the Kingdom. It
runs in the existing EDP Aggregator GKE cluster.

Per-environment values (DataProvider, spot-data blob URI, Kingdom target)
come from a committed textproto under
`src/main/terraform/gcloud/cmms/envs/<env>/`, copied into the
`edp-aggregator-config` ConfigMap at deploy time and mounted into the CronJob
pod. Per-EDP TLS cert/key (e.g. `edp7_tls.pem`, `edp7_tls.key`)
ship in a dedicated `edp7-tls` K8s Secret mounted at
`/etc/halo-cmms/edp-aggregator/edp7-tls/`. The Kingdom's root CA cert ships
in the same `edp-aggregator-config` ConfigMap as the textproto. The same
Kustomization deploys cleanly across dev, head, and qa.

For background on the underlying sync semantics (idempotency, per-EventGroup
isolation, retries, sync modes), see the KDoc on `EventGroupActivitySync`.

## Before You Start

1.  An EDP Aggregator deployment exists in the target environment per the
    [deployment guide](../edpaggregator/deployment-guide.md).
2.  The EDP has a registered `DataProvider` in the Kingdom and at least one
    `EventGroup`.
3.  A spot-data JSON file exists at a known GCS object. Schema is a JSON
    array of records, each:

    ```json
    {"parent": "dataProviders/<dp>/eventGroups/<eg>", "event_group_activity_date": "2026-06-30T00:00:00Z"}
    ```

    PATCH input additionally requires an explicit `operation` of `append` or
    `delete`. Every record in a PATCH file must include it:

    ```json
    {"parent": "dataProviders/<dp>/eventGroups/<eg>", "event_group_activity_date": "2026-06-30T00:00:00Z", "operation": "append"}
    ```

    In production this file is produced by an upstream pipeline. In
    environments without such a pipeline yet (currently qa/head), seed a
    minimal file by hand — a few dates against one real EventGroup are
    enough to exercise the CronJob wiring end-to-end with `--mode=sync --preview`:

    ```shell
    cat > /tmp/spot-data.json <<'EOF'
    [
      {"parent": "dataProviders/<dp>/eventGroups/<eg>", "event_group_activity_date": "2026-06-25T00:00:00Z"},
      {"parent": "dataProviders/<dp>/eventGroups/<eg>", "event_group_activity_date": "2026-06-26T00:00:00Z"}
    ]
    EOF
    gcloud storage cp /tmp/spot-data.json \
      gs://secure-computation-storage-<env>-bucket/edp/<edp>/spot-data.json
    ```

    List real EventGroups for the DataProvider so the `parent`
    values reference resources that actually exist in the Kingdom:

    ```shell
    grpcurl -cert <client.pem> -key <client.key> \
      v2alpha.kingdom.<env>.halo-cmm.org:8443 \
      wfa.measurement.api.v2alpha.EventGroups/ListEventGroups
    ```

## Provision a Workload Identity binding for the CronJob's SA

The CronJob pods run as the K8s ServiceAccount `sync-event-group-activities`,
which is bound via Workload Identity to a GCP ServiceAccount that has
`roles/storage.objectViewer` (or `objectAdmin`) on the spot-data bucket.

For each environment, bind the k8s SA to a GCP SA *that already has access to
the bucket* (the dev environment reuses `edpa-event-group-sync` since it
already has `storage.objectAdmin` on the EDPA bucket):

```shell
gcloud iam service-accounts add-iam-policy-binding \
  <GCP_SA>@halo-cmm-<env>.iam.gserviceaccount.com \
  --project=halo-cmm-<env> \
  --role=roles/iam.workloadIdentityUser \
  --member="serviceAccount:halo-cmm-<env>.svc.id.goog[default/sync-event-group-activities]"
```

(Currently performed out-of-band; tracked for Terraformization in #4127.)

**Verify the binding is in place in every environment you plan to deploy
to** — dev, qa, and head all currently share the `edpa-event-group-sync`
GCP SA, so the same command must have been run against each project. Deploys
into an environment without the binding will fail at CronJob runtime with a
GCS auth error, not at `kubectl apply`.

If using a different GCP SA per environment, update the
`_iamServiceAccountName` for `#SyncEventGroupActivitiesServiceAccount` in
[`edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue)
or thread it through a per-env tag.

## Configure the per-EDP environment file

For each EDP being synced, commit an `EventGroupActivitySyncConfig` textproto
under `src/main/terraform/gcloud/cmms/envs/<env>/`. For edp7, the filename is
`event-group-activity-sync-edp7-config.textproto`. For dev/edp7:

```textproto
# proto-file: wfa/measurement/config/edpaggregator/event_group_activity_sync_config.proto
# proto-message: wfa.measurement.config.edpaggregator.EventGroupActivitySyncConfig
data_provider: "dataProviders/T5RryPMNong"
spot_data_blob_uri: "gs://secure-computation-storage-dev-bucket/edp/edp7/spot-data.json"
gcs_project: "halo-cmm-dev"
kingdom_public_api_target: "v2alpha.kingdom.dev.halo-cmm.org:8443"
```

The deploy copies the selected environment's committed file into the
kustomization directory. If the file is missing, the workflow fails at the
"Copy sync-event-group-activities config (<edp>)" step before `kubectl apply`.
Commit `src/main/terraform/gcloud/cmms/envs/<env>/event-group-activity-sync-<edp>-config.textproto`
or remove the matching entry from `_syncEventGroupActivitiesArgs` in
[`edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue)
if that EDP should no longer be deployed.

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

The workflow stages three new files into the kustomization dir before
`kubectl apply`:

-   The committed
    `src/main/terraform/gcloud/cmms/envs/<env>/event-group-activity-sync-<edp>-config.textproto`
    into the
    `edp-aggregator-config` ConfigMap.
-   `kingdom_root.pem` (from `src/main/k8s/testing/secretfiles/`) into the
    same ConfigMap, used as the CronJob's `--cert-collection-file` for
    Kingdom mTLS verification.
-   The per-EDP TLS cert + key (from `src/main/k8s/testing/secretfiles/`)
    into the `edp7-tls` Secret.

## Validate a Run

The dev overlay deploys with `--mode=sync --preview` by default; the cron
computes creates and deletes and reports them via metrics without touching the Kingdom.
To validate against a fresh deploy, trigger an immediate run instead of
waiting for the schedule:

```shell
kubectl create job --from=cronjob/sync-event-group-activities-edp7-cronjob \
  sync-event-group-activities-edp7-manual-$(date +%s)
```

Inspect the pod logs:

```shell
kubectl logs -l app=sync-event-group-activities-edp7-app --tail=200
```

Expect a structured `Sync result:` block:

```
Sync result:
  totalInputRecords: <N>
  eventGroupsSucceeded: <N>
  eventGroupsFailed: 0
  activitiesCreated: <N>
  activitiesDeleted: 0
  activitiesUnchanged: 0
  activitiesWouldDelete: <N>
```

To verify against the Kingdom directly:

```shell
grpcurl \
  -cacert src/main/k8s/testing/secretfiles/kingdom_root.pem \
  -cert src/main/k8s/testing/secretfiles/edp7_tls.pem \
  -key src/main/k8s/testing/secretfiles/edp7_tls.key \
  -authority localhost \
  -d '{"parent": "dataProviders/<dp>/eventGroups/<eg>"}' \
  v2alpha.kingdom.<env>.halo-cmm.org:8443 \
  wfa.measurement.api.v2alpha.EventGroupActivities/ListEventGroupActivities
```

## Switch to Mutating Modes

The cron ships in `--mode=sync --preview` (no mutations). Select input semantics
explicitly for each run:

-   `patch` — apply only explicit `append` and `delete` operations. Dates omitted
    from the file remain untouched, so partial files cannot cause mass deletion.
-   `sync` — apply creates and omission-based deletes until the Kingdom exactly
    matches the input. Use only when the input is complete and authoritative.

Add `--preview` to either mode to report its planned changes without applying
mutations.

To change input semantics or enable writes, edit the `--mode=...` and
`--preview` entries in
[`_syncEventGroupActivitiesArgs.edp7`](../../src/main/k8s/dev/edp_aggregator_gke.cue)
and redeploy via `configure-edp-aggregator.yml`.

## Monitoring

OpenTelemetry metrics published by `EventGroupActivitySyncMetrics`:

-   `edpa.event_group_activity.activities_created`
-   `edpa.event_group_activity.activities_deleted`
-   `edpa.event_group_activity.activities_unchanged`
-   `edpa.event_group_activity.event_groups_processed{outcome=success|failed}`
-   `edpa.event_group_activity.sync_errors{error_type=...}`
-   `edpa.event_group_activity.would_delete_count{mode=sync|patch}`
-   `edpa.event_group_activity.sync_latency`

All metrics carry a `data_provider_name` attribute, so dashboards/alerts
split cleanly per EDP across multiple CronJob entries.

A run exits non-zero only when `eventGroupsFailed > 0` (an RPC failure
during list/batch). `--preview` runs that suppress planned mutations exit zero —
alert on the `would_delete_count` counter, not the K8s Job failure count.

## Adding More EDPs

The wiring is currently hardcoded for `edp7`. Adding a second EDP means
duplicating the `edp7` cluster across the files listed below — there's no
loop or matrix to extend. This is intentional until a second EDP is
actually needed; a real parameterization (CUE loop over a list of EDP
names driving secret mounts, textproto filenames, and workflow steps) is
its own follow-up refactor.

To add `edp8` today, copy each occurrence of `edp7` / `EDP7` in the
following files:

1.  [`src/main/k8s/dev/edp_aggregator_gke.cue`](../../src/main/k8s/dev/edp_aggregator_gke.cue)
    — add a sibling entry to `_syncEventGroupActivitiesArgs` keyed by the
    new EDP name, with `tlsSecret: "<edp>-tls"` and an `args` list
    referencing `event-group-activity-sync-config-<edp>.textproto` and the
    new EDP's TLS cert/key paths under `/etc/halo-cmms/edp-aggregator/<edp>-tls/`.
    The shared `_mounts` block in
    [`edp_aggregator.cue`](../../src/main/k8s/edp_aggregator.cue) reads
    `tlsSecret` from each entry and mounts the right Secret automatically;
    no change needed there.
2.  [`src/main/k8s/testing/secretfiles/BUILD.bazel`](../../src/main/k8s/testing/secretfiles/BUILD.bazel)
    — add a new `<edp>_tls_files` `pkg_files` + `pkg_tar` +
    `kustomization_dir` chain mirroring `edp7_tls`.
3.  Add `<edp>_tls_kustomization.yaml` (secretGenerator) in both
    `src/main/k8s/testing/secretfiles/` and `src/main/k8s/dev/`.
4.  [`src/main/k8s/dev/BUILD.bazel`](../../src/main/k8s/dev/BUILD.bazel)
    — add a `kustomization_dir` entry for `<edp>_tls` and add it to the
    `edp_aggregator` kustomization's deps.
5.  [`src/main/k8s/dev/edp_aggregator_config_kustomization.yaml`](../../src/main/k8s/dev/edp_aggregator_config_kustomization.yaml)
    — add the new EDP's textproto filename to the `configMapGenerator.files`
    list.
6.  [`configure-edp-aggregator.yml`](../../.github/workflows/configure-edp-aggregator.yml)
    — duplicate the "Extract edp7 tls files archive" step for the new
    EDP, and duplicate the "Copy sync-event-group-activities config
    (edp7)" step so it copies the new EDP's committed per-environment file.
7.  Commit
    `src/main/terraform/gcloud/cmms/envs/<env>/event-group-activity-sync-<edp>-config.textproto`
    for dev, head, and qa.
8.  If the new EDP uses a different GCP SA for GCS access, either add a
    second SA to the dev overlay or change `_iamServiceAccountName` on
    `#SyncEventGroupActivitiesServiceAccount` (currently shared across
    all per-EDP CronJobs).

## Removing an EDP

Reverse of adding: delete the entry from `_syncEventGroupActivitiesArgs`
in the dev overlay, remove the matching workflow steps and Bazel/CUE
kustomization entries, and delete the committed
`event-group-activity-sync-<edp>-config.textproto` file from each environment.
The workflow's `kubectl apply`
uses `--prune --applyset=configmaps/kubectl-edpa`, so the CronJob,
its NetworkPolicy, and its Secret / ConfigMap references will be
garbage-collected on next deploy. Verify with
`kubectl get cronjob sync-event-group-activities-<edp>-cronjob`
(expect `NotFound`).

## Troubleshooting

-   **Pod CrashLoopBackOff with TLS errors:** the per-EDP `tls-cert-file` /
    `tls-key-file` paths must reference files that exist in the EDP's TLS
    Secret. List with
    `kubectl get secret edp7-tls -o jsonpath='{.data}' | jq 'keys'`.
-   **`Trust anchor for certification path not found`** during Kingdom mTLS:
    `--cert-collection-file` must point at the Kingdom's root cert, which
    ships in the `edp-aggregator-config` ConfigMap as `kingdom_root.pem`.
-   **OOMKilled (exit 137):** the dev cluster's LimitRange defaults
    containers to 192Mi. The CronJob requests/limits 512Mi explicitly; if
    you see OOM on a different deploy, verify the rendered YAML actually
    carries the `resources` block.
-   **`Connect timed out`** on GCS reads: the pod's ServiceAccount is not
    bound via Workload Identity, or the bound GCP SA lacks
    `storage.objectViewer` on the bucket. See "Provision a Workload Identity
    binding" above.
-   **`Blob not found for URI:`** the spot-data file does not exist at the
    URI configured in the textproto. Check the upstream pipeline. The
    CronJob does not create a placeholder.
-   **`Record parent ... is not an EventGroup under DataProvider`:** the
    input file contains records for a different DataProvider, or has a
    malformed parent. This aborts the entire sync; fix the input upstream.
-   **`activitiesWouldDelete > 0` on a `--preview` run:** PATCH mode counts only
    explicit deletes. SYNC mode counts omission-based deletes; an unexpectedly
    high count can indicate a truncated input file.
-   **Workflow fails at "Copy sync-event-group-activities config (<edp>)" with
    `cannot stat .../envs/<env>/event-group-activity-sync-<edp>-config.textproto`:**
    the committed config is missing for the selected environment. Add the file
    under `src/main/terraform/gcloud/cmms/envs/<env>/` and re-trigger the
    workflow, or remove that EDP's workflow and kustomization wiring.
