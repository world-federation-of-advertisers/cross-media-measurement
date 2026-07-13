# Debugging a Report End-to-End

This guide helps you debug a single report that is **stuck or failed** — one that
never reaches a result — by tracing it through every stage of its lifecycle and
pointing you at the exact service logs to read at each hop.

It is scoped to *did the report complete*, not *are the numbers correct*. Debugging
a report that SUCCEEDED but whose values look wrong (measurement accuracy, noise,
or consistency of the reported figures) is a separate exercise and out of scope
here.

A report is not one thing that succeeds or fails atomically. It fans out into
metrics, then into Kingdom **measurements**, then into **requisitions** (one per
participating Event Data Provider), which are fulfilled, then computed by the
Duchies, whose results flow back into the report and are finally post-processed.
A failure at any of those stages presents as "my report isn't done" — the job is
to find *which* stage.

Two fulfillment paths exist for an EDP, and this guide covers both:

- **EDP Aggregator (EDPA):** the EDP's requisitions are fulfilled by the
  aggregator pipeline (requisition-fetcher → data-watcher → results-fulfiller).
- **Direct EDP:** the EDP runs its own data-provider server, polls the Kingdom,
  and fulfills requisitions itself.

A single measurement can mix both kinds of EDP. The Reporting, Kingdom, and
Duchy stages are common to both; only stage 4 branches.

## Golden rule: trace one report, in order

Pick one report, walk it down the lifecycle, and stop at the first stage that is
broken. Do not start by counting states across all rows — that conflates stale
work from other reports with yours. Carry concrete identifiers down the chain: the
report name, then its metrics, then the Kingdom `ExternalMeasurementId`s, then a
specific requisition. Read the databases and the service logs; do not infer
pipeline state from CI job status.

## Lifecycle overview

```
S1  Report created (Reporting public API)
      Report or BasicReport validated, persisted; metrics enumerated.
        └ container: reporting-v2alpha-public-api-server  (+ internal, access)

S2  Metrics → Kingdom Measurements (Reporting "measurement supplier")
      Each RUNNING metric's measurements are created in the Kingdom.
        └ container: reporting-v2alpha-public-api-server

S3  Measurement → Requisitions (+ params)  [Kingdom]
      Measurement starts PENDING_REQUISITION_PARAMS. For MPC protocols the
      Duchies must set participant requisition params; only then does the
      measurement become PENDING_REQUISITION_FULFILLMENT and requisitions
      become UNFULFILLED (visible to EDPs). Direct-protocol measurements skip
      straight to PENDING_REQUISITION_FULFILLMENT.
        └ containers: v2alpha-public-api-server, system-api-server,
                      gcp-kingdom-data-server

S4  Requisitions fulfilled  (per EDP — two branches)
      A) EDP Aggregator: requisition-fetcher → data-watcher → results-fulfiller
      B) Direct EDP: EDP polls Kingdom, calls FulfillDirectRequisition (direct)
         or streams FulfillRequisition to a Duchy (MPC)

S5  Computation  [Duchies, MPC protocols only]
      Herald picks up the computation; mills run the crypto across worker1,
      worker2, aggregator; the aggregator reports the result to the Kingdom.
      Measurement → SUCCEEDED.
        └ containers: <duchy>-herald-daemon, <duchy>-mill-job-scheduler,
                      <duchy>-llv2-mill / <duchy>-hmss-mill,
                      <duchy>-computation-control-server, <duchy>-internal-api-server

S6  Results sync + post-processing  [Reporting]
      Metric/report results are synced from the Kingdom lazily on read. For a
      BasicReport, once the Report SUCCEEDS the results are post-processed
      (noise correction / consistency) and the BasicReport → SUCCEEDED.
        └ containers: reporting-v2alpha-public-api-server (sync on read),
                      report-result-post-processor (cronjob, incl. init
                      container basic-reports-reports)
```

Direct-protocol measurements skip S5 (the result is available at fulfillment).
Only BasicReports go through the S6 post-processor; a plain Report is "done" when
all its metrics succeed.

## State enums

**Kingdom `Measurement.State`** (`internal/kingdom/measurement.proto`):

| # | Name | Meaning |
|---|------|---------|
| 1 | `PENDING_REQUISITION_PARAMS` | awaiting participant params from Duchies |
| 2 | `PENDING_REQUISITION_FULFILLMENT` | requisitions available, awaiting EDP fulfillment |
| 3 | `PENDING_PARTICIPANT_CONFIRMATION` | all requisitions fulfilled |
| 4 | `PENDING_COMPUTATION` | Duchies computing |
| 5 | `SUCCEEDED` | terminal, has a result |
| 6 | `FAILED` | terminal |
| 7 | `CANCELLED` | terminal |

When reading the raw integer `State` column in Spanner, note that the terminal
success value is `5` (`SUCCEEDED`); `6` and `7` are the failure/cancel states.

**Kingdom `Requisition.State`** (nested in `measurement.proto`):

| # | Name |
|---|------|
| 1 | `PENDING_PARAMS` (not yet visible to EDPs) |
| 2 | `UNFULFILLED` |
| 3 | `FULFILLED` |
| 4 | `REFUSED` |
| 5 | `WITHDRAWN` |

**Kingdom `ComputationParticipant.State`** (`internal/kingdom/computation_participant.proto`):

| # | Name |
|---|------|
| 1 | `CREATED` (Duchy has not set params yet) |
| 2 | `REQUISITION_PARAMS_SET` |
| 3 | `READY` |
| 4 | `FAILED` (parent Measurement → FAILED) |

**EDPA `RequisitionMetadataState`** (`internal/edpaggregator/requisition_metadata_state.proto`):

| # | Name |
|---|------|
| 1 | `STORED` (fetched, not yet fulfilled) |
| 2 | `QUEUED` |
| 3 | `PROCESSING` |
| 4 | `FULFILLED` |
| 5 | `REFUSED` |
| 6 | `WITHDRAWN` |

**Reporting `BasicReport.State`** (`internal/reporting/v2/basic_report.proto`):

| # | Name |
|---|------|
| 1 | `CREATED` |
| 2 | `REPORT_CREATED` |
| 3 | `UNPROCESSED_RESULTS_READY` (waiting for the post-processor) |
| 4 | `SUCCEEDED` |
| 5 | `FAILED` |
| 6 | `INVALID` |

Public-API requisition states (via `grpcurl`) are the human-readable
`UNFULFILLED` / `FULFILLED` / `REFUSED`.

## The two reporting stores (and how to hop between them)

The reporting internal API server is backed by **two databases**, and which one a
row lives in determines how you inspect it. Knowing this is what lets you localize
a break at the reporting layer.

- **Postgres** (Cloud SQL, database `reporting-v2`) holds the classic v2 entities:
  **Reports, Metrics, ReportingSets, MetricCalculationSpecs, ReportSchedules**, and
  the reporting-side **Measurements** bookkeeping table. Query it with a Postgres
  client (`psql` / Cloud SQL), **not** `gcloud spanner`. In practice you usually
  inspect Reports/Metrics via the reporting public API (get/list) rather than the
  DB directly; go to Postgres when you need the raw row (e.g. a Metric stuck
  `RUNNING`, or the `CmmsMeasurementId` link).
- **Spanner** (database `reporting`) holds the BasicReport / result entities:
  **BasicReports, ReportResults, ReportingSetResults, ReportingWindowResults**.
  The `gcloud spanner databases execute-sql reporting ...` pattern used elsewhere
  in this guide **does** apply here.

**The cross-store join key is `CmmsMeasurementId`.** When the reporting service
creates a Kingdom measurement it stores that measurement's resource ID in the
Postgres `Measurements` table (`CmmsMeasurementId`, plus a reporting-side `State`).
That value is the Kingdom measurement's resource name — i.e. it maps to the
Kingdom's `ExternalMeasurementId`. So a full report trace hops stores like this:

```
Report / Metric               → Postgres  (reporting-v2): Reports, Metrics, MetricMeasurements
  └ Metric's Measurements      → Postgres: Measurements.CmmsMeasurementId + State
      └ that CmmsMeasurementId  → Kingdom Spanner (kingdom): Measurements.ExternalMeasurementId, Requisitions, ...
BasicReport / results          → Spanner   (reporting): BasicReports, ReportResults, ...
```

When a report is stuck, this tells you where to look: no `CmmsMeasurementId` set
on a Postgres `Measurements` row → the break is at S2 (measurement never created
in the Kingdom); `CmmsMeasurementId` set but the Kingdom row stuck → S3–S5; Kingdom
`SUCCEEDED` but the reporting `Measurements.State` not updated → the read-time sync
(S2/S6) hasn't run or failed; a BasicReport stuck in Spanner at
`UNPROCESSED_RESULTS_READY` → the post-processor (S6).

## Which log to read (container reference)

GKE services log to `k8s_container`; Cloud Functions to `cloud_run_revision`; the
results-fulfiller (a Confidential-VM MIG) to `gce_instance` / the
`edpa.results_fulfiller` log name. Container name = `<deployment>-container`.
Cluster names are environment-specific except where fixed in code (the reporting
cluster is `reporting-v2`; duchy identities are `worker1` / `worker2` /
`aggregator`). Confirm cluster and namespace names for your environment.

| Subsystem | Component | Container / log target |
|-----------|-----------|------------------------|
| Reporting | public v2alpha API (report/metric/basic-report services, read-time sync) | `reporting-v2alpha-public-api-server-container` (cluster `reporting-v2`) |
| Reporting | internal API (Postgres + reporting Spanner) | `postgres-internal-reporting-server-container` |
| Reporting | authz | `access-public-api-server-container`, `access-internal-api-server-container` |
| Reporting | report scheduling (creates scheduled reports) | `report-scheduling-container` (cronjob) |
| Reporting | **post-processor** (+ init `basic-reports-reports`) | `report-result-post-processor-container` (cronjob) |
| Kingdom | public v2alpha API (create measurement, list/fulfill requisitions) | `v2alpha-public-api-server-container` |
| Kingdom | system API (Duchy-facing: set params, confirm, set result) | `system-api-server-container` |
| Kingdom | internal data server (runs the state-machine writers) | `gcp-kingdom-data-server-container` |
| Duchy | herald (watches Kingdom, creates local computations) | `<duchy>-herald-daemon-container` |
| Duchy | mill job scheduler | `<duchy>-mill-job-scheduler-container` |
| Duchy | LLv2 / HMSS mill (crypto; runs as K8s Jobs) | `<duchy>-llv2-mill-container` / `<duchy>-hmss-mill-container` |
| Duchy | inter-duchy comms | `<duchy>-computation-control-server-container`, `<duchy>-async-computation-control-server-container` |
| Duchy | internal computations API (duchy Spanner) | `<duchy>-internal-api-server-container` |
| Duchy | requisition fulfillment endpoint (EDPs send fulfilled data here for MPC) | `<duchy>-requisition-fulfillment-server-container` |
| EDPA | requisition-fetcher (Kingdom → GCS) | Cloud Function `requisition-fetcher` |
| EDPA | data-availability-sync (registers impression metadata) | Cloud Function `data-availability-sync` |
| EDPA | data-watcher (GCS → work queue) | Cloud Function `data-watcher` |
| EDPA | results-fulfiller (decrypt + fulfill) | `edpa.results_fulfiller` log name (GCE MIG, `gce_instance`) |
| EDPA | requisition-/impression-metadata public API (v1alpha) | `edp-aggregator-system-api-server-container` |
| EDPA | requisition-/impression-metadata internal API (edp-aggregator Spanner) | `edp-aggregator-internal-api-server-container` |

Filter shape:

```bash
gcloud logging read \
  'resource.type="k8s_container"
   AND resource.labels.cluster_name="<CLUSTER>"
   AND resource.labels.container_name="<CONTAINER>"
   AND timestamp>="<START>"' \
  --project=<ENV> --limit=40 --format='value(timestamp,severity,textPayload,jsonPayload.message)'
```

## Telemetry: metrics and traces (check before grepping logs)

Several components are instrumented with OpenTelemetry, and where the OTEL
exporters are configured for an environment the signals go to **Cloud Monitoring**
(metrics) and **Cloud Trace** (traces). The results-fulfiller (TEE app) exports
both unconditionally (`OTEL_METRICS_EXPORTER=google_cloud_monitoring`,
`OTEL_TRACES_EXPORTER=google_cloud_trace` in its terraform); the other EDPA Cloud
Functions and the Kingdom/Duchy/Reporting services export only when their OTEL
exporter env vars are set for the environment, so confirm before relying on them.
A metric or trace often localizes a stall faster than a log grep — check these
first, then drill into logs for the stack trace.

Useful metric families (all under the `edpa.*` namespace unless noted):

| Signal | Metric | Tells you |
|--------|--------|-----------|
| Fetcher throughput/health | `edpa.requisition_fetcher.requisitions_fetched`, `edpa.requisition_fetcher.storage_writes`, `edpa.requisition_fetcher.report_failures`, `edpa.requisition_fetcher.report_refusals`, `edpa.requisition_fetcher.fetch_latency` | whether S4-A stage 2 is fetching, storing, and how many reports it is failing/refusing |
| Impression data availability | `edpa.data_availability.records_synced`, `edpa.data_availability.cmms_rpc_errors`, `edpa.data_availability.sync_duration`, `edpa.data_availability_monitor.date_status` | whether the EDP's impression metadata is being registered (a precondition for S4-A fulfillment), and Kingdom RPC errors during sync |
| Fulfillment | `edpa.results_fulfiller.requisitions_processed`, `edpa.results_fulfiller.fulfillment_latency`, `edpa.results_fulfiller.report_failures`, `edpa.results_fulfiller.status` (dimensioned by `error_type`, `fulfiller_type`, `model_line`) | whether S4-A stage 4 is processing and succeeding, and the dominant error type |
| Work dispatch | `edpa.data_watcher.queue_writes`, `edpa.data_watcher.processing_duration` | whether S4-A stage 3 is dispatching work items to the results-fulfiller queue |
| Event-group sync | `edpa.event_group.sync_success`, `edpa.event_group.sync_failure`, `edpa.event_group.sync_latency` | S1 event-group registration health |
| Duchy computation | Duchy mill emits `stage_wall_clock_duration_ms`, `crypto_wall_clock_duration_ms`, `crypto_cpu_duration_ms` | S5 stage progress / where a computation spends time or stalls |

The Kingdom also runs a synthetic end-to-end health prober
(`MeasurementSystemProber`) that periodically creates and watches a probe
measurement; its metrics are a good first check for "is the whole pipeline
healthy right now, independent of my report?". Reporting emits
`reporting.unreachable_basic_reports` for BasicReports it cannot advance.

Traces (when exported) let you follow a single request across services — e.g. a
fulfillment span from the results-fulfiller through its Kingdom/Duchy RPCs —
without correlating timestamps across log streams by hand.

## The playbook

### S1 — Report creation

The public API server handles both `createReport` (advanced API) and
`createBasicReport` (simplified API). Creation-time failures surface here as gRPC
errors to the caller and as log lines on
`reporting-v2alpha-public-api-server-container`.

Common failures:

- `INVALID_ARGUMENT` — bad input: unset report, invalid report ID, no reporting
  metric entries, invalid `MetricCalculationSpec` name. For a BasicReport, a bad
  CEL expression in an impression-qualification filter is rejected here
  **before** anything is persisted (deliberate — it prevents orphaned rows that
  would later fail EDP fulfillment).
- `FAILED_PRECONDITION` — "measurement consumer not found", or (BasicReport)
  campaign-group invalid.
- `ALREADY_EXISTS` — a report with that ID already exists.
- `NOT_FOUND` — a referenced `ReportingSet`, `MetricCalculationSpec`, or
  `ReportSchedule` does not exist (disambiguated by the message text).
- Permission errors — the `authorization.check` call to the Access service
  denied `reporting.reports.create` (or `CREATE_WITH_DEV_MODEL_LINE` when a
  metric uses a dev model line). Check `access-public-api-server-container`.

If creation returned success, the report row exists; move to S2.

### S2 — Metrics → Kingdom measurements

When a metric enters the `RUNNING` state, the reporting service's measurement
supplier builds signed `CreateMeasurement` requests and calls the Kingdom public
API to create the underlying measurements, then records the returned CMMS
measurement IDs. This runs on the same public API server.

If the report is stuck with no Kingdom measurements:

- Look on `reporting-v2alpha-public-api-server-container` for measurement-creation
  errors. Model-line resolution calls the Kingdom (`getModelLine`); a
  `ModelLineNotFoundException` surfaces as `FAILED_PRECONDITION`, other model-line
  errors as `INTERNAL`. A missing per-MC config surfaces as `INTERNAL`
  ("Config not found for <mc>").
- Confirm whether the Kingdom measurements exist by checking the Postgres
  `Measurements` rows for the metric (`reporting-v2` DB, joined via
  `MetricMeasurements`): a row with a blank `CmmsMeasurementId` means the Kingdom
  measurement was never created (the break is here, S2); a populated
  `CmmsMeasurementId` means it was, so carry that value into the Kingdom trace
  (S3). See [The two reporting stores](#the-two-reporting-stores-and-how-to-hop-between-them).

**Result sync is lazy.** There is no background job polling the Kingdom for
results; the reporting service syncs measurement results **when a client reads**
the metric/report. So a report can be fully computed in the Kingdom yet still
show pending until it is read. If numbers seem stale, read the report again and
watch the public API server logs for the sync path. A metric SUCCEEDS only when
all its measurements are SUCCEEDED; if a metric is FAILED, look for
`buildMetricResult exception` (SEVERE) — a variance/result computation problem
marks the metric FAILED with reason `MEASUREMENT_RESULT_INVALID`.

### S3 — Measurement → requisitions and params (Kingdom)

Trace the **specific** measurement you carried down from S2, not the newest rows.
The `CmmsMeasurementId` stored on the Postgres `Measurements` row is the Kingdom
measurement's resource ID; its measurement segment decodes to the numeric
`ExternalMeasurementId` (URL-safe base64 of the 8-byte big-endian int64 — the same
decode trick used for model-line IDs under
[Model-line mismatch](#model-line-mismatch)). Use that to query the one
measurement:

```bash
gcloud spanner databases execute-sql kingdom \
  --instance=<SPANNER_INSTANCE> --project=<ENV> \
  --sql="SELECT ExternalMeasurementId, State, CreateTime, UpdateTime
         FROM Measurements WHERE ExternalMeasurementId = <EXT_MEASUREMENT_ID>"
```

(If you don't yet have the ID — e.g. you're looking at recent activity rather than
tracing a known report — `ORDER BY CreateTime DESC LIMIT 20` will show recent
measurements, but pin down a single `ExternalMeasurementId` before going further.)

Then its requisitions and computation participants:

```bash
gcloud spanner databases execute-sql kingdom \
  --instance=<SPANNER_INSTANCE> --project=<ENV> \
  --sql="SELECT r.ExternalRequisitionId, r.State, d.ExternalDataProviderId, r.FulfillingDuchyId
         FROM Requisitions r
         JOIN Measurements m USING (MeasurementConsumerId, MeasurementId)
         JOIN DataProviders d USING (DataProviderId)
         WHERE m.ExternalMeasurementId = <EXT_MEASUREMENT_ID>"

gcloud spanner databases execute-sql kingdom \
  --instance=<SPANNER_INSTANCE> --project=<ENV> \
  --sql="SELECT cp.DuchyId, cp.State, cp.UpdateTime
         FROM ComputationParticipants cp
         JOIN Measurements m USING (MeasurementConsumerId, MeasurementId)
         WHERE m.ExternalMeasurementId = <EXT_MEASUREMENT_ID>"
```

Interpret the measurement state:

- **`PENDING_REQUISITION_PARAMS` (1), not advancing** — for an MPC measurement,
  the Duchies have not all set their participant requisition params, so the
  requisitions are still `PENDING_PARAMS` and **not yet visible to any EDP**. This
  is the classic "a Duchy is down" symptom. Find the blocking Duchy: any
  `ComputationParticipants` row in state `CREATED (1)` while its siblings are
  `REQUISITION_PARAMS_SET (2)` is the one that never called
  `setParticipantRequisitionParams`. Confirm on the Kingdom
  `system-api-server-container` that no `setParticipantRequisitionParams` call
  arrived for that computation from that Duchy, and check that Duchy's
  `<duchy>-herald-daemon-container` (the herald is what drives the Duchy's side of
  participant setup). The transition to `PENDING_REQUISITION_FULFILLMENT` (and the
  flip of requisitions to `UNFULFILLED`) only fires when the **last** Duchy sets
  its params.
- **`PENDING_REQUISITION_FULFILLMENT` (2)** — requisitions are `UNFULFILLED` and
  available to EDPs. Go to S4 for each unfulfilled requisition.
- **`PENDING_PARTICIPANT_CONFIRMATION` (3) / `PENDING_COMPUTATION` (4)** — all
  requisitions fulfilled; the Duchies are confirming/computing. Go to S5.
- **`FAILED` (6)** — check `MeasurementDetails` for the failure, and if it's an
  MPC measurement, the Duchy logs (S5). A `ComputationParticipant` in state
  `FAILED (4)` fails the whole measurement.

Per requisition: `UNFULFILLED` → that EDP hasn't fulfilled yet (S4);
`FULFILLED` → done; `REFUSED` → the EDP refused (reason in `RequisitionDetails`).

### S4 — Fulfillment (per EDP)

Determine, from the EDP's onboarding config, whether the unfulfilled
requisition's EDP is on the EDP Aggregator or is a direct EDP.

#### S4-A — EDP Aggregator path

Pipeline: requisition-fetcher → data-watcher → results-fulfiller. Backing metadata
lives in the `edp-aggregator` Spanner database, fronted by two service layers:

- **Internal** (`edp-aggregator-internal-api-server`, Spanner-backed, cluster-only)
  — where requisition-metadata and impression-metadata rows physically live.
- **External v1alpha** (`edp-aggregator-system-api-server`) — the public wrapper
  the fetcher and data-availability-sync call, which delegates to the internal
  service.

Precondition: the EDP's impression data for the dates the requisition spans must
already be registered (the `data-availability-sync` Cloud Function turns `done`
blobs into `ImpressionMetadata` rows). If fulfillment fails with a missing-blob or
no-data error at step 4, that registration — not the fetch — is the real gap (see
[Missing impression blobs](#missing-impression-blobs)).

Trace:

1. **EDPA RequisitionMetadata:**

   ```bash
   gcloud spanner databases execute-sql edp-aggregator \
     --instance=<SPANNER_INSTANCE> --project=<ENV> \
     --sql="SELECT CmmsRequisition, State, GroupId, BlobUri, CreateTime, UpdateTime
            FROM RequisitionMetadata ORDER BY CreateTime DESC LIMIT 10"
   ```

   - Row `FULFILLED (4)` but Kingdom still `UNFULFILLED` → the fulfillment RPC to
     the Kingdom/Duchy failed; check the results-fulfiller logs (step 4).
   - Row `STORED (1)` → stored, not fulfilled; continue.
   - No row → the fetcher never stored it; go to step 2.

2. **requisition-fetcher** (Cloud Function). It polls the Kingdom and is also
   HTTP-triggered, writing a grouped-requisitions blob to
   `<edp>/requisitions/<groupId>` and registering it via the **v1alpha
   RequisitionMetadata service**.

   ```bash
   gcloud logging read \
     'resource.type="cloud_run_revision" AND resource.labels.service_name="requisition-fetcher" AND timestamp>="<START>"' \
     --project=<ENV> --limit=30 --format=json | python3 -c '
   import json, sys
   for l in reversed(json.load(sys.stdin)):
       m = l.get("jsonPayload", {}).get("message", "") or l.get("textPayload", "")
       if m and "otel" not in m.lower():
           print(l["timestamp"][:19], m[:200])'
   ```

   - `Wrote grouped requisitions blob ... groupId=<X>` → stored; continue.
   - `Fetched N requisitions` but no "Wrote grouped" → already-stored/deduped; a
     new measurement should produce a fresh write.
   - `SEVERE: Failed to process report <reportId> for <dp>` → a fetch/registration
     error. If it's a metadata write/validation error, also check
     `edp-aggregator-system-api-server-container` (v1alpha wrapper) and
     `edp-aggregator-internal-api-server-container` (Spanner writes).
   - `httpRequest.status` 504 → fetcher timed out (often too many stale
     requisitions in `<edp>/requisitions/`). 401/403 → stale auth token (see
     [Capacity exhaustion masquerading as auth failures](#capacity-exhaustion-masquerading-as-auth-failures)).

   Metric cross-check (Cloud Monitoring): `edpa.requisition_fetcher.storage_writes`
   should climb when new requisitions are stored; a rising
   `edpa.requisition_fetcher.report_failures` or
   `edpa.requisition_fetcher.storage_fails` means the fetcher is erroring rather
   than idle, and `edpa.requisition_fetcher.report_refusals` means it is refusing
   reports it cannot satisfy.

3. **data-watcher** (Cloud Function). A GCS `object.finalized` Eventarc trigger
   fires it; it submits a work item to the results-fulfiller queue.

   ```bash
   gcloud logging read \
     'resource.type="cloud_run_revision" AND resource.labels.service_name="data-watcher" AND timestamp>="<START>"' \
     --project=<ENV> --limit=30 --format=json | python3 -c '
   import json, sys
   for l in reversed(json.load(sys.stdin)):
       m = l.get("jsonPayload", {}).get("message", "") or l.get("textPayload", "")
       if m and ("Submitted work item" in m or "matched path" in m or "Received data path" in m):
           print(l["timestamp"][:19], m[:200])'
   ```

   - `Submitted work item to control plane queue: ... queue=results-fulfiller-queue`
     → dispatched; continue.
   - Nothing for the blob → the Eventarc trigger did not fire. Verify the blob
     exists and the trigger is healthy. Grep **both** `jsonPayload.message` and
     `textPayload` and widen the window before concluding "not dispatched".

   Metric cross-check (Cloud Monitoring): `edpa.data_watcher.queue_writes` counts
   work items written to the queue; if it is flat while blobs are landing, the
   data-watcher is not dispatching (Eventarc trigger or the data-watcher itself).

4. **results-fulfiller** (GCE MIG). Application logs under `edpa.results_fulfiller`
   (real content in `textPayload`).

   ```bash
   gcloud logging read \
     'logName="projects/<ENV>/logs/edpa.results_fulfiller" AND timestamp>="<START>"' \
     --project=<ENV> --limit=40 --format="value(timestamp,severity,textPayload)" \
     | grep -iE 'fulfill|process|error|SEVERE|reach|model|Waiting for messages'

   gcloud compute instance-groups managed list --project=<ENV> --regions=us-central1 \
     --format="table(name,targetSize,status.isStable)" | grep results-fulfiller

   gcloud pubsub subscriptions pull results-fulfiller-queue-dlq-sub --project=<ENV> --limit=3 \
     --format="value(message.publishTime,message.attributes.CloudPubSubDeadLetterSourceDeliveryCount)"
   ```

   Interpretations map to [common failure modes](#common-edp-aggregator-failure-modes):
   `NoSuchElementException ... is missing in the map` → model-line mismatch;
   `ImpressionReadException ... BLOB_NOT_FOUND` → missing impression blob;
   `Unsupported key URI` → KMS-type mismatch; `Waiting for messages` + empty DLQ +
   MIG 0 → MIG stuck at zero; fulfillment logged but Kingdom still UNFULFILLED →
   the `FulfillRequisition` RPC to the Duchy/Kingdom failed (error follows).

   **On the dead-letter queue:** a message lands in the DLQ only after the
   fulfiller has failed it `max_delivery_attempts` times (5 by default) — so a
   requisition in the DLQ is one that repeatedly failed and **will not be retried**
   again. The `CloudPubSubDeadLetterSourceDeliveryCount` attribute shows the
   attempt count. Match a DLQ message to your requisition by its work-item payload
   (it carries the requisitions blob path / groupId). If your requisition is in the
   DLQ, the fulfiller logs from those attempt windows hold the actual error
   (model-line, blob, or KMS above); the DLQ tells you it is terminally stuck, the
   logs tell you why. A metric cross-check:
   `edpa.results_fulfiller.requisitions_processed` should climb on success and
   `edpa.results_fulfiller.report_failures` on failure — the `error_type` /
   `fulfiller_type` / `model_line` dimensions on
   `edpa.results_fulfiller.status` isolate the dominant failure.

#### S4-B — Direct EDP path

A direct EDP runs its own data-provider server, polls the Kingdom public API for
its requisitions, and fulfills them itself:

- **Direct protocol:** calls `FulfillDirectRequisition` on the Kingdom public
  v2alpha `Requisitions` API. The receiving surface is the Kingdom
  `v2alpha-public-api-server-container`.
- **MPC protocol (LLv2/HMSS):** streams `FulfillRequisition` to a Duchy's
  requisition-fulfillment endpoint — `<duchy>-requisition-fulfillment-server-container`
  (the same endpoint the EDPA results-fulfiller uses).

The EDP's own server logs are the primary source (they are outside the CMMS
deployment). On the CMMS side, look at the Kingdom public API container (direct)
or the duchy requisition-fulfillment-server container (MPC) for the received call
and any rejection.

### S5 — Computation (Duchies, MPC only)

Reached when all requisitions are `FULFILLED` but the measurement is not yet
`SUCCEEDED` (states 3–4). Direct measurements skip this — their result is
available at fulfillment.

Flow: the **herald** on each Duchy watches the Kingdom, creates the local
computation, and confirms participation; the **mill job scheduler** spawns
**mill** Jobs (`llv2-mill` for Liquid Legions v2 and reach-only LLv2; `hmss-mill`
for Honest Majority Share Shuffle) that run the crypto; mills exchange
intermediate data through the **computation-control** servers; the **aggregator**
Duchy performs the final aggregation and reports the result to the Kingdom, which
sets the measurement `SUCCEEDED`.

Detecting a stalled computation:

- **Kingdom-side:** measurement stuck in `PENDING_COMPUTATION` with all
  requisitions `FULFILLED`.
- **Duchy-side:** on the relevant duchy cluster (`worker1` / `worker2` /
  `aggregator`):
  - `<duchy>-herald-daemon-container` — `Non-transient error` (SEVERE),
    `Unexpected global computation state`, retry warnings.
  - `<duchy>-llv2-mill-container` / `<duchy>-hmss-mill-container` — mill work; a
    stage crash logs SEVERE from `failComputation`; after too many attempts,
    `Failing computation due to too many failed ComputationStageAttempts`.
    `Computations server not ready` means the internal API wasn't reachable.
  - `<duchy>-computation-control-server-container` — inter-duchy comms; gRPC
    errors reaching a peer duchy indicate that peer is down or unreachable, and
    the computation stalls with no progress.
  - `<duchy>-internal-api-server-container` — the duchy computations DB; the
    computation stage not advancing (or growing `ComputationStageAttempts`)
    indicates a stuck computation.

A duchy down mid-computation shows up as peer duchies' mills logging transient
gRPC failures to the dead duchy's computation-control target, with the stage not
advancing.

> Note: TrusTEE is referenced in the duchy code but is not part of the standard
> mill-job-scheduler deployment; confirm whether a TrusTEE mill is actually
> deployed in your environment before looking for a `trustee-mill` container.

### S6 — Results sync and post-processing (Reporting)

Once the Kingdom measurements SUCCEED:

- **Metric/report results** are synced lazily when the report is read (S2). If a
  Report is not showing results, read it and watch
  `reporting-v2alpha-public-api-server-container`.
- **BasicReport post-processing.** After a BasicReport's associated Report
  SUCCEEDS, the init container `basic-reports-reports` (of the
  `report-result-post-processor` cronjob) advances the BasicReport
  `REPORT_CREATED (2)` → `UNPROCESSED_RESULTS_READY (3)`. The main
  `report-result-post-processor` container (a Python cronjob, runs every 5 min)
  then reads reports in `UNPROCESSED_RESULTS_READY`, runs the noise-correction /
  consistency solver, writes corrected values, and advances the BasicReport to
  `SUCCEEDED (4)`.

  Check `report-result-post-processor-container` (cluster `reporting-v2`). Failure
  modes:

  - **Solver did not converge** → `QP_SOLUTION_NOT_FOUND`. The solver tries the
    primary method then falls back (log line "Switching to OSQP solver as HIGHS
    solver failed to converge."). A `PARTIAL_SOLUTION_FOUND_*` means a residual
    above tolerance.
  - **Crash / data parse error** → `INTERNAL_ERROR`; the job logs "Failed to
    process BasicReport <name> for MeasurementConsumer <mc>" and marks the
    BasicReport `FAILED`.
  - **State precondition** → a `FAILED_PRECONDITION` writing processed results is
    disambiguated by an `ErrorInfo` reason `BASIC_REPORT_STATE_INVALID`. If the
    report already advanced (`SUCCEEDED`/`FAILED`) it is skipped; if it did not
    advance (a real integrity error, e.g. a missing `ReportingSetResult` or
    `ReportingWindowResult`) it is marked `FAILED`.
  - **Transient** (`UNAVAILABLE`/`DEADLINE_EXCEEDED`) → left in
    `UNPROCESSED_RESULTS_READY` and retried on the next tick.
  - **Non-fatal quality issues** are logged (e.g. large corrections, independence
    or zero-variance consistency checks) without necessarily failing the report.

  If a BasicReport is stuck in `UNPROCESSED_RESULTS_READY`, the cronjob is either
  not running, erroring every tick (check its logs), or hitting a transient it
  keeps retrying.

## Common EDP Aggregator failure modes

These apply to the S4-A fulfillment path.

### Model-line mismatch

Three model-line references must be consistent; the simplest correct configuration
is that the measurement's model line and the results-fulfiller's model line are the
**same** line, and the impression data was generated under that line.

1. The model line the **measurement** is created with (becomes the requisition's
   model line; must have a **ModelRollout**).
2. The **results-fulfiller's** model line (its `--model-line`), the only key(s) in
   its model-line info map; must equal the line the **impression data** was
   generated under.
3. An optional `model_line_map` alias in the results-fulfiller config.

The fulfiller does `modelLineInfoMap.getValue(requisition.modelLine)`. If the
requisition's line (#1) is not a key in the map (built from #2), you get
`NoSuchElementException: ... is missing in the map` and every requisition
dead-letters.

**The `model_line_map` alias does not do what its name suggests.** It does not add
a new key or rewrite the requisition line before lookup; it only sets the
impression-lookup line for a key that already exists. It cannot bridge a
requisition line that differs from the info-map key. The fix for a split is to make
the two model lines equal, not to add an alias.

Find which model line actually has the impression data:

```bash
gcloud spanner databases execute-sql edp-aggregator \
  --instance=<SPANNER_INSTANCE> --project=<ENV> \
  --sql="SELECT CmmsModelLine, EventGroupReferenceId,
                COUNT(DISTINCT IntervalStartTime) AS dates, COUNT(*) AS num_rows
         FROM ImpressionMetadata WHERE EventGroupReferenceId = '<reference-id>'
         GROUP BY CmmsModelLine, EventGroupReferenceId"
```

Before concluding a line has no ModelRollout, decode its resource ID to the numeric
external ID (URL-safe base64 of the 8-byte big-endian int64) and check the
`ModelRollouts` table. A bare rollout is not always enough: a measurement's model
line needs a rollout whose `ModelRelease` references the `Population` the
measurement uses. Join `ModelRollouts → ModelReleases` and confirm the release's
population matches before assuming the rollout is valid.

### Missing impression blobs

`ImpressionReadException ... BLOB_NOT_FOUND` means an `ImpressionMetadata` row
references a GCS blob that does not exist. Causes: a partial data generation wrote
rows but not blobs; blobs "deleted" on a **versioned** bucket (archived, not
removed — the row stays live but the current object is gone); or rows for one date
in a different layout/model-line than the others. For the model line the
requisition resolves to, **every date the requisition spans must have a metadata
row whose blob exists and decrypts.** Fix by regenerating: delete stale rows first
(the metadata store enforces a unique `BlobUri` index; re-registering a claimed URI
fails `ALREADY_EXISTS`), re-trigger the sync by rewriting the `done` blob, and
verify decryption afterward.

### KMS-type mismatch

`Unsupported key URI` means the EDP's configured `kms_type` disagrees with how the
data was encrypted (`gcp-kms://` vs `aws-kms://`). Check the KEK URI in a metadata
blob (`gcloud storage cat ... metadata.binpb | strings | grep -i kms`) against the
EDP's config, and regenerate with the matching type. Never hand-edit
`metadata.binpb`; that breaks Tink envelope encryption
(`No matching key found for the ciphertext`).

### MIG stuck at zero

The data-watcher dispatched a work item but the results-fulfiller MIG never scaled
up. The autoscaler scales on the queue's undelivered-message metric; inspect it
(`gcloud compute instance-groups managed describe results-fulfiller-mig-v2 ...`).
If items are queued but the MIG stays at 0, the autoscaler or subscription is
unhealthy. If it scaled and then back to 0 with requisitions still unfulfilled,
suspect overlapping runs (below).

### Capacity exhaustion masquerading as auth failures

On a shared environment, a common cause of aggregator fulfillment stalling is
**not** IAM — it is Confidential-VM capacity exhaustion
(`ZONE_RESOURCE_POOL_EXHAUSTED`) on the results-fulfiller MIG. While GCE retries for
capacity, the requisition sits unfulfilled and any auth token minted earlier for a
fetcher trigger can expire, so a later fetcher call returns 401 — looking like an
IAM bug but actually a stale token. Confirm before touching IAM:

```bash
gcloud logging read \
  'protoPayload.status.message:"ZONE_RESOURCE_POOL_EXHAUSTED" AND timestamp>="<START>"' \
  --project=<ENV> --limit=10 \
  --format="value(timestamp,protoPayload.resourceName,protoPayload.status.message)"
```

If present, the immediate fix is a re-trigger once capacity is available. (This is a
runtime symptom; longer-term MIG zone/capacity tuning is out of scope for this
guide.)

### Overlapping runs and stale requisitions

Never drive two report/fulfillment runs against the same environment
simultaneously — the MIG, requisition storage, and Spanner state are shared, and
one run's MIG can fulfill the other's requisitions then scale down before late ones
are processed, hanging the second (symptom: most requisitions FULFILLED, one or two
stuck in STORED). Wait for the pipeline to be idle before re-triggering. Likewise,
requisitions created **before** a config fix keep retrying from the queue and
re-emit their old errors; identify them by `CreateTime` and confirm a SEVERE log
line belongs to the current work before treating it as live.

## Guarding against self-inflicted breakage

Most "it fails only for me" situations come from mutated configuration. Before deep
debugging, diff the environment's current config against a known-good state (the
measurement/results-fulfiller model lines, the data-watcher config, the EDPs
config, the requisition-fetcher config) and restore the known-good value rather
than inventing a new one. Always back up an environment variable before changing
it, and never delete one without confirming it is truly unused — values often feed
Cloud Functions or Terraform that a simple grep misses.

## Anti-patterns

- Inferring pipeline state from CI job status instead of the databases and logs.
- Treating "my report is stuck" as one failure — walk the lifecycle and localize
  the stage first.
- Reading `PENDING_REQUISITION_PARAMS` as an EDP problem — until the Duchies set
  params, the requisitions are not even visible to EDPs; look at the
  `ComputationParticipants` states and the herald.
- Looking for a direct EDP's requisition in the aggregator pipeline (or vice
  versa) — check which path the EDP uses first.
- Concluding a stage is broken from one narrow log grep — widen the window and
  check both `jsonPayload.message` and `textPayload`.
- Reading `jsonPayload.message` for the results-fulfiller (often empty) instead of
  `textPayload` (has the stack trace).
- Assuming a model line has no rollout without decoding its ID and checking, or
  assuming any rollout is sufficient (it must reference the measurement's
  Population's release).
- Hand-editing `metadata.binpb` to "fix" a blob URI or model line — regenerate
  instead.
