# Reporting scale test

The **Run Reporting Scale Test** workflow exercises the deployed Reporting HTTP
gateway and public APIs, following the EventGroupSync scale test pattern. It
exercises geographically segmented
reports through creation, EDPA fulfillment, measurement computation, and final
report assembly. A case passes only when the BasicReport, its underlying Report,
and every linked metric and Kingdom measurement succeed, and every requisition
is present and fulfilled. Creation and fulfillment run as separate invocations.
The requisition-fetcher scheduler is paused before each creation attempt. Failed
creation cancels unfinished measurements belonging to that exact test report,
withdrawing their unfulfilled requisitions. Successful creation verifies all
resource links and counts before fetching resumes and fulfillment is tested.
Cases finish before the next case starts.

## Reuse existing data

This test reads registered EventGroups and creates only fresh campaign groups
and BasicReports. It does not generate impressions, upload data, rewrite raw
files, register raw uploads, or change an existing dataset. Every case creates a
fresh campaign group so earlier report metrics cannot satisfy the new creation
workload.

The defaults reuse the high-overlap EDP Aggregator dataset introduced by
[PR #4469](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/4469),
configured in qa/head by
[PR #4570](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/4570),
and extended by
[PR #4643](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/4643).
Its April–June flight supports a 60-day report directly. See
[High Overlap Data Shape](../edpaggregator/high-overlap-data-shape.md).

To reuse another existing flight, supply its EventGroup reference IDs, first date
and existing ModelLine. The EventGroups
must already be registered for the configured MeasurementConsumer and span
exactly three EDPs. Multiple EventGroups from the same EDP can be included. The
reporting window must be within their data availability and the ModelLine's
active interval. No dataset seeding workflow is called by this test.

When reference IDs and ModelLine are omitted, the test uses the existing high
overlap `all4` EventGroups for `edp7`, `edpa_meta` and `edpa_video_pub`, and
`HIGH_OVERLAP_MODEL_LINE`. Seed that dataset with **Run EDPA Reporting Integration
Test** first if needed. The default report starts 2026-04-01 and covers 60 days.

The workflow uses committed CMMS environment configuration and existing
synthetic test certificates and Reporting principal setup. The workflow reuses the existing `MC_API_KEY` secret to read Kingdom
measurements and requisitions. No additional environment settings are required. Use synthetic test datasets.

## Run

Dispatch **Run Reporting Scale Test** for `dev`, `head`, or `qa`, or call the
reusable workflow after the chosen dataset is available.

| Input | Default | Meaning |
|---|---|---|
| `reuse-paused-fetcher` | `false` | Reuse the scheduler pause from your preceding failed scale-test attempt |
| `report-days` | `60` | Report length in days; use `91` for 13 weeks |
| `report-start` | `2026-04-01` | First reporting date in the existing dataset |
| `event-group-reference-ids` | empty | Existing references; empty selects high overlap `all4` |
| `model-line` | empty | Existing ModelLine; empty uses `HIGH_OVERLAP_MODEL_LINE` |
| `edp-tls-names` | `edp7,edpa_meta,edpa_video_pub` | Synthetic TLS fixtures in distinct provider order from the selected EventGroups |
| `demographic-breakdown` | `age,gender` | Keep both initially; choose `age`, `gender`, or `none` to isolate a failure |
| `state-counts` | `50` | Exactly 50 states, retaining the selected demographics |
| `qualification-filters` | `ami,mrc` | Both existing qualification filters |
| `fulfillment-timeout-seconds` | `3600` | Per-case completion and verification timeout, separate from the HTTP deadline |
| `create-deadline-seconds` | `60` | Whole HTTP request deadline, including response reading |

The synthetic schema has three age groups and two genders. Keeping both gives
six demographic cells per state: 300 cells for exactly 50 states. Counts `1`
through `50` use one filtered pair of result groups per state, excluding DC, with
age/gender grouping retained. Count `51` uses native grouping on
`common.us_state`, whose enum includes DC, giving 306 cells with both demographic
fields. Count `0` retains the selected demographics and omits state cuts.

Weekly results request cumulative and non-cumulative reach, component reach,
and each component's non-cumulative unique contribution. Whole-interval results
request component and combined R&F, impressions/average frequency, unique reach,
and stacked incremental reach. The API requires stacked incremental reach to
use total frequency, so weekly groups use their supported unique reach fields.
Frequency is measured only over the whole interval. A 60-day window has eight
full weeks and a final partial week.

Start with age and gender retained. For a threshold search, use
`state-counts: 5,10,25,50`, then run `51` separately to test native grouping.
If creation fails, reduce demographics, isolate `ami` versus `mrc`, or reduce
`report-days`. Cases run serially and stop on the first failure. The HTTP deadline accepts 1 through 14,400 seconds. Its default remains
60 seconds; increasing it does not establish that creation latency improved.

Run on a quiet environment. The workflow checks for active or queued EDPA cloud
tests, reporting integration tests, deployments, and reporting scale runs in the
same environment. It refuses to start when one is present. If the fetcher scheduler is already
paused, explicitly set `reuse-paused-fetcher` to retry your own failed scale test. This preflight does
not prevent another workflow from starting later; keep the environment reserved
for the duration and check overlapping runs when interpreting timings. The
workflow pauses the scheduler identified by `REQUISITION_FETCHER_ENDPOINT`; it
does not disable the Cloud Run service or interrupt existing invocations. Allow
an existing invocation to finish before starting an isolated creation test.
Cleanup is scoped to the failed case's exact signed BasicReport metadata; it
preserves completed measurements and leaves other reports untouched. If cleanup
cannot be verified, the scheduler stays paused and its state is recorded in the
artifacts. After successful cleanup it stays paused, so the next attempt can reuse that
pause without waiting for partial EDPA fulfillment. Fetching resumes only after
the creation gate passes.

## Server settings for capacity diagnostics

The Reporting public API, scheduler and BasicReports result assembly job support `--internal-api-timeout-seconds` and
`--kingdom-api-timeout-seconds` (both default to 30 seconds), preserving the
channel's existing retry policy. A longer HTTP deadline alone cannot override
these gRPC channel deadlines. Dev currently uses 600 seconds for these calls and
for PostgreSQL's statement timeout.

`--report-metric-batch-concurrency` defaults to three concurrent batches.
Dev uses one batch at a time to avoid repeated PostgreSQL serialization
conflicts. Each batch still dispatches Kingdom measurement requests with the
separately configured `--kingdom-measurement-batch-concurrency`.
Record database capacity and deployed settings when comparing timings.

## Results

The workflow summary and `summary.json` record creation timing (headers and full
response separately), final states, metric and measurement counts, actual
requisition counts by data provider, and failure stage. Population provider
requisitions appear separately from the three EDPA EDPs. Artifacts include the
request, create response, BasicReport, underlying Report, measurement states,
requisition names and states, fixture references, and test log.

Creation verification reads the persisted BasicReport and underlying Report plan,
then every referenced metric and its stored Kingdom measurement links. It checks
those links against the signed measurement metadata and verifies each measurement's
expected provider requisitions. Fresh and reused resources are counted separately.
Requisition verification reads internal Kingdom pages of 1,000, scoped to the
MeasurementConsumer and a lower update-time bound from the oldest linked
measurement. It retains exact measurement/provider pairs and discards encrypted
payloads as each page is read. The cursor includes update time, provider ID and
requisition ID. A requisition that changes state between creation-audit pages is
deduplicated by its immutable ID; different requisitions for the same pair fail
verification. Creation includes PENDING_PARAMS rows, and final verification
requires every expected requisition to be FULFILLED. Verification no longer
scans every EDP's complete public history.

The creation gate waits up to 15 minutes for the persisted BasicReport's
underlying Report association. That association is written after Report creation
returns. A RUNNING API acknowledgement alone does not pass. The summary records
the observed resource-ready timestamp and its upper-bound elapsed time separately
from the HTTP response and exhaustive audit timings.
Metrics and measurements may still be RUNNING at this gate.

A failed create triggers a bounded read of the same resource without retrying
creation. A persisted RUNNING BasicReport cannot pass the creation gate. Cleanup
uses the public CancelMeasurement API, skips terminal measurements, and verifies
two scans with no active measurements. Cancellation withdraws unfinished
requisitions; already fulfilled requisitions remain fulfilled.

After the creation gate passes, the workflow resumes and immediately triggers
the fetcher scheduler. A read-only follow-up invocation waits for the same report
to succeed and verifies every final metric result link, measurement state, and
fulfilled requisition. Creation, creation verification, fulfillment, and final
verification timings are recorded separately. Increasing the HTTP deadline does
not establish that creation latency improved.

This is a capacity and lifecycle test. The existing high-overlap dataset has
five populated states; the remaining state cuts exercise empty audiences and
may expose threshold or report postprocessing failures. Completion does not
establish statistical accuracy of each noisy result; use the high-overlap
correctness tests for numerical assertions. Report a postprocessing failure
separately from an EDPA fulfillment failure, using the linked measurement and
requisition evidence.

A successful create followed by a transport failure during verification preserves
the created report and leaves fetching paused. Retry verification on the same
report with `REPORTING_SCALE_PHASE=creation` and
`REPORTING_SCALE_EXISTING_BASIC_REPORT`; this records
`creation_verified_existing` after the full creation gate passes.

For read-only follow-up on a prior scale test from the dev VM, set
`REPORTING_SCALE_EXISTING_BASIC_REPORT` to its full resource name and select one
state count matching that report. This mode records `creation_reused: true` and
`fulfilled_existing`; it cannot establish fresh creation latency or an isolated
capacity result. It creates no campaign group or report. Normal workflow runs
create fresh reports during the creation invocation and reuse them during the
fulfillment invocation.

For creation diagnostics from the dev VM, pause the fetcher scheduler first and
set `REPORTING_SCALE_PHASE=creation` and
`REPORTING_SCALE_CANCEL_FAILED_CREATION=true`. Supply `PDP_NAME` from the CMMS
environment configuration. A successful creation-only invocation records
`creation_verified: true` and `creation_succeeded`; it does not claim fulfillment.
Resume fetching only after that gate passes, then follow up with
`REPORTING_SCALE_EXISTING_BASIC_REPORT` and `REPORTING_SCALE_PHASE=full`.

### Recover using existing metrics

For recovery from the dev VM, set `REPORTING_SCALE_CAMPAIGN_GROUP` to the
previous campaign group and `REPORTING_SCALE_SYNC_REPORT` to its underlying
Report. Keep the original dates, model, demographic fields, states and filters.
The test reads the previous Report to sync metric states from Kingdom before
creating a replacement BasicReport. Reporting reuses valid RUNNING and
SUCCEEDED metrics and replaces FAILED or INVALID metrics. Failed and cancelled
Kingdom measurements remain terminal; recovery creates replacement resources.

Creation verification records new and reused metric and measurement counts.
Set `REPORTING_SCALE_CANCEL_FAILED_CREATION=false` to preserve work during
recovery. Pass the replacement's underlying Report name through
`REPORTING_SCALE_UNDERLYING_REPORT` when following up. A recovery run measures
the remaining work and cannot establish fresh full-workload creation latency.

Results-fulfiller workers process stored requisition groups in sequential batches
of at most 100. This also bounds retries of older groups that exceed the fetcher's
current group limit. Each batch makes a separate pass over impression data;
smaller batches reduce peak vector memory but can increase read and processing time.

### Reporting transport limits

The geographically segmented profile produces internal Reporting requests and responses larger
than gRPC Java's default 4 MiB limit. The Reporting internal server is configured with
`--max-inbound-message-size=67108864`, and Reporting internal API clients default to
64 MiB through `--internal-api-max-inbound-message-size`. This limit applies to each
uncompressed protobuf message.

The common-jvm patch in `build/common_jvm/patches/grpc_message_size.patch` exposes these
transport settings; its default server and channel limit remains 4 MiB.

The Reporting HTTP gateway uses `--reporting-public-api-max-inbound-message-size`
with the same 64 MiB bound, so completed BasicReport responses can be read. The
scale test uses that bound for underlying Report and metric readback as well.

In dev, full gRPC payload logging is disabled on the two Reporting servers
through `_verboseReportingGrpcServerLogging`. The logging interceptor formats
batch payloads before delivering requests and sending responses, so enabling
it can consume RPC deadlines during scaled creation.


Creation verification reads the persisted internal Report plan because RUNNING public Reports
do not yet include metric results. It also reads internal Kingdom requisitions so that
PENDING_PARAMS rows can be verified before Duchy setup finishes. The controller opens temporary
private API connections with a separate kubeconfig and closes them after each phase.
For direct Bazel runs, provide REPORTING_SCALE_INTERNAL_API_TARGET and
REPORTING_SCALE_KINGDOM_INTERNAL_API_TARGET. Fulfillment verifies public metrics and measurements, plus the complete internal requisition set.
