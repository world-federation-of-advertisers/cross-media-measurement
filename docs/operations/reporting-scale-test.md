# Reporting scale test

The **Run Reporting Scale Test** workflow exercises the deployed Reporting HTTP
gateway and public APIs, following the EventGroupSync scale test pattern. It
exercises geographically segmented
reports through creation, EDPA fulfillment, measurement computation, and final
report assembly. A case passes only when the BasicReport, its underlying Report,
and every linked metric and Kingdom measurement succeed, and every requisition
is present and fulfilled. Cases finish before the next case starts.

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
| `report-days` | `60` | Report length in days; use `91` for 13 weeks |
| `report-start` | `2026-04-01` | First reporting date in the existing dataset |
| `event-group-reference-ids` | empty | Existing references; empty selects high overlap `all4` |
| `model-line` | empty | Existing ModelLine; empty uses `HIGH_OVERLAP_MODEL_LINE` |
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
`report-days`. Cases run serially and stop on the first failure. Change the HTTP
deadline only when explicitly comparing deadline sensitivity. Its default remains
60 seconds; increasing it does not establish that creation latency improved.

Run on a quiet environment. The workflow checks for active or queued EDPA cloud
tests, reporting integration tests, deployments, and reporting scale runs in the
same environment. It refuses to start when one is present. This preflight does
not prevent another workflow from starting later; keep the environment reserved
for the duration and check overlapping runs when interpreting timings. Neither
this test nor its workflow cancels another test or modifies its data.

## Results

The workflow summary and `summary.json` record creation timing (headers and full
response separately), final states, metric and measurement counts, actual
requisition counts by data provider, and failure stage. Population provider
requisitions appear separately from the three EDPA EDPs. Artifacts include the
request, create response, BasicReport, underlying Report, measurement states,
requisition names and states, fixture references, and test log.

A bounded initial measurement sample matches the exact BasicReport name in
signed `MeasurementSpec.reporting_metadata` and identifies its underlying
Report. Its counts are labeled as observations, not totals. Complete IDs come
from final metric results, so verification does not scan the entire measurement
consumer or depend on measurement-list pagination. Reads use batches of 50
measurements or 100 metrics and at most four concurrent calls. Polling
BasicReports tolerates bounded transient HTTP errors. Final verification checks
all metric result measurement links, every measurement's expected provider
requisitions, and positive participation by all three EDPs. Missing entries and
empty result sets fail the test. Verification time is distinct from creation
and fulfillment time.

A failed create triggers a bounded read of the same resource without retrying
creation. A persisted RUNNING BasicReport is diagnostic evidence; it cannot pass
creation or fulfillment checks. A timeout or terminal failure records the last
observed states and resource names for further tracing.

This is a capacity and lifecycle test. The existing high-overlap dataset has
five populated states; the remaining state cuts exercise empty audiences and
may expose threshold or report postprocessing failures. Completion does not
establish statistical accuracy of each noisy result; use the high-overlap
correctness tests for numerical assertions. Report a postprocessing failure
separately from an EDPA fulfillment failure, using the linked measurement and
requisition evidence.

For read-only follow-up on a prior scale test from the dev VM, set
`REPORTING_SCALE_EXISTING_BASIC_REPORT` to its full resource name and select one
state count matching that report. This mode records `creation_reused: true` and
`fulfilled_existing`; it cannot establish fresh creation latency or an isolated
capacity result. It creates no campaign group or report. Normal workflow runs
always create fresh reports.

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
