# Reporting creation scale test

The **Run Reporting Scale Test** workflow exercises the deployed Reporting HTTP
gateway and public APIs, following the EventGroupSync scale test pattern. It
compares a report with no geographic cuts against geographically segmented
reports and stops after creation and a successful readback. Computation
continues asynchronously; this test does not assert report completion or values.

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
synthetic test certificates and Reporting principal setup. No additional GitHub
environment variables or secrets are required. Use synthetic test datasets.

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
| `state-counts` | `0,50` | No geographic cuts, followed by exactly 50 states |
| `qualification-filters` | `ami,mrc` | Both existing qualification filters |
| `repetitions` | `1` | Fresh reports per state count |
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
`state-counts: 0,5,10,25,50`, then run `51` separately to test native grouping.
If creation fails, reduce demographics, isolate `ami` versus `mrc`, or reduce
`report-days`. Cases run serially and stop on the first failure. Change the HTTP
deadline only when explicitly comparing deadline sensitivity.

## Results

The workflow summary records creation time, HTTP status, readback state and
outcome. The artifact includes `summary.json` and each case's request, create
response, readback response and test log. It records the existing EventGroups,
EDPs and ModelLine used, demographic selection, dates, request/response bytes,
failure stage, and BasicReport name for tracing. A failed create triggers a
bounded read of that same resource, identifying a partially persisted report
without retrying creation.

A pass means the HTTP create completed within its deadline and the same report's
requested result groups can be read back. It does not mean measurements have
been fulfilled. Unpopulated states may produce empty or noisy results; that is
outside this creation test's assertions.
