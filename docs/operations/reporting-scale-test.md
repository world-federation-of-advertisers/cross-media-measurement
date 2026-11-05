# Reporting creation scale test

The **Run Reporting Scale Test** workflow exercises the deployed Reporting HTTP
gateway and public APIs, following the EventGroupSync scale test pattern. It
compares an unsegmented report with geographically segmented reports and stops
after creation and a successful readback. Computation continues asynchronously;
this test does not assert report completion or measured values.

## Setup

Run **Run EDPA Reporting Integration Test** in the chosen environment first if
the high overlap dataset has not been seeded. The scale test uses the existing
`all4` EventGroups for three EDPs; it does not regenerate impressions or change
the dataset. Every case creates a fresh campaign group and BasicReport, so
previous report metrics cannot satisfy the new creation workload.

The workflow uses committed CMMS environment configuration and the existing
synthetic test certificates and Reporting principal setup. No additional GitHub
environment variables or secrets are required.

## Run

Dispatch **Run Reporting Scale Test** for `dev`, `head`, or `qa`, or call the
reusable workflow after dataset seeding. Defaults are:

| Input | Default | Meaning |
|---|---|---|
| `weeks` | `13` | Whole weeks starting 2026-04-01, within the seeded flight |
| `state-counts` | `0,50` | Unsegmented baseline followed by exactly 50 states |
| `qualification-filters` | `ami,mrc` | Both existing qualification filters |
| `repetitions` | `1` | Fresh reports per state count |
| `create-deadline-seconds` | `60` | Whole HTTP request deadline, including reading its response |

Counts `1` through `50` use one filtered pair of result groups per state,
excluding DC. Count `51` uses native grouping on `common.us_state`, whose enum
includes the 50 states and DC. This exercises the native grouping expansion as
well as exact-cardinality filtered result groups. Neither profile groups by
gender or age.

Weekly results request cumulative and non-cumulative reach, component reach,
and each component's non-cumulative unique contribution. Whole-interval results
request component and combined R&F, impressions/average frequency, unique reach,
and stacked incremental reach. The BasicReport API requires stacked incremental
reach to use total frequency, so the weekly group uses its supported unique
reach fields. Frequency is measured only over the whole report interval.

For a threshold search, use `state-counts: 0,5,10,25,50`, then try `51` to test
native grouping. If creation fails, isolate `ami` versus `mrc` or reduce `weeks`.
Cases run serially and the test stops on the first failure. Change the HTTP
deadline only when explicitly comparing deadline sensitivity.

## Results

The workflow summary records creation time, HTTP status, readback state and
outcome. The results artifact includes `summary.json` and each case's request,
create response, readback response and test log. It records request/response
bytes, the failure stage, and the BasicReport resource name for report tracing.
A failed create triggers a bounded read of that same resource, helping identify
a partially persisted report without retrying creation.

A creation pass means the HTTP create completed within the deadline and the
same report's requested result groups can be read back. It does not mean its
measurements have been fulfilled. Many unpopulated states legitimately produce
empty or noisy results; that is outside this creation test's assertions.
