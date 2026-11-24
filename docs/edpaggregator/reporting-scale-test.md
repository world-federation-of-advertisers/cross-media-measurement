# Full report scale test

This opt-in test is deferred until multi-measurement report creation, large-result
retrieval, and bounded, recoverable final result writes can support the complete
profile. It also uses the assembly improvements in PR #4844 and the expanded
seed fixture in PR #5013. Passing smaller reports or metadata checks does not
establish that this full report succeeds.

The reporting workflow accepts `stress-report=true` to include a single large
BasicReport. It reports over every seeded entity and all four aggregator EDPs
from 2026-04-01 to 2026-06-30, exclusive: 90 days across 13 weekly windows.
The final window is six days. The seed data still covers 91 days; expectations
exclude the final day and retain the source timestamp distribution.

| Result group family | Population grouping or event cut | Periods | Filters |
| --- | --- | --- | --- |
| Geography and demographics, weekly | 51 state/DC values × 2 genders × 3 ages × 2 parental statuses | 13 | AMI, MRC |
| Geography and demographics, total | Same 612 cells | 1 | AMI, MRC |
| Device and media, weekly | 20 filtered groups: 5 devices × 4 media enum values | 13 | AMI, MRC |
| Device and media, total | 20 groups with the same device/media cuts | 1 | AMI, MRC |
| Signed-in device and media | 20 groups with a signed-in event filter | 1 | AMI, MRC |
| Signed-out device and media | 20 groups with a signed-out event filter | 1 | AMI, MRC |

There are 82 result groups in one BasicReport. OTHER and NATIVE media have no
seeded impressions and serve as zero-reach controls. Only the two population groups
use `DimensionSpec.grouping`. Device, media, and signed-in fields are event
attributes, so their cuts use CEL filters on primitive ReportingSet components.
For example, a DESKTOP/VIDEO component uses
`common.device_type == 1 && common.media_type == 1`. Weekly and total groups
reuse the same filtered primitives. The report omits `campaign_group`, allowing
the server to synthesize it from the ReportingSet components; the environment
must enable ReportingSet reporting-unit components.

Weekly groups request cumulative reach and non-cumulative reach, K+ reach,
impressions, average frequency, and per-EDP reach. The total demographic group
also requests ordered incremental contributions. The API permits those
contributions only for total-period groups. Only person-attribute groups request
population denominators.

The test checks every cell, period, filter, and demographic population
denominator. Across the complete matrix, it checks the presence of requested
cumulative/non-cumulative metric messages, K+ vector lengths, four total
demographic incremental contributions, and each component's requested metrics.
Scalar zero values are valid and have no protobuf presence. Average frequency
and percent reach are also checked against the returned counts and denominator.

Source-derived numerical checks replay seven demographic reference cells,
including DC's zero-impression control. They cover every requested weekly and
total metric, including impressions, average frequency, percent reach, and
component reach. Impressions are clipped separately per EDP, as required by the
metric configuration. Counts allow the configured noise; sparse cells use the
derived ratio check when a truth-based average frequency would be unstable.

Three device/media references (`DESKTOP/VIDEO`, `TV/DISPLAY`, `OTHER/DISPLAY`)
cover earlier weekly values, K+ reach, impressions, average frequency, component
reach, and signed-in/out totals. They include populated AMI/display results and
MRC video zero controls. Full-flight union reach is additionally checked for
every device/media cell and signed-in filter by merging source VID intervals;
this also checks the last cumulative week without replaying the entire dataset.
The test records the report resource, result count, serialized response size,
and comparison results for later tracing. Focused regressions reject missing
metrics and corrupt earlier weeks, component values, total counts, and ratios.

To retry only the scale report, set the workflow's `test-filter` to
`z scaled.*` along with `stress-report=true`. Leave the filter empty for the
complete reporting suite. The metadata audit runs in either case.

The test is opt-in because creation, response size, and fulfillment at this scale
remain benchmark targets. Ordinary cloud runs keep the two existing smaller
reports. The test does not raise service deadlines or message limits. The device
and media panels are separate from the full demographic Cartesian product;
combining every event dimension with every demographic and week is a larger test.
