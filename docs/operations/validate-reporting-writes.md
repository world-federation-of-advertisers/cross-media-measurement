# Validate Reporting writes

The internal Reporting servers expose two independent rollout switches. Both
default to `true`:

| Flag | RPC | Validation |
| --- | --- | --- |
| `--validate-insert-basic-report` | `InsertBasicReport` | Require `external_report_id` and component ReportingSet IDs. Resolve the Campaign Group and component sets under the report's MeasurementConsumer, and check request, summary, membership, and metric identities. |
| `--validate-create-reporting-set` | `CreateReportingSet` | Require nonempty primitive membership and complete EventGroup keys, validate composite operations and references, and check membership in any supplied Campaign Group. |

A Campaign Group must be a nonempty primitive ReportingSet whose Campaign Group
ID equals its own ID. Custom components keep their exact ReportingSet IDs and
filters. DataProvider component summaries must identify unfiltered primitive
sets covering that provider's Campaign Group membership. Metric entries may
cover only some components and appear in a different order, but their keys must
agree with the request and summaries.

Standalone custom ReportingSets may omit `external_campaign_group_id` with
validation enabled. When the public `CreateBasicReport` request uses custom
components without a Campaign Group, it creates or reuses an effective Campaign
Group before storing the report. The standalone sets remain unchanged.

`InsertBasicReport` imports completed reports. Its report ID requirement does
not apply to `CreateBasicReport`, which assigns the associated Report later.

## Migrating legacy callers

Prefer fixing callers to supply consistent identities before enabling these
checks. For a deployment that still imports malformed legacy payloads, set the
affected switch explicitly to `false`, for example
`--validate-insert-basic-report=false`. The other switch remains enabled.

Disabling a switch skips the new validation and permits legacy malformed writes,
including empty primitive membership.
Existing required fields, database constraints, reference existence checks in
the writer, and ImpressionQualificationFilter validation still apply. The
server does not repair malformed data or rename metric keys when validation is
disabled.

Backfill existing BasicReports using the
[Reporting backfill tools](/src/main/kotlin/org/wfanet/measurement/reporting/deploy/v2/gcloud/spanner/tools/README.md),
correct remaining callers, and restore both switches to `true`. Intentionally
standalone ReportingSets do not need a Campaign Group backfill.
