# Reporting Spanner CLI Tools

Command-line tools for Reporting operators that act directly on the Reporting
databases.

## `BackfillBasicReportExternalReportIds`

Backfills `BasicReport.external_report_id` on stored `BasicReport`s.

`BasicReport`s written through the internal `InsertBasicReport` method may omit
this field even when an associated `Report` exists. Dashboard queries use the
field as a report key, so missing values cause unrelated reports to be grouped
under the same null key and prevent requisition metadata from joining to the
report.

Run the tool with `--help` for usage information.

### What it does

Every `BasicReport` in state `SUCCEEDED`, across all `MeasurementConsumer`s, is
examined one `MeasurementConsumer` at a time and one page at a time. Use
`--cmms-measurement-consumer-id` to restrict the scan. Use `--create-time-after`
to restrict which `BasicReport`s are eligible for updates; older rows are still
considered when checking that a Report is not already claimed.

For each `BasicReport` missing `external_report_id`, the tool reads the
associated `Report` from Reporting Postgres using either of these explicit
links:

1.  `Report.details.basic_report` equals the full `BasicReport` resource name.
2.  `Report.create_report_request_id` equals the stored
    `BasicReport.create_report_request_id`.

Some integrations deliberately reuse the Report's external ID as the
BasicReport's external ID without storing either explicit link. For those
integrations, `--match-external-basic-report-id` adds a third candidate when a
Postgres Report with that external ID exists. This option requires
`--cmms-measurement-consumer-id` so the convention is enabled only for a known
integration.

When the available links resolve to exactly one `Report`, its existing
`external_report_id` is copied into the `BasicReports` Spanner row without
changing the report state. The tool first plans the complete selected scope and
requires a one-to-one mapping in both directions. If any `BasicReport` maps to
multiple Reports, or any Report maps to multiple `BasicReport`s, a real run
fails before issuing any writes. A dry run reports every conflict. If no
`Report` is found, the `BasicReport` is logged and skipped. No new identifiers
are minted.

Run with `--dry-run` first. Resolve every ambiguous mapping before applying the
backfill. Unresolved rows may remain when no trustworthy link exists; they are
left unchanged.

### Examples

This assumes that the `BackfillBasicReportExternalReportIds` target has been
built. For brevity, the examples do not include the full path to the
executable.

*   Report what would change without writing to either database:

    ```shell
    BackfillBasicReportExternalReportIds \
      --spanner-project=PROJECT \
      --spanner-instance=INSTANCE \
      --spanner-database=reporting \
      --postgres-cloud-sql-connection-name=CONNECTION \
      --postgres-database=reporting-v2 \
      --postgres-user=USER \
      --cmms-measurement-consumer-id=MEASUREMENT_CONSUMER_ID \
      --match-external-basic-report-id \
      --create-time-after=2026-06-01T00:00:00Z \
      --dry-run
    ```

*   Apply the backfill by rerunning the command without `--dry-run`.

The tool uses Application Default Credentials for Spanner and Cloud SQL IAM
authentication. See the database-access instructions below for the required
operator identity and permissions.

## `BackfillBasicReportReportingSets`

Backfills
`ResultGroup.MetricMetadata.ReportingUnitComponentSummary.external_reporting_set_id`
on stored `BasicReport`s.

`BasicReport`s written through the internal `InsertBasicReport` method can
contain two related inconsistencies: a component summary can omit
`external_reporting_set_id`, and `BasicReport.external_campaign_group_id` can
refer to an ordinary primitive ReportingSet instead of a Campaign Group. A
valid Campaign Group is a non-empty primitive ReportingSet whose
`CampaignGroupId` points to itself.

The temporary public read fallback serves a blank component without
`reporting_set`. This tool completes safely resolvable reports so the fallback
can be removed. See
[issue #4289](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4289).

Run the tool with `--help` for usage information.

### What it does

Every `BasicReport` in state `SUCCEEDED`, across all
`MeasurementConsumer`s, is examined one `MeasurementConsumer` and one page
at a time. Use `--create-time-after` to restrict this to `BasicReport`s
created after a given time. A bounded run cannot establish that the full
environment is ready to remove the fallback.

For a report that already references a valid Campaign Group, the tool first
determines the identity of each blank component:

*   For a caller-supplied ReportingSet component, the ReportingSet ID in the
    stored request is authoritative. The tool verifies that it identifies a
    non-empty primitive ReportingSet, uses that ReportingSet's EventGroups for
    membership validation, and preserves the exact ID and any filter.
*   For a DataProvider component, the tool resolves membership from its
    `event_group_summaries`. A blank `cmms_data_provider_id` is filled only
    when the stored request explicitly identifies DataProvider components,
    every EventGroup resolves uniquely to the same requested DataProvider,
    and the metric identities agree. A unique EventGroup owner alone does
    not prove that the component was intended to be a DataProvider component.
    Populated, consistent DataProvider IDs are preserved. Custom ReportingSet
    components keep their intentionally blank DataProvider ID.

Every resolved EventGroup key must belong to the Campaign Group universe. For a
DataProvider component, the tool reuses an unfiltered primitive ReportingSet
under the Campaign Group when its membership is an exact match, or creates one
otherwise. It never substitutes that unfiltered ReportingSet for a
caller-supplied ReportingSet component.

For a report whose Campaign Group reference is instead a non-empty ordinary
primitive ReportingSet with no `CampaignGroupId`, the tool treats that
ReportingSet as evidence of the caller's intended Campaign Group. It resolves
DataProvider components from the MeasurementConsumer's EventGroup directory and
caller-supplied ReportingSet components from the stored request and ReportingSet
membership. It uses the union of all component EventGroups as the replacement
Campaign Group universe. The referenced ReportingSet's EventGroups must appear in
that union and must exactly match at least one component; the report may contain
additional EventGroups in other components. Only when all components resolve
and those consistency checks pass does the tool:

1.  reuse or create an equivalent non-empty primitive, self-linked Campaign
    Group with exactly the report's EventGroups;
2.  change `BasicReport.external_campaign_group_id` to that valid Campaign
    Group; and
3.  populate DataProvider component summaries under the valid Campaign Group
    while preserving the exact IDs of caller-supplied ReportingSet components,
    including their filter semantics.

A caller-supplied ReportingSet component must name the same MeasurementConsumer
as the `BasicReport`. Its recorded component EventGroups must match the
referenced ReportingSet's membership, including the number of occurrences of
each EventGroup ID. A foreign-consumer key or conflicting EventGroup metadata
blocks the report in both dry-run and applied runs and keeps readiness at `no`.

The original ordinary ReportingSet is never modified. The Campaign Group
change and all component IDs are written to the `BasicReport` in one Spanner
mutation. Creating or finding the replacement Campaign Group in Postgres is
idempotent.

A Campaign Group change is not attempted when the `BasicReport` has a linked
`ReportResultId`. Those results contain ReportingSet references that this tool
cannot safely rewrite. The report is left completely unchanged, counted under
`Linked results blocking migration`, and the run reports that the read-path
fallback is not ready for removal.

If different reports reuse the same ordinary ReportingSet reference but contain
different EventGroup unions, each distinct union resolves to its own Campaign
Group. The tool does not force those reports into one shared universe.

The tool does not guess. A missing, empty, composite, or already-assigned
Campaign Group reference is left unchanged. A report is also left unchanged
when a component has no EventGroups, references an unknown EventGroup, has a
blank DataProvider without explicit request evidence or unique membership,
identifies a custom ReportingSet that is missing, empty, or not primitive, has an existing
ReportingSet ID without stored request evidence of its identity, disagrees with
the custom component identity stored in the request, omits an EventGroup
contained in the referenced Campaign Group source, or has linked results that
would also need migration. These rows are counted as blocking, the scan
continues, and the environment is reported as not ready to remove the read fallback.

No Campaign Group or component ReportingSet is created and no `BasicReport`
is updated unless every component summary that needs resolution can be
resolved and the metric identities are consistent. A blank DataProvider ID
with explicit request evidence is checked even if the component ReportingSet
ID is already populated.

Entries in `metric_set.reporting_set_components` carry no membership, so an
entry missing `external_reporting_set_id` cannot be backfilled. A populated
custom metric ID must belong to the validated component identity set for that
result group. Metric entries may be reordered or cover only some requested
components. DataProvider metric keys must also agree with the component
identities. The tool does not rename conflicting metric keys or change metric
values.

Missing metric ReportingSet IDs and conflicting metric identities are checked
before any writes, including on reports whose summary IDs are complete. The
entire report is left unchanged and counted as blocking. The summary reports
`Unresolvable metric_set components`, `Conflicting metric_set identities`, and
`DataProvider identities filled` separately.

### Operator flow

No new flags or prerequisite tool are required.

1.  Run an unbounded `--dry-run` against the intended environment. Review the
    planned writes, the blocking counts, and the per-report warnings.
2.  If the plan is acceptable, run the same tool without `--dry-run`. It
    validates each report before creating Campaign Groups or component
    ReportingSets, then updates that report. A conflicting report is left
    unchanged while the scan continues through other reports.
3.  If any rows are blocking, save the diagnostics. Ask the report owner to
    compare the original request, the exact referenced ReportingSets and
    filters, and the source results when available. Repair stored data only
    through a separately approved, evidence-backed process. Do not guess an
    ID, blindly rename metric keys, or mutate ordinary ReportingSets.
4.  After a justified repair, rerun an unbounded dry-run and then an applied
    run. If the intended identity cannot be proven, the migration remains
    incomplete and readiness must stay `no`.
5.  Keep the read fallback until the completion conditions below are met.
    The fallback tolerates blank summary ReportingSet IDs; it does not repair
    invalid Campaign Groups or contradictory metric identities.

### After the backfill

The public read path temporarily tolerates a `BasicReport` whose component
summaries still lack `external_reporting_set_id`: it omits `reporting_set`
rather than failing, so an incomplete resource is served without any error.

An applied, unbounded run reports `Ready to remove read-path fallback: yes`
only when no examined `SUCCEEDED` row remains blocked by these checks. This
is a check of the scanned rows, not a guarantee about other states or future
writes. A bounded run never reports ready because it does not examine older rows. A dry
run reports ready only when it finds that no writes or primitive Campaign Group
migrations would be needed.

Each such `BasicReport` is logged once per read, at `WARNING`, by the Reporting
public API server:

```
BasicReport <id> of MeasurementConsumer <id> has N component summary/summaries
without external_reporting_set_id.
```

After a successful unbounded run, that warning staying quiet confirms that no
new incomplete records are being written. If it recurs, a write path is still
storing incomplete component summaries.

This migration does not strengthen `InsertBasicReport` validation. Callers of
that internal method must supply a valid self-linked primitive Campaign Group
and populated component ReportingSet IDs. Add write-time validation separately
before treating a quiet warning as a permanent guarantee.

The tolerance in the read path should be removed only once the warning has
stayed silent over a sustained period, not on a schedule.

### Concurrency

Do not run two instances of this tool against the same environment at the same
time.

Each run builds its own in-memory view of the `ReportingSet`s that already exist
under a Campaign Group. If two runs read that view before either has created a
`ReportingSet` for the same missing membership, both create one. The minted
`external_reporting_set_id` is a random UUID, so there is no unique constraint
to violate: neither run fails, and neither summary reports anything unusual. The
result is duplicate `ReportingSet`s with identical membership under the same
Campaign Group, which silently defeats the reuse the tool otherwise guarantees.

There is no lock or lease. Serialising runs is the operator's responsibility.

### Database access

The tool reads and writes the Reporting Spanner database, and reads and writes
the Reporting Postgres database. `BasicReport`s are stored in Spanner while
`ReportingSet`s are stored in Postgres, so both must be reachable.

Credentials come from
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
There is no credentials option: the Spanner client falls back to Application
Default Credentials, and the Postgres connection uses Cloud SQL IAM database
authentication, which resolves them as well.

#### Identify the service account to run as

The Postgres schema is created by the internal Reporting server's schema-update
init containers, so the service account that server runs as owns the reporting
tables. In PostgreSQL only a table's owner can grant access to it, so running
the tool as that same service account avoids provisioning any additional
database credentials.

Its Cloud SQL IAM database username is the value the internal Reporting server
passes as `--postgres-user`:

```shell
kubectl get deployment postgres-internal-reporting-server-deployment \
  -o jsonpath='{.spec.template.spec.containers[0].args}' | tr ',' '\n' | grep postgres-user
```

For a deployment provisioned by the Reporting Terraform this is
`reporting-internal@PROJECT.iam`, corresponding to the service account
`reporting-internal@PROJECT.iam.gserviceaccount.com`.

#### Authenticate as it

```shell
gcloud auth application-default login \
  --impersonate-service-account=SERVICE_ACCOUNT_EMAIL
```

Then pass its database username, the value read above, to `--postgres-user`.

That service account already holds `roles/cloudsql.instanceUser`,
`roles/cloudsql.client` and `roles/spanner.databaseUser`, so no further database
setup is required. To impersonate it, your account needs
`roles/iam.serviceAccountTokenCreator` on it. The Reporting Terraform grants
this to every member of `reporting_operators`; a deployment not provisioned by
that Terraform must grant it separately.

Running as a human user instead requires provisioning a separate identity: a
`roles/cloudsql.instanceUser` grant, a `CLOUD_IAM_USER` Cloud SQL user, table
privileges granted by the table owner, and `roles/spanner.databaseUser`.

### Examples

This assumes that you have built the `BackfillBasicReportReportingSets` target,
which outputs to `bazel-bin` by default. For brevity, the examples do not
include the full path to the executable.

*   Reporting what would change, without writing to either database

    ```shell
    BackfillBasicReportReportingSets \
      --spanner-project=halo-cmm-dev \
      --spanner-instance=dev-instance \
      --spanner-database=reporting \
      --postgres-cloud-sql-connection-name=halo-cmm-dev:us-central1:dev-postgres \
      --postgres-database=reporting-v2 \
      --postgres-user=reporting-v2-internal@halo-cmm-dev.iam \
      --create-time-after=2026-06-01T00:00:00Z \
      --dry-run
    ```

*   Applying the backfill

    ```shell
    BackfillBasicReportReportingSets \
      --spanner-project=halo-cmm-dev \
      --spanner-instance=dev-instance \
      --spanner-database=reporting \
      --postgres-cloud-sql-connection-name=halo-cmm-dev:us-central1:dev-postgres \
      --postgres-database=reporting-v2 \
      --postgres-user=reporting-v2-internal@halo-cmm-dev.iam \
      --create-time-after=2026-06-01T00:00:00Z
    ```

### Output

Each run ends with a summary of the `BasicReport`s examined, those already
valid, those updated, any blocking unbackfilled records, the Campaign Groups
repaired or migrated, the `ReportingSet`s reused and created, and any
components or linked results that block migration.

Run with `--dry-run` first. A non-zero blocking count means some component
summaries would remain incomplete or some linked results cannot be migrated;
the per-`BasicReport` reasons are logged and the readiness result is `no`.

Check the reported `create_time` range as well. It covers only the
`BasicReport`s that were backfilled, so it can be compared against the window in
which the affected `BasicReport`s are known to have been created.
