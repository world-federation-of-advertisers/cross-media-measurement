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

For a report that already references a valid Campaign Group, each blank
component is resolved from its own `event_group_summaries`:

1.  If `cmms_data_provider_id` is populated, the tool pairs it with every
    EventGroup ID. If it is blank, the DataProvider is inferred only when the
    Campaign Group universe has exactly one matching EventGroup key.
2.  Every resolved EventGroup key must belong to the Campaign Group universe.
3.  The tool reuses an unfiltered primitive ReportingSet under the Campaign
    Group when its membership is an exact match. Otherwise it creates one.

For a report whose Campaign Group reference is instead a non-empty ordinary
primitive ReportingSet with no `CampaignGroupId`, the tool treats that
ReportingSet's EventGroups as the proposed universe. It first validates every
component against that universe. Only when all components resolve does it:

1.  reuse or create an equivalent non-empty primitive, self-linked Campaign
    Group with exactly the same EventGroups;
2.  change `BasicReport.external_campaign_group_id` to that valid Campaign
    Group; and
3.  re-resolve every component summary under the valid Campaign Group,
    including components that already contained an ID.

The original ordinary ReportingSet is never modified. The Campaign Group
change and all component IDs are written to the `BasicReport` in one Spanner
mutation. Creating or finding the replacement Campaign Group in Postgres is
idempotent.

The tool does not guess. A missing, empty, composite, or already-assigned
ReportingSet is left unchanged. A report is also left unchanged when a
component has no EventGroups, references an EventGroup outside the proposed
universe, or has a blank DataProvider that cannot be inferred uniquely. These
rows are counted as blocking, the scan continues, and the environment is
reported as not ready to remove the read fallback.

No Campaign Group or component ReportingSet is created and no `BasicReport`
is updated unless every component summary that needs resolution can be
resolved. If every component already has `external_reporting_set_id`, the
report is left untouched.

Entries in `metric_set.reporting_set_components` carry no membership, so an
entry missing `external_reporting_set_id` cannot be backfilled. Such entries
are counted and reported.

### After the backfill

The public read path temporarily tolerates a `BasicReport` whose component
summaries still lack `external_reporting_set_id`: it omits `reporting_set`
rather than failing, so an incomplete resource is served without any error.

An applied, unbounded run reports `Ready to remove read-path fallback: yes`
only when every examined `SUCCEEDED` row has complete component summaries. A
bounded run never reports ready because it does not examine older rows. A dry
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
components that could not be resolved.

Run with `--dry-run` first. A non-zero blocking count means some component
summaries would remain incomplete; the per-`BasicReport` reasons are logged and
the readiness result is `no`.

Check the reported `create_time` range as well. It covers only the
`BasicReport`s that were backfilled, so it can be compared against the window in
which the affected `BasicReport`s are known to have been created.
