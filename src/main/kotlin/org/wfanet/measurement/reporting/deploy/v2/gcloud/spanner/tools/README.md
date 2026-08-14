# Reporting Spanner CLI Tools

Command-line tools for Reporting operators that act directly on the Reporting
databases.

## `BackfillBasicReportReportingSets`

Backfills
`ResultGroup.MetricMetadata.ReportingUnitComponentSummary.external_reporting_set_id`
on stored `BasicReport`s.

`BasicReport`s written through the internal `InsertBasicReport` method before
that field was validated have it unset. The public read path assembles a
`ReportingSet` resource name from it, so such a `BasicReport` cannot be served
by `GetBasicReport`, and fails the whole page for `ListBasicReports`. See
[issue #4289](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4289).

Run the tool with `--help` for usage information.

### What it does

Every `BasicReport` in state `SUCCEEDED`, across all `MeasurementConsumer`s, is
examined. For each component summary missing `external_reporting_set_id`:

1.  The component's membership is taken from its own `event_group_summaries`,
    paired with the component's `cmms_data_provider_id`.
2.  The Campaign Group's unfiltered primitive children are searched for a
    `ReportingSet` whose membership is exactly that set. If one is found, its ID
    is used.
3.  Otherwise a `ReportingSet` with that membership is created under the
    Campaign Group, and its ID is used.

A `BasicReport` is written only when all of its component summaries resolve. A
component summary with no `event_group_summaries` cannot be resolved, and its
`BasicReport` is left unmodified and reported.

Note that `metric_set.reporting_set_components` entries carry no membership, so
an entry missing `external_reporting_set_id` cannot be backfilled. Any such
entry is counted and reported.

### Database access

The tool reads and writes the Reporting Spanner database, and reads and writes
the Reporting Postgres database. `BasicReport`s are stored in Spanner while
`ReportingSet`s are stored in Postgres, so both must be reachable.

Credentials come from
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).
There is no credentials option: the Spanner client falls back to Application
Default Credentials, and the Postgres connection uses Cloud SQL IAM database
authentication, which resolves them as well.

Run the tool as the Reporting internal service account, by impersonating it:

```shell
gcloud auth application-default login \
  --impersonate-service-account=reporting-internal@PROJECT.iam.gserviceaccount.com
```

That service account owns the Postgres tables and already holds
`roles/cloudsql.instanceUser`, `roles/cloudsql.client` and
`roles/spanner.databaseUser`, so no further database setup is required. To
impersonate it, your account needs `roles/iam.serviceAccountTokenCreator` on
that service account. The Reporting Terraform grants this to every member of
`reporting_operators`.

Pass that same service account to `--postgres-user`, without the
`.gserviceaccount.com` suffix, as that is its Cloud SQL IAM database username.

Running as a human user instead requires provisioning a separate identity: a
`roles/cloudsql.instanceUser` grant, a `CLOUD_IAM_USER` Cloud SQL user, table
privileges granted by the table owner, and `roles/spanner.databaseUser`. None of
that is provisioned by the Reporting Terraform.

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
      --postgres-user=reporting-v2-internal@halo-cmm-dev.iam
    ```

### Output

Each run ends with a summary of the `BasicReport`s examined, those already
valid, those updated, those skipped, the `ReportingSet`s reused and created, and
any components that could not be resolved.

Run with `--dry-run` first. The unresolved counts should both be zero; a
non-zero count means some component summaries would remain unserveable, and the
per-`BasicReport` reasons are logged.
