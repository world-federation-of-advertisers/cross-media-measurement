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
examined, one `MeasurementConsumer` at a time and a page at a time. Use
`--create-time-after` to restrict this to `BasicReport`s created after a given
time, which both bounds the work and makes it impossible to modify a
`BasicReport` outside that window.

For each component summary missing `external_reporting_set_id`:

1.  The component's membership is taken from its own `event_group_summaries`,
    paired with the component's `cmms_data_provider_id`.
2.  The Campaign Group's unfiltered primitive children are searched for a
    `ReportingSet` whose membership is exactly that set. If one is found, its ID
    is used.
3.  Otherwise a `ReportingSet` with that membership is created under the
    Campaign Group, and its ID is used.

Neither database is written for a `BasicReport` unless all of its component
summaries resolve. Every membership is resolved before any `ReportingSet` is
created, so a skipped `BasicReport` leaves no `ReportingSet` behind. A component
summary with no `event_group_summaries` cannot be resolved, and its
`BasicReport` is left unmodified and reported.

Note that `metric_set.reporting_set_components` entries carry no membership, so
an entry missing `external_reporting_set_id` cannot be backfilled. Any such
entry is counted and reported.

### After the backfill

The public read path tolerates a `BasicReport` whose component summaries still
lack `external_reporting_set_id`: it omits `reporting_set` rather than failing,
so an incomplete resource is served without any error. A run of this tool that
misses records therefore produces no visible symptom.

Each such `BasicReport` is logged once per read, at `WARNING`, by the Reporting
public API server:

```
BasicReport <id> of MeasurementConsumer <id> has N component summary/summaries
without external_reporting_set_id.
```

That warning is the completion signal. It going quiet means every affected
`BasicReport` has been repaired and no new ones are being written. It recurring
after a completed run means records are still being written without the field,
by a path that the `InsertBasicReport` validation does not cover.

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
valid, those updated, those skipped, the `ReportingSet`s reused and created, and
any components that could not be resolved.

Run with `--dry-run` first. The unresolved counts should both be zero; a
non-zero count means some component summaries would remain unserveable, and the
per-`BasicReport` reasons are logged.

Check the reported `create_time` range as well. It covers only the
`BasicReport`s that were backfilled, so it can be compared against the window in
which the affected `BasicReport`s are known to have been created.
