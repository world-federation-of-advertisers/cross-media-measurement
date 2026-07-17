# EDPA Reporting Dashboard Deployment Guide

The EDPA Reporting Dashboard provides BigQuery-based operational monitoring for
EDP Aggregator deployments. It materializes data from Kingdom, Reporting, and
EDP Aggregator databases into BigQuery tables with per-EDP row-level isolation.

## Architecture Overview

The dashboard creates the following resources:

*   **1 BigQuery dataset** (`dashboard`) containing 5 materialized tables
*   **4 BigQuery connections** (3 Spanner with Data Boost, 1 Cloud SQL Postgres)
*   **1 UDF** (`externalIdToApiId`) for internal-to-API ID conversion
*   **5 hourly scheduled queries** using atomic MERGE to populate tables
*   **Per-EDP service accounts** with table-level IAM and row access policies
*   **Compliance check CLI** for ongoing security auditing

### Tables

| Table | Audience | Description |
|-------|----------|-------------|
| `requisition_overview` | Shared (row-filtered) | Requisition status, fulfillment times, report state |
| `mc_details` | Platform only | Event group details with cross-EDP coverage metrics |
| `mc_details_edp` | Per-EDP | Event group details (own data only) |
| `report_detail` | Platform only | Per-report event group associations with EDP count |
| `report_detail_edp` | Per-EDP | Per-report event group associations (own data only) |

### Security Model

*   **Table-level IAM**: EDP service accounts have `dataViewer` on 3 tables
    only (`requisition_overview`, `mc_details_edp`, `report_detail_edp`).
    Platform-only tables return 403.
*   **Row access policies**: Each EDP sees only rows matching their
    `DataProviderResourceId` or `CmmsDataProvider`. Policies survive scheduled
    query runs (MERGE is atomic and does not recreate the table).
*   **Connection isolation**: Only the Terraform service account has
    `connectionUser` on BigQuery connections. EDP service accounts cannot run
    `EXTERNAL_QUERY` against any Spanner or Postgres database.
*   **Spanner-side decoding**: Proto fields are decoded inside Spanner via
    `TO_JSON()`. Sensitive fields (e.g., `data_provider_keys` in
    `resultGroupSpecs`) never cross the connection boundary.

## Prerequisites

Before deploying the dashboard, ensure the following are in place:

1.  The CMMS Terraform configuration is deployed (Kingdom, Reporting, EDP
    Aggregator Spanner databases exist).
2.  The Reporting v2 Postgres database is deployed and accessible.
3.  The **Service Usage API** (`serviceusage.googleapis.com`) is enabled on
    the project. This is a one-time bootstrap: nothing else can enable other
    APIs (or force-provision service agents) until this is on. Requires a
    human with `roles/serviceusage.serviceUsageAdmin` (or Owner):
    ```shell
    gcloud services enable serviceusage.googleapis.com --project=MY_PROJECT
    ```
    If you see `ERROR: (gcloud.beta.services.identity.create) PERMISSION_DENIED:
    Service Usage API has not been used in project ... before or it is disabled`
    on the first `terraform apply`, this is the fix.

    Once Service Usage is on, enable the two BigQuery APIs the dashboard uses
    that are not on by default. Otherwise `terraform apply` fails with
    `Error creating Connection: googleapi: Error 403: BigQuery Connection API
    has not been used in project ... before or it is disabled` (for
    `google_bigquery_connection` resources) or the equivalent 403 on
    scheduled-query creation:
    ```shell
    gcloud services enable \
      bigqueryconnection.googleapis.com \
      bigquerydatatransfer.googleapis.com \
      --project=MY_PROJECT
    ```
4.  The Terraform service account has the following project-level roles
    (granted out-of-band — Terraform does not bootstrap its own credentials):
    *   `roles/bigquery.admin` — create datasets, tables, connections,
        scheduled queries, row access policies.
    *   `roles/iam.serviceAccountAdmin` — create per-EDP service accounts and
        the `dashboard-compliance` SA.
    *   `roles/resourcemanager.projectIamAdmin` — grant project-level IAM
        bindings, including the self-grants below.
    *   `roles/iam.roleAdmin` — required *only* for the very first apply, to
        create the `dashboardComplianceChecker` custom project role. The
        Terraform code adds a `terraform_role_admin` self-grant so subsequent
        applies maintain it automatically. Without this on the first apply,
        Terraform fails with: "Unable to verify whether custom project role
        projects/<project>/roles/dashboardComplianceChecker already exists
        and must be undeleted."

        **Security note for adopters.** The reference Terraform grants
        project-wide `roles/iam.roleAdmin` because there is no built-in
        Google-managed role that covers `iam.roles.*` scoped to a single
        custom role. This lets the Terraform SA create or modify *any*
        custom role in the project. For your own deployments, prefer one of:
        (a) grant `roles/iam.roleAdmin` only for the first apply, then revoke
        and let subsequent applies maintain the role via the self-grant —
        works if you can accept one-shot bootstrap privilege;
        (b) replace `terraform_role_admin` in `dashboard.tf` with a project
        custom role that bundles only
        `iam.roles.{get,list,create,update,undelete,delete}`, granted via
        `google_project_iam_member` with an IAM condition restricting the
        resource to
        `resource.name.startsWith("projects/<project>/roles/dashboardComplianceChecker")`.
        The scoped-role refactor is tracked in
        [#4135](https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/4135).
        The dev/head/qa reference environments accept the broader grant
        because their blast radius is bounded to test data and audit-logged
        CI; production deployments should not follow this pattern without
        one of the above narrowings.
5.  Proto bundles are registered in Kingdom and Reporting Spanner databases
    (required for `TO_JSON()` decoding).
6.  The BigQuery Connection service agent
    (`service-${project_number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com`)
    exists. GCP creates this service-managed agent on demand the first time
    a project uses the BigQuery Connection API — but on-demand creation
    happens too late for Terraform, which tries to grant IAM to the agent in
    the same apply. Terraform force-provisions it via a `terraform_data`
    resource that shells out to `gcloud beta services identity create`; this
    is a one-time bootstrap and is idempotent on subsequent applies. Requires
    the Service Usage API from step 3.

## Step 1: Configure Terraform Variables

Dashboard configuration has two homes depending on how you deploy:

*   **CI deploys** — the dashboard's EDPs, operators, region, and deletion
    protection all live in the single `DASHBOARD_CONFIG_CONTENT` GitHub Actions
    variable (see *GitHub Actions Environment Variables* below); the workflow
    generates the Terraform vars from it.
*   **Manual `terraform apply`** — provide them as the HCL variables in your
    `.tfvars` file — see *Required Variables* and *Optional Variables* below.

### Required Variables

```hcl
# Map of EDP short name to their DataProviderResourceId (API resource ID).
# Must match the EDPs registered in the Kingdom.
data_provider_resource_ids = {
  edp1 = "AbCdEf_12345"
  edp2 = "GhIjKl_67890"
  edp3 = "MnOpQr_24680"
}

# Users or groups granted full platform access to all dashboard tables
# and the ability to impersonate EDP service accounts for testing.
# Use Google Groups for production deployments.
dashboard_operators = ["group:edpa-dashboard-operators@example.com"]

# Spanner project and instance for each database connection.
# In a single-project setup, all can point to the same project/instance.
kingdom_spanner_project    = "my-kingdom-project"
kingdom_spanner_instance   = "my-spanner-instance"
reporting_spanner_project  = "my-reporting-project"
reporting_spanner_instance = "my-spanner-instance"
edp_aggregator_spanner_project  = "my-edpa-project"
edp_aggregator_spanner_instance = "my-spanner-instance"
```

The fourth BigQuery connection — `reporting-postgres-conn` (Cloud SQL
Postgres) — reuses the base CMMS Postgres instance configured by the
Reporting v2 module (`postgres_instance_name` / `postgres_password`, see
prerequisite 2). There are no separate Postgres variables specific to the
dashboard.

### Optional Variables

`dashboard_deletion_protection` defaults to `true`. On the CI path it comes from
the `deletion_protection` field of `DASHBOARD_CONFIG_CONTENT`. The
`envs/*.tfvars` files no longer set it, so a manual `terraform apply` now gets
`true` — pass `false` (e.g. for dev/head) to allow table recreation:

```shell
terraform apply -var-file=envs/dev.tfvars -var=dashboard_deletion_protection=false
```

### GitHub Actions Environment Variables

If using the CI workflow, set the following in your GitHub environment. The
`terraform-cmms-v2.yml` workflow's "Write dashboard tfvars" step reads the
single `DASHBOARD_CONFIG_CONTENT` JSON var (per the `DashboardConfig` schema)
to generate the `dashboard.auto.tfvars.json` Terraform consumes and to build
the isolation-test matrix; without it the deploy fails.

| Variable | Required | Description |
|----------|----------|-------------|
| `DASHBOARD_CONFIG_CONTENT` | yes | Single JSON object per the `DashboardConfig` proto. Consumed by Terraform (as `*.tfvars.json`), the compliance check, and the isolation-test matrix. See the field reference below. |

`DASHBOARD_CONFIG_CONTENT` is one JSON object matching the `DashboardConfig`
proto (src/main/proto/wfa/measurement/config/edpaggregator/dashboard_config.proto).
The CI step requires **all four** top-level keys to be present — a missing key
fails the deploy rather than silently falling back to a Terraform default:

| Field | Type | Description |
|-------|------|-------------|
| `bigquery_region` | string | BigQuery region hosting the dashboard dataset (e.g. `"us-central1"`). Used by the compliance check and isolation-test matrix. |
| `deletion_protection` | bool | Whether the dashboard BigQuery dataset has deletion protection. Set `false` in dev to allow table recreation; `true` in production. |
| `operators` | string[] | IAM principals granted platform-wide access to the dashboard tables (`user:` / `group:`). Use Google Groups in production. |
| `edps` | object[] | The EDPs the dashboard exposes; each is isolated to its own rows. |
| `edps[].name` | string | Short EDP name used for per-EDP resource naming (e.g. `"meta"`): SA `edp-<name>-dashboard@`, row policy `<name>_filter`, and CI matrix entry. |
| `edps[].resource_id` | string | The **bare** `DataProvider` resource ID — final path segment only (e.g. `"J3-pzhqS9Lo"`), **not** the full `dataProviders/...` name. Must match the value in Spanner / `EDPA_EDPS_CONFIG`. |

Example (pretty-printed here; store it as compact JSON in the variable):

```json
{
  "bigquery_region": "us-central1",
  "deletion_protection": false,
  "operators": ["group:edpa-dashboard-operators@example.com"],
  "edps": [
    {"name": "edp1", "resource_id": "AbCdEf_12345"},
    {"name": "edp2", "resource_id": "GhIjKl_67890"}
  ]
}
```

Pre-existing CMMS environment variables (`GCLOUD_PROJECT`,
`SPANNER_INSTANCE`, `POSTGRES_INSTANCE_NAME`, `POSTGRES_PASSWORD`) are
already consumed by the broader workflow and do not need to be re-added for
the dashboard.

## Step 2: Deploy with Terraform

Apply the Terraform configuration. The dashboard resources are defined in
`dashboard.tf` and deployed alongside the rest of the CMMS infrastructure.

```shell
terraform plan -var-file=my-env.tfvars
terraform apply -var-file=my-env.tfvars
```

Terraform creates all resources in this order:

1.  BigQuery dataset and connections
2.  UDF and table schemas
3.  Row access policies and table-level IAM
4.  Per-EDP service accounts with `jobUser` and `dataViewer` grants
5.  Scheduled query transfer configs (hourly MERGE)
6.  Spanner and Cloud SQL IAM grants for connection service agents

## Step 3: Verify Initial Deployment

After Terraform applies successfully, the tables are empty. The scheduled
queries run hourly via MERGE. To populate tables immediately:

`<REGION>` in the examples below is the GCP region the dashboard resources
are deployed in — the CI reads it from the `bigquery_region` field of
`DASHBOARD_CONFIG_CONTENT`. All dashboard tables, connections, and scheduled
queries are created via `data.google_client_config.default.region` in
`dashboard.tf`, so any GCP region GCP supports for BigQuery Data Transfer
works.

### Trigger Scheduled Queries Manually

```shell
# List dashboard transfer configs
bq ls --transfer_config --transfer_location=<REGION> --project_id=MY_PROJECT \
  --filter='dataSourceIds:scheduled_query'

# Trigger a manual run for each config. CONFIG_RESOURCE_NAME is the
# `projects/.../locations/.../transferConfigs/...` value from `bq ls`.
RUN_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
bq mk --transfer_run --run_time="$RUN_TIME" CONFIG_RESOURCE_NAME
```

`gcloud transfer` is the *Storage* Transfer Service and does not manage
BigQuery scheduled queries — use `bq` as shown above.

### Verify Tables Are Populated

```sql
SELECT table_id, row_count, TIMESTAMP_MILLIS(last_modified_time) AS last_modified
FROM `MY_PROJECT.dashboard.__TABLES__`
ORDER BY table_id;
```

All 5 tables should have rows after the scheduled queries complete.

## Step 4: Run the Compliance Check

The `DashboardComplianceCheck` CLI verifies the security posture of the
deployed dashboard. Run it after initial deployment and periodically thereafter.
Impersonate the least-privilege `dashboard-compliance` service account (created
by Terraform in `dashboard.tf`) rather than running as a human user or the
broader operator SA — the dedicated SA's `dashboardComplianceChecker` custom
role grants only what the check needs.

```shell
bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dashboard/tools:DashboardComplianceCheck -- \
  --impersonate-service-account=dashboard-compliance@MY_PROJECT.iam.gserviceaccount.com \
  --project=MY_PROJECT \
  --dashboard-config=/path/to/dashboard-config.json
```

where `dashboard-config.json` holds the environment's `DASHBOARD_CONFIG_CONTENT`
(the EDPs and region are read from it).

To impersonate, your account needs `roles/iam.serviceAccountTokenCreator` on
the `dashboard-compliance` SA. The dashboard Terraform grants this to every
member of `dashboard_operators`.

The compliance check verifies:

*   **Data isolation**: Each EDP sees only their own rows
*   **IAM boundary**: EDP SAs cannot access platform-only tables (403)
*   **EXTERNAL_QUERY bypass**: EDP SAs cannot run queries against Spanner/Postgres
    connections (403)
*   **UDF validation**: `externalIdToApiId` produces valid output
*   **Drift detection**: Expected tables, UDFs, schemas, row access policies,
    and connections all exist with correct configuration
*   **Data correctness**: Tables are non-empty and data is fresh (< 3 hours old)

## Step 5: Set Up CI Integration

The dashboard isolation tests are wired into the `update-cmms.yml` workflow.
After Terraform deploys and other cloud tests generate data, the workflow:

1.  Triggers all dashboard scheduled queries
2.  Waits for completion
3.  Runs `DashboardIsolationTest` for each EDP: the workflow authenticates as the terraform SA via Workload Identity Federation, then impersonates `edp-<name>-dashboard@PROJECT.iam.gserviceaccount.com` (chained through `dashboard-compliance@PROJECT.iam.gserviceaccount.com`) so each check runs with only the permissions that EDP would have in production
4.  Runs `DashboardComplianceCheck` impersonating
    `dashboard-compliance@PROJECT.iam.gserviceaccount.com` (the least-privilege
    SA, not the broader operator/terraform SA)

Ensure the GitHub environment has the required variables
(`DASHBOARD_CONFIG_CONTENT`, `GCLOUD_PROJECT`, `TF_SERVICE_ACCOUNT`,
`WORKLOAD_IDENTITY_PROVIDER`).

## Ongoing Operations

### Adding a New EDP

1.  Add the EDP to the `edps` array in the `DASHBOARD_CONFIG_CONTENT` GitHub
    environment variable (the single source of truth):
    ```json
    {"name": "edp4", "resource_id": "StUvWx_13579"}
    ```
2.  Re-run the `Update CMMS` deploy workflow. The "Write dashboard tfvars" step regenerates
    `data_provider_resource_ids` from `DASHBOARD_CONFIG_CONTENT`, then
    `terraform apply` creates the new EDP's service account, row access
    policies, and table-level IAM grants.
3.  Run the compliance check to verify the new EDP's isolation.

### Removing an EDP

1.  Remove the EDP from the `edps` array in `DASHBOARD_CONFIG_CONTENT`.
2.  Re-run the `Update CMMS` deploy workflow (or `terraform apply` for a manual deploy).
    This destroys the EDP's service account, row access policies, and IAM
    grants.
3.  The EDP's historical data remains in the tables but is no longer accessible
    (no row access policy grants visibility).

### Updating Dashboard Operators

1.  Update the `operators` array in the `DASHBOARD_CONFIG_CONTENT` GitHub
    environment variable (the single source of truth):
    ```json
    "operators": ["group:new-operators-group@example.com"]
    ```
2.  Re-run the `Update CMMS` deploy workflow. The "Write dashboard tfvars" step regenerates
    `dashboard_operators` from `DASHBOARD_CONFIG_CONTENT`, then `terraform
    apply` updates the row access policies and impersonation grants. (For a
    manual apply, edit `dashboard_operators` in your `.tfvars` instead.)

### Monitoring

*   **Scheduled query failures**: Check the BigQuery Data Transfer Service
    console or query `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (requires
    `bigquery.jobs.listAll`) for errors. `INFORMATION_SCHEMA.JOBS` shows only
    the current user's own jobs; scheduled queries run as the transfer
    service account and will not appear there. To also catch transfer- or
    config-level failures that never produce a query job, inspect the
    transfer run history with `bq show --transfer_run`.
*   **Stale data**: The compliance check's staleness threshold is 3 hours. If
    data is older, investigate scheduled query logs.
*   **Row access policy drift**: The compliance check verifies policies exist
    on every run.

### Scheduled compliance check

Beyond the post-`terraform apply` CI check, a Cloud Function
(`dashboard-compliance-check`) runs the same checks daily at 06:00 UTC,
triggered by Cloud Scheduler over OIDC. It runs independently against the
*live* state to catch drift between deploys (manual IAM changes, Terraform
state drift) that a redeploy would silently re-reconcile.

Failures surface three ways: each failed check is logged at `SEVERE` with an
`ALERT:` prefix in the `dashboard-compliance-check` Cloud Function logs; the
number of failed checks is recorded to the
`edpa.dashboard_compliance.failed_checks` Cloud Monitoring metric; and the
"Dashboard Compliance Check Failures" alert policy fires when that metric is
greater than 0. Attach Slack/PagerDuty channels via the
`dashboard_alert_notification_channels` Terraform variable — empty creates the
policy without notifications, so no one is paged until a channel is attached.

The scheduled deploy is gated on `dashboard_compliance_uber_jar_path`: when
that variable is unset, the Cloud Function, scheduler, and alert policy are not
created.

## Troubleshooting

### Tables are empty after deployment

The scheduled queries run hourly. Either wait for the next run or trigger
manually (see Step 3). If tables remain empty after a triggered run, check:

*   The Terraform SA has `connectionUser` on all 4 connections.
*   The BigQuery Connection service agent has `databaseReaderWithDataBoost`
    on all 3 Spanner databases and `cloudsql.client` on the project.
*   The Spanner proto bundles are registered (required for `TO_JSON()`).

### Row access policies missing

If the compliance check reports missing policies, run `terraform apply` to
re-create them. MERGE-based scheduled queries preserve policies across runs.
If you previously used `WRITE_TRUNCATE`, policies were dropped on each run
and must be re-applied.

### EDP sees zero rows

Verify the EDP's `resource_id` in `DASHBOARD_CONFIG_CONTENT` matches the
value produced by `externalIdToApiId` for that EDP's internal Spanner ID (it
flows into the generated `data_provider_resource_ids` and the row access policy
filter). A mismatch causes the filter to not match any rows.

Since `DASHBOARD_CONFIG_CONTENT` is the single source for both the Terraform
vars and the compliance/isolation tools, a stale or wrong `resource_id` shows
up as a `returns other EDPs' data` false-positive OR a `report_detail_edp is
empty` false-negative for that EDP. Fix by aligning the EDP's `resource_id` in
`DASHBOARD_CONFIG_CONTENT` with the value in `EDPA_EDPS_CONFIG` and re-running
the deploy.

### `report_detail_edp is empty` on a freshly-onboarded EDP

The compliance check `report_detail_edp is empty (expected data after
scheduled queries)` and the corresponding isolation test both FAIL until the
new EDP has at least one BasicReport in state `SUCCEEDED` that references one
of its event groups. On fresh environments (and after adding any new EDPA
EDP), you must seed a BasicReport manually &mdash; the CI `run-tests` job
only creates reports against simulator event groups (`sim-eg-*` prefix), not
against EDPA-owned event groups. See the *Seed a BasicReport for the new
EDP* step in the onboarding guide.

### `rerun-failed-jobs` doesn't re-trigger scheduled queries

`rerun-failed-jobs` on `Update CMMS` only re-runs jobs whose prior attempt
was `failure`. The `run-dashboard-isolation-test / trigger-scheduled-queries`
job typically succeeds on its first run (kicking the queries off is
independent of whether the queries return data), so subsequent
`rerun-failed-jobs` calls preserve it as `success` and **do not re-fire the
scheduled queries**. If the compliance/isolation checks are failing because
`report_detail_edp` hasn't yet been refreshed to include a new BasicReport:

1.  Verify the BasicReport is `state: SUCCEEDED` (via the Reporting API).
2.  Check `bq show --format=prettyjson <project>:dashboard.report_detail_edp
    | jq '{numRows, lastModifiedTime}'` &mdash; if `lastModifiedTime` is
    before the BasicReport completed, the query hasn't cycled yet.
3.  Wait for the natural hourly cycle, OR trigger the query manually via the
    BigQuery Data Transfer REST API:
    ```shell
    curl -sS -X POST -H "Authorization: Bearer $(gcloud auth print-access-token)" \
      -H 'Content-Type: application/json' \
      --data "{\"requestedRunTime\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"}" \
      "https://bigquerydatatransfer.googleapis.com/v1/projects/<project>/locations/<region>/transferConfigs/<config-id>:startManualRuns"
    ```
    (You need `roles/bigquerydatatransfer.admin` on the project; find
    `<config-id>` via `bq ls --transfer_config --transfer_location=<region> --project_id=<project> --filter='dataSourceIds:scheduled_query'`.)
4.  Once `report_detail_edp` has been refreshed, `gh api
    .../actions/runs/<RUN_ID>/rerun-failed-jobs --method POST` will re-run
    the remaining failing dashboard checks against the updated data.

### Cross-project connection failures

Verify the BigQuery Connection service agent
(`service-{project_number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com`)
has appropriate grants in the target project:

*   `roles/spanner.databaseReaderWithDataBoost` on each target Spanner database
*   `roles/cloudsql.client` on the project containing the Postgres instance
