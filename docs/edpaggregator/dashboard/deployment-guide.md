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
    gcloud services enable serviceusage.googleapis.com --project=<DASHBOARD_PROJECT>
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
      --project=<DASHBOARD_PROJECT>
    ```
4.  The Terraform service account has the following project-level roles
    (granted out-of-band — Terraform does not bootstrap its own credentials):
    *   `roles/bigquery.admin` — create datasets, tables, connections,
        scheduled queries, row access policies.
    *   `roles/iam.serviceAccountAdmin` — create per-EDP service accounts and
        the `dashboard-compliance` SA.
    *   `roles/resourcemanager.projectIamAdmin` — grant project-level IAM
        bindings, including the self-grants below.
    *   `roles/iam.roleAdmin` — needed **only to bootstrap the first apply**:
        1.  Grant it to the Terraform SA out-of-band.
        2.  Re-run the `Update CMMS` workflow (or `terraform apply` for a manual
            deploy). Terraform creates the two custom roles
            (`dashboardComplianceChecker` and `terraformDashboardRoleAdmin`) and
            grants itself the scoped `terraformDashboardRoleAdmin`.
        3.  Revoke `roles/iam.roleAdmin`; later applies use the scoped role.

        Skip step 1 and the first apply fails with "Unable to verify whether
        custom project role .../roles/dashboardComplianceChecker already exists
        and must be undeleted."

        **Security note.** After the bootstrap above, the Terraform SA holds
        only the scoped `terraformDashboardRoleAdmin`, which allows it to run the
        `iam.roles.*` verbs but only on the dashboard's two custom roles. It
        can't create or modify any other role in the project.
5.  Proto bundles are registered in Kingdom and Reporting Spanner databases
    (required for `TO_JSON()` decoding).
6.  The BigQuery Connection service agent
    (`service-<PROJECT_NUMBER>@gcp-sa-bigqueryconnection.iam.gserviceaccount.com`)
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
# Use Google Groups for shared/non-dev environments.
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
proto (`src/main/proto/wfa/measurement/config/edpaggregator/dashboard_config.proto`).
The CI step requires **all four** top-level keys to be present — a missing key
fails the deploy rather than silently falling back to a Terraform default:

| Field | Type | Description |
|-------|------|-------------|
| `bigquery_region` | string | BigQuery region hosting the dashboard dataset (e.g. `"us-central1"`). Used by the compliance check and isolation-test matrix. |
| `deletion_protection` | bool | Whether the dashboard BigQuery dataset has deletion protection. Set `false` in dev to allow table recreation; `true` for non-dev environments. |
| `operators` | string[] | IAM principals granted platform-wide access to the dashboard tables (`user:` / `group:`). Use Google Groups for shared/non-dev environments. |
| `edps` | object[] | The EDPs the dashboard exposes; each is isolated to its own rows. |
| `edps[].name` | string | Short EDP name used for per-EDP resource naming (e.g. `"meta"`): SA `edp-<name>-dashboard`, row policy `<name>_filter`, and CI matrix entry. |
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
bq ls --transfer_config --transfer_location=<REGION> --project_id=<DASHBOARD_PROJECT> \
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
FROM `<DASHBOARD_PROJECT>.dashboard.__TABLES__`
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
  --impersonate-service-account=dashboard-compliance@<DASHBOARD_PROJECT>.iam.gserviceaccount.com \
  --project=<DASHBOARD_PROJECT> \
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

The dashboard isolation tests are wired into the `Update CMMS` workflow.
After Terraform deploys and other cloud tests generate data, the workflow:

1.  Triggers all dashboard scheduled queries
2.  Waits for completion
3.  Runs `DashboardIsolationTest` for each EDP: the workflow authenticates as
    the terraform SA via Workload Identity Federation, then impersonates
    `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com` so each check
    runs with only the permissions that EDP itself has
4.  Runs `DashboardComplianceCheck` impersonating
    `dashboard-compliance@<DASHBOARD_PROJECT>.iam.gserviceaccount.com` (the least-privilege
    SA, not the broader operator/terraform SA)

Ensure the GitHub environment has the required variables
(`DASHBOARD_CONFIG_CONTENT`, `GCLOUD_PROJECT`, `TF_SERVICE_ACCOUNT`,
`WORKLOAD_IDENTITY_PROVIDER`).

## Ongoing Operations

### Adding or Removing an EDP

See the EDP Onboarding Guide — *Operator Steps: Onboarding a New EDP* and
*Operator Steps: Offboarding an EDP* — the single source for the EDP lifecycle
(resource-ID lookup, seeding a BasicReport, sharing credentials).

### Updating Dashboard Operators

1.  Update the `operators` array in the `DASHBOARD_CONFIG_CONTENT` GitHub
    environment variable (the single source of truth):
    ```json
    "operators": ["group:new-operators-group@example.com"]
    ```
2.  Re-run the `Update CMMS` deploy workflow to update the platform row access
    policies, table-level IAM, and impersonation grants. (For a manual apply,
    edit `dashboard_operators` in your local `.tfvars` and `terraform apply`
    directly.)

### Monitoring

*   **Scheduled query failures**: Check the BigQuery Data Transfer Service
    console or query `INFORMATION_SCHEMA.JOBS_BY_PROJECT` (requires
    `bigquery.jobs.listAll`) for errors. `INFORMATION_SCHEMA.JOBS` shows only
    the current user's own jobs; scheduled queries run as the transfer
    service account and will not appear there. To also catch transfer- or
    config-level failures that never produce a query job, inspect the
    transfer run history with `bq show --transfer_run`.

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
created. In CI it is set automatically (the deploy workflow downloads the
published jar and passes it). For a manual `terraform apply`, build the jar
yourself and pass it explicitly:

```shell
bazel build //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dashboard/tools:DashboardComplianceCheckFunction_deploy.jar
terraform apply -var-file=envs/dev.tfvars \
  -var="dashboard_compliance_uber_jar_path=/abs/path/to/DashboardComplianceCheckFunction_deploy.jar"
```

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

### EDP sees zero rows

Verify the EDP's `resource_id` in `DASHBOARD_CONFIG_CONTENT` matches the value
`externalIdToApiId` produces for that EDP's internal Spanner ID — cross-check
against `EDPA_EDPS_CONFIG`. It flows into the row access policy filter, so a
wrong value matches no rows. Correct it and re-run the deploy.

### `report_detail_edp is empty` on a freshly-onboarded EDP

The compliance check `report_detail_edp is empty (expected data after
scheduled queries)` and the corresponding isolation test both FAIL until the
new EDP has at least one BasicReport in state `SUCCEEDED` that references one
of its event groups. On fresh environments (and after adding any new EDPA
EDP), you must seed a BasicReport manually — the CI `run-tests` job
only creates reports against simulator event groups (`sim-eg-*` prefix), not
against EDPA-owned event groups. See the *Seed a BasicReport for the new
EDP* step in the onboarding guide.

### `rerun-failed-jobs` doesn't re-trigger scheduled queries

If a new EDP's compliance and isolation checks fail because `report_detail_edp`
hasn't refreshed yet, `rerun-failed-jobs` on `Update CMMS` won't help: the
`trigger-scheduled-queries` job already succeeded (firing the queries succeeds
regardless of their output), so it isn't re-run and the checks re-run against
stale data. Confirm the BasicReport is `SUCCEEDED`, refresh the data (wait for
the next hourly cycle or trigger the query manually — see *Trigger Scheduled
Queries Manually*), then re-run the failed jobs.

### Cross-project connection failures

Verify the BigQuery Connection service agent
(`service-<PROJECT_NUMBER>@gcp-sa-bigqueryconnection.iam.gserviceaccount.com`)
has appropriate grants in the target project:

*   `roles/spanner.databaseReaderWithDataBoost` on each target Spanner database
*   `roles/cloudsql.client` on the project containing the Postgres instance
