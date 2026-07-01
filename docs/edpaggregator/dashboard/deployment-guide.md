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

Add the following variables to your environment's `.tfvars` file or GitHub
Actions environment variables.

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
kingdom_spanner_instance   = "halo-cmms"
reporting_spanner_project  = "my-reporting-project"
reporting_spanner_instance = "halo-cmms"
edp_aggregator_spanner_project  = "my-edpa-project"
edp_aggregator_spanner_instance = "halo-cmms"
```

The fourth BigQuery connection — `reporting-postgres-conn` (Cloud SQL
Postgres) — reuses the base CMMS Postgres instance configured by the
Reporting v2 module (`postgres_instance_name` / `postgres_password`, see
prerequisite 2). There are no separate Postgres variables specific to the
dashboard.

### Optional Variables

```hcl
# Set to false for dev environments to allow table recreation.
# Defaults to true.
dashboard_deletion_protection = false
```

### GitHub Actions Environment Variables

If using the CI workflow, set the following in your GitHub environment. The
`terraform-cmms-v2.yml` workflow's "Write dashboard tfvars" step reads
`DATA_PROVIDER_RESOURCE_IDS` and `DASHBOARD_OPERATORS` to generate the
`dashboard.auto.tfvars` Terraform consumes; without them the deploy writes an
empty/invalid tfvars and fails. `DASHBOARD_EDP_CONFIG` is used by the
isolation test matrix; `DASHBOARD_DELETION_PROTECTION` is optional.

| Variable | Required | Description | Example |
|----------|----------|-------------|---------|
| `DATA_PROVIDER_RESOURCE_IDS` | yes | JSON object mapping EDP short name → DataProviderResourceId, consumed by the Terraform `data_provider_resource_ids` map var | `{"edp1":"AbCdEf_12345","edp2":"GhIjKl_67890"}` |
| `DASHBOARD_OPERATORS` | yes | JSON array of `user:` / `group:` IAM members granted operator access (impersonation, platform-table reads) | `["group:edpa-dashboard-operators@example.com"]` |
| `DASHBOARD_EDP_CONFIG` | yes | JSON array of EDP configs for the CI isolation test matrix | `[{"name":"edp1","resource_id":"AbCdEf_12345"},{"name":"edp2","resource_id":"GhIjKl_67890"}]` |
| `DASHBOARD_DELETION_PROTECTION` | no (default `true`) | Set `"false"` in dev to allow table recreation | `"false"` |

Pre-existing CMMS environment variables (`GOOGLE_CLOUD_PROJECT`,
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

### Trigger Scheduled Queries Manually

```shell
# List dashboard transfer configs
bq ls --transfer_config --transfer_location=us-central1 --project_id=MY_PROJECT \
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
  --region=us-central1 \
  --edp=edp1:AbCdEf_12345 \
  --edp=edp2:GhIjKl_67890 \
  --edp=edp3:MnOpQr_24680
```

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
3.  Runs `DashboardIsolationTest` as each EDP (via Workload Identity Federation)
4.  Runs `DashboardComplianceCheck` impersonating
    `dashboard-compliance@PROJECT.iam.gserviceaccount.com` (the least-privilege
    SA, not the broader operator/terraform SA)

Ensure the GitHub environment has the required variables (`DASHBOARD_EDP_CONFIG`,
`GOOGLE_CLOUD_PROJECT`, `BIGQUERY_REGION`, `TF_SERVICE_ACCOUNT`,
`WORKLOAD_IDENTITY_PROVIDER`).

## Ongoing Operations

### Adding a New EDP

1.  Add the EDP to `data_provider_resource_ids` in your `.tfvars`:
    ```hcl
    data_provider_resource_ids = {
      edp1 = "AbCdEf_12345"
      edp2 = "GhIjKl_67890"
      edp3 = "MnOpQr_24680"
      edp4    = "StUvWx_13579"   # New EDP
    }
    ```
2.  Update `DASHBOARD_EDP_CONFIG` in the GitHub environment to include the new
    EDP.
3.  Run `terraform apply`. This creates the new EDP's service account, row
    access policies, and table-level IAM grants.
4.  Run the compliance check to verify the new EDP's isolation.

### Removing an EDP

1.  Remove the EDP from `data_provider_resource_ids` and
    `DASHBOARD_EDP_CONFIG`.
2.  Run `terraform apply`. This destroys the EDP's service account, row access
    policies, and IAM grants.
3.  The EDP's historical data remains in the tables but is no longer accessible
    (no row access policy grants visibility).

### Updating Dashboard Operators

1.  Update `dashboard_operators` in your `.tfvars`:
    ```hcl
    dashboard_operators = ["group:new-operators-group@example.com"]
    ```
2.  Run `terraform apply`. This updates row access policies and impersonation
    grants.

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
    on every run. Automated daily runs via Cloud Scheduler are tracked in
    #3930.

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

Verify the EDP's `DataProviderResourceId` in `data_provider_resource_ids`
matches the value produced by `externalIdToApiId` for that EDP's internal
Spanner ID. A mismatch causes the row access policy filter to not match any
rows.

### Cross-project connection failures

Verify the BigQuery Connection service agent
(`service-{project_number}@gcp-sa-bigqueryconnection.iam.gserviceaccount.com`)
has appropriate grants in the target project:

*   `roles/spanner.databaseReaderWithDataBoost` on each target Spanner database
*   `roles/cloudsql.client` on the project containing the Postgres instance
