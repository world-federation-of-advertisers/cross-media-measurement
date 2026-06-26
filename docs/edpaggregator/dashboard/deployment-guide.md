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
3.  The Terraform service account has the following project-level roles:
    *   `roles/bigquery.admin` (or sufficient permissions to create datasets,
        tables, connections, scheduled queries, and row access policies)
    *   `roles/iam.serviceAccountAdmin` (to create per-EDP service accounts)
4.  Proto bundles are registered in Kingdom and Reporting Spanner databases
    (required for `TO_JSON()` decoding).

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

### Optional Variables

```hcl
# Set to false for dev environments to allow table recreation.
# Defaults to true.
dashboard_deletion_protection = false
```

### GitHub Actions Environment Variables

If using the CI workflow, set the following in your GitHub environment:

| Variable | Description | Example |
|----------|-------------|---------|
| `DASHBOARD_EDP_CONFIG` | JSON array of EDP configs for the CI test matrix | `[{"name":"edp1","resource_id":"AbCdEf_12345"},{"name":"edp2","resource_id":"GhIjKl_67890"}]` |

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
gcloud transfer configs list \
  --project=MY_PROJECT \
  --transfer-location=us-central1 \
  --filter="displayName:Dashboard" \
  --format="value(name)"

# Trigger a manual run for each config
RUN_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ)
gcloud transfer runs create \
  --project=MY_PROJECT \
  --config=CONFIG_NAME \
  --run-time="$RUN_TIME"
```

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

```shell
bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dashboard/tools:DashboardComplianceCheck -- \
  --project=MY_PROJECT \
  --region=us-central1 \
  --edp=edp1:AbCdEf_12345 \
  --edp=edp2:GhIjKl_67890 \
  --edp=edp3:MnOpQr_24680
```

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
4.  Runs `DashboardComplianceCheck` as the Terraform SA (via impersonation)

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
    console or query `INFORMATION_SCHEMA.JOBS` for errors.
*   **Stale data**: The compliance check's staleness threshold is 3 hours. If
    data is older, investigate scheduled query logs.
*   **Row access policy drift**: The compliance check verifies policies exist
    on every run. File a follow-up issue (#3930) to wire the compliance check
    into Cloud Scheduler for daily automated runs.

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
