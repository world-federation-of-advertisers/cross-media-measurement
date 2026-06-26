# EDPA Reporting Dashboard EDP Onboarding Guide

This guide explains how to onboard an Event Data Provider (EDP) to the EDPA
Reporting Dashboard, enabling them to view their own operational data through
BigQuery or Looker Studio.

## Overview

Each EDP gets a dedicated Google Cloud service account that provides access to
3 BigQuery tables. Row access policies ensure each EDP sees only their own data.

### What the EDP Can See

| Table | What It Shows |
|-------|---------------|
| `requisition_overview` | Requisition status, fulfillment times, report state, refusal reasons |
| `mc_details_edp` | Event group details: entity keys, campaigns, brands, media types, data availability |
| `report_detail_edp` | Per-report event group associations: which of their event groups are in each report |

### What the EDP Cannot See

*   Other EDPs' event groups, entity keys, campaigns, or brand names
*   Which other EDPs are in the same report
*   How many other EDPs are in a report (`EdpCount` is platform-only)
*   Platform-only tables (`mc_details`, `report_detail`) return 403
*   Raw Spanner or Postgres data (no `EXTERNAL_QUERY` access)

## Operator Steps: Onboarding a New EDP

### Step 1: Get the EDP's DataProviderResourceId

The `DataProviderResourceId` is the base64url-encoded API resource ID for the
EDP's `DataProvider` in the Kingdom. You can find it by querying the Kingdom
API:

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=v2alpha.kingdom.dev.halo-cmm.org:8443 \
  data-providers \
  list
```

The resource ID is the last segment of the `DataProvider` resource name
(e.g., for `dataProviders/AbCdEf_12345`, the ID is `AbCdEf_12345`).

### Step 2: Add the EDP to Terraform Variables

Add the EDP to `data_provider_resource_ids` in your environment's `.tfvars`
file:

```hcl
data_provider_resource_ids = {
  edp1 = "AbCdEf_12345"
  edp2 = "GhIjKl_67890"
  edp3 = "MnOpQr_24680"
  edp4    = "StUvWx_13579"   # Add new EDP here
}
```

The key (e.g., `edp4`) is a short name used for:

*   The service account name: `edp-edp4-dashboard@PROJECT.iam.gserviceaccount.com`
*   Row access policy IDs: `edp4_filter`
*   CI test matrix entries

### Step 3: Update GitHub Actions Environment

Add the new EDP to the `DASHBOARD_EDP_CONFIG` GitHub environment variable:

```json
[
  {"name": "edp1", "resource_id": "AbCdEf_12345"},
  {"name": "edp2", "resource_id": "GhIjKl_67890"},
  {"name": "edp3", "resource_id": "MnOpQr_24680"},
  {"name": "edp4", "resource_id": "StUvWx_13579"}
]
```

### Step 4: Apply Terraform

```shell
terraform plan -var-file=my-env.tfvars
terraform apply -var-file=my-env.tfvars
```

This creates:

*   Service account `edp-edp4-dashboard@PROJECT.iam.gserviceaccount.com`
*   `roles/bigquery.dataViewer` on `requisition_overview`, `mc_details_edp`,
    `report_detail_edp`
*   `roles/bigquery.jobUser` on the project
*   Row access policies on all 3 tables filtering to the EDP's resource ID

### Step 5: Verify Isolation

Run the compliance check with the new EDP included:

```shell
bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dashboard/tools:DashboardComplianceCheck -- \
  --project=MY_PROJECT \
  --region=us-central1 \
  --edp=edp1:AbCdEf_12345 \
  --edp=edp2:GhIjKl_67890 \
  --edp=edp3:MnOpQr_24680 \
  --edp=edp4:StUvWx_13579
```

Verify all checks pass, including:

*   `edp4: requisition_overview returns only own data`
*   `edp4: correctly denied access to mc_details (403)`
*   `edp4: correctly denied EXTERNAL_QUERY via kingdom-conn (403)`

### Step 6: Share Credentials with the EDP

Provide the EDP with their service account details:

*   **Service account email**:
    `edp-edp4-dashboard@PROJECT.iam.gserviceaccount.com`
*   **Project ID**: The GCP project hosting the dashboard BigQuery dataset
*   **Dataset**: `dashboard`
*   **Accessible tables**: `requisition_overview`, `mc_details_edp`,
    `report_detail_edp`

The EDP authenticates using their service account via Workload Identity
Federation, a service account key, or impersonation from their own GCP project.

## EDP Steps: Accessing the Dashboard

### Option A: Programmatic Access (BigQuery Client)

Use the BigQuery client library with your dashboard service account credentials.

#### Python Example

```python
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'edp-dashboard-key.json',
    scopes=['https://www.googleapis.com/auth/bigquery']
)
client = bigquery.Client(project='DASHBOARD_PROJECT', credentials=credentials)

query = """
SELECT
    Report,
    RequisitionState,
    CmmsCreateTime,
    FulfillmentDurationSeconds
FROM `DASHBOARD_PROJECT.dashboard.requisition_overview`
ORDER BY CmmsCreateTime DESC
LIMIT 100
"""
results = client.query(query).result()
for row in results:
    print(row)
```

#### bq CLI

```shell
# Authenticate as the dashboard service account
gcloud auth activate-service-account \
  edp-edp4-dashboard@PROJECT.iam.gserviceaccount.com \
  --key-file=edp-dashboard-key.json

# Query requisition overview
bq query --project_id=DASHBOARD_PROJECT --nouse_legacy_sql \
  'SELECT * FROM `DASHBOARD_PROJECT.dashboard.requisition_overview` LIMIT 10'

# Query event group details
bq query --project_id=DASHBOARD_PROJECT --nouse_legacy_sql \
  'SELECT CmmsDataProvider, EventGroupCount, EntityTypes, EntityIds, CampaignNames, MediaTypes
   FROM `DASHBOARD_PROJECT.dashboard.mc_details_edp`'

# Query report details
bq query --project_id=DASHBOARD_PROJECT --nouse_legacy_sql \
  'SELECT ExternalReportId, EventGroupCount, CmmsEventGroupIds, EntityTypes
   FROM `DASHBOARD_PROJECT.dashboard.report_detail_edp`'
```

### Option B: Looker Studio

1.  Open [Looker Studio](https://lookerstudio.google.com/) and create a new
    report.
2.  Add a BigQuery data source.
3.  Authenticate as the dashboard service account (or use a service account
    with Looker Studio access).
4.  Select the project, dataset `dashboard`, and one of the accessible tables.
5.  Build visualizations. Row access policies filter automatically — the EDP
    sees only their own data.

### Available Fields

#### `requisition_overview`

| Field | Type | Description |
|-------|------|-------------|
| `DataProviderResourceId` | STRING | Your EDP's API resource ID |
| `Report` | STRING | Report resource name |
| `CmmsMeasurementConsumer` | STRING | Advertiser's API resource ID |
| `RequisitionState` | STRING | STORED, QUEUED, PROCESSING, FULFILLED, REFUSED, WITHDRAWN |
| `RefusalMessage` | STRING | Reason for refusal (if REFUSED) |
| `CmmsCreateTime` | TIMESTAMP | When the requisition was created |
| `FulfilledTime` | TIMESTAMP | When the requisition was fulfilled |
| `FulfillmentDurationSeconds` | INT64 | Time from creation to fulfillment |
| `ReportState` | STRING | Overall report state (CREATED through SUCCEEDED/FAILED) |
| `ReportStartDate` | DATE | Report period start |
| `ReportEndDate` | DATE | Report period end |
| `ImpressionQualificationFilters` | STRING | Impression qualification criteria (JSON) |
| `ReportTitle` | STRING | Human-readable report title |
| `ResultGroupTitles` | STRING | Comma-separated result group titles |
| `ResultGroupMetricFrequencies` | STRING | Comma-separated metric frequencies (weekly/total) |

#### `mc_details_edp`

| Field | Type | Description |
|-------|------|-------------|
| `CmmsMeasurementConsumer` | STRING | Advertiser's API resource ID |
| `CmmsDataProvider` | STRING | Your EDP's API resource ID |
| `EventGroupCount` | INT64 | Number of your event groups for this advertiser |
| `ProvidedEventGroupIds` | ARRAY\<STRING\> | Your event group reference IDs |
| `EntityTypes` | ARRAY\<STRING\> | Entity types in your ad system (e.g., "campaign") |
| `EntityIds` | ARRAY\<STRING\> | Entity IDs in your ad system |
| `CampaignNames` | ARRAY\<STRING\> | Campaign names from event group metadata |
| `BrandNames` | ARRAY\<STRING\> | Brand names from event group metadata |
| `EventTemplates` | ARRAY\<STRING\> | Event template types (JSON) |
| `EntityMetadata` | ARRAY\<STRING\> | Entity-specific metadata (JSON) |
| `MediaTypes` | ARRAY\<STRING\> | Media channels (VIDEO, DISPLAY, OTHER) |
| `AccountIds` | ARRAY\<STRING\> | Client account reference IDs |
| `DataAvailabilityStartTime` | TIMESTAMP | Earliest data availability across your event groups |
| `DataAvailabilityEndTime` | TIMESTAMP | Latest data availability across your event groups |

#### `report_detail_edp`

| Field | Type | Description |
|-------|------|-------------|
| `ExternalReportId` | STRING | Report's external API resource ID |
| `CmmsDataProvider` | STRING | Your EDP's API resource ID |
| `EventGroupCount` | INT64 | Number of your event groups in this report |
| `CmmsEventGroupIds` | ARRAY\<STRING\> | CMMS API IDs for your event groups in this report |
| `CampaignNames` | ARRAY\<STRING\> | Campaign names for your event groups in this report |
| `BrandNames` | ARRAY\<STRING\> | Brand names for your event groups in this report |
| `EntityTypes` | ARRAY\<STRING\> | Entity types for your event groups in this report |
| `EntityIds` | ARRAY\<STRING\> | Entity IDs for your event groups in this report |

## Operator Steps: Offboarding an EDP

1.  Remove the EDP from `data_provider_resource_ids` in `.tfvars` and from
    `DASHBOARD_EDP_CONFIG` in the GitHub environment.
2.  Run `terraform apply`. This destroys the EDP's service account, row access
    policies, and table-level IAM grants.
3.  The EDP's historical data remains in the tables but is inaccessible (no
    row access policy grants visibility, no service account to authenticate).
4.  Run the compliance check to verify the EDP no longer has access.

## Security Notes for EDPs

*   Your service account has `bigquery.jobUser` at the project level, which
    allows you to submit BigQuery queries. You can only read the 3 tables
    granted to you via table-level IAM.
*   Row access policies filter data at the BigQuery engine level. You see only
    rows where the EDP identifier column matches your resource ID.
*   You cannot run `EXTERNAL_QUERY` against any of the underlying Spanner or
    Postgres databases.
*   You cannot access `INFORMATION_SCHEMA` views for the dataset (requires
    dataset-level permissions you do not have).
*   You cannot call UDFs in the dataset (requires dataset-level permissions).
*   Data refreshes hourly via scheduled queries. If data appears stale,
    contact the operator.
