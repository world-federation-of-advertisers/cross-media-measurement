# EDPA Reporting Dashboard EDP Onboarding Guide

This guide explains how to onboard an Event Data Provider (EDP) to the EDPA
Reporting Dashboard, enabling them to view their own operational data through
BigQuery or Looker Studio.

## Overview

Each EDP gets a dedicated Google Cloud service account that provides access to
3 BigQuery tables. Row access policies ensure each EDP sees only their own data.

Deployment-specific values appear as placeholders throughout this guide:

*   `<KINGDOM_PUBLIC_API_TARGET>` — the Kingdom public API host:port
*   `<REPORTING_API_HOST>` — the Reporting public API host
*   `<REPORTING_API_AUDIENCE>` / `<REPORTING_API_ISSUER>` /
    `<REPORTING_API_JWKS_KEYSET>` — the audience / OIDC issuer / signing
    keyset configured in the Reporting server's `OpenIdProvidersConfig`

Substitute the values for your deployment. The reference `halo-cmm.org`
environments follow the pattern `v2alpha.kingdom.<env>.halo-cmm.org:8443` /
`v2alpha.reporting.<env>.halo-cmm.org` / `reporting.<env>.halo-cmm.org` /
`https://auth.halo-cmm.local` and the checked-in
`src/main/k8s/testing/secretfiles/open_id_provider.tink` keyset. An adopter
running their own CMMS deployment substitutes their own hosts, audience,
issuer, and JWKS keyset — the values come from the deployment's Kingdom
`v2alpha` and Reporting `v2alpha` public API configuration and the Reporting
server's `OpenIdProvidersConfig`.

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
EDP's `DataProvider` in the Kingdom. The `v2alpha` `DataProvidersService`
does not expose `ListDataProviders`, so look it up from the source of truth
where it's already recorded:

*   **`EDPA_EDPS_CONFIG` GitHub environment variable** — each entry has a
    `data_provider: "dataProviders/<id>"` field. The portion after the slash
    is the resource ID.
*   The EDP's registration record (whatever you used during the initial
    `DataProvider` creation).

If the full resource name is already known, you can confirm the EDP exists
with:

```shell
MeasurementSystem \
  --tls-cert-file=secretfiles/mc_tls.pem \
  --tls-key-file=secretfiles/mc_tls.key \
  --cert-collection-file=secretfiles/kingdom_root.pem \
  --kingdom-public-api-target=<KINGDOM_PUBLIC_API_TARGET> \
  data-providers \
  get --name=dataProviders/AbCdEf_12345
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

**Critical: `DASHBOARD_EDP_CONFIG` and `data_provider_resource_ids` MUST
match** on both keys AND resource IDs. Terraform's `for_each` over
`data_provider_resource_ids` creates the `edp-<key>-dashboard@` SAs and row
access policies (filtered by resource ID). The compliance and isolation tools
iterate `DASHBOARD_EDP_CONFIG` to decide which `edp-<name>-dashboard@` SA to
impersonate and which resource ID's data to expect. If the two disagree you
get one of two failure modes:

*   **Keys don't match** &rarr; tool tries to impersonate an SA terraform
    never created &rarr; `404 Not Found; Gaia id not found for email
    edp-<name>-dashboard@PROJECT.iam.gserviceaccount.com`.
*   **Resource IDs don't match** &rarr; SA authenticates fine but its row
    access policy filters everything out &rarr; the compliance check reports
    `returns other EDPs' data` (false-positive leak) or `empty` (false
    negative).

The common failure mode is a copy-paste from another environment's
`data_provider_resource_ids` &mdash; the keys look right but the resource IDs
are wrong for the target environment. Cross-check against the environment's
`EDPA_EDPS_CONFIG` (the proto that drives the actual EDPA pipeline) as the
source of truth for resource IDs.

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

### Step 5: Seed a BasicReport for the new EDP

The dashboard's `report_detail_edp` scheduled query only populates a row for an
EDP when at least one BasicReport in state `SUCCEEDED` references one of that
EDP's event groups. On a fresh EDP with no reports yet, the compliance check
`report_detail_edp is empty` and the isolation test both FAIL &mdash; a
false-negative that will keep failing every deploy until at least one
BasicReport exists for the EDP.

The CI `run-tests` job only creates BasicReports against simulator event groups
(`sim-eg-*` prefix), never against EDPA-owned event groups. New EDPA EDPs must
be seeded manually.

To seed:

1.  Generate a self-signed access token via `OpenIdProvider` for the target
    environment's Reporting API:

    ```shell
    bazel run //src/main/kotlin/org/wfanet/measurement/common/tools:OpenIdProvider -- \
      --issuer='<REPORTING_API_ISSUER>' \
      --keyset=<REPORTING_API_JWKS_KEYSET> \
      generate-access-token \
      --audience='<REPORTING_API_AUDIENCE>' \
      --subject='mc-user@example.com' \
      --scope='*' \
      --ttl=1h
    ```

    The `--scope='*'` wildcard is required &mdash; the reporting API's
    `Authorization.checkScopes()` returns `PERMISSION_DENIED` without it.

2.  Look up an existing event group on the new EDP via the Reporting API:

    ```shell
    curl -sk -H "Authorization: Bearer $TOKEN" \
      "https://<REPORTING_API_HOST>/v2alpha/measurementConsumers/<MC_ID>/eventGroups?pageSize=200" \
      | jq '.eventGroups[] | select(.cmmsDataProvider == "dataProviders/<NEW_EDP_RESOURCE_ID>")'
    ```

3.  `POST` a `reportingSets` (as a campaign group) + `basicReports` pair
    scoped to that event group. See the reporting-v2 API reference for the
    request body shape; a minimal report with
    `impressionQualificationFilters/ami`, `resultGroupSpecs[].reportingUnit.components
    = [dataProviders/<NEW_EDP_RESOURCE_ID>]`, and a single-day
    `reportingInterval` is sufficient.
4.  Poll the BasicReport with `GET .../basicReports/{id}` until `state:
    SUCCEEDED`. On qa this takes ~15&ndash;25 minutes (real TEE fulfillment);
    on dev typically 2&ndash;5 minutes.
5.  Wait for the next `report_detail_edp` scheduled-query cycle to pull the
    Report in (hourly schedule) or trigger it manually &mdash; see the
    deployment guide's *Triggering Scheduled Queries Manually* section.
6.  Verify with `bq show MY_PROJECT:dashboard.report_detail_edp` that
    `numRows` has grown and `lastModifiedTime` is after the BasicReport's
    completion time.

The seed BasicReport can be trivial (single-day reporting interval, single
event group, `impressionQualificationFilters/ami`) &mdash; it just needs to
exist and be `SUCCEEDED`. It stays in the environment permanently; no cleanup
is required.

### Step 6: Verify Isolation

Run the compliance check with the new EDP included, impersonating the
least-privilege `dashboard-compliance` service account (matches the
deployment guide's Step 4 and the CI pattern from #4128 — running without
impersonation either fails outright or silently passes with broader-than-
tested permissions):

```shell
bazel run //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dashboard/tools:DashboardComplianceCheck -- \
  --impersonate-service-account=dashboard-compliance@MY_PROJECT.iam.gserviceaccount.com \
  --project=MY_PROJECT \
  --region=<REGION> \
  --edp=edp1:AbCdEf_12345 \
  --edp=edp2:GhIjKl_67890 \
  --edp=edp3:MnOpQr_24680 \
  --edp=edp4:StUvWx_13579
```

To impersonate, your account needs `roles/iam.serviceAccountTokenCreator` on
the `dashboard-compliance` SA. The dashboard Terraform grants this to every
member of `dashboard_operators`.

Verify all checks pass, including:

*   `edp4: requisition_overview returns only own data`
*   `edp4: correctly denied access to mc_details (403)`
*   `edp4: correctly denied EXTERNAL_QUERY via kingdom-conn (403)`

### Step 7: Share Credentials with the EDP

Provide the EDP with their service account details:

*   **Service account email**:
    `edp-edp4-dashboard@PROJECT.iam.gserviceaccount.com`
*   **Project ID**: The GCP project hosting the dashboard BigQuery dataset
*   **Dataset**: `dashboard`
*   **Accessible tables**: `requisition_overview`, `mc_details_edp`,
    `report_detail_edp`

The EDP authenticates using their service account via one of the keyless options
in Option A below (Workload Identity Federation or service account impersonation
from their own GCP project). Service account keys are supported as a last resort
but should be avoided — see the preference order in Option A.

## EDP Steps: Accessing the Dashboard

### Option A: Programmatic Access (BigQuery Client)

Authenticate **without** downloading service-account keys whenever possible.
Downloaded keys are a long-lived credential and are called out as a top risk
in #3930's threat model.

In preference order:

1.  **Service account impersonation** (the EDP has a Google identity in any
    GCP project). The operator grants the EDP's user/SA
    `roles/iam.serviceAccountTokenCreator` on
    `edp-<name>-dashboard@PROJECT.iam.gserviceaccount.com`, then the EDP
    impersonates it.
2.  **Workload Identity Federation** (the EDP runs outside GCP, e.g. AWS or
    on-prem). The CMMS terraform already provisions
    `edp-workload-identity-pool` / `edp-aws-workload-identity-pool` for each
    EDP's main SA. Reusing these for the dashboard requires one additional
    IAM binding — grant the EDP's federated principalSet
    `roles/iam.workloadIdentityUser` (or `serviceAccountTokenCreator`) on
    `edp-<name>-dashboard`. The dashboard Terraform does not create this
    binding today; have an operator add it before the EDP onboards.
3.  **Downloaded service-account key** — last resort, only if neither of the
    above is feasible. If used, scope tightly, store in a secret manager,
    and rotate on a schedule.

#### Python Example (impersonation)

```python
from google.auth import default, impersonated_credentials
from google.cloud import bigquery

source_credentials, _ = default(
    scopes=['https://www.googleapis.com/auth/cloud-platform']
)
target_credentials = impersonated_credentials.Credentials(
    source_credentials=source_credentials,
    target_principal='edp-edp4-dashboard@DASHBOARD_PROJECT.iam.gserviceaccount.com',
    target_scopes=['https://www.googleapis.com/auth/bigquery'],
    lifetime=3600,
)
client = bigquery.Client(project='DASHBOARD_PROJECT', credentials=target_credentials)

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
for row in client.query(query).result():
    print(row)
```

#### bq CLI (impersonation)

```shell
# Once: have your operator grant your Google identity
#   roles/iam.serviceAccountTokenCreator on the dashboard SA.

export CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT=\
edp-edp4-dashboard@DASHBOARD_PROJECT.iam.gserviceaccount.com

bq query --project_id=DASHBOARD_PROJECT --nouse_legacy_sql \
  'SELECT * FROM `DASHBOARD_PROJECT.dashboard.requisition_overview` LIMIT 10'

bq query --project_id=DASHBOARD_PROJECT --nouse_legacy_sql \
  'SELECT CmmsDataProvider, EventGroupCount, EntityTypes, EntityIds, CampaignNames, MediaTypes
   FROM `DASHBOARD_PROJECT.dashboard.mc_details_edp`'

bq query --project_id=DASHBOARD_PROJECT --nouse_legacy_sql \
  'SELECT ExternalReportId, EventGroupCount, CmmsEventGroupIds, EntityTypes
   FROM `DASHBOARD_PROJECT.dashboard.report_detail_edp`'
```

#### Last resort: service-account key

If keyless auth isn't feasible:

```python
from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file(
    'edp-dashboard-key.json',  # treat as a secret; rotate
    scopes=['https://www.googleapis.com/auth/bigquery'],
)
client = bigquery.Client(project='DASHBOARD_PROJECT', credentials=credentials)
```

### Option B: Looker Studio

Looker Studio comes in two flavors with different authentication models —
choose based on what you have access to:

#### Looker Studio Pro (recommended)

Pro lets a data source run as a service account, which is what the dashboard
RAP grants. The EDP sees their own data without any policy changes.

1.  Open Looker Studio Pro and create a new report.
2.  Add a BigQuery data source.
3.  In the data source settings, configure it to run as
    `edp-<name>-dashboard@DASHBOARD_PROJECT.iam.gserviceaccount.com`.
4.  Select the project, dataset `dashboard`, and one of the accessible tables.
5.  Build visualizations. Row access policies filter automatically.

#### Free Looker Studio

The free tier authenticates with the **human's** Google identity, not a
service account. Because only `edp-<name>-dashboard` is named in the row
access policy, an EDP human user would see **0 rows** by default. To use the
free tier, an operator must add the EDP's human Google accounts (or a Google
Group) as grantees on the EDP's row access policy via `bq`:

```shell
# Add EDP humans to each of the 3 EDP-visible tables' row access policies.
# Re-run for every (table, EDP) pair where Free-tier access is needed.
# --row_access_policy is a boolean toggle; identify the policy via
# --policy_id + --target_table. --grantees is a single comma-separated list.
# On update, re-supply the existing per-EDP predicate or it will be cleared.
bq update --row_access_policy \
  --project_id=DASHBOARD_PROJECT \
  --policy_id=<EDP>_filter \
  --target_table='dashboard.requisition_overview' \
  --grantees='group:<edp-name>-dashboard-users@example.com,user:<analyst>@<edp-domain>' \
  --filter_predicate='<existing per-EDP predicate>'
```

**Caveat — this is wiped on the next `terraform apply`.** The dashboard
Terraform manages each row access policy via `google_bigquery_row_access_policy`
with a fixed grantee list (the per-EDP service account only). When Terraform
re-asserts the policy on the next deploy, manually-added human grantees are
removed. Until the dashboard module accepts a `per_edp_human_grantees` input
(not yet on the roadmap), free-tier setups require the operator to re-run
the `bq update` commands above after every dashboard Terraform apply.
**Prefer Looker Studio Pro whenever feasible** to avoid this drift.

Once the policy is updated:

1.  Open [Looker Studio](https://lookerstudio.google.com/) and create a new
    report.
2.  Add a BigQuery data source for the dashboard dataset.
3.  Select "Viewer's credentials" so each viewer's identity is used for row
    filtering.
4.  Build visualizations.

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
| `ReportState` | STRING | Overall report state; one of CREATED, REPORT_CREATED, UNPROCESSED_RESULTS_READY, SUCCEEDED, FAILED, INVALID, UNSPECIFIED |
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
