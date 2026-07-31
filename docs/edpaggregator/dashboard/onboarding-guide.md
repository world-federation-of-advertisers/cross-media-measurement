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
*   How many other EDPs are in a report
*   Platform-only tables
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

### Step 2: Add the EDP to the Dashboard Config

Add the EDP to the `edps` array of the `DASHBOARD_CONFIG_CONTENT` GitHub
environment variable — the single source of truth. The CI workflow regenerates
the Terraform `data_provider_resource_ids` map from it:

```json
{"name": "edp4", "resource_id": "StUvWx_13579"}
```

The `name` (e.g., `edp4`) is a short label used for:

*   The service account name: `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`
*   Row access policy IDs: `<name>_filter`
*   CI test matrix entries

The full `edps` array then looks like:

```json
[
  {"name": "edp1", "resource_id": "AbCdEf_12345"},
  {"name": "edp2", "resource_id": "GhIjKl_67890"},
  {"name": "edp3", "resource_id": "MnOpQr_24680"},
  {"name": "edp4", "resource_id": "StUvWx_13579"}
]
```

### Step 3: Apply Terraform

Apply the change one of two ways:

*   **CI (normal):** Re-run the `Update CMMS` deploy workflow. It regenerates
    `data_provider_resource_ids` from `DASHBOARD_CONFIG_CONTENT` and runs
    `terraform apply`.
*   **Manual:** A local `.tfvars` is *not* regenerated from
    `DASHBOARD_CONFIG_CONTENT`, so add the EDP to the `data_provider_resource_ids`
    map in your `.tfvars` first, then apply:

    ```shell
    terraform plan -var-file=my-env.tfvars
    terraform apply -var-file=my-env.tfvars
    ```

This creates:

*   Service account `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`
*   `roles/bigquery.dataViewer` on `requisition_overview`, `mc_details_edp`,
    `report_detail_edp`
*   `roles/bigquery.jobUser` on the project
*   Row access policies on all 3 tables filtering to the EDP's resource ID

### Step 4: Seed a BasicReport for the new EDP

The dashboard's `report_detail_edp` scheduled query only populates a row for an
EDP when at least one BasicReport in state `SUCCEEDED` references one of that
EDP's event groups. On a fresh EDP with no reports yet, the compliance check
`report_detail_edp is empty` and the isolation test both FAIL — a
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

    The `--scope='*'` wildcard is required — the reporting API's
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
    SUCCEEDED`. On qa this takes ~15–25 minutes (real TEE fulfillment);
    on dev typically 2–5 minutes.
5.  Wait for the next `report_detail_edp` scheduled-query cycle to pull the
    Report in (hourly schedule) or trigger it manually — see the
    deployment guide's *Trigger Scheduled Queries Manually* section.
6.  Verify with `bq show <DASHBOARD_PROJECT>:dashboard.report_detail_edp` that
    `numRows` has grown and `lastModifiedTime` is after the BasicReport's
    completion time.

The seed BasicReport can be trivial (single-day reporting interval, single
event group, `impressionQualificationFilters/ami`) — it just needs to
exist and be `SUCCEEDED`. It stays in the environment permanently; no cleanup
is required.

### Step 5: Verify Isolation

Run the compliance check with the new EDP in the config and confirm it passes
for that EDP. See the deployment guide's *Run the Compliance Check* for the
command, impersonation setup, and what it verifies.

### Step 6: Share Credentials with the EDP

Provide the EDP with their service account details:

*   **Service account email**:
    `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`
*   **Project ID**: The GCP project hosting the dashboard BigQuery dataset
*   **Dataset**: `dashboard`
*   **Accessible tables**: `requisition_overview`, `mc_details_edp`,
    `report_detail_edp`

The EDP authenticates using their service account. See Option A below for the
options in preference order — keyless (Workload Identity Federation or service
account impersonation from their own GCP project) is preferred; downloaded
service-account keys only as a last resort.

## EDP Steps: Accessing the Dashboard

### Option A: Programmatic Access (BigQuery Client)

Authenticate **without** downloading service-account keys whenever possible.
Downloaded keys are a long-lived credential and are called out as a top risk
in the dashboard's security model.

In preference order:

1.  **Service account impersonation** (the EDP has a Google identity in any
    GCP project). The operator grants the EDP's user/SA
    `roles/iam.serviceAccountTokenCreator` on
    `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`, then the EDP
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
    target_principal='edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com',
    target_scopes=['https://www.googleapis.com/auth/bigquery'],
    lifetime=3600,
)
client = bigquery.Client(project='<DASHBOARD_PROJECT>', credentials=target_credentials)

query = """
SELECT
    Report,
    RequisitionState,
    CmmsCreateTime,
    FulfillmentDurationSeconds
FROM `<DASHBOARD_PROJECT>.dashboard.requisition_overview`
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
edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com

bq query --project_id=<DASHBOARD_PROJECT> --nouse_legacy_sql \
  'SELECT * FROM `<DASHBOARD_PROJECT>.dashboard.requisition_overview` LIMIT 10'

bq query --project_id=<DASHBOARD_PROJECT> --nouse_legacy_sql \
  'SELECT CmmsDataProvider, EventGroupCount, EntityTypes, EntityIds, CampaignNames, MediaTypes
   FROM `<DASHBOARD_PROJECT>.dashboard.mc_details_edp`'

bq query --project_id=<DASHBOARD_PROJECT> --nouse_legacy_sql \
  'SELECT ExternalReportId, EventGroupCount, CmmsEventGroupIds, EntityTypes
   FROM `<DASHBOARD_PROJECT>.dashboard.report_detail_edp`'
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
client = bigquery.Client(project='<DASHBOARD_PROJECT>', credentials=credentials)
```

### Option B: Looker Studio

A central **report builder** builds the reports; each EDP only views. An EDP has
nothing to configure — they just open the report link the builder shares (no
account setup, service-account access, or IAM grants).

Looker Studio runs the report's BigQuery data source under the EDP's
`edp-<name>-dashboard` SA — the identity the dashboard's row access policies are
written for. Every query then executes as the SA, so the policy is satisfied
and the EDP sees only their own data, with no policy changes. This works the
same in the free and Pro tiers.

**Requirements:**

*   The report builder must sign in with a **Workspace / Cloud Identity
    managed account**. Consumer `@gmail.com` accounts cannot select the
    **Service account credentials** option.
*   One-time IAM setup by an operator on
    `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`:
    *   Grant the Looker Studio service agent (Google-managed, not a human)
        `roles/iam.serviceAccountTokenCreator` on the SA — lets Looker Studio
        impersonate the SA at runtime to run queries as it.
    *   Grant the report builder's human Google account
        `roles/iam.serviceAccountUser` (`actAs`) on the SA — a config-time
        authorization to wire a data source to run as the SA; it does not mint
        tokens or query as the SA itself. Grant per-SA (not project-wide) to
        keep the footprint minimal.

**Setup** (report builder, per EDP):

1.  Create a report and add a BigQuery data source.
2.  Under data credentials, choose **Service account credentials** and enter
    that EDP's SA:
    `edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`.
3.  Select the project, dataset `dashboard`, and an accessible table.
4.  Build visualizations — row access policies filter automatically.
5.  Share the report only with that EDP's designated viewers.

**Isolation warning:** a report runs as its configured SA for everyone who opens
it, so the row access policy can't stop the wrong viewer from seeing data —
cross-EDP isolation depends entirely on sharing scope. Build one report per EDP
and share each only with that EDP; never use "anyone with the link," and never
reuse a report or SA across EDPs.

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

1.  Remove the EDP from the `edps` array in `DASHBOARD_CONFIG_CONTENT` (for a
    manual deploy, remove it from `data_provider_resource_ids` in your local
    `.tfvars`).
2.  Re-run the `Update CMMS` deploy workflow (or `terraform apply` for a manual
    deploy). This destroys the EDP's service account, row access policies, and
    table-level IAM grants.
3.  Confirm the removal: the EDP's service account and row access policies
    should be gone — check the apply output, or that
    `gcloud iam service-accounts describe edp-<name>-dashboard@<DASHBOARD_PROJECT>.iam.gserviceaccount.com`
    returns `NOT_FOUND`. The compliance check no longer covers a removed EDP, so
    verify the destroy directly.

The EDP's historical data remains in the tables but is inaccessible (no row
access policy grants visibility, no service account to authenticate).

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
