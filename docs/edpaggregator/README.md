# EDP Aggregator Documentation

Single source of truth for deploying and operating the **EDP Aggregator** (EDPA) and
for integrating **Event Data Providers** (EDPs) with it.

The EDP Aggregator lets EDPs hand off encrypted, VID-labeled impression logs and
event-group metadata through a Cloud Storage bucket and have requisitions fulfilled
by trusted workloads running in Confidential Space TEEs.

## Guides

### Deployment & operations (market operator)

| Guide | What it covers |
| --- | --- |
| [Deployment Guide](deployment-guide.md) | Full operator deployment: components, Terraform configuration for every service, storage lifecycle & versioning, GKE APIs, and end-to-end validation. |
| [Metadata Operator Guide](metadata-operator-guide.md) | RequisitionFetcher + ImpressionMetadata internals, tuning knobs, behavior at scale, and troubleshooting. |
| [Report Debugging Guide](report-debugging-guide.md) | Trace a report end-to-end across the stack and diagnose common failure modes. |
| [Self-Serve Onboarding Guide](self-serve-onboarding.md) | Operator-side linking of Measurement Consumers to client accounts for automatic event-group registration. |

### EDP integration (data provider)

| Guide | What it covers |
| --- | --- |
| [EDP Onboarding Guide](edp-onboarding.md) | Integrate a data provider: KMS/key setup, Workload Identity, schemas, encryption, upload paths, the daily workflow, and optional TrusTEE. |
| [AWS KMS Setup Guide](aws-kms-setup.md) | Use AWS KMS instead of GCP KMS for a data provider's key. |

### Reporting dashboard

| Guide | What it covers |
| --- | --- |
| [Dashboard Deployment Guide](dashboard/deployment-guide.md) | Deploy the EDPA reporting dashboard. |
| [Dashboard EDP Onboarding Guide](dashboard/onboarding-guide.md) | Give an EDP access to its dashboard data. |

## Where to start

* **Operator standing up a market:** [Deployment Guide](deployment-guide.md), then
  validate with its cloud test.
* **EDP integrating for the first time:** [EDP Onboarding Guide](edp-onboarding.md)
  (and [AWS KMS Setup](aws-kms-setup.md) if you use AWS KMS).
* **Debugging a live report:** [Report Debugging Guide](report-debugging-guide.md).

## Conventions

The deployment and integration guides use **generic placeholders** (e.g.
`PROJECT_ID`, `EDPA_STORAGE_BUCKET`, `dataProviders/DATA_PROVIDER_ID`, `<edp-id>`).
Substitute your market's real values. Proto files under
`src/main/proto/wfa/measurement/...` are the authoritative schemas; generate against
them rather than copying inline snippets.
