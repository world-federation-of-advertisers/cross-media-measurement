# Google Cloud Storage Bucket

Terraform module for creating a Google Cloud Storage bucket with configurable
lifecycle management rules.

## Lifecycle Rule Configuration

Configure lifecycle rules to automatically delete objects based on retention periods.
Each rule targets a specific prefix (e.g., per-EDP folders). Markets should choose retention
periods that meet their reporting and data retention requirements. Set `retention_days` and,
when fallback deletion is enabled, `fallback_retention_days` explicitly for each prefix.

```hcl
module "shared_bucket" {
  source = "../storage-bucket"

  name     = "shared-impression-bucket"
  location = "US"

  lifecycle_rules = [
    {
      name                    = "edp-alpha"
      prefix                  = "edp/edp-alpha/"
      retention_days          = 90
      fallback_retention_days = 90
    },
    {
      name                    = "edp-beta"
      prefix                  = "edp/edp-beta/"
      retention_days          = 90
      fallback_retention_days = 90
    },
  ]
}
```

## Lifecycle Rule Behavior

Each configuration entry creates up to two lifecycle rules:

1. **Custom-Time based deletion**: Deletes objects after `retention_days` days
   since the object's Custom-Time metadata (e.g., impression date).

2. **Fallback age-based deletion** (optional, enabled by default): Deletes objects
   after `fallback_retention_days` days since upload. This rule also applies when
   Custom-Time is present, so configure it alongside the impression-date retention period.
