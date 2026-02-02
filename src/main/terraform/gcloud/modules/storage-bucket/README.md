# Google Cloud Storage Bucket

Terraform module for creating a Google Cloud Storage bucket with configurable
lifecycle management rules.

## Lifecycle Rule Configuration

Configure lifecycle rules to automatically delete objects based on retention periods.
Each rule targets a specific prefix (e.g., per-EDP folders).

```hcl
module "shared_bucket" {
  source = "../storage-bucket"

  name     = "shared-impression-bucket"
  location = "US"

  lifecycle_rules = [
    {
      name           = "edp7"
      prefix         = "edp/edp7/"
      retention_days = 1460  # 4 years
    },
    {
      name           = "edp_meta"
      prefix         = "edp/edp_meta/"
      retention_days = 1460  # 4 years
    },
  ]
}
```

## Lifecycle Rule Behavior

Each configuration entry creates up to two lifecycle rules:

1. **Custom-Time based deletion**: Deletes objects after `retention_days` days
   since the object's Custom-Time metadata (e.g., impression date).

2. **Fallback age-based deletion** (optional, enabled by default): Safety net
   that deletes objects after `fallback_retention_days` days since upload, for
   objects without Custom-Time metadata.
