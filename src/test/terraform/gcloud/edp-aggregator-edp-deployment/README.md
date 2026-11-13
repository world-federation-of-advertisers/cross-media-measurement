# EDP Terraform permission tests

Run with Terraform 1.11 or later:

```bash
terraform init -backend=false
terraform test
```

These tests use mocked providers and plan only. They verify the public modules'
workload trust and key permissions without calling cloud APIs or creating resources.
