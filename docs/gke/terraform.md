# Terraform

Using Terraform to configure infrastructure for CMMS components in a Google
Cloud Project.

## Examples

This repository contains [examples](../../src/main/terraform/gcloud/examples)
for configuring CMMS components using custom
[modules](../../src/main/terraform/gcloud/modules). These can be used directly
by copying the [parent directory](../../src/main/terraform/gcloud) and adding a
backend configuring in the desired example directory.

## Backend configuration

When deploying on Google Cloud it is common to use the `gcs` backend. Simply add
`backend.tf` file to the appropriate directory, indicating the Google Cloud
Storage bucket where you wish to persist your Terraform state.

```terraform
terraform {
  backend "gcs" {
    bucket = "my_bucket"
    prefix = "terraform/state/halo-cmms"
  }
}
```
