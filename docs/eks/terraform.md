# Terraform

Using Terraform to configure infrastructure for CMMS components in a AWS Cloud
Project.

## Examples

This repository contains [examples](../../src/main/terraform/aws/examples) for
configuring CMMS components using custom
[modules](../../src/main/terraform/aws/modules). These can be used directly by
copying the [parent directory](../../src/main/terraform/aws) and adding a
backend configuring in the desired example directory.

## Backend configuration

When deploying on AWS Cloud it is common to use the `s3` backend. Simply add
`backend.tf` file to the appropriate directory, indicating the AWS S3 bucket
where you wish to persist your Terraform state.

```terraform
terraform {
  backend "s3" {
    bucket = "my_bucket"
    key    = "terraform/state/halo-cmms"
    region = "us-west-2"
  }
}
```
