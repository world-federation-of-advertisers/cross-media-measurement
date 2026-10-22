# Environment configuration

Non-secret configuration for the Halo `dev`, `head`, and `qa` environments is committed here. Each `cmms.json` file is the source of truth for values shared by deployment and test workflows. Workflows load the selected file with `.github/actions/load-cmms-environment`.

Terraform-only settings remain in `src/main/terraform/gcloud/cmms/envs/<environment>.tfvars`. Structured application configuration remains alongside those files as textproto, JSON, or PEM files.

GitHub environments continue to hold secrets. Non-secret deployment configuration, including workload-identity provider and service-account resource names, is committed here so every value used by the workflows is reviewed and versioned together.

When changing a shared resource name or endpoint, update only the corresponding `cmms.json` file. Secret mapping values name Secret Manager resources and mount paths; secret contents must remain in GitHub or Google Secret Manager.
