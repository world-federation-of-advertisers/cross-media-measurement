# Environment configuration

Non-secret configuration for the Halo `dev`, `head`, and `qa` environments is committed in several files according to how it is consumed:

- `.github/env-configs/<environment>/cmms.json` contains values shared by deployment and test workflows. Workflows load the selected file with `.github/actions/load-cmms-environment`, which exports each key to subsequent steps and also provides the complete JSON for expression-only consumers such as action inputs.
- `src/main/terraform/gcloud/cmms/envs/<environment>.tfvars` contains Terraform inputs, including Cloud Function runtime environment-variable strings and Secret Manager mount mappings.
- `src/main/terraform/gcloud/cmms/envs/<environment>.backend.hcl` selects the Terraform state bucket.
- `src/main/terraform/gcloud/cmms/envs/<environment>/` contains structured application configuration as textproto, JSON, or PEM files.

This layout keeps the configuration reviewed and versioned in the repository, but it is not a single-file source of truth. Some Terraform runtime environment-variable strings embed project IDs, endpoints, or bucket names that are also represented in `cmms.json` or another tfvars value. Changes to those shared values must update every applicable committed representation.

Halo workflows read non-secret per-environment configuration from committed files rather than GitHub environment variables. GitHub environments hold secrets such as API keys and credentials. GitHub Actions variables are reserved for repository-wide CI and release integrations, such as artifact publishing, image signing, and notifications. Workflow-dispatch overrides remain inputs.

Secret mapping values committed in tfvars name Secret Manager resources and filesystem mount paths; secret contents must remain in GitHub or Google Secret Manager.

`REQUISITION_FETCHER_SCHEDULER_JOB_NAME` in `cmms.json` selects the scheduler paused by Update CMMS. The workflow also passes this value to Terraform as `requisition_fetcher_scheduler_job_name`, so both operations address the same job.

Set Spanner capacity with `spanner_processing_units` in the selected tfvars file. Terraform reapplies that value during deployment, so commit sustained capacity increases there to preserve them across deployments.

The VID-labeling healing controller is opt-in through `vid_labeling_healing_controller_enabled` in tfvars. Configure its private persistence endpoint, runtime settings, TLS secret mappings, and labeled-output deletion prefixes there before enabling it; see [the operator guide](../../docs/operations/healing-vid-labeling-uploads.md#deployment-configuration).
