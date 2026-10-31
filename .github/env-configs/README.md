# Environment configuration

Non-secret configuration for the Halo `dev`, `head`, and `qa` environments is committed here. Each `cmms.json` file is the source of truth for values shared by deployment and test workflows. Workflows load the selected file with `.github/actions/load-cmms-environment`, which exports each key to subsequent steps and also provides the complete JSON for expression-only consumers such as action inputs.

Terraform-only settings remain in `src/main/terraform/gcloud/cmms/envs/<environment>.tfvars`. Structured application configuration remains alongside those files as textproto, JSON, or PEM files.

GitHub environments continue to hold secrets, such as API keys and credentials. GitHub Actions variables are reserved for repository-wide CI and release integrations that are not configuration of a specific Halo environment, such as artifact-publishing, image-signing, and notification settings. Workflow-dispatch overrides remain inputs. Halo resource names, endpoints, identities, and test key-resource identifiers belong in the selected `cmms.json` file so they are reviewed and versioned together.

When changing a shared resource name or endpoint, update only the corresponding `cmms.json` file. Secret mapping values name Secret Manager resources and mount paths; secret contents must remain in GitHub or Google Secret Manager.
