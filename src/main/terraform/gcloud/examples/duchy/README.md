# Duchy

This illustrates how to configure the infrastructure for a Duchy in a Google
Cloud Project.

Note that this configuration is not optimized and may not be suitable for
production loads.

## Resources

*   [Common resources](../../modules/common)
*   Service account for internal API server
    *   IAM membership for Kubernetes service account to impersonate
*   Service account for storage access
    *   IAM membership for Kubernetes service account to impersonate
*   Google Cloud Spanner instance
    *   Database
        *   IAM membership for internal server service account to access
*   Google Cloud Storage bucket
    *   IAM memberships for service accounts to access
*   GKE [cluster](../../modules/cluster) with application-level secret
    encryption in the specified location
    *   Default node pool
    *   Spot VM node pool
    *   Kubernetes service account for internal server

## Preconditions

*   The Google Cloud Project has APIs enabled for the above resources.
*   The account running Terraform has permissions to manage the above resources.
*   [Default values](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#provider-default-values-configuration)
    are specified in the environment the `google` provider.
