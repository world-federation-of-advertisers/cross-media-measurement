# Reporting

This illustrates how to configure the infrastructure for the Reporting system in
a Google Cloud Project.

Note that this configuration is not optimized and may not be suitable for
production loads.

## Resources

*   [Common resources](../../modules/common)
*   Service accounts for internal API server and Access internal API server
    *   IAM membership for Kubernetes service account to impersonate
*   Google Cloud SQL instance
    *   Reporting database
        *   IAM membership for internal server service account to access
*   Google Cloud Spanner instance
    *   Access database
        *   IAM membership for Access internal server service account to access
*   GKE [cluster](../../modules/cluster) with application-level secret
    encryption in the specified location
    *   Default node pool
    *   Kubernetes service accounts for internal server and internal Access
        server

## Preconditions

*   The Google Cloud Project has APIs enabled for the above resources.
*   The account running Terraform has permissions to manage the above resources.
*   [Default values](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#provider-default-values-configuration)
    are specified in the environment the `google` provider.
