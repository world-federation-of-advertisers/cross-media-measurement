# EDP Simulators

This illustrates how to configure the infrastructure for EDP simulators in a
Google Cloud Project.

## Resources

*   [Common resources](../../modules/common)
*   IAM service account
    *   IAM membership for Kubernetes service account to impersonate
*   GKE [cluster](../../modules/cluster) with application-level secret
    encryption in the specified location
    *   Default node pool
    *   Spot VM node pool

## Preconditions

*   The Google Cloud Project has APIs enabled for the above resources.
*   The account running Terraform has permissions to manage the above resources.
*   [Default values](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#provider-default-values-configuration)
    are specified in the environment the `google` provider.
