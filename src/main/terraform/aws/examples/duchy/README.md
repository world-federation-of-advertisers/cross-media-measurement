# Duchy

This illustrates how to configure the infrastructure for a Duchy in a AWS Account.

Note that this configuration is not optimized and may not be suitable for
production loads.

## Resources

*   Service account for internal API server
    *   IAM role for Kubernetes service account to impersonate
*   Service account for storage access
    *   IAM role for Kubernetes service account to impersonate
*   AWS [RDS Postgres](../../modules/rds-postgres) instance
    *   Database
        *   IAM role for internal server service account to access
*   AWS [S3 bucket](../../modules/s3-bucket)
    *   IAM role for service accounts to access
*   AWS [EKS cluster](../../modules/eks-cluster) with application-level secret
    encryption in the specified location
    *   Default node pool
    *   Spot VM node pool
    *   Kubernetes service account for internal server and storage
*   AWS [EKS cluster-addons](../../modules/eks-cluster-addons) with following addons:
    *   aws-load-balancer-controller addon that help assign elastic IP to load balancers

## Preconditions

*   The account running Terraform has permissions to manage the above resources.
*   Local setup refer to [Authentication and Configuration](https://registry.terraform.io/providers/hashicorp/aws/latest/docs#authentication-and-configuration)
