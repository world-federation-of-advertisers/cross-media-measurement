
# EDP Aggregator System

This Terraform module deploys the EDP Aggregator components on a Google Cloud Managed Instance Group (MIG) backed by Confidential VMs.

The module provisions:

- A **service account** for the MIG, with IAM bindings for Pub/Sub, Secret Manager, logging, and Confidential Computing.
- A **Confidential VM instance template**, configured with:
    - AMD SEV-enabled confidential compute
    - Shielded VM secure boot
    - Logging and monitoring enabled
    - Metadata for signed container images and runtime configuration
- A **regional Managed Instance Group**, using the above instance template.
- A **Pub/Sub autoscaler** that scales instances based on the number of undelivered messages in a subscription.

## Network Configuration and MIG Recreation

Google Compute Engine does not allow modifying the network or subnetwork of an existing instance template.
Any change to the network (for example, assigning a new subnetwork) causes Terraform to fail when updating the MIG in place.

To handle this safely:

- The value of `managed_instance_group_name` must change.

This forces Terraform to create a new MIG with a new name, while the The **create_before_destroy** setting ensures no downtime during replacement.