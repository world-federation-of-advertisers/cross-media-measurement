# GKE Cluster Configuration

Helpful tips for configuring clusters on Google Kubernetes Engine (GKE). Note
that this does not outline a secure configuration. You should consult with your
Google Cloud security admin.

## Cluster Service Account

See
[Use least privilege Google service accounts](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa)
on how to create a least privilege service account. You should use service
account for your clusters rather than the Compute Engine default service
account.

## Workload Identity

If your clusters are running under the least privilege service account as
described above, then it means that your pods will not have access to any other
Google Cloud resources by default. You should
[use Workload Identity](https://cloud.google.com/kubernetes-engine/docs/how-to/workload-identity)
to configure specific service accounts with access to the necessary resources.

It's easiest if you create your cluster with Workload Identity enabled from the
start. This way, your default node pool will already be configured to use it.

### Granting Cloud Spanner database access

One example of access you can grant to a Workload Identity service account is to
databases within a Cloud Spanner instance.

For example, if you want to grant a `kingdom-internal` service account in the
`halo-kingdom-demo` project access to the `kingdom` database within the instance
named `dev-instance`, you could run the following command:

```shell
gcloud spanner databases add-iam-policy-binding kingdom \
  --instance=dev-instance --role=roles/spanner.databaseUser \
  --member="serviceAccount:kingdom-internal@halo-kingdom-demo.iam.gserviceaccount.com"
```

## Network Policy

Kubernetes (K8s) network policies can be used to limit network ingress and
egress. To use this feature on GKE, you must enable network policy for your
cluster. There is a checkbox to enable this in the Cloud Console. If you're
using the `gcloud` CLI, you can pass the `--enable-network-policy` option to the
command.

Our [`dev`](../../src/main/k8s/dev/) configuration uses a policy to deny ingress
and egress by default, and then defines specific policies for subsets of pods.

## Encrypting Kubernetes Secrets

K8s secrets can be encrypted at the application layer using a key from Cloud
KMS. Use the `--database-encryption-key` option if you're creating a cluster
using the `gcloud` CLI. See
[Encrypt secrets at the application layer](https://cloud.google.com/kubernetes-engine/docs/how-to/encrypting-secrets)
for more information.
