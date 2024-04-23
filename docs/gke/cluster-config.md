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

### Granting Cloud SQL instance access

See https://cloud.google.com/sql/docs/postgres/add-manage-iam-users#gcloud

Assuming we have a `dev-postgres` instance and a
`reporting-internal@halo-cmm-dev.iam.gserviceaccount.com` service account in the
`halo-cmm-dev` project, follow the steps below:

1.  Grant the "Cloud SQL Instance User" role to the service account

    ```shell
    gcloud projects add-iam-policy-binding halo-cmm-dev \
      --member='serviceAccount:reporting-internal@halo-cmm-dev.iam.gserviceaccount.com' \
      --role=roles/cloudsql.instanceUser
    ```

1.  Create a Cloud SQL instance user for the service account

    ```shell
    gcloud sql users create reporting-internal@halo-cmm-dev.iam \
      --instance=dev-postgres \
      --type=cloud_iam_service_account
    ```

1.  Grant the instance user access

    Connect to your instance and run a `GRANT` SQL statement. For example, to
    grant access to the `reporting` database:

    ```sql
    GRANT ALL PRIVILEGES ON DATABASE reporting TO "reporting-internal@halo-cmm-dev.iam";
    ```

    Tip: You can use `gcloud sql connect` to connect via Cloud Shell.

### Granting Cloud Storage bucket access

Granting a service account access to a Cloud Storage bucket can be done from the
[Cloud Storage Browser](https://console.cloud.google.com/storage/browser), or
using [`gsutil`](https://cloud.google.com/storage/docs/gsutil/commands/iam).

For example, if you want to grant a `duchy-storage` service account in the
`halo-worker1-demo` project full access to objects in the `worker1-duchy`
bucket:

```shell
gsutil iam ch \
  serviceAccount:duchy-storage@halo-worker1-demo.iam.gserviceaccount.com:objectAdmin \
  gs://worker1-duchy
```

### Granting BigQuery table access

Grant the service account the `roles/bigquery.jobUser` role at the project
level, and the `roles/bigquery.dataViewer` role at the table level.

This can be done through the Cloud Console, but here are sample commands using
the `gcloud` and `bq` CLI tools.

```shell
gcloud projects add-iam-policy-binding halo-cmm-dev \
  --role=roles/bigquery.jobUser \
  --member='serviceAccount:simulator@halo-cmm-dev.iam.gserviceaccount.com'
```

```shell
bq add-iam-policy-binding --table=true \
  --role=roles/bigquery.dataViewer \
  --member='serviceAccount:simulator@halo-cmm-dev.iam.gserviceaccount.com' \
  'demo.labelled_events'
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

## Setting Default Resource Requirements

It's a good idea to set default resource requirements for your cluster namespace
using a `LimitRange`. See the corresponding
[guide](https://kubernetes.io/docs/tasks/administer-cluster/manage-resources/memory-default-namespace/)
for more information.

For the `dev` environment, we use
[resource_requirements.yaml](../../src/main/k8s/dev/resource_requirements.yaml)
