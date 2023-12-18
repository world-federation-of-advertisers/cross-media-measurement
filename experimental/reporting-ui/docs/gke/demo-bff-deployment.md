# Demo UI BFF Deployment
## Overview
...

## Build and push the container images
If you aren't using pre-built release images, you can build the images yourself from source and push them to a container registry. For example, if you're using the [Google Container Registry](https://cloud.google.com/container-registry), you would specify `gcr.io` as your container registry and your Cloud project name as your image repository prefix.

Assuming a project named halo-cmm-dev and an image tag build-0001, run the following to build and push the images:

```shell
bazel run -c opt //experimental/reporting-ui/src/main/docker:push_all_reporting_bff_gke_images \
  --define container_registry=gcr.io \
  --define image_repo_prefix=halo-cmm-dev --define image_tag=build-0001
```

## Create resources for the cluster

See [GKE Cluster Configuration](cluster-config.md) for background.

### IAM Service Accounts

Be sure to create a two IAM Service Accounts, one for each of the gRPC and gateway servers.

We'll want to [create a least privilege service account](https://cloud.google.com/kubernetes-engine/docs/how-to/hardening-your-cluster#use_least_privilege_sa) that our cluster will run under. Follow the steps in the linked guide to do
this.

We'll additionally want to create a service account that we'll use to allow the internal API server to access the database. See [Granting Cloud SQL database access](cluster-config.md#granting-cloud-sql-instance-access) for how to make sure this service account has the appropriate role.

## Create the K8s ServiceAccount

Be sure to repeat these steps for both the gRPC and gateway servers. The directions below are an example for the gRPC server.

In order to use the IAM service account that we created earlier from our cluster, we need to create a K8s ServiceAccount and give it access to that IAM
service account.

For example, to create a K8s ServiceAccount named `reporting-bff-server`,
run

```shell
kubectl create serviceaccount reporting-bff-server
```

Supposing the IAM service account you created in a previous step is named `reporting-bff` within the `halo-cmm-dev` project. You'll need to allow the K8s service account to impersonate it

```shell
gcloud iam service-accounts add-iam-policy-binding \
  reporting-bff@halo-cmm-dev.iam.gserviceaccount.com \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:halo-cmm-dev.svc.id.goog[default/reporting-bff-server]"
```

Finally, add an annotation to link the K8s service account to the IAM service
account:

```shell
kubectl annotate serviceaccount reporting-bff-server \
    iam.gke.io/gcp-service-account=reporting-bff@halo-cmm-dev.iam.gserviceaccount.com
```

## Customize the K8S secrets

We use K8s secrets to hold sensitive information, such as private keys.

### Certificates and signing keys

First, prepare all the files we want to include in the Kubernetes secret. The
`dev` configuration assumes the files have the following names:

1.  `all_root_certs.pem`

    This makes up the trusted root CA store. It's the concatenation of the root
    CA certificates for all the entities that the Reporting server interacts
    with, including:

    *   All Measurement Consumers
    *   Any entity which produces Measurement results (e.g. the Aggregator Duchy
        and Data Providers)
    *   The Kingdom
    *   The Reporting server itself (for internal traffic)

    Supposing your root certs are all in a single folder and end with
    `_root.pem`, you can concatenate them all with a simple shell command:

    ```shell
    cat *_root.pem > all_root_certs.pem
    ```

    Note: This assumes that all your root certificate PEM files end in newline.

1.  `reporting_tls.pem`

    The Reporting server's TLS certificate.

1.  `reporting_tls.key`

    The private key for the Reporting server's TLS certificate.

In addition, you'll need to include the encryption and signing private keys for
the Measurement Consumers that this Reporting server instance needs to act on
behalf of. The encryption keys are assumed to be in Tink's binary keyset format.
The signing private keys are assumed to be DER-encoded unencrypted PKCS #8.

#### Testing keys

There are some [testing keys](../../src/main/k8s/testing/secretfiles) within the
repository. These can be used to create the above secret for testing, but **must
not** be used for production environments as doing so would be highly insecure.

Generate the archive:

```shell
bazel build //src/main/k8s/testing/secretfiles:archive
```

Extract the generated archive to the `src/main/k8s/dev/reporting_v2_secrets/` path
within the Kustomization directory.

### Measurement Consumer config

Contents:

1.  `measurement_consumer_config.textproto`

    [`MeasurementConsumerConfig`](../../src/main/proto/wfa/measurement/config/reporting/measurement_consumer_config.proto)
    protobuf message in text format.

### Generator

Place the above files into the `src/main/k8s/dev/reporting_v2_secrets/` path within
the Kustomization directory.

Create a `kustomization.yaml` file in that path with the following content,
substituting the names of your own keys:

```yaml
secretGenerator:
- name: signing
  files:
  - all_root_certs.pem
  - reporting_root.pem
  - mc_tls.key
  - mc_tls.pem
```

## Apply the K8S Kustomization

Within the Kustomization directory, run

```shell
kubectl apply -k src/main/k8s/dev/reporting_v2
```

## Reserve an external IP

The `reporting-v2alpha-public-api-server` service has an external load balancer
IP so that it can be accessed from outside the cluster. By default, the assigned
IP address is ephemeral. We can reserve a static IP to make it easier to access.
See [Reserving External IPs](cluster-config.md#reserving-external-ips).


## Edit the K8S Config

There is an example yaml config file of how the configuration may look. It is suggested to use `//experimental/reporting-ui/src/main/k8s/reporting_bff.yaml` as a base example but copy it and modify the copy for your specific usage. The following will assume the file has the same name and copied to another directory.

Set the `serviceAccountName`s to the names created above. There is one for the gRPC and one for the Gateway deployments.

```shell
serviceAccountName: reporting-bff-server
```

Be sure to set the deployment container args to your specifications in both the gRPC and Gateway containers in the deployments.

Set the volume secret `secretName` to the one specified by K8S.

```shell
secretName: signing-h98g2ddfgf
```

If you reserve an IP address, change the load balancer IPs for both the gRPC and Gateway services.

```shell
loadBalancerIP: 34.121.171.2
```

## Verify Cluster

To verify the cluster, run the command:

```shell
kubectl get all
```

NOTE: You can also run the command for the specific resources (pod, service, deployment, replica set).

You should see two pod entries like:

```
NAME                                                                  READY   STATUS    RESTARTS   AGE
pod/reporting-bff-gateway-v1alpha-public-api-server-deploymentgs9vp   1/1     Running   0          46m
pod/reporting-bff-v1alpha-public-api-server-deployment-5b779bcpnpfr   1/1     Running   0          46m
```

You should see two service entries like:

```
NAME                                   TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)          AGE
service/reporting-bff-gateway-server   LoadBalancer   10.79.85.238   34.41.59.152    8080:30656/TCP   46m
service/reporting-bff-server           LoadBalancer   10.79.93.136   34.121.171.2    8443:32336/TCP   46m
```

You should see two deployments and replica sets that look like:

```
NAME                                                                         READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/reporting-bff-gateway-v1alpha-public-api-server-deployment   1/1     1            1           46m
deployment.apps/reporting-bff-v1alpha-public-api-server-deployment           1/1     1            1           46m


NAME                                                                                    DESIRED   CURRENT   READY   AGE
replicaset.apps/reporting-bff-gateway-v1alpha-public-api-server-deployment-699d479b8d   1         1         1       46m
replicaset.apps/reporting-bff-v1alpha-public-api-server-deployment-5b779bc54b           1         1         1       46m
```

If there was an error, go to the particular resource and review the logs.
