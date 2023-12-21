# Demo UI Deployment
## Overview

Since the Demo UI is based on showing reports, it will be deployed to the Reporting K8S cluster. We will assume the cluster is already created.

To host the Demo UI, we are using NGINX as a simple web server. In order to mount some configuration, it requires additional resource capacity. If the Reporting cluster was configured with the minimum, it may need to be reconfigured with an SSD boot disk and additional max node count. You can also create a new cluster in a similar fashion with the additional compute capabilities.

To set up and configure the environment, you can generally follow these directions:
https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver

They will be summarized below assuming we are updating a cluster.

## Setup the GCS Bucket

### Create a Bucket

Create a bucket following these directions: https://cloud.google.com/storage/docs/creating-buckets.

### Upload Files to the Storage Bucket

Build the website using the command:

```shell
bazel build //experimental/reporting-ui/src/main/react/reporting-ui:webpack_bundle
```

Copy the files under the `bazel-bin/experimental/reporting-ui/src/main/react/reporting-ui/public` folder and the file `bazel-bin/experimental/reporting-ui/src/main/react/reporting-ui/webpack_bundle/app.bundle.js` to the storage bucket under the directory `website`.

Create a passwords file for Basic Authentication using a method like the following: https://docs.nginx.com/nginx/admin-guide/security-controls/configuring-http-basic-authentication/#creating-a-password-file, saving the passwords to a file.

Upload the passwords with any file name (`YOUR_FILE_NAME` for future reference) to the storage bucket under the directory `config`.

## Configure the Cluster

### Add Cloud Storage FUSE CSI Driver

The cluster needs to be configured with the Cloud Storage FUSE CSI driver enabled. This enables us to mount a GCS Bucket to the K8S pods using a sidecar. Replace `CLUSTER_NAME` and `COMPUTER_REGION` with your appropriate values.

```shell
gcloud container clusters update CLUSTER_NAME \
    --update-addons GcsFuseCsiDriver=ENABLED \
    --region=COMPUTE_REGION
```

### Create and Configure a GKE Workload Identity

See the [Workload Identity](../../../../docs/gke/cluster-config.md#workload-identity) docs for more information.

Create a K8S service account replacing `KSA_NAME` with your service account name.

```shell
kubectl create serviceaccount KSA_NAME \
    --namespace NAMESPACE
```

Create an IAM service account replacing `GCS_NAME` with your GCS account name and `GSA_PROJECT` with your GCS project name.

```shell
gcloud iam service-accounts create GSA_NAME \
    --project=GSA_PROJECT
```

Add permissions to access the GCS bucket replacing `BUCKET_NAME` with the bucket you created earlier and `ROLE_NAME` as `roles/storage.objectViewer`.

```shell
gcloud storage buckets add-iam-policy-binding gs://BUCKET_NAME \
    --member "serviceAccount:GSA_NAME@GSA_PROJECT.iam.gserviceaccount.com" \
    --role "ROLE_NAME"
```

You can use other role names if you like, but the Demo UI is only reading the website and password files, so read-only access is the minimum required: `roles/storage.objectViewer`.

Allow the K8S account to use the IAM role replacing `GSA_NAME` with the service account name created above, `GSA_PROJECT` with your GCS project name, `PROJECT_ID` with your GCS project id, `NAMESPACE` with your K8S namespace (likely `default` unless you specified one), and `KSA_NAME` with the K8S service account name created above.

```shell
gcloud iam service-accounts add-iam-policy-binding GSA_NAME@GSA_PROJECT.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:PROJECT_ID.svc.id.goog[NAMESPACE/KSA_NAME]"
```

Annotate the K8S account replacing `KSA_NAME` with the K8S service account created above, `NAMESPACE` with your K8S namespace (likely `default` unless you specified one), and `GSA_PROJECT` with your GCS project name.

```shell
kubectl annotate serviceaccount KSA_NAME \
    --namespace NAMESPACE \
    iam.gke.io/gcp-service-account=GSA_NAME@GSA_PROJECT.iam.gserviceaccount.com
```

## Update the K8S Object Configuration

The K8S yaml config file follows the rest of the directions to setup GCS as a mount. The yaml config file is an example of how the configuration may look. It is suggested to use `//experimental/reporting-ui/src/main/k8s/dev/reporting_ui.yaml` as a base example but copy it and modify the copy for your specific usage. The following will assume the file has the same name and copied to another directory.

### Update the GCS Bucket Name

In the `reporting_ui.yaml` file, replace `YOUR_BUCKET_NAME` with the name of the bucket created above.

```yaml
- name: website
  csi:
    driver: gcsfuse.csi.storage.gke.io
    readOnly: true
    volumeAttributes:
        bucketName: YOUR_BUCKET_NAME
        mountOptions: "implicit-dirs"
```

### Update the Password File Name

Modify the server config section replacing `YOUR_FILE_NAME` with the name of the password file created above.

```
location / {
    auth_basic "Authentication is required";
    auth_basic_user_file /etc/nginx/passwords/YOUR_FILE_NAME;
}
```

## Apply the K8S Object Configuration

```shell
kubectl apply -f path/to/reporting_ui.yaml
```

Where the path is to your copied and modified version.

## Verify Cluster

To verify the cluster, run the command:

```shell
kubectl get all
```

NOTE: You can also run the command for the specific resources (pod, service, deployment, replica set).

You should see a pod entry like:

```
NAME                          READY   STATUS    RESTARTS   AGE
pod/demo-ui-6d75d587d-xvt2l   2/2     Running   0          4d19h
```

There are two containers, one for the NGINX server and one for a sidecar to mount a GCS Bucket.

You should see a service entry like:

```
NAME                 TYPE           CLUSTER-IP     EXTERNAL-IP     PORT(S)        AGE
service/demo-ui      LoadBalancer   10.79.82.198   34.68.112.248   80:30738/TCP   4d19h
```

The exteranl IP is the URL you can use in the browser to verify the web server deployed correctly.

You should see a deployment and replica set that look like:

```
NAME                      READY   UP-TO-DATE   AVAILABLE   AGE
deployment.apps/demo-ui   1/1     1            1           4d19h

NAME                                DESIRED   CURRENT   READY   AGE
replicaset.apps/demo-ui-6d75d587d   1         1         1       4d19h
```

If there was an error, go to the particular resource and review the logs.
