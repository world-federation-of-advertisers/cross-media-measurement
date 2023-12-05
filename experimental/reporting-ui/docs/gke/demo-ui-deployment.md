# Overview

Since the Demo UI is based on showing reports, it will be deployed to the Reporting K8S cluster. We will assume the cluster is already created.

To host the Demo UI, we are using NGINX as a simple webserver. In order to mount some configuration, it requires additional resource capacity. If the Reporting cluster was configured with the minimum, it may need to be reconfigured with an SSD boot disk and additional max node count. You can also create a new cluster in a similar fashion with the additional compute capabilities.

To setup and configure the environment, you can generally follow these directions:
https://cloud.google.com/kubernetes-engine/docs/how-to/persistent-volumes/cloud-storage-fuse-csi-driver

They will be summarized below assuming we are updating a cluster.

# Setup the GCS Bucket

## Create a Bucket

In your GCS account, create a storage bucket and note the name. In the `reporting_ui.yaml` file, update this section to use your bucket name.

```
- name: website
  csi:
    driver: gcsfuse.csi.storage.gke.io
    readOnly: true
    volumeAttributes:
        bucketName: YOUR_BUCKET_NAME
        mountOptions: "implicit-dirs"
```

## Upload Files to the Storage Bucket

Build and upload the React app to the storage bucket under the directory `website`.

Create a passwords file for Basic Authentication using a method like the following: https://docs.nginx.com/nginx/admin-guide/security-controls/configuring-http-basic-authentication/#creating-a-password-file

Upload the file called `YOUR_FILE_NAME` to the storage bucket under the directory `config`.

Modify the server config section to use your file name:

```
location / {
    auth_basic "Authentication is required";
    auth_basic_user_file /etc/nginx/passwords/YOUR_FILE_NAME;
}
```

# Configure the Cluster

## Add Cloud Storage FUSE CSI Driver

The cluster needs to be configured with the Cloud Storage FUSE CSI driver enabled. This enables us to mount a GCS Bucket to the K8S pods using a sidecar.

```
gcloud container clusters update CLUSTER_NAME \
    --update-addons GcsFuseCsiDriver=ENABLED \
    --region=COMPUTE_REGION
```

## Create and Configure a GKE Workload Identity

Create a K8S service account:

```
kubectl create serviceaccount KSA_NAME \
    --namespace NAMESPACE
```

Create an IAM service account:

```
gcloud iam service-accounts create GSA_NAME \
    --project=GSA_PROJECT
```

Add permissions to access the GCS bucket:

```
gcloud storage buckets add-iam-policy-binding gs://BUCKET_NAME \
    --member "serviceAccount:GSA_NAME@GSA_PROJECT.iam.gserviceaccount.com" \
    --role "ROLE_NAME"
```

For the Demo UI, you can use `roles/storage.objectViewer` as the role name since we don't need to write to the bucket.

Allow the K8S account to use the IAM role:

```
gcloud iam service-accounts add-iam-policy-binding GSA_NAME@GSA_PROJECT.iam.gserviceaccount.com \
    --role roles/iam.workloadIdentityUser \
    --member "serviceAccount:PROJECT_ID.svc.id.goog[NAMESPACE/KSA_NAME]"
```

Annotate the K8S account:

```
kubectl annotate serviceaccount KSA_NAME \
    --namespace NAMESPACE \
    iam.gke.io/gcp-service-account=GSA_NAME@GSA_PROJECT.iam.gserviceaccount.com
```

# Apply the Config

The K8S yaml config file follows the rest of the directions to setup GCS as a mount.

```
kubectl apply experimental/reporting-ui/src/main/k8s/dev/reporting_ui
```

# Verify Cluster

To verify the cluster, run the command:

```
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
