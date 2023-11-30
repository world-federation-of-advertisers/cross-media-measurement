# Overview

Since the Demo UI is based on showing reports, it will be deployed to the 
Reporting K8S cluster. We will assume the cluster is already created.

To host the Demo UI, we are using NGINX as a simple webserver. In order to 
mount some configuration, it requires additional resource capacity. If 
the Reporting cluster was configured with the minimum, it may need to be 
reconfigured with an SSD boot disk and additional max node count. You 
can also create a new cluster in a similar fashion with the additional 
compute capabilities.

# Setup K8S and GCloud

Be sure to login to gcloud:
```
gcloud container clusters get-credentials CLUSTER_NAME --region=COMPUTE_REGION
```
Replace CLUSTER_NAME and COMPUTE_REGION with your cluster's values.

For example:
```
gcloud container clusters get-credentials demo_reporting_ui --region=us-central1
```

Be sure kubectl is set to the correct K8S context:
```
kubectl config current-context
```

This should show the full name of the reporting cluster. If not, be sure to set 
the context to the correct cluster.

```
kubectl config set-context NAME --cluster=CLUSTER_NAME
```
Replace NAME with a string you can easily identify the cluster later. This is 
an alias to make changing clusters easier.
Replace CLUSTER_NAME with the name of the cluster.

# Apply the Config

The configuration is already setup. We'll simply apply it to the cluster.

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
