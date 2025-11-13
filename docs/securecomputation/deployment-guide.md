# Components

Secure Computation is a distributed system composed of multiple services deployed across various Google Cloud components. The system architecture spans the following Google Cloud services:

* **Kubernetes (GKE)**
* **Spanner DB**
* **Confidential VMs (Managed Instance Groups)**
* **Cloud Storage**
* **Cloud Functions**
* **Google Secrets**
* **Google Pub/Sub**

## Components description

### Data Watcher Storage

### Data Watcher Storage config

The **Data Watcher Config Storage** is a separate Cloud Storage bucket used to store configuration files that are dynamically pulled by cloud functions at runtime.

These configuration files define operational parameters required by various services in the pipeline.

This bucket is **private** and access is restricted to **authorized service accounts** only. **Only** the Secure Computation API Operator has access to this bucket.

### Google secrets

Secrets are managed by the Secure Computation Operator.

Used to certificates required for mutual authentication across components. These secrets are accessed only by authorized service accounts at runtime.

#### **Certificates:**

* ***`securecomputation-root-ca`**:*
  Root certificate for the Secure Computation API.

* ***`tee-app-tls-key`**, **`tee-app-tls-pem`***:
  Used by the application running inside the TEE to authenticate with the Secure Computation API and update the status of WorkItems after processing. Must be signed by ***`securecomputation-root-ca`***.

* ***`data-watcher-tls-key`**, **`data-watcher-tls-pem`***:
  Used by the DataWatcher to authenticate to the Secure Computation API when creating new WorkItems. Must be signed by ***`securecomputation-root-ca`.***

### Data Watcher

#### Overview

The **Data Watcher** is a Google Cloud Function automatically triggered whenever a new file is written to the **Secure Computation Storage** bucket. Upon invocation, it receives the Google Cloud Storage blob URI of the newly created file and determines, based on configuration, whether and how to process it.

#### Configuration

The Data Watcher relies on a configuration file that defines a list of **watched paths**.
Each watched path specifies:

* A **regular expression** used to match file paths.
* The **processing flow** to be activated when a match occurs.

When a new file path matches one of the configured regex patterns, the Data Watcher triggers the corresponding flow and executes the appropriate logic for that file type.

#### Supported Use Cases

The Data Watcher currently supports the following scenarios:

1. **Secure Computation Sink**

2. **Cloud Function Sync**

**Permissions**
The Data Watcher executes under a dedicated **service account** with the following permissions:

* Read access to **Config Storage**
* Access to secrets in **Secret Manager**
* Event trigger permissions to invoke any cloud functions.

#### Deployment

The Data Watcher can be deployed using the [Data Watcher module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/gcs-bucket-cloud-function).

##### Environment Variables

The DataWatcher needs environment variables to operate. These variables are provided using the [data_availability_env_var](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L185) terraform variable.

* CERT_FILE_PATH - the data_watcher_tls.pem file. **Must match the path defined in DataWatcher secret mapping.**
* PRIVATE_KEY_FILE_PATH - the data_watcher_tls.key file. **Must match the path defined in DataWatcher secret mapping.**
* CERT_COLLECTION_FILE_PATH - the secure_computation_root.pem file. **Must match the path defined in DataWatcher secret mapping.**
* CONTROL_PLANE_TARGET - the grpc target of Secure Computation API
* CONTROL_PLANE_CERT_HOST
* CONFIG_STORAGE_BUCKET - where the DataWatcher configuration are pulled from
* GOOGLE_PROJECT_ID - The Google project id where the CONFIG_STORAGE_BUCKET is deployed

This is an example of DataWatcher env variable:

| CERT_FILE_PATH=/secrets/cert/data_watcher_tls.pem,PRIVATE_KEY_FILE_PATH=/secrets/key/data_watcher_tls.key,CERT_COLLECTION_FILE_PATH=/secrets/ca/secure_computation_root.pem,CONTROL_PLANE_TARGET=v1alpha.secure-computation.dev.halo-cmm.org:8443,CONTROL_PLANE_CERT_HOST=data-watcher.secure-computation.dev.halo-cmm.org,GOOGLE_PROJECT_ID=halo-cmm-dev,CONFIG_STORAGE_BUCKET=gs://configs-storage-dev-bucket |
|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|

##### Secret Mappings

**Secret mappings** define how secrets from **Google Secret Manager** are mounted into the Cloud Function’s local file system.

When the Cloud Function starts, the specified secrets are automatically fetched from Secret Manager and made available as files in local memory at the configured mount paths.

This secret mapping is set using the [data_watcher_secret_mapping](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L160) terraform variable.

This is an example of DataWatcher secret mapping:

| /secrets/key/data_watcher_tls.key=data-watcher-tls-key:latest,/secrets/cert/data_watcher_tls.pem=data-watcher-tls-pem:latest,/secrets/ca/secure_computation_root.pem=securecomputation-root-ca:latest |
|:------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|

**Important:**

* /secrets/key/data_watcher_tls.key must be the same value used for PRIVATE_KEY_FILE_PATH in the [DataWatcher env var](#heading=h.6d22lnx49yj6).
* /secrets/ca/secure_computation_root.pem must be the same value used for CERT_COLLECTION_FILE_PATH in the [DataWatcher env var](#heading=h.6d22lnx49yj6).

##### DataWatcher config file

```protobuf
# proto-file: wfa/measurement/config/securecomputation/data_watcher_config.proto
# proto-message: wfa.measurement.config.securecomputation.DataWatcherConfig
watched_paths {
  identifier: "cloud-function-sink"
  source_path_regex: "gs://secure-computation-storage-dev-bucket/some-source-path/(.*)"
  http_endpoint_sink {
    endpoint_uri: "https://some-cloud-function/endpoint"
    app_params {
      ...
    }
  }
}
watched_paths {
  identifier: "control-plane-sink"
  source_path_regex: "gs://secure-computation-storage-dev-bucket/some-other-source-path/(.*)"
  control_plane_queue_sink {
    queue: "some-work-queue"
    app_params {
      ...
    }
  }
}
```

The local file path for this config file is set using the [data_watcher_config_file_path](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L111) terraform variable.

###### *Variable definition for the cloud function watched path*

This configuration is passed from the **DataWatcher** to the cloud function when the DataWatcher triggers it.

* **identifier:** unique identifier
* **source_path_regex:** indicates the path to watch. 

The **`http_endpoint_sink`** block defines the target function to invoke and the parameters required for synchronization.

* **endpoint_uri:**  The URL of the Cloud Function to be invoked.
* **app_params**: A structured set of parameters passed to the invoked function.

###### *Variable definition for the watched paths*

This configuration is passed from the **DataWatcher** to the **Secure Computation API** component.

* **Identifier**: unique identifier for each type of watched path.
* **source_path_regex**: A regular expression that matches new files written. When a file path matches this regex, the DataWatcher forwards the event to the sink.

The **control_plane_queue_sink** block defines the queue and parameters that the DataWatcher uses to submit work items for processing.

* **queue**: The name of the Cloud Pub/Sub  queue where the work item is sent.
* **app_params**: A structured payload containing all the configuration details required by the TEE App to process inputs and produce necessary outputs.

### Secure Computation API

The Secure Computation API resides in a Google Kubernetes Engine (GKE) cluster and is accessible from the DataWatcher function. When a new WorkItem is created by the DataWatcher, it invokes the Secure Computation API, which performs the following actions:

* Stores the WorkItem in the Spanner database, which is deployed alongside the API.
* Makes a request to Google Pub/Sub to enqueue the WorkItem for processing by the Trusted Execution Environment (TEE) Application.

Different types of WorkItems are forwarded to different queues, as specified in a configuration file. If the DataWatcher attempts to enqueue an item to a non-configured queue, the API will throw an error.

### Google Pub/Sub

Google Pub/Sub provides a queue mechanism where each queue is associated with a different type of WorkItem.

### Managed Instance Group (MIG)

The Managed Instance Group consists of a pool of Confidential VMs that privately process WorkItem requests. Each application within a single MIG is developed using the [BaseTeeApplication](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/teesdk/BaseTeeApplication.kt), which automatically subscribes to the Google Pub/Sub subscription ID for the specific type of WorkItem it must listen to.

The MIG is configured with an autoscaler that monitors the number of undelivered messages in Google Pub/Sub. It automatically scales up and down to accommodate spikes in requests, ensuring efficient processing of WorkItems.

# Deployment

### **Deploying DataWatcher for Multiple Buckets**

[When deploying the DataWatcher](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/gcs-bucket-cloud-function/main.tf#L96) (a Google Cloud Function v2) using the gcloud functions deploy command, a **bucket-specific event filter** is used to trigger the function:

gcloud functions deploy datawatcher \\
\--gen2 \\
\--runtime=\<runtime\> \\
\--region=\<region\> \\
\--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \\
\--trigger-event-filters="bucket=\<bucket_name\>" \\
\--trigger-service-account=\<service_account\>

For **multiple buckets**, a single trigger cannot be  configured to listen to more than one bucket. Instead, there must be **one trigger per bucket**.

**Single Function (DataWatcher), Multiple Event Triggers**
**Create multiple event triggers** pointing to the same function code. Currently, gcloud functions deploy only allows specifying one trigger per deployment, so this need to change to:

* Create the function without a trigger, and
* Use gcloud eventarc triggers create to add multiple triggers, each with its own bucket filter.

Example:

Deploy the function without a trigger
```text
gcloud functions deploy datawatcher \
  --gen2 \
  --runtime=<runtime> \
  --region=<region> \
  --no-trigger \
  --service-account=<service_account>

```
Add triggers for each bucket
```text
gcloud eventarc triggers create datawatcher-trigger-bucket-a \
  --location=<region> \
  --destination-run-service=datawatcher \
  --destination-run-region=<region> \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=<bucket_a>"
```
```text
gcloud eventarc triggers create datawatcher-trigger-bucket-b \
  --location=<region> \
  --destination-run-service=datawatcher \
  --destination-run-region=<region> \
  --event-filters="type=google.cloud.storage.object.v1.finalized" \
  --event-filters="bucket=<bucket_b>"
```

## Deploy secure computation API on GKE

### Background

The configuration for the [dev environment](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/secure_computation_gke.cue) can be used as the basis for deploying Secure Computation API components using Google Kubernetes Engine (GKE) on another Google Cloud project.
This is the [terraform module](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/secure-computation/main.tf) for deploying the cluster and spanner DB.

#### *Disclaimer*:

* This guide is just one way of achieving the goal. Other equally valid deployment paths exist.
* Almost all steps can be done via either the [Google Cloud Console](https://console.cloud.google.com/) UI or the [gcloud CLI](https://cloud.google.com/sdk/gcloud/reference). The doc picks the easier one for each step. But you are free to do it in an alternative way.
* All names used in this doc can be replaced with something else. We use specific names in the doc for ease of reference.
* All quotas and resource configs are just examples, adjust the quota and size based on the actual usage.
* In the doc, we assume we are deploying to a single region, i.e. us-central1. If you are deploying to another region or multiple regions, just need to adjust each step mentioning "region" accordingly.

### [Build and Push the Container images](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md#build-and-push-the-container-images-optional)

### Generate the K8s Kustomization

Populating a cluster is generally done by applying a K8s Kustomization. You can use the dev configuration as a base to get started. The Kustomization is generated using Bazel rules from files written in [CUE](https://cuelang.org/).

To generate the dev Kustomization, run the following (substituting your own values):

| bazel build //src/main/k8s/dev:secure_computation.tar \\   \--define google_cloud_project=halo-kingdom-demo \\   \--define spanner_instance=halo-cmms \\   \--define kingdom_public_api_address_name=kingdom-v2alpha \\   \--define kingdom_system_api_address_name=kingdom-system-v1alpha \\   \--define container_registry=ghcr.io \\   \--define image_repo_prefix=world-federation-of-advertisers \\   \--define image_tag=0.5.2 |
| :---- |

Extract the generated archive to some directory. It is recommended that you extract it to a secure location, as you will be adding sensitive information to it in the following step. It is also recommended that you persist this directory so that you can use it to apply updates.

You can customize this generated object configuration with your own settings such as the number of replicas per deployment, the memory and CPU requirements of each container, and the JVM options of each container.

### Customize the K8s secrets

We use K8s secrets to hold sensitive information, such as private keys.

#### Certificates and signing keys

First, prepare all the files we want to include in the Kubernetes secret. The dev configuration assumes the files have the following names:

1. **all_root_certs.pem**
   This makes up the trusted root CA store. It's the concatenation of the root CA certificates for all the entities that the Secure Computation server interacts with.
2. Supposing your root certs are all in a single folder and end with _root.pem, you can concatenate them all with a simple shell command:

####

| cat \*_root.pem \> all_root_certs.pem |
| :---- |

####

Note: This assumes that all your root certificate PEM files end in newline.

3. **secure_computation_root.pem**
   The Secure Computation server's root CA certificate.
4. **secure_computation_tls.pem**
   The Secure Computation server's TLS certificate.
5. **secure_computation_tls.key**
   The private key for the Secure Computation server's TLS certificate.
6. **data_watcher_tls.pem**
   The Data Watcher Cloud Function’s TLS certificate.
7. **data_watcher_tls.key**
   The private key for the Data Watcher Cloud Function’'s TLS certificate.
8. **tee_app_tls.pem**
   The Tee App’s TLS certificate.
9. **tee_app_tls.key**
   The private key for the Tee App’s TLS certificate.

#### Testing keys

There are some [testing keys](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles) within the repository. These can be used to create the above secret for testing, but must not be used for production environments as doing so would be highly insecure.

#### Generate the archive

####

| Bazel build //src/main/k8s/testing/secretfiles:archive |
| :---- |

####

Extract the generated archive to the src/main/k8s/dev/secure_computation_secrets/ path within the Kustomization directory.

### Customize the K8s ConfigMap

Configuration that may frequently change is stored in a K8s configMap. The dev configuration uses one named config-files.

* queues_config.textproto
    * [QueuesConfig](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/securecomputation/queues_config.proto) \[[Example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/securecomputation/controlplane/v1alpha/queues_config.textproto)\]

Place these files into the src/main/k8s/dev/secure_computation_config_files/ path within the Kustomization directory.

### Apply the K8s Kustomization

Use kubectl to apply the Kustomization. From the Kustomization directory run:

| kubectl apply \-k src/main/k8s/dev/secure_computation |
| :---- |

Now all Kingdom components should be successfully deployed to your GKE cluster. You can verify by running:

| kubectl get deployments |
| :---- |

and:

| kubectl get services |
| :---- |

You should see something like the following:

####

| NAME                                                 READY   UP-TO-DATE   AVAILABLE secure-computation-internal-api-server-deployment    1/1     1            1 secure-computation-public-api-server-deployment      1/1     1            1 |
| :---- |

####

| NAME                                      TYPE          CLUSTER_IP   EXTERNAL_IP     PORTS secure-computation-internal-api-server    ClusterIp     10.92.9.75   \<none\>          8443/TCP secure-computation-public-api-server      LoadBalancer  10.92.11.73  34.55.81.140    8443:31298/TCP |
| :---- |

## Workload Identity Pool Provider Creation

To enable attestation, a **Workload Identity Pool Provider** must be created.
Attestation based on **Signature Builds** will be available in future releases.
For now, the attribute set that can be validated is:

* The application runs inside a Confidential Space
* The application uses a **STABLE** (production) disk boot image
* The request originates from the expected service account (the one running in the VM where the **TEE** app is deployed)
  **Steps to create the provider:**

Log into the **KMS Google account** and run:

```text
gcloud iam workload-identity-pools providers create-oidc <provider_name> \
  --location="global" \
  --workload-identity-pool="<name configured when deploying resources>" \
  --issuer-uri="https://confidentialcomputing.googleapis.com/" \
  --allowed-audiences="https://sts.googleapis.com" \
  --attribute-mapping="google.subject='assertion.sub'" \
  --attribute-condition="assertion.swname == 'CONFIDENTIAL_SPACE' &&
    'STABLE' in assertion.submods.confidential_space.support_attributes &&
    '<service_account running in the VM>' in assertion.google_service_accounts"
```

**Note:**
If you are running a **debug confidential image** (`confidential-space-debug`), the condition
`'STABLE' in assertion.submods.confidential_space.support_attributes` will not work.
You must remove it from the `--attribute-condition` parameter.

## Debugging Notes

The current Terraform configuration uses a **production SEV Confidential Space image type**, which does **not** support logging.
Additionally, the image must be built using a **base image with root access* [link1](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/MODULE.bazel#L359)), otherwise it will not be able to read the OIDC attestation token at runtime.

For easier debugging, it is recommended to use a **debug base image** with logging enabled:

1. Replace `"confidential-space"` with `"confidential-space-debug"` 
2. Add the following metadata entry [here](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/mig/main.tf#L19):
   `tee-container-log-redirect = "true"`
