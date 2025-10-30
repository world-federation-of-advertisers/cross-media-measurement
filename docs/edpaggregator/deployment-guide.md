# Components

HaloDCR is a distributed system composed of multiple services deployed across various Google Cloud components. The system architecture spans the following Google Cloud services:

* **Kubernetes (GKE)**
* **Spanner DB**
* **Confidential VMs (Managed Instance Groups)**
* **Cloud Storage**
* **Cloud Functions**
* **Google Secrets**
* **Google Pub/Sub**
* **Cloud Scheduler**

## Components description

### EDP Aggregator Storage

The **EDP Aggregator Storage** is a Cloud Storage bucket that hosts the core datasets required for the aggregation pipeline:

* **Event Groups**: Upload triggers event group registration with the Kingdom..
* **Requisitions**: Automatically polled from Kingdom.
* **Impressions and Associated Metadata**: Uploaded by EDPs. Used for requisition fulfillment.

This bucket is configured with **private access**, and only **authorized service accounts** are permitted to read or write data.

The EDP Aggregator bucket can either be a shared bucket serving all EDPs, or separate buckets provisioned per EDP.

### EDP Aggregator Storage config

The **EDP Aggregator Config Storage** is a separate Cloud Storage bucket used to store configuration files that are dynamically pulled by cloud functions at runtime. The EDP Aggregator Operator creates and manages this config.

These configuration files define operational parameters required by various services in the pipeline.

This bucket is **private** and access is restricted to **authorized service accounts** only. **Only** the EDP Aggregator Operator has access to this bucket. The EDPs, themselves, do not.

### Google secrets

EDPs, themselves, do not create secrets. This is managed by the EDP Aggregator Operator.

Used to store EDP and service certificates required for mutual authentication across EDP Aggregator components. These secrets are accessed only by authorized service accounts at runtime.

#### **Certificates used across HaloDCR:**

* ***`securecomputation-root-ca`**:*
  Root certificate for the Secure Computation API.

* ***`edpa-tee-app-tls-key`**, **`edpa-tee-app-tls-pem`***:
  Used by the application running inside the TEE to authenticate with the Secure Computation API and update the status of WorkItems after processing. Must be signed by ***`securecomputation-root-ca`***.

* ***`edpa-requisition-fetcher-tls-key`**, **`edpa-requisition-fethcer-tls-pem`***:
  Used by the RequisitionFetcher to authenticate to the Metadata Storage API. Must be signed by ***`edpaggregator-root-ca`.***

* ***`edpa-data-availability-tls-key`**, **`edpa-data-availability-tls-pem`***:
  Used by the DataAvailability to authenticate to the Metadata Storage API. Must be signed by ***`edpaggregator-root-ca`.***

* ***`edpa-data-watcher-tls-key`**, **`edpa-data-watcher-tls-pem`***:
  Used by the DataWatcher to authenticate to the Secure Computation API when creating new WorkItems. Must be signed by ***`securecomputation-root-ca`.***

* ***`kingdom-root-ca`***:
  Root certificate for authenticating with the Kingdom public API.
* ***`duchy-(number)-root-ca`***:
  Root certificate for authenticating with the duchies from the ResultsFulfiller.

#### **Per-EDP-specific certificates:**

**These certificates were previously created by EDPs but are now created and managed solely by the EDP Aggregator Operator.**

* ***`edp-cert-der`***
* ***`edp-private-der`***
* ***`edp-enc-private`***
* ***`edp-tls-key`***
* ***`edp-tls-pem`***

### Data Watcher

#### Overview

The **Data Watcher** is a Google Cloud Function automatically triggered whenever a new file is written to the **EDP Aggregator Storage** bucket. Upon invocation, it receives the Google Cloud Storage blob URI of the newly created file and determines, based on configuration, whether and how to process it.

#### Configuration

The Data Watcher relies on a configuration file that defines a list of **watched paths**.
Each watched path specifies:

* A **regular expression** used to match file paths.
* The **processing flow** to be activated when a match occurs.

When a new file path matches one of the configured regex patterns, the Data Watcher triggers the corresponding flow and executes the appropriate logic for that file type.

#### Supported Use Cases

The Data Watcher currently supports the following scenarios:

1. **Requisition Detection**
   When a new requisition file is written to storage, the Data Watcher forwards the event to the **Secure Computation API**, enabling the **Results Fulfiller** to process the requisition.

2. **Event Group and Impressions Detection**

    * **Event Groups:** When a new Event Group file is detected, the Data Watcher invokes the **EventGroupSync** function to synchronize the event group with **CMMS**.
    * **Impressions:** When an Impressions file is detected, the Data Watcher invokes the **DataAvailabilitySync** function to synchronize data availability with **CMMS**.

In practice, for each EDP, the Data Watcher configuration must include three watched paths:

* event-groups
* data-availability
* results-fulfiller

**Permissions**
The Data Watcher executes under a dedicated **service account** with the following permissions:

* Read and write access to **EDP Aggregator Storage**
* Read access to **EDP Aggregator Config Storage**
* Access to secrets in **Secret Manager**
* Event trigger permissions to invoke:
    * **EventGroupSync Cloud Function**
    * **DataAvailabilitySync Cloud Function**

#### Deployment

The Data Watcher can be deployed using the [Data Watcher module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/gcs-bucket-cloud-function).
The corresponding IAM permissions are defined in the [EDP Aggregator Terraform module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator).

##### Environment Variables

The DataWatcher needs environment variables to operate. These variables are provided using the [data\_availability\_env\_var](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L185) terraform variable.

* CERT\_FILE\_PATH \- the data\_watcher\_tls.pem file. **Must match the path defined in DataWatcher secret mapping.**
* PRIVATE\_KEY\_FILE\_PATH \- the data\_watcher\_tls.key file. **Must match the path defined in DataWatcher secret mapping.**
* CERT\_COLLECTION\_FILE\_PATH \- the secure\_computation\_root.pem file. **Must match the path defined in DataWatcher secret mapping.**
* CONTROL\_PLANE\_TARGET \- the grpc target of Secure Computation API
* CONTROL\_PLANE\_CERT\_HOST
* EDPA\_CONFIG\_STORAGE\_BUCKET \- [The config bucket](#edp-aggregator-storage-config), where the DataWatcher configuration are pulled from
* GOOGLE\_PROJECT\_ID \- The Google project id where the EDPA\_CONFIG\_STORAGE\_BUCKET is deployed

This is an example of DataWatcher env variable:

| CERT\_FILE\_PATH=/secrets/cert/data\_watcher\_tls.pem,PRIVATE\_KEY\_FILE\_PATH=/secrets/key/data\_watcher\_tls.key,CERT\_COLLECTION\_FILE\_PATH=/secrets/ca/secure\_computation\_root.pem,CONTROL\_PLANE\_TARGET=v1alpha.secure-computation.dev.halo-cmm.org:8443,CONTROL\_PLANE\_CERT\_HOST=data-watcher.secure-computation.dev.halo-cmm.org,GOOGLE\_PROJECT\_ID=halo-cmm-dev,EDPA\_CONFIG\_STORAGE\_BUCKET=gs://edpa-configs-storage-dev-bucket |
| :---- |

##### Secret Mappings

**Secret mappings** define how secrets from **Google Secret Manager** are mounted into the Cloud Function’s local file system.

When the Cloud Function starts, the specified secrets are automatically fetched from Secret Manager and made available as files in local memory at the configured mount paths.

This secret mapping is set using the [data\_watcher\_secret\_mapping](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L160) terraform variable.

This is an example of DataWatcher secret mapping:

| /secrets/key/data\_watcher\_tls.key=edpa-data-watcher-tls-key:latest,/secrets/cert/data\_watcher\_tls.pem=edpa-data-watcher-tls-pem:latest,/secrets/ca/secure\_computation\_root.pem=securecomputation-root-ca:latest |
| :---- |

**Important:**

* /secrets/key/data\_watcher\_tls.key must be the same value used for PRIVATE\_KEY\_FILE\_PATH in the [DataWatcher env var](#heading=h.6d22lnx49yj6).
* /secrets/ca/secure\_computation\_root.pem must be the same value used for CERT\_COLLECTION\_FILE\_PATH in the [DataWatcher env var](#heading=h.6d22lnx49yj6).

##### DataWatcher config file

This configuration file demonstrates how to define the three required watched paths for a single EDP named **edp7**.

```protobuf
# proto-file: wfa/measurement/config/securecomputation/data_watcher_config.proto
# proto-message: wfa.measurement.config.securecomputation.DataWatcherConfig
watched_paths {
  identifier: "event-groups"
  source_path_regex: "gs://secure-computation-storage-dev-bucket/edp7/event-groups/(.*)"
  http_endpoint_sink {
    endpoint_uri: "https://us-central1-halo-cmm-dev.cloudfunctions.net/event-group-sync"
    app_params {
      fields {
        key: "dataProvider"
        value { string_value: "dataProviders/T5RryPMNong" }
      }
      fields {
        key: "eventGroupsBlobUri"
        value { string_value: "gs://secure-computation-storage-dev-bucket/edp7/event-groups/edp7-event-group.pb" }
      }
      fields {
        key: "eventGroupMapBlobUri"
        value { string_value: "gs://secure-computation-storage-dev-bucket/edp7/event-groups-map/edp7-event-group.pb" }
      }
      fields {
        key: "cmmsConnection"
        value {
          struct_value {
            fields {
              key: "certFilePath"
              value { string_value: "/secrets/cert/edp7_tls.pem" }
            }
            fields {
              key: "privateKeyFilePath"
              value { string_value: "/secrets/key/edp7_tls.key" }
            }
            fields {
              key: "certCollectionFilePath"
              value { string_value: "/secrets/ca/kingdom_root.pem" }
            }
          }
        }
      }
      fields {
        key: "eventGroupStorage"
        value {
          struct_value {
            fields {
              key: "gcs"
              value {
                struct_value {
                  fields {
                    key: "projectId"
                    value { string_value: "halo-cmm-dev" }
                  }
                  fields {
                    key: "bucketName"
                    value { string_value: "secure-computation-storage-dev-bucket" }
                  }
                }
              }
            }
          }
        }
      }
      fields {
        key: "eventGroupMapStorage"
        value {
          struct_value {
            fields {
              key: "gcs"
              value {
                struct_value {
                  fields {
                    key: "projectId"
                    value { string_value: "halo-cmm-dev" }
                  }
                  fields {
                    key: "bucketName"
                    value { string_value: "secure-computation-storage-dev-bucket" }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
watched_paths {
  identifier: "results-fulfiller"
  source_path_regex: "gs://secure-computation-storage-dev-bucket/edp7/requisitions/(.*)"
  control_plane_queue_sink {
    queue: "results-fulfiller-queue"
    app_params {
      [type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.ResultsFulfillerParams] {
        data_provider: "dataProviders/T5RryPMNong"
        storage_params {
          labeled_impressions_blob_details_uri_prefix: "gs://secure-computation-storage-dev-bucket"
          gcs_project_id: "halo-cmm-dev"
        }
        consent_params {
          result_cs_cert_der_resource_path: "/tmp/edp_certs/edp7_cs_cert.der"
          result_cs_private_key_der_resource_path: "/tmp/edp_certs/edp7_cs_private.der"
          private_encryption_key_resource_path: "/tmp/edp_certs/edp7_enc_private.tink"
          edp_certificate_name: "dataProviders/T5RryPMNong/certificates/Zskl3_MNorU"
        }
        cmms_connection {
          client_cert_resource_path: "/tmp/edp_certs/edp7_tls.pem"
          client_private_key_resource_path: "/tmp/edp_certs/edp7_tls.key"
        }
        noise_params {
          noise_type: CONTINUOUS_GAUSSIAN
        }
      }
    }
  }
}
watched_paths {
  identifier: "data-availability"
  source_path_regex: "^gs://secure-computation-storage-dev-bucket/edp/edp7/[^/]+/done$"
  http_endpoint_sink {
    endpoint_uri: "https://us-central1-halo-cmm-dev.cloudfunctions.net/data-availability-sync"
    app_params {
      fields {
        key: "dataProvider"
        value { string_value: "dataProviders/T5RryPMNong" }
      }
      fields {
        key: "dataAvailabilityStorage"
        value {
          struct_value {
            fields {
              key: "gcs"
              value {
                struct_value {
                  fields {
                    key: "projectId"
                    value { string_value: "halo-cmm-dev" }
                  }
                  fields {
                    key: "bucketName"
                    value { string_value: "secure-computation-storage-dev-bucket" }
                  }
                }
              }
            }
          }
        }
      }
      fields {
        key: "cmmsConnection"
        value {
          struct_value {
            fields {
              key: "certFilePath"
              value { string_value: "/secrets/cert/edp7_tls.pem" }
            }
            fields {
              key: "privateKeyFilePath"
              value { string_value: "/secrets/key/edp7_tls.key" }
            }
            fields {
              key: "certCollectionFilePath"
              value { string_value: "/secrets/ca/kingdom_root.pem" }
            }
          }
        }
      }
      fields {
        key: "impressionMetadataStorageConnection"
        value {
          struct_value {
            fields {
              key: "certFilePath"
              value { string_value: "/secrets/cert/data_availability/data_availability_tls.pem" }
            }
            fields {
              key: "privateKeyFilePath"
              value { string_value: "/secrets/key/data_availability/data_availability_tls.key" }
            }
            fields {
              key: "certCollectionFilePath"
              value { string_value: "/secrets/ca/metadata_storage/edp_aggregator_root.pem" }
            }
          }
        }
      }
    }
  }
}

```

The local file path for this config file is set using the [data\_watcher\_config\_file\_path](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L111) terraform variable.

###### *Variable definition for the EventGroupSync watched path*

This configuration is passed from the **DataWatcher** to the **EventGroupSync** function when the DataWatcher triggers it. The EventGroupSync relies on this configuration to determine how to connect to CMMS, where to read input data, and where to write synchronized results.

* **identifier:** "event-groups" In case of multiple “watched path” for event groups for different EDPs, the same identifier can be used.
* **source\_path\_regex:** indicates the path to watch. In this case the path where the edp can upload the event groups.

The **`http_endpoint_sink`** block defines the target function to invoke and the parameters required for synchronization.

* **endpoint\_uri:**  The URL of the Cloud Function to be invoked, here pointing to the deployed **`event-group-sync`** function.
* **app\_params**: A structured set of parameters passed to the invoked function, that conforms to this [proto message definition](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/edpaggregator/event_group_sync_config.proto). These parameters include all the information EventGroupSync needs to connect to CMMS and access Cloud Storage resources.

Within **app\_params**, the key fields are:

* **dataProvider**: The CMMS data provider identifier associated with this EDP.
* **eventGroupsBlobUri**: The URI of the Event Group protobuf file stored in the EDP Aggregator Storage bucket.
* **eventGroupMapBlobUri**: The URI of the Event Group Map protobuf file in the same bucket.
* **cmmsConnection**: TLS connection details used by the EventGroupSync to communicate securely with the CMMS API:
    * **`certFilePath`**: Path to the edp cert file, which must match the [event group secret mapping](#secret-mappings).
    * **`privateKeyFilePath`**: Path to the edp private key file, which must match the [event group secret mapping](#secret-mappings).
    * **`certCollectionFilePath`**: Path to the root CA file, which must match the [event group secret mapping](#secret-mappings).
    * **eventGroupStorage** and **eventGroupMapStorage**: Configuration for the Cloud Storage buckets containing the Event Group and Event Group Map files, including:
        * **`projectId`**: The Google Cloud project ID where the bucket resides.
        * **`bucketName`**: The name of the Cloud Storage bucket used for these files.

###### *Variable definition for the ResultsFulfiller watched path*

This configuration is passed from the **DataWatcher** to the **Results Fulfiller** component when a new requisition file is detected in the EDP Aggregator Storage bucket. The Results Fulfiller requires this configuration to determine how to connect to CMMS, retrieve inputs, apply consent and noise parameters, and store the resulting data.

* **Identifier**: "results-fulfiller" In case of multiple “watched path” for results fulfiller for different EDPs, the same identifier can be used.
*  identifies that this configuration handles requisition fulfillment.
* **source\_path\_regex**: A regular expression that matches new files written under the EDP’s requisitions folder. When a file path matches this regex, the DataWatcher forwards the event to the Results Fulfiller via the control-plane queue.

The **control\_plane\_queue\_sink** block defines the queue and parameters that the DataWatcher uses to submit work items for processing.

* **queue**: The name of the Cloud Pub/Sub  queue where the work item is sent. In this example, it is "results-fulfiller-queue".
* **app\_params**: A structured payload, that conforms to [this protofub definition](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/edpaggregator/v1alpha/results_fulfiller_params.proto), containing all the configuration details required by the Results Fulfiller to process the requisition.

Within **app\_params**, the main configuration fields are:

* **data\_provider**: The CMMS data provider resource name.
* **storage\_params**: Configuration for reading and writing data to Cloud Storage:

    * **labeled\_impressions\_blob\_details\_uri\_prefix**: The base URI prefix for impression data.
    * **gcs\_project\_id**: The Google Cloud project ID where the storage bucket resides.

* **consent\_params**: Paths and metadata related to encryption, signing, and consent management:

    * **result\_cs\_cert\_der\_resource\_path**: Path to the DER-encoded certificate used for result signing, must match the [event group secret mapping](#secret-mappings). //TODO
    * **result\_cs\_private\_key\_der\_resource\_path**: Path to the DER-encoded private key corresponding to the result certificate, must match the [event group secret mapping](#secret-mappings). //TODO
    * **private\_encryption\_key\_resource\_path:** Path to the Tink keyset file containing the private encryption key, must match the [event group secret mapping](#secret-mappings). //TODO
    * **edp\_certificate\_name**: The fully qualified resource name of the EDP’s certificate in CMMS, must match the [event group secret mapping](#secret-mappings). //TODO

* **cmms\_connection**: TLS connection details for secure communication with CMMS:

    * **client\_cert\_resource\_path**: Path to the edp cert file, which must match the [event group secret mapping](#secret-mappings). //TODO
    * **client\_private\_key\_resource\_path**: Path to the edp key file, which must match the [event group secret mapping](#secret-mappings). // TODO
* **noise\_params**: Configuration for differential privacy noise generation, as defined in [this protobuf message](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/0e509a5f5a64acddc8761acf50c1369398347cd9/src/main/proto/wfa/measurement/internal/duchy/noise_mechanism.proto#L23).

### EventGroupSync Function

#### Overview

The **EventGroupSync** function is a Google Cloud Function triggered by the **Data Watcher** whenever a new Event Group file is stored in the **EDP Aggregator Storage** bucket.

Its primary responsibility is to synchronize the EDP’s Event Groups with the **Kingdom public API**, ensuring that all Event Groups are properly registered and updated.

To enable Event Group synchronization for a new EDP, a new **watched path** corresponding to the Event Group folder in the **EDP Aggregator Storage** bucket must be added to the [Data Watcher configuration](#configuration).

**Permissions:**

EventGroupSync runs with a service account that has access to:

* Read/write to EDP Aggregator Storage
* Access secrets from Secret Manager

#### Deployment

The Event Group Sync can be deployed using the [Http Cloud Function module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/http-cloud-function).
The corresponding IAM permissions are defined in the [EDP Aggregator Terraform module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator).

##### Environment Variables

The EventGroupSync needs environment variables to operate. These variables are provided using the [event\_group\_env\_var](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L175) terraform variable.

* KINGDOM\_TARGET \- the grpc target of the Kingdom public API**.**

This is an example of EventGroupSync env variable:

| KINGDOM\_TARGET=v2alpha.kingdom.dev.halo-cmm.org:8443 |
| :---- |

##### Secret Mappings

**Secret mappings** define how secrets from **Google Secret Manager** are mounted into the Cloud Function’s local file system.

When the Cloud Function starts, the specified secrets are automatically fetched from Secret Manager and made available as files in local memory at the configured mount paths.

This secret mapping is set using the [event\_group\_secret\_mapping](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L180) terraform variable.

This is an example of EventGroup secret mapping for two different EDP:

| /secrets/key-edp2/edpa\_meta\_tls.key=edpa\_meta-tls-key:latest,/secrets/cert-edp2/edpa\_meta\_tls.pem=edpa\_meta-tls-pem:latest,/secrets/key/edp7\_tls.key=edp7-tls-key:latest,/secrets/cert/edp7\_tls.pem=edp7-tls-pem:latest,/secrets/ca/kingdom\_root.pem=trusted-root-ca:latest |
| :---- |

Each EDP needs the tls.key and tls.pem file.

**Important:**

* /secrets/key/edp7\_tls.key must be the same value used for **privateKeyFilePath** in the **cmmsConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the [DataWatcher config file](#datawatcher-config-file).
* /secrets/cert/edp7\_tls.pem must be the same value used for **certFilePath** in the **cmmsConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the  [DataWatcher config file](#datawatcher-config-file).
* /secrets/ca/kingdom\_root.pem must be the same value used for **certCollectionFilePath** in the **cmmsConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the [DataWatcher config file](#datawatcher-config-file).

### RequisitionFetcher Function

A Google Cloud Function automatically triggered by Cloud Scheduler to pull requisitions from the Kingdom public API. If new requisitions are found, these are written to the EDP Aggregator Storage bucket for the DataWatcher to pick them up and create appropriate WorkItems.

To enable Requisitions synchronization for a new EDP, a new [**data provider requisition config**](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto) entry must be added,

The local file path for this config file is set using the [requisition\_fetcher\_config\_file\_path](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L117) terraform variable.

**Permissions:**

RequisitionFetcher runs with a service account that has access to:

* Read/write to EDP Aggregator Storage
* Read from EDP Aggregator Config Storage
* Access secrets from Secret Manager

#### Deployment

The Requisition Fetcher can be deployed using the [Http Cloud Function module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/http-cloud-function).
The corresponding IAM permissions are defined in the [EDP Aggregator Terraform module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator).

##### Environment Variables

The EventGroupSync needs environment variables to operate. These variables are provided using the [event\_group\_env\_var](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L175) terraform variable.

* KINGDOM\_TARGET \- the grpc target of the Kingdom public API**.**
* EDPA\_CONFIG\_STORAGE\_BUCKET \- the EDPA Config storage bucket**.**
* GOOGLE\_PROJECT\_ID \- the GCP where the cloud function is deployed**.**
* GRPC\_REQUEST\_INTERVAL \- throttle used to rate limit api requests**.**
* METADATA\_STORAGE\_TARGET \- the grpc target of the EDP Aggregator API**.**

This is an example of Requisition Fetcher  env variable:

| KINGDOM\_TARGET=v2alpha.kingdom.dev.halo-cmm.org:8443,EDPA\_CONFIG\_STORAGE\_BUCKET=gs://edpa-configs-storage-dev-bucket,GOOGLE\_PROJECT\_ID=halo-cmm-dev,GRPC\_REQUEST\_INTERVAL=1s,METADATA\_STORAGE\_TARGET=34.27.41.70:8443,METADATA\_STORAGE\_CERT\_HOST=localhost |
| :---- |

##### Secret Mappings

**Secret mappings** define how secrets from **Google Secret Manager** are mounted into the Cloud Function’s local file system.

When the Cloud Function starts, the specified secrets are automatically fetched from Secret Manager and made available as files in local memory at the configured mount paths.

This secret mapping is set using the [requisition\_fetcher\_secret\_mapping](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L170) terraform variable.

This is an example of Requisition Fetcher secret mapping for two different EDP:

| /secrets/key-edp2/edpa\_meta\_tls.key=edpa\_meta-tls-key:latest,/secrets/cert-edp2/edpa\_meta\_tls.pem=edpa\_meta-tls-pem:latest,/secrets/key/edp7\_tls.key=edp7-tls-key:latest,/secrets/cert/edp7\_tls.pem=edp7-tls-pem:latest,/secrets/ca/kingdom\_root.pem=trusted-root-ca:latest,/secrets/private/edp7\_enc\_private.tink=edp7-enc-private:latest,/secrets/private-edp2/edpa\_meta\_enc\_private.tink=edpa\_meta-enc-private:latest,/secrets/cert\_requisiton\_fetcher/requisition\_fetcher\_tls.pem=edpa-requisition-fetcher-tls-pem:latest,/secrets/key\_requisiton\_fetcher/requisition\_fetcher\_tls.key=edpa-requisition-fetcher-tls-key:latest,/secrets/ca/cert\_metadata\_storage/edp\_aggregator\_root.pem=edpaggregator-root-ca:latest |
| :---- |

Each EDP needs the tls.key and tls.pem file.

##### Requisition Fetcher Config file

This configuration defines how the **RequisitionFetcher** function connects to CMMS, retrieves requisitions, and stores them in the EDP Aggregator Storage bucket. It provides the function with all necessary parameters, including storage locations, security credentials, and the EDP’s unique identifiers.

```protobuf
# proto-file: wfa/measurement/config/edpaggregator/requisition_fetcher_config.proto
# proto-message: wfa.measurement.config.edpaggregator.RequisitionFetcherConfig
configs {
  data_provider: "dataProviders/T5RryPMNong"
  requisition_storage {
    gcs {
      project_id: "halo-cmm-dev"
      bucket_name: "secure-computation-storage-dev-bucket"
    }
  }
  storage_path_prefix: "edp7/requisitions"
  cmms_connection {
    cert_file_path: "/secrets/cert/edp7_tls.pem"
    private_key_file_path: "/secrets/key/edp7_tls.key"
    cert_collection_file_path: "/secrets/ca/kingdom_root.pem"
  }
  edp_private_key_path: "/secrets/private/edp7_enc_private.tink"
  requisition_metadata_storage_connection {
    cert_file_path: "/secrets/cert_requisiton_fetcher/requisition_fetcher_tls.pem"
    private_key_file_path: "/secrets/key_requisiton_fetcher/requisition_fetcher_tls.key"
    cert_collection_file_path: "/secrets/ca/cert_metadata_storage/edp_aggregator_root.pem"
  }
}
```

The above example is for a single EDP. To add a second one, a new “config” object must be added to the same file.

Each **`configs`** entry describes how the RequisitionFetcher should operate for a specific EDP.
In this example, the configuration defines the setup for **EDP `edp7`**.

* **data\_provider**: The CMMS data provider identifier associated with this EDP.
* **requisition\_storage**: Defines the Cloud Storage location where fetched requisitions are stored:

    * **`project_id`**: The Google Cloud project ID containing the bucket.
    * **`bucket_name`**: The name of the Cloud Storage bucket where requisition files are written.

* **storage\_path\_prefix**: The relative path within the storage bucket where new requisition files are stored. For **`edp7`**, requisitions are placed under **`edp7/requisitions`**.
* **cmms\_connection**: TLS configuration for secure communication with the CMMS API:

    * **`cert_file_path`**: Path to the EDP’s TLS certificate file mounted from Secret Manager, must match the [requisition fetcher secret mapping](#secret-mappings-1).
    * **`private_key_file_path`**: Path to the EDP’s private key file. This must correspond to the secret stored at **`/secrets/key/edp7_tls.key`**, , must match the [requisition fetcher secret mapping](#secret-mappings-1).
    * **`cert_collection_file_path`**: Path to the root CA certificate used to validate CMMS’s TLS certificate, , must match the [requisition fetcher secret mapping](#secret-mappings-1).

* **edp\_private\_key\_path**: Path to the Tink keyset file containing the EDP’s private encryption key, used to securely handle and decrypt fetched requisitions, , must match the [requisition fetcher secret mapping](#secret-mappings-1).
* **requisition\_metadata\_storage\_connection**: TLS configuration for secure communication with the Edp Aggregator API (AKA metadata storage. This section is identical for config objects listed in the RequisitionFetcher config file):

    * **`cert_file_path`**: Path to the Requisition Fetcher’s TLS certificate file. This must match to the secret mapped at /secrets/key\_requisiton\_fetcher/requisition\_fetcher\_tls.pem in the [requisition fetcher secret mapping](#secret-mappings-1).
    * **`private_key_file_path`**: Path to the Requisition Fetcher’s private key file. This must match to the secret mapped at /secrets/key\_requisiton\_fetcher/requisition\_fetcher\_tls.key in the [requisition fetcher secret mapping](#secret-mappings-1).
    * **`cert_collection_file_path`**: Path to the root CA certificate used to validate Edp Aggregator API (metadata storage)’s TLS certificate. This must match to the secret mapped at /secrets/ca/cert\_metadata\_storage/edp\_aggregator\_root.pem in the [requisition fetcher secret mapping](#secret-mappings-1).

### Data Availability Sync

#### Overview

The **DataAvailabilitySync** function is a Google Cloud Function triggered by the **Data Watcher** whenever a new empty file called “done” is stored in the **EDP Aggregator Storage** bucket. This empty blob is used to signal that impressions data and associated metadata for a particular day have been written to storage.

The function scans the Google Cloud Storage directory where the “done” blob was created, collects all metadata files in that location, and records impressions data availability in a Spanner database, ensuring synchronization with CMMS.

To enable Data Availability synchronization for a new EDP, a new **watched path** corresponding to the Event Group folder in the **EDP Aggregator Storage** bucket must be added to the [Data Watcher configuration](#configuration).

**Permissions:**

DataAvailabilitySync runs with a service account that has access to:

* Read/write to EDP Aggregator Storage
* Access secrets from Secret Manager

#### Deployment

The Data Availability Sync can be deployed using the [Http Cloud Function module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/http-cloud-function).
The corresponding IAM permissions are defined in the [EDP Aggregator Terraform module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator).

##### Environment Variables

The DataAvailabilitySync needs environment variables to operate. These variables are provided using the [data\_availability\_env\_var](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L185) terraform variable.

* KINGDOM\_TARGET \- the grpc target of the Kingdom public API**.**
* IMPRESSION\_METADATA\_TARGET \- the grpc target of the Edp Aggregator API (Metadata storage)**.**

This is an example of DataAvailabilitySync env variable:

| KINGDOM\_TARGET=v2alpha.kingdom.dev.halo-cmm.org:8443,IMPRESSION\_METADATA\_TARGET=system.edp-aggregator.dev.halo-cmm.org:8443 |
| :---- |

##### Secret Mappings

**Secret mappings** define how secrets from **Google Secret Manager** are mounted into the Cloud Function’s local file system.

When the Cloud Function starts, the specified secrets are automatically fetched from Secret Manager and made available as files in local memory at the configured mount paths.

This secret mapping is set using the [data\_availability\_secret\_mapping](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/variables.tf#L190) terraform variable.

This is an example of DataAvailability secret mapping for two different EDP:

| /secrets/key/edp7\_tls.key=edp7-tls-key:latest,/secrets/cert/edp7\_tls.pem=edp7-tls-pem:latest,/secrets/ca/kingdom\_root.pem=trusted-root-ca:latest,/secrets/cert/data\_availability/data\_availability\_tls.pem=edpa-data-availability-tls-pem:latest,/secrets/key/data\_availability/data\_availability\_tls.key=edpa-data-availability-tls-key:latest,/secrets/ca/metadata\_storage/edp\_aggregator\_root.pem=edpaggregator-root-ca:latest,/secrets/key\_edpa\_meta/edpa\_meta\_tls.key=edpa\_meta-tls-key:latest,/secrets/cert\_edpa\_meta/edpa\_meta\_tls.pem=edpa\_meta-tls-pem:latest |
| :---- |

Each EDP needs the tls.key and tls.pem file. The data availability leaf certs (key and cert) need to be signed with the metadata storage root certificate.

**Important:**

* /secrets/key/edp7\_tls.key must be the same value used for **privateKeyFilePath** in the **cmmsConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the [DataWatcher config file](#datawatcher-config-file).
* /secrets/cert/edp7\_tls.pem must be the same value used for **certFilePath** in the **cmmsConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the  [DataWatcher config file](#datawatcher-config-file).
* /secrets/ca/kingdom\_root.pem must be the same value used for **certCollectionFilePath** in the **cmmsConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the [DataWatcher config file](#datawatcher-config-file).
* /secrets/key/data\_availability/data\_availability\_tls.key must be the same value used for **privateKeyFilePath** in the **impressionMetadataStorageConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the [DataWatcher config file](#datawatcher-config-file).
* /secrets/cert/data\_availability/data\_availability\_tls.pem must be the same value used for **certFilePath** in the **impressionMetadataStorageConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the  [DataWatcher config file](#datawatcher-config-file).
* /secrets/ca/metadata\_storage/edp\_aggregator\_root.pem must be the same value used for **certCollectionFilePath** in the **impressionMetadataStorageConnection** definition of the watched path corresponding to the EDP’s Event Group configuration defined in the [DataWatcher config file](#datawatcher-config-file).

The Data Availability needs to access Gcloud secrets as [defined here](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/edp-aggregator/main.tf#L112-L120).

### Results fulfiller

The Results fulfiller is the application running in the Google Confidential Space (Tee app) and it’s responsible for fuflfilling requisition either to the Kingdom (direct measurement) or to duchies (hmss). It’s deployed using a set of flags and it pulls a config file when deployed with instruction on how to impersonate the EDPs that are onboarded to the system.

##### Results Fulfiller Config file

This configuration defines how the **Results Fulfiller** function connects to CMMS, retrieves requisitions, and encrypt results.

```protobuf
# proto-file: wfa/measurement/config/edpaggregator/event_data_provider_configs.proto
# proto-message: wfa.measurement.config.edpaggregator.EventDataProviderConfigs
event_data_provider_config: {
  data_provider: "dataProviders/T5RryPMNong"
  kms_config: {
    kms_audience: "//iam.googleapis.com/projects/472172784441/locations/global/workloadIdentityPools/edp-workload-identity-pool/providers/edp-wip-provider-k-res"
    service_account: "primus-sa@halo-cmm-dev-edp.iam.gserviceaccount.com"
  }
  tls_config: {
    tls_key_secret_id: "edp7-tls-key"
    tls_key_local_path: "/tmp/edp_certs/edp7_tls.key"
    tls_pem_secret_id: "edp7-tls-pem"
    tls_pem_local_path: "/tmp/edp_certs/edp7_tls.pem"
  }
  consent_signaling_config: {
    cert_der_secret_id: "edp7-cert-der"
    cert_der_local_path: "/tmp/edp_certs/edp7_cs_cert.der"
    enc_private_der_secret_id: "edp7-private-der"
    enc_private_der_local_path: "/tmp/edp_certs/edp7_cs_private.der"
    enc_private_secret_id: "edp7-enc-private"
    enc_private_local_path: "/tmp/edp_certs/edp7_enc_private.tink"
  }
}
```


In this example, the configuration defines the setup for **EDP `edp7`**. To onboard a new EDP a new **event\_data\_provider\_config**  object must be added to the config file.

* **data\_provider**: The CMMS data provider identifier associated with this EDP.
* **kms\_config**:
    * **kms\_audience:** The EDP workload identity proivider
    * **service\_account:** The edp service account with KMS access
* **tls\_config:**
    * **tls\_key\_secret\_id:** the secret ID where the EDP cert key is stored. Must match the secrets naming as explain in the [Secret ID section](#secrets-ids).
    * **tls\_key\_local\_path:** the EDP cert key path where the file is stored in the Resutls Fulfiller memory. Must match the value in the **cmms\_connection** of the results fulfiller watched path in the [Data Watcher config file.](#datawatcher-config-file)
    * **tls\_pem\_secret\_id:** the secret ID where the EDP cert is stored. Must match the secrets naming as explain in the [Secret ID section](#secrets-ids).
    * **tls\_pem\_local\_path:** the EDP cert path where the file is stored in the Resutls Fulfiller memory. Must match the value in the **cmms\_connection** of the results fulfiller watched path in the [Data Watcher config file.](#datawatcher-config-file)
* **consent\_signaling\_config:**
    * **cert\_der\_secret\_id:** the secret ID where the EDP cert der is stored. Must match the secrets naming as explain in the [Secret ID section](#secrets-ids).
    * **cert\_der\_local\_path:** the EDP cert der path where the file is stored in the Resutls Fulfiller memory. Must match the value in the **consent\_params** of the results fulfiller watched path in the [Data Watcher config file.](#datawatcher-config-file)
    * **enc\_private\_der\_secret\_id:** the secret ID where the EDP private der is stored. Must match the secrets naming as explain in the [Secret ID section](#secrets-ids).
    * **enc\_private\_der\_local\_path:** the EDP private der path where the file is stored in the Resutls Fulfiller memory. Must match the value in the **consent\_params** of the results fulfiller watched path in the [Data Watcher config file.](#datawatcher-config-file)
    * **enc\_private\_secret\_id:** the secret ID where the EDP enc privateis stored. Must match the secrets naming as explain in the [Secret ID section](#secrets-ids).
    * **enc\_private\_local\_path:** the EDP enc private
    *  path where the file is stored in the Resutls Fulfiller memory. Must match the value in the **consent\_params** of the results fulfiller watched path in the [Data Watcher config file.](#datawatcher-config-file)

###

### Secure Computation API

The Secure Computation API resides in a Google Kubernetes Engine (GKE) cluster and is accessible from the DataWatcher function whenever new requisitions are stored in the EDP Aggregator Storage Bucket. When a new WorkItem is created by the DataWatcher, it invokes the Secure Computation API, which performs the following actions:

* Stores the WorkItem in the Spanner database, which is deployed alongside the API.
* Makes a request to Google Pub/Sub to enqueue the WorkItem for processing by the Trusted Execution Environment (TEE) Application.

Different types of WorkItems are forwarded to different queues, as specified in a configuration file. If the DataWatcher attempts to enqueue an item to a non-configured queue, the API will throw an error.

### EDP Aggregator API

The EDP Aggregator API (also known as Metadata Storages) is deployed within a Google Kubernetes Engine (GKE) cluster. It is accessible by the following components:

* RequisitionFetcher
* DataAvailability functions
* ResultsFulfiller TEE app

The API provides two main storage services:

1. Requisitions Metadata Storage
    * Tracks requisitions that have already been fetched from the CMMS and persisted to Google Cloud Storage.
2. Impression Metadata Storage
    * Stores metadata related to impression data uploaded by the EDPs.

### Google Pub/Sub

Google Pub/Sub provides a queue mechanism where each queue is associated with a different type of WorkItem. Currently, there is only one queue configured for the ResultsFulfiller TEE Application, which is responsible for fulfilling requisitions.

### Managed Instance Group (MIG)

The Managed Instance Group consists of a pool of Confidential VMs that privately process WorkItem requests. Each application within a single MIG is developed using the [BaseTeeApplication](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/kotlin/org/wfanet/measurement/securecomputation/teesdk/BaseTeeApplication.kt), which automatically subscribes to the Google Pub/Sub subscription ID for the specific type of WorkItem it must listen to.

The MIG is configured with an autoscaler that monitors the number of undelivered messages in Google Pub/Sub. It automatically scales up and down to accommodate spikes in requests, ensuring efficient processing of WorkItems.

# Deployment

## EDP Aggregator Storage Bucket(s)

We recommend using **one storage bucket per EDP**.
This approach enforces **data separation** and simplifies **cost tracking** for each EDP individually.

The **Halo Terraform example** is configured with a **single bucket** by default.
If you need to enable **multiple buckets**, there is an important consideration:

**The DataWatcher function requires one trigger per bucket.**

### **Deploying DataWatcher for Multiple Buckets**

[When deploying the DataWatcher](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/gcs-bucket-cloud-function/main.tf#L96) (a Google Cloud Function v2) using the gcloud functions deploy command, a **bucket-specific event filter** is used to trigger the function:

gcloud functions deploy datawatcher \\
\--gen2 \\
\--runtime=\<runtime\> \\
\--region=\<region\> \\
\--trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \\
\--trigger-event-filters="bucket=\<bucket\_name\>" \\
\--trigger-service-account=\<service\_account\>

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

## Step 1 \- Deploy infra using Terraform

A Terraform example that sets up the EDP Aggregator infrastructure can be found [here](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf).

### **Terraform Modules**

The following modules are used to organize the infrastructure:

* edp-aggregator
* gcs-bucket-cloud-function
* http-cloud-function
* mig (managed instance group)
* pubsub
* secret
* secure-computation
* cloud-scheduler

### EDP Aggregator module

This is the top-level orchestrator module. It integrates several submodules and resources to deploy the full infrastructure required by the EDP Aggregator. Specifically, it provisions:

* **Storage Buckets**:

    * EDP Aggregator Storage bucket
    * EDP Aggregator Config Storage bucket

* **Configuration Uploads**:

    * **datawatcher** config file
    * **requisitionfetcher** config file

* **Secrets**:

    * Uploads all required TLS certificates to Secret Manager

* **Service Accounts**:

    * For the DataWatcher Cloud Function
    * For the RequisitionFetcher Cloud Function
      For the EventGroupSync Cloud Function

* **Pub/Sub**:

    * Topic and subscriptions for WorkItem delivery

* **Confidential VMs**:

    * Deploys the Results Fulfiller TEE app

* **IAM Permissions**:

    * Grants Results Fulfiller VM read/write access to EDP Aggregator Storage
    * Grants admin access to RequisitionFetcher and EventGroupSync Cloud Functions over EDP Aggregator Storage
    * Grants read access to RequisitionFetcher and DataWatcher Cloud Functions over EDP Aggregator Config Storage

**(\*)** The KEK and KMS key ring must be created by the EDPs. They are not part of the EDP Aggregator infrastructure.
The EDP Aggregator acquires access to the KEK via remote attestation, which must be configured separately. In this case, they are included in the Terraform to facilitate running the Cloud Test.

### Commands to run and variable to set

To deploy the Terraform infrastructure:

1. Create a **main.tf** file similar to the one provided in the CMMS example.
2. Run the following commands:

| terraform init terraform plan terraform apply |
| :---- |

### Terraform variables

During **terraform plan**, the following variables must be provided:

* **secure\_computation\_storage\_bucket\_name**:
  The EDP Aggregator bucket (shared across multiple EDPs).
* **edpa\_config\_files\_bucket\_name**:
  Bucket that stores Cloud Function configuration files (one per deployment).
* **terraform\_service\_account**:
  Service account used to run Terraform. Required to allow Terraform to create the Cloud Function service accounts.
* **data\_watcher\_config\_file\_path**:
  Path to the local DataWatcher config file to upload to the Config Storage bucket.
* **requisition\_fetcher\_config\_file\_path**:
  Path to the local RequisitionFetcher config file to upload to the Config Storage bucket.
* **event\_data\_provider\_configs\_file\_path**
  Path to the local EventDataProvider config file to upload to the Config Storage bucket. This is used by the \`ResultsFulfiller\` to impersonate edps against the kingdom.
* **storage\_bucket\_location**:
  Region for deploying the storage buckets.
* **kingdom\_public\_api\_target**:
  Endpoint URL for the Kingdom public API. **(\*)**
* **secure\_computation\_public\_api\_target**:
  Endpoint URL for the Secure Computation public API.
* **image\_tag**:
  Image tag for the Results Fulfiller TEE app. In the CMMS example, this assumes the image is hosted publicly on GitHub Container Registry.
* **data\_watcher\_function\_name**:
  Name of the DataWatcher function
* **requisition\_fetcher\_function\_name**:
  Name of the RequisitionFetcher function
* **event\_group\_sync\_function\_name**:
  Name of the EventGroupSync function
* **data\_watcher\_env\_var**
  Env variables needed for the DataWatcher to operate
* **data\_watcher\_secret\_mapping**
  Google Secrets ids that are mounted and made available to the Cloud function at run time
* **requisition\_fetcher\_env\_var**
  Env variables needed for the RequisitionFetcher to operate
* **requisition\_fetcher\_secret\_mapping**
  Google Secrets ids that are mounted and made available to the Cloud function at run time
* **event\_group\_env\_var**
  Env variables needed for the EventGroup Sync to operate
* **event\_group\_secret\_mapping**
  Google Secrets ids that are mounted and made available to the Cloud function at run time
* **data\_watcher\_uber\_jar\_path**
  DataWatcher Uber jar (bazel build //src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/datawatcher:DataWatcherFunction\_deploy.jar)
* **requisition\_fetcher\_uber\_jar\_path**
  DataWatcher Uber jar (bazel build //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/requisitionfetcher:RequisitionFetcherFunction\_deploy.jar)
* **event\_group\_uber\_jar\_path**
  DataWatcher Uber jar  (bazel build //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/eventgroups:EventGroupSyncFunction\_deploy.jar)
* **results\_fulfiller\_event\_proto\_descriptor\_path**
  Local path of the compiled proto message of event template
* **results\_fulfiller\_event\_proto\_descriptor\_blob\_uri**
  Google storage blob uri where the event proto descriptor is stored and can be pulled from. This variable is passed as flag to the ResultsFulfiller Application. Eg. gs://edpa-configs-storage-dev-bucket/results\_fulfiller\_event\_proto\_descriptor.pb
* **results\_fulfiller\_event\_template\_type\_name**
  Type name of the proto resource used as event template. Eg. wfa.measurement.api.v2alpha.event\_templates.testing.TestEvent
* **results\_fulfiller\_population\_spec\_blob\_uri**
  Google storage blob uri where the population spec file is stored and can be pulled from. This variable is passed as flag to the ResultsFulfiller Application
* **results\_fulfiller\_population\_spec\_file\_path**
  File path to a [population spec proto file](https://github.com/world-federation-of-advertisers/cross-media-measurement-api/blob/main/src/main/proto/wfa/measurement/api/v2alpha/population_spec.proto).
* **duchy\_worker1\_id**
  ID of the 1st duchy worker
* **duchy\_worker1\_target**
  Endpoint URL for the worker1 duchy API.
* **duchy\_worker2\_id**
  ID of the 2nd duchy worker
* **duchy\_worker2\_target**
  Endpoint URL for the worker2 duchy API.
* **results\_fulfiller\_trusted\_root\_ca\_collection\_file\_path**
  Single file path containing trusted root CA certs for:
    * Kingdom
    * Duchy worker1
    * Duchy worker2
* **data\_availability\_env\_var**
* Env variables needed for the DataAvailability Sync to operate
* **data\_availability\_secret\_mapping**
* Google Secrets ids that are mounted and made available to the Cloud function at run time
* **data\_availability\_uber\_jar\_path**
* **DataAvailability Uber jar** (bazel build //src/main/kotlin/org/wfanet/measurement/edpaggregator/deploy/gcloud/dataavailability:DataAvailabilitySyncFunction\_deploy.jar)


**(\*)** This assumes that Secure Computation API have been already deployed.


## Step 2 \- Deploy secure computation API on GKE

### Background

The configuration for the [dev environment](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/secure_computation_gke.cue) can be used as the basis for deploying Secure Computation API components using Google Kubernetes Engine (GKE) on another Google Cloud project.
This guide assumes that the [Kingdom](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/examples/kingdom) cluster has been already deployed on GKE. [This is the reference guide](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md) and this is the [terraform module](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/secure-computation/main.tf) for deploying the cluster and spanner DB.

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

| bazel build //src/main/k8s/dev:secure\_computation.tar \\   \--define google\_cloud\_project=halo-kingdom-demo \\   \--define spanner\_instance=halo-cmms \\   \--define kingdom\_public\_api\_address\_name=kingdom-v2alpha \\   \--define kingdom\_system\_api\_address\_name=kingdom-system-v1alpha \\   \--define container\_registry=ghcr.io \\   \--define image\_repo\_prefix=world-federation-of-advertisers \\   \--define image\_tag=0.5.2 |
| :---- |

Extract the generated archive to some directory. It is recommended that you extract it to a secure location, as you will be adding sensitive information to it in the following step. It is also recommended that you persist this directory so that you can use it to apply updates.

You can customize this generated object configuration with your own settings such as the number of replicas per deployment, the memory and CPU requirements of each container, and the JVM options of each container.

### Customize the K8s secrets

We use K8s secrets to hold sensitive information, such as private keys.

#### Certificates and signing keys

First, prepare all the files we want to include in the Kubernetes secret. The dev configuration assumes the files have the following names:

1. **all\_root\_certs.pem**
   This makes up the trusted root CA store. It's the concatenation of the root CA certificates for all the entities that the Secure Computation server interacts with, including:
    * All Measurement Consumers
    * Any entity which produces Measurement results (e.g. the Aggregator Duchy and Data Providers)
    * The Kingdom
2. Supposing your root certs are all in a single folder and end with \_root.pem, you can concatenate them all with a simple shell command:

####

| cat \*\_root.pem \> all\_root\_certs.pem |
| :---- |

####

Note: This assumes that all your root certificate PEM files end in newline.

3. **secure\_computation\_root.pem**
   The Secure Computation server's root CA certificate.
4. **secure\_computation\_tls.pem**
   The Secure Computation server's TLS certificate.
5. **secure\_computation\_tls.key**
   The private key for the Secure Computation server's TLS certificate.
6. **data\_watcher\_tls.pem**
   The Data Watcher Cloud Function’s TLS certificate.
7. **data\_watcher\_tls.key**
   The private key for the Data Watcher Cloud Function’'s TLS certificate.
8. **edpa\_tee\_app\_tls.pem**
   The EDPA Tee App’s TLS certificate.
9. **edpa\_tee\_app\_tls.key**
   The private key for the EDPA Tee App’s TLS certificate.

#### Testing keys

There are some [testing keys](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles) within the repository. These can be used to create the above secret for testing, but must not be used for production environments as doing so would be highly insecure.

#### Generate the archive

####

| Bazel build //src/main/k8s/testing/secretfiles:archive |
| :---- |

####

Extract the generated archive to the src/main/k8s/dev/secure\_computation\_secrets/ path within the Kustomization directory.

### Customize the K8s ConfigMap

Configuration that may frequently change is stored in a K8s configMap. The dev configuration uses one named config-files.

* queues\_config.textproto
    * [QueuesConfig](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/config/securecomputation/queues_config.proto) \[[Example](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/proto/wfa/measurement/securecomputation/controlplane/v1alpha/queues_config.textproto)\]

Place these files into the src/main/k8s/dev/secure\_computation\_config\_files/ path within the Kustomization directory.

### Apply the K8s Kustomization

Use kubectl to apply the Kustomization. From the Kustomization directory run:

| kubectl apply \-k src/main/k8s/dev/secure\_computation |
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

| NAME                                      TYPE          CLUSTER\_IP   EXTERNAL\_IP     PORTS secure-computation-internal-api-server    ClusterIp     10.92.9.75   \<none\>          8443/TCP secure-computation-public-api-server      LoadBalancer  10.92.11.73  34.55.81.140    8443:31298/TCP |
| :---- |

####

## Step 3 \- Deploy edp aggregator API on GKE

### Background

The configuration for the [dev environment](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/dev/edp_aggregator_gke.cue) can be used as the basis for deploying Edp Aggregator API components using Google Kubernetes Engine (GKE) on another Google Cloud project.
This guide assumes that the [Kingdom](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/examples/kingdom) cluster has been already deployed on GKE. [This is the reference guide](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/docs/gke/kingdom-deployment.md) and this is the [terraform edp aggregator module](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/main/src/main/terraform/gcloud/modules/edp-aggregator) for deploying the cluster and spanner DB (the resources are at the [bottom of the](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/edp-aggregator/main.tf#L464) ).

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

| bazel build //src/main/k8s/dev:edp\_aggregator.tar \\   \--define google\_cloud\_project=halo-kingdom-demo \\   \--define spanner\_instance=halo-cmms \\   \--define kingdom\_public\_api\_address\_name=kingdom-v2alpha \\   \--define kingdom\_system\_api\_address\_name=kingdom-system-v1alpha \\   \--define container\_registry=ghcr.io \\   \--define image\_repo\_prefix=world-federation-of-advertisers \\   \--define image\_tag=0.5.2 |
| :---- |

Extract the generated archive to some directory. It is recommended that you extract it to a secure location, as you will be adding sensitive information to it in the following step. It is also recommended that you persist this directory so that you can use it to apply updates.

You can customize this generated object configuration with your own settings such as the number of replicas per deployment, the memory and CPU requirements of each container, and the JVM options of each container.

### Customize the K8s secrets

We use K8s secrets to hold sensitive information, such as private keys.

#### Certificates and signing keys

First, prepare all the files we want to include in the Kubernetes secret. The dev configuration assumes the files have the following names:

10. **all\_root\_certs.pem**
    This makes up the trusted root CA store. It's the concatenation of the root CA certificates for all the entities that the Secure Computation server interacts with, including:
    * All Measurement Consumers
    * Any entity which produces Measurement results (e.g. the Aggregator Duchy and Data Providers)
    * The Kingdom
11. Supposing your root certs are all in a single folder and end with \_root.pem, you can concatenate them all with a simple shell command:

####

| cat \*\_root.pem \> all\_root\_certs.pem |
| :---- |

####

    Note: This assumes that all your root certificate PEM files end in newline.

12. **metadata\_storage\_root.pem**
    The Metada storage server's root CA certificate.
13. **secure\_computation\_root.pem**
    The Secure Computation server's root CA certificate.
14. **edp\_aggregator\_tls.pem**
    The Metadata storage server's TLS certificate.
15. **edp\_aggregator\_tls.key**
    The private key for the Metadata Storage server's TLS certificate.
16. **requisition\_fetcher\_tls.pem**
    The Requisition Fetcher Cloud Function’s TLS certificate.
17. **requisition\_fetcher\_tls.key**
    The private key for the Requisition Fetcher Cloud Function’'s TLS certificate.
18. **edpa\_tee\_app\_tls.pem**
    The EDPA Tee App’s TLS certificate.
19. **edpa\_tee\_app\_tls.key**
    The private key for the EDPA Tee App’s TLS certificate.
20. **data\_availability\_tls.pem**
    The Data Availability Cloud Function’s TLS certificate.
21. **data\_availability\_tls.key**
    The private key for the Data Availability Cloud Function’'s TLS certificate.

#### Testing keys

There are some [testing keys](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/k8s/testing/secretfiles) within the repository. These can be used to create the above secret for testing, but must not be used for production environments as doing so would be highly insecure.

#### Generate the archive

####

| Bazel build //src/main/k8s/testing/secretfiles:archive |
| :---- |

####

Extract the generated archive to the src/main/k8s/dev/edp\_aggregator\_secrets/ path within the Kustomization directory.

### Apply the K8s Kustomization

Use kubectl to apply the Kustomization. From the Kustomization directory run:

| kubectl apply \-k src/main/k8s/dev/edp\_aggregator |
| :---- |

Now all Kingdom components should be successfully deployed to your GKE cluster. You can verify by running:

| kubectl get deployments |
| :---- |

and:

| kubectl get services |
| :---- |

You should see something like the following:

####

| NAME                                                 READY   UP-TO-DATE   AVAILABLE edp-aggregator-internal-api-server-deployment    1/1     1            1 edp-aggregator-public-api-server-deployment      1/1     1            1 |
| :---- |

####

| NAME                                      TYPE          CLUSTER\_IP   EXTERNAL\_IP     PORTS edp-aggregator-internal-api-server    ClusterIp     10.92.9.75   \<none\>          8443/TCP edp-aggregator-public-api-server      LoadBalancer  10.92.11.73  34.55.81.140    8443:31298/TCP |
| :---- |

# Secrets IDs

Edp certs are uploaded to Google Secrets using secrets.
These use an automatic naming resolution that follow this pattern:

* \[edp\_name\]-cert-der
* \[edp\_name\]-private-der
* \[edp\_name\]-enc-private
* \[edp\_name\]-tls-key
* \[edp\_name\]-tls-pem

Where the EDP name is defined [in terraform](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L17). For an EDP called **edp7** secret ids will be:

* edp7-private-der
* edp7-cert-der
* edp7-enc-private
* edp7-tls-key
* edp7-tls-pem

# How to run the cloud test

Once all infrastructure has been deployed, you can verify the setup by running the **Cloud Test**.

## Prerequisites

1. **Create a new Data Provider resource** in the Kingdom by following the [existing documentation](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/main/kotlin/org/wfanet/measurement/kingdom/deploy/tools).

2. **Set up the Data Provider** by creating its KMS and Workload Identity Pool Provider. ([This draft PR](https://github.com/world-federation-of-advertisers/cross-media-measurement/pull/2749/files#diff-dc9ef56483872b486e4173bd023223b9d872be435527df747840a5addf36ba8b) can be used as a reference for creating EDP-specific resources.)

3. **Generate and encrypt synthetic data** using the Data Provider KMS. You can use the [**SyntheticDataGenerator CLI**](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/tools/GenerateSyntheticData.kt) for this. Example command:

| bazel \--host\_jvm\_args=-Xmx20g run //src/main/kotlin/org/wfanet/measurement/loadtest/edpaggregator/tools:GenerateSyntheticData \-- \--event-group-reference-id=event-group-reference-id/edpa-eg-reference-id-1 \--output-bucket=secure-computation-storage-dev-bucket \--schema=gs:// \--kms-type=GCP \--kek-uri=gcp-kms://projects/halo-cmm-dev-edp/locations/global/keyRings/edp-key-ring/cryptoKeys/edp-kek \--population-spec-resource-path=small\_population\_spec.textproto \--data-spec-resource-path=small\_data\_spec.textproto |
| :---- |

(\*) Note that \--event-group-reference-id must follow the pattern: event-group-reference-id/\<value\_you\_want\_to\_use\_here\>
(\*\*) \--kek-uri points to the EDP key encryption key

4. **Launch the Cloud Test**, which consists of the following steps:



## [EDPA Cloud Test](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt#L303) Steps

1. [**Event Group Creation**](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt#L124)
2. [**Upload Event Group**](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt#L112) to the storage bucket created for the EDP.
3. **Create a Measurement Request**.
4. [**Trigger the RequisitionFetcher**](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/test/kotlin/org/wfanet/measurement/integration/k8s/EdpAggregatorCorrectnessTest.kt#L191) to pull the newly created requisitions.
5. **Store requisitions** in the storage bucket. The **DataWatcher** detects them, creates a **WorkItem request**, and sends it to the **Secure Computation API**.
6. **Secure Computation API** stores the request in the Spanner DB (**WorkItem** table) and publishes a message to **Google Pub/Sub**.
7. **ResultsFulfiller application**, acting as a Google Pub/Sub subscriber, receives the message, processes it, and fulfills the requisitions against the Kingdom.
8. **Evaluate the results**.

## EDP Workload Identity Pool Provider Creation

To enable attestation, a **Workload Identity Pool Provider** must be created for the EDP.
Attestation based on **Signature Builds** will be available in future releases.
For now, the attribute set that can be validated is:

* The application runs inside a Confidential Space
* The application uses a **STABLE** (production) disk boot image
* The request originates from the expected service account (the one running in the VM where the **ResultsFulfiller** app is deployed)
  **Steps to create the provider:**

Log into the **EDP Google account** and run:

```text
gcloud iam workload-identity-pools providers create-oidc <provider_name> \
  --location="global" \
  --workload-identity-pool="<name configured when deploying EDP resources>" \
  --issuer-uri="https://confidentialcomputing.googleapis.com/" \
  --allowed-audiences="https://sts.googleapis.com" \
  --attribute-mapping="google.subject='assertion.sub'" \
  --attribute-condition="assertion.swname == 'CONFIDENTIAL_SPACE' &&
    'STABLE' in assertion.submods.confidential_space.support_attributes &&
    '<service_account running in the ResultsFulfiller VM>' in assertion.google_service_accounts"
```

**Note:**
If you are running a **debug confidential image** (`confidential-space-debug`), the condition
`'STABLE' in assertion.submods.confidential_space.support_attributes` will not work.
You must remove it from the `--attribute-condition` parameter.

## Debugging Notes

The current Terraform configuration uses a **production SEV Confidential Space image type**, which does **not** support logging.
Additionally, the image must be built using a **base image with root access** ([link1](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/src/main/kotlin/org/wfanet/measurement/edpaggregator/resultsfulfiller/BUILD.bazel#L181) and [link2](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/c925d452f37785f822d80c4ca49b7dcfa03fbd03/MODULE.bazel#L359)), otherwise it will not be able to read the OIDC attestation token at runtime.

For easier debugging, it is recommended to use a **debug base image** with logging enabled:

1. Replace `"confidential-space"` with `"confidential-space-debug"` [here](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/cmms/edp_aggregator.tf#L195).
2. Add the following metadata entry [here](https://github.com/world-federation-of-advertisers/cross-media-measurement/blob/main/src/main/terraform/gcloud/modules/mig/main.tf#L19):
   `tee-container-log-redirect = "true"`

* **\--model-line** with an existing value from the DB
* **\--kek-uri** with the kek uri of edp 7

The command can temporary be run using this branch: [marcopremier/update\_synthetic\_data\_generation](https://github.com/world-federation-of-advertisers/cross-media-measurement/tree/marcopremier/update_synthetic_data_generation).