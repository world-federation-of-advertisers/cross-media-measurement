# CLASS
## DataWatcher
* Extends CloudEventsFunction - https://raw.githubusercontent.com/GoogleCloudPlatform/functions-framework-java/main/functions-framework-api/src/main/java/com/google/cloud/functions/CloudEventsFunction.java
* Use the following imports
* org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsService
* com.google.events.cloud.storage.v1.StorageObjectData
* org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
* org.wfanet.measurement.securecomputation.DataWatcherConfig
* com.google.cloud.functions.CloudEventsFunction
* org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
* java.nio.charset.StandardCharsets
* org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
* org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
* org.wfanet.measurement.common.pack
* com.google.protobuf.Any

# CONSTRUCTORS
## DataWatcher `(workItemsService: GooglePubSubWorkItemsService, projectId: String, dataWatcherConfigs: List<DataWatcherConfig>)`
### USAGE
### IMPL

# METHODS
## accept(event: CloudEvent)
### USAGE
* Accepts CloudEvent
### IMPL
1. Parse the StorageObjectData from the binary data field - https://raw.githubusercontent.com/cloudevents/spec/refs/heads/main/cloudevents/formats/cloudevents.proto
2. Get the blobKey and bucket from the StorageObjectData and construct a path - https://raw.githubusercontent.com/GoogleCloudPlatform/java-docs-samples/main/functions/v2/hello-gcs/src/main/java/functions/HelloGcs.java
3. Use a for each and loop through the dataWatcherConfigs and check if the regex matches the blob url
a. If so, create a CreateWorkItemRequest using CreateWorkItemRequest.newBuilder(). Get the queue name and the work item params from the data watcher config. Set the name to a random UUID
   val workItemParams: Any = DiscoveredWork.newBuilder()
    .setType(queueConfig.appConfig.pack())
    .setPath(path)
    .build()
    .pack()
   val workItem = WorkItem.newBuilder()
     .setName("workItems/$workItemId")
     .setQueue(topicId)
     .setWorkItemParams(workItemParams)
     .build()
   val createWorkItemRequest = CreateWorkItemRequest.newBuilder()
     .setWorkItemId(workItemId)
     .setWorkItem(workItem)
     .build()
b. Call createWorkItem. https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/refs/heads/marcopremier/controlplane-api/src/test/kotlin/org/wfanet/measurement/securecomputation/controlplane/v1alpha/GooglePubSubWorkItemsServiceTest.kt
   Make sure to wrap it in runBlocking {}