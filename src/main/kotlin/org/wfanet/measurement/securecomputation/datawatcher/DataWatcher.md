# CLASS
## DataWatcher
* Extends CloudEventsFunction - https://raw.githubusercontent.com/GoogleCloudPlatform/functions-framework-java/main/functions-framework-api/src/main/java/com/google/cloud/functions/CloudEventsFunction.java

# CONSTRUCTORS
## DataWatcher `(workItemsService: WorkItemsService, topicId: String, dataWatcherConfigs: List<DataWatcherConfig>)`
### USAGE
### IMPL

# METHODS
## accept(event: CloudEvent)
### USAGE
* Accepts CloudEvent
### IMPL
1. Parse the StorageObjectData from the binary data field - https://raw.githubusercontent.com/cloudevents/spec/refs/heads/main/cloudevents/formats/cloudevents.proto
2. Get the blobKey and bucket from the StorageObjectData https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/main/src/main/kotlin/org/wfanet/measurement/storage/testing/GcsSubscribingStorageClient.kt
3. Use a for each and loop through the dataWatcherConfigs
a. Check if the regex matches the blob url
b. If so, publish a message. Get the queue name and the message from the data watcher config  
