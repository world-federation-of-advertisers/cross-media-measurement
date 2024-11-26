# CLASS
## DataWatcher
* Extends CloudEventsFunction - https://raw.githubusercontent.com/GoogleCloudPlatform/functions-framework-java/150676451da73a9c697af21a3c914a6a32914043/functions-framework-api/src/main/java/com/google/cloud/functions/CloudEventsFunction.java

# CONSTRUCTORS
## DataWatcher `dataWacherConfigs: List<DataWatcherConfig>)
### USAGE
### IMPL

# METHODS
## accept(event: CloudEvent)
### USAGE
* Accepts CloudEvent
### IMPL
1. Parse the StorageObjectData from the binary data field - https://raw.githubusercontent.com/cloudevents/spec/refs/heads/main/cloudevents/formats/cloudevents.proto
2. Get the blobKey and bucket from the StorageObjectData https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/1a7a85bfc4969bb00e303b5faefa1be8adfb050f/src/main/kotlin/org/wfanet/measurement/storage/testing/GcsSubscribingStorageClient.kt
3. Use a for each and loop through the dataWatcherConfigs
a. Check if the regex matches the blob url
b. If so, print the queue name