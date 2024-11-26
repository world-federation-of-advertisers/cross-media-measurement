# CLASS
## SimpleIntegrationTest

# CONSTRUCTORS

# METHODS
## vidLabel()
### USAGE
* Use org.junit.Test annotation
* A junit test for e2e vid labeling through data watcher and vid labeling app
### IMPL
1. Create a temporary test paths
a. input events
b. output events
c. location of vid model
2. Write out a test vid model (CompiledNode) to the temp vid model location
3. Create a DataWatcherConfig to watch a regex matching that test path
4. Construct a DataWatcher using that config - https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/de1008784fd4704c2384236116ad61f3d705320d/src/main/kotlin/org/wfanet/measurement/securecomputation/datawatcher/DataWatcher.kt
5. Construct a SubscribingStorageClient that wraps an InMemoryStorageClient and subscribes to that storage path - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/1a7a85bfc4969bb00e303b5faefa1be8adfb050f/src/test/kotlin/org/wfanet/measurement/storage/testing/GcsSubscribingStorageClientTest.kt
6. Create a MesosRecordIoStorageClient that wraps the SubscribingStorageClient - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/1a7a85bfc4969bb00e303b5faefa1be8adfb050f/src/test/kotlin/org/wfanet/measurement/storage/MesosRecordIoStorageClientTest.kt
7. Create some test LabelerInput and write those to the temporary test path - https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/refs/heads/main/src/test/kotlin/org/wfanet/virtualpeople/core/labeler/LabelerTest.kt
8. Create a GooglePubSubEmulatorClient - https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/1c57ac5d7f08ae6e9493c9907e82fba4147adefb/src/test/kotlin/org/wfanet/measurement/securecomputation/controlplane/v1alpha/GooglePubSubWorkItemsServiceTest.kt
9. Create CmmWork Vid Labeling Work item
a. set the input events to the blob you wrote labeler input to
b. set the output events path to the temp folder location
c. set the vid model location to the temp folder location
10. Pack the CmmWork into an Any
11. Create a CreateWorkItemRequest with the packed CmmWork
12. Send it to a workItemsService
13. Build a Subscriber to the same GooglePubSubClient with the same project and subscription id
14. Create a VidLabelerApp that uses that same Subscriber - https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/44711ab8df97f34c0d11ca6542dd8985d4b2cb1b/src/main/kotlin/org/wfanet/measurement/securecomputation/vidlabeling/VidLabeler.kt
15. Create a loop to wait up to 30 seconds for the LabelerOutput to be written to the output events folder
16. Parse the output events
