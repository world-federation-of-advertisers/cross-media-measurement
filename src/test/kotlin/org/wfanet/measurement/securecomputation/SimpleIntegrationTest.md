# CLASS
## SimpleIntegrationTest
* Use org.junit.Test

# CONSTRUCTORS

# METHODS
## vidLabel()
### USAGE
* A junit test for e2e vid labeling through data watcher and vid labeling app
### IMPL
1. Create a temporary test path
2. Create a DataWatcherConfig to watch a regex matching that test path
3. Construct a DataWatcher using that config - https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/de1008784fd4704c2384236116ad61f3d705320d/src/main/kotlin/org/wfanet/measurement/securecomputation/datawatcher/DataWatcher.kt
4. Construct a SubscribingStorageClient that wraps an InMemoryStorageClient and subscribes to that storage path - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/1a7a85bfc4969bb00e303b5faefa1be8adfb050f/src/test/kotlin/org/wfanet/measurement/storage/testing/GcsSubscribingStorageClientTest.kt
5. Create a MesosRecordIoStorageClient that wraps the SubscribingStorageClient - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/1a7a85bfc4969bb00e303b5faefa1be8adfb050f/src/test/kotlin/org/wfanet/measurement/storage/MesosRecordIoStorageClientTest.kt
6. Create some test LabelerInput and write those to the temporary test path - https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/refs/heads/main/src/test/kotlin/org/wfanet/virtualpeople/core/labeler/LabelerTest.kt
7. Create a GooglePubSubEmulatorClient - https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/1c57ac5d7f08ae6e9493c9907e82fba4147adefb/src/test/kotlin/org/wfanet/measurement/securecomputation/controlplane/v1alpha/GooglePubSubWorkItemsServiceTest.kt
8. Pack the CmmWork
