# CLASS
## SimpleIntegrationTest
### Usage
### Impl
* A Test class with an empty constructor
* Wrap the class in @RunWith(JUnit4::class)
* Package org.wfanet.measurement.securecomputation
  Set up a test rule for the GooglePubSubEmulatorProvider
  @Rule
  @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private lateinit var workItemsService: GooglePubSubWorkItemsService
  private lateinit var googlePubSubClient: GooglePubSubEmulatorClient

  private val projectId = "test-project-id"
  private val topicId = "test-topid-id"
  private val workItemId = "test-work-item-1"
  private val subscriptionId = "test-subscription-id"

  @Before
  fun setup() {
    googlePubSubClient = GooglePubSubEmulatorClient(
      host = pubSubEmulatorProvider.host,
      port = pubSubEmulatorProvider.port,
    )
    workItemsService = GooglePubSubWorkItemsService(projectId, googlePubSubClient)
    runBlocking {
      if (googlePubSubClient.topicExists(projectId, topicId)) {
        googlePubSubClient.deleteTopic(projectId, topicId)
      }
    }
  }

  @After
  fun clear() {
    runBlocking {
      if (googlePubSubClient.topicExists(projectId, topicId)) {
        googlePubSubClient.deleteTopic(projectId, topicId)
      }
    }
  }
* Here are some imports you will need:
* org.mockito.kotlin.mock
* org.wfanet.virtualpeople.common.labelerInput
* org.wfanet.virtualpeople.common.LabelerInput
* kotlinx.coroutines.flow.flowOf
* com.google.protobuf.ByteString
* org.wfanet.measurement.securecomputation.vidlabeling.VidLabelerApp
* org.wfanet.measurement.storage.testing.InMemoryStorageClient
* com.google.protobuf.Any
* org.wfanet.measurement.securecomputation.DataWatcherConfig
* org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
* org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
* kotlinx.coroutines.runBlocking
* org.wfanet.measurement.common.parseTextProto
* org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
* com.google.cloud.functions.CloudEventsFunction
* io.cloudevents.CloudEvent
* kotlinx.coroutines.delay
* java.util.UUID
* java.io.BufferedInputStream
* org.wfanet.measurement.gcloud.pubsub.Subscriber
* java.net.URL
* org.junit.runner.RunWith 
* org.junit.runners.JUnit4
* org.junit.Rule
* kotlinx.coroutines.launch
* kotlinx.coroutines.cancelAndJoin

# CONSTRUCTORS

# METHODS
## vidLabel()
### USAGE
* Use org.junit.Test annotation
* A junit test for e2e vid labeling through data watcher and vid labeling app
### IMPL
0. Any time you use writeBlob, make sure to wrap it in runBlocking {}
1. Create a temporary test paths. These are just strings. Not Files.
a. input events
b. output events
c. location of vid model
d. create a storageClient of type InMemoryStorageClient
2. Write out a test vid model (CompiledNode) to the temp vid model location 
   https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/main/resources/labeler/single_id_model.textproto
   val vidModelData = BufferedInputStream(new URL(FILE_URL).openStream()).use { input -> ByteString.readFrom(input) }
   storageClient.writeBlob(vid_model_path, vidModelData)
3. Create a DataWatcherConfig
   Create CmmWork Vid Labeling Work item
   a. set the input events to the blob you wrote labeler input to
   b. set the output events path to the temp folder location
   c. set the vid model location to the temp folder location 
   Pack the CmmWork into an Any and set that as the appConfig
   Set the queue_name as well to the topicId
   Set the regex to match the input events path
4. Construct a DataWatcher using that config - See DataWatcher.kt
   val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()
   val googlePubSubClient = GooglePubSubEmulatorClient(
     host = pubSubEmulatorProvider.host,
     port = pubSubEmulatorProvider.port,
   )
   val workItemsService = GooglePubSubWorkItemsService(projectId, googlePubSubClient)
   val dataWatcher = DataWatcher(listOf(workItemsService, projectId, listOf(dataWatcherConfig))
5. Construct a SubscribingStorageClient that wraps the main storageClient and subscribes to that storage path - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/main/src/test/kotlin/org/wfanet/measurement/storage/testing/GcsSubscribingStorageClientTest.kt
   val subscribingStorageClient = GcsSubscribingStorageClient(underlyingClient)
   subscribingStorageClient.subscribe(dataWatcher)
6. Create a MesosRecordIoStorageClient that wraps the SubscribingStorageClient - https://raw.githubusercontent.com/world-federation-of-advertisers/common-jvm/main/src/test/kotlin/org/wfanet/measurement/storage/MesosRecordIoStorageClientTest.kt
7. Create some test LabelerInput and write those to the temporary test path - https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/refs/heads/main/src/test/kotlin/org/wfanet/virtualpeople/core/labeler/LabelerTest.kt
a. You can read in this text proto again using new BufferedInputStream(new URL(FILE_URL).openStream()) - https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/main/resources/labeler/labeler_input_01.textproto
b. https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/test/kotlin/org/wfanet/virtualpeople/core/labeler/LabelerIntegrationTest.kt
8. Create a GooglePubSubEmulatorClient - https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/marcopremier/controlplane-api/src/test/kotlin/org/wfanet/measurement/securecomputation/controlplane/v1alpha/GooglePubSubWorkItemsServiceTest.kt
9. Build a pubSubClient Subscriber to the same GooglePubSubClient with the same project and subscription id
10. Create a VidLabelerApp that uses that same pubSubClient - See VidLaberlApp.kt
    Example: https://raw.githubusercontent.com/world-federation-of-advertisers/cross-media-measurement/marcopremier/tee-sdk-base-class/src/test/kotlin/org/wfanet/measurement/securecomputation/teesdk/BaseTeeApplicationTest.kt
    val queueSubscriber = Subscriber(projectId = projectId, googlePubSubClient = emulatorClient)
    val vidLabelerApp = VidLabelerApp(mesosStorageClient, subscriptionId, queueSubscriber, DiscoveredWork.parser())
    val job = launch { vidLabelerApp.run() }
11. Create a loop to wait up to 30 seconds delaying 100ms each time you check for the LabelerOutput to be written to the output events folder
12. Parse the output events of LabelerOutput
13. cancel the job - job.cancelAndJoin()

