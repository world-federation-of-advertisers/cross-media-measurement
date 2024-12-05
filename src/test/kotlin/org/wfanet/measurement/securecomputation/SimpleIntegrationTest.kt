package org.wfanet.measurement.securecomputation

import org.mockito.kotlin.mock
import org.wfanet.virtualpeople.common.labelerInput
import kotlinx.coroutines.flow.flowOf
import com.google.protobuf.ByteString
import org.wfanet.measurement.securecomputation.vidlabeling.VidLabelerApp
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import com.google.protobuf.Any
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.parseTextProto
import com.google.cloud.functions.CloudEventsFunction
import kotlinx.coroutines.delay
import com.google.protobuf.util.JsonFormat
import io.grpc.StatusRuntimeException
import io.grpc.Status
import io.cloudevents.CloudEvent
import java.nio.charset.StandardCharsets
import java.util.UUID
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.flowOf
import org.junit.Test
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import java.io.BufferedInputStream
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import java.net.URL
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.Rule
import org.junit.After
import org.junit.Before
import kotlinx.coroutines.launch
import kotlinx.coroutines.cancelAndJoin

@RunWith(JUnit4::class)
class SimpleIntegrationTest() {

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

    @Test
    fun vidLabel() {
      runBlocking {
        googlePubSubClient.createTopic(projectId, topicId)
        googlePubSubClient.createSubscription(projectId, subscriptionId, topicId)
        println("Starting")
        val inputEventsPath = "gs://bucket/input-events"
        val outputEventsPath = "gs://bucket/output-events"
        val vidModelPath = "gs://bucket/vid-model"

        val storageClient = InMemoryStorageClient()

        val vidModelData = BufferedInputStream(URL("https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/main/resources/labeler/single_id_model.textproto").openStream()).use { input ->
          ByteString.readFrom(input)
        }
        storageClient.writeBlob(vidModelPath, flowOf(vidModelData))

        val dataWatcherConfig = DataWatcherConfig.newBuilder()
          .setSourcePathRegex("gs://bucket/input.*")
          .setQueue(DataWatcherConfig.QueueConfig.newBuilder()
            .setQueueName(topicId)
            .setAppConfig(Any.pack(CmmWork.newBuilder().setVidLabelingWork(CmmWork.VidLabelingWork.newBuilder()
              .setInputBasePath(inputEventsPath)
              .setOutputBasePath(outputEventsPath)
              .setVidModelPath(vidModelPath)
              .build()).build()))
            .build())
          .build()

        val dataWatcher = DataWatcher(workItemsService, projectId, listOf(dataWatcherConfig))

        val subscribingStorageClient = GcsSubscribingStorageClient(storageClient)
        subscribingStorageClient.subscribe(dataWatcher)

        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)

        val labelerInputData = BufferedInputStream(URL("https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/main/resources/labeler/labeler_input_01.textproto").openStream()).use { input ->
          ByteString.readFrom(input)
        }
        storageClient.writeBlob(inputEventsPath, flowOf(labelerInputData))

        val queueSubscriber = Subscriber(projectId, googlePubSubClient)
        val vidLabelerApp = VidLabelerApp(mesosRecordIoStorageClient, subscriptionId, queueSubscriber, DiscoveredWork.parser())
        println("Running VID")
        val job = launch { vidLabelerApp.run() }

        val labelerOutputPath = "gs://bucket/output-events/labeler-output"
        println("waiting")
        withTimeout(5000) {
          while (true) {
            val blob = mesosRecordIoStorageClient.getBlob(labelerOutputPath)
            if (blob != null) {
              val labelerOutputData = blob.read().toList()
              // Parse and validate labeler output
              println("Found Data")
              break
            } else {
              println("Did not find data")
            }
            delay(100)
          }
        }
        job.cancelAndJoin()

    }
  }
}
