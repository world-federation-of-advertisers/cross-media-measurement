package org.wfanet.measurement.securecomputation

import com.google.protobuf.Any
import com.google.protobuf.ByteString
import java.io.BufferedInputStream
import java.net.URL
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withTimeout
import org.junit.After
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.gcloud.pubsub.Subscriber
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.GooglePubSubWorkItemsService
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.vidlabeling.VidLabelerApp
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.measurement.common.flatten
import com.google.protobuf.TextFormat

@RunWith(JUnit4::class)
class SimpleIntegrationTest() {

  @Rule @JvmField val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

  private lateinit var workItemsService: GooglePubSubWorkItemsService
  private lateinit var googlePubSubClient: GooglePubSubEmulatorClient

  private val projectId = "test-project-id"
  private val topicId = "test-topic-id"
  private val workItemId = "test-work-item-1"
  private val subscriptionId = "test-subscription-id"

  @Before
  fun setup() {
    googlePubSubClient =
      GooglePubSubEmulatorClient(
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
      val queueSubscriber = Subscriber(projectId, googlePubSubClient)
      val inMemoryStorageClient = InMemoryStorageClient()
      val subscribingStorageClient = GcsSubscribingStorageClient(inMemoryStorageClient)
      val vidLabelerApp =
        VidLabelerApp(subscribingStorageClient, subscriptionId, queueSubscriber, Any.parser())
      println("Running VID")
      val job = launch { vidLabelerApp.run() }
      println("Starting")
      val inputEventsPath = "gs://fake-bucket/input-events"
      val outputEventsPath = "gs://fake-bucket/output-events"
      val vidModelPath = "gs://fake-bucket/vid-model"

      val vidModelData =
        BufferedInputStream(
            URL(
                "https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/main/resources/labeler/single_id_model.textproto"
              )
              .openStream()
          )
          .use { input -> ByteString.readFrom(input) }
      inMemoryStorageClient.writeBlob(
        vidModelPath.removePrefix("gs://fake-bucket/"),
        flowOf(vidModelData)
      )

      val dataWatcherConfig =
        DataWatcherConfig.newBuilder()
          .setSourcePathRegex("gs://fake-bucket/input-events")
          .setQueue(
            DataWatcherConfig.QueueConfig.newBuilder()
              .setQueueName(topicId)
              .setAppConfig(
                Any.pack(
                  CmmWork.newBuilder()
                    .setVidLabelingWork(
                      CmmWork.VidLabelingWork.newBuilder()
                        .setInputBasePath(inputEventsPath)
                        .setOutputBasePath(outputEventsPath)
                        .setVidModelPath(vidModelPath)
                        .build()
                    )
                    .build()
                )
              )
              .build()
          )
          .build()

      val dataWatcher = DataWatcher(workItemsService, projectId, listOf(dataWatcherConfig))
      subscribingStorageClient.subscribe(dataWatcher)

      val labelerInputData =
        BufferedInputStream(
            URL(
                "https://raw.githubusercontent.com/world-federation-of-advertisers/virtual-people-core-serving/main/src/main/resources/labeler/labeler_input_01.textproto"
              )
              .openStream()
          )
          .use { input -> ByteString.readFrom(input) }
      val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)
      println("*********************************")
      println("IntegrationTest: Writing inputs to $inputEventsPath")
      println("*********************************")
      mesosRecordIoStorageClient.writeBlob(inputEventsPath, flowOf(labelerInputData, labelerInputData))

      withTimeout(60000) {
        while (true) {
          val blob = mesosRecordIoStorageClient.getBlob(outputEventsPath)
          if (blob != null) {
            println("*********************************")
            println("IntegrationTest: Found Outputs at $outputEventsPath")
            println("*********************************")
            val labelerOutputData = blob.read().map { byteString ->
                LabelerOutput.getDefaultInstance()
                  .newBuilderForType()
                  .apply {
                    TextFormat.Parser.newBuilder().build().merge(byteString.toStringUtf8(), this)
                  }
                  .build() as LabelerOutput
            }.toList()
            // Parse and validate labeler output
            for (data in labelerOutputData) { println("Found Data: $data") }
            break
          }
          delay(100)
        }
      }
      job.cancelAndJoin()
    }
  }
}
