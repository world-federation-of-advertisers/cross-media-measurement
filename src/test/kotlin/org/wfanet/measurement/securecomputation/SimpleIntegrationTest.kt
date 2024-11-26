package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import kotlin.test.Test
import kotlin.test.BeforeTest
import kotlin.test.AfterTest
import kotlin.test.BeforeClass
import kotlin.test.AfterClass
import org.junit.Test
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.measurement.storage.GcsSubscribingStorageClient
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import com.google.protobuf.Any
import com.google.protobuf.StringValue
import com.google.pubsub.v1.TopicName
import org.wfanet.measurement.securecomputation.CmmWork
import org.wfanet.measurement.securecomputation.vidlabeling.VidLabelerApp

class SimpleIntegrationTest {

    fun vidLabel() = runBlocking {
        val inputEventsPath = Files.createTempFile("input-events", ".tmp").toString()
        val outputEventsPath = Files.createTempFile("output-events", ".tmp").toString()
        val vidModelPath = Files.createTempFile("vid-model", ".tmp").toString()

        val compiledNode = CompiledNode.newBuilder().setName("test-node").build()
        Files.write(Path.of(vidModelPath), compiledNode.toByteArray())

        val dataWatcherConfig = DataWatcherConfig.newBuilder().setSourcePathRegex(".*").build()
        val dataWatcher = DataWatcher(listOf(dataWatcherConfig))

        val inMemoryStorageClient = InMemoryStorageClient()
        val subscribingStorageClient = GcsSubscribingStorageClient(inMemoryStorageClient)
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)

        val labelerInput = LabelerInput.newBuilder().setEventId("test-event").build()
        inMemoryStorageClient.writeBlob(inputEventsPath, flowOf(labelerInput.toByteString()))

        val googlePubSubClient = GooglePubSubEmulatorClient()
        googlePubSubClient.startEmulator()
        val topicName = TopicName.of("test-project-id", "test-queue")
        googlePubSubClient.createTopic(topicName.project(), topicName.topic())

        val vidLabelingWork = CmmWork.VidLabelingWork.newBuilder()
            .setInputEventsPath(inputEventsPath)
            .setOutputEventsPath(outputEventsPath)
            .setVidModelPath(vidModelPath)
            .build()

        val cmmWork = CmmWork.newBuilder().setVidLabelingWork(vidLabelingWork).build()
        val packedCmmWork = Any.pack(cmmWork)

        val subscriber = googlePubSubClient.buildSubscriber("test-project-id", "subscription-id", Duration.ofSeconds(30)) { _, _ -> }
        subscriber.startAsync().awaitRunning()

        val vidLabelerApp = VidLabelerApp(subscribingStorageClient, "test-queue", queueSubscriber, CmmWork.parser())

        repeat(30) {
            val outputBlob = mesosRecordIoStorageClient.getBlob(outputEventsPath)
            if (outputBlob != null) {
                val outputData = outputBlob.read().toList().flatten()
                // Parse or assert on outputData
                return@runBlocking
            }
            delay(1000)
        }
        error("LabelerOutput not found in time.")
    }
}
