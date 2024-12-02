package org.wfanet.measurement.securecomputation.controlplane.v1alpha

import org.junit.Test
import com.google.protobuf.Any
import org.wfanet.measurement.securecomputation.CmmWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher
import org.wfanet.measurement.securecomputation.teesdk.VidLabelerApp
import org.wfanet.measurement.storage.testing.GcsSubscribingStorageClientTest
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.virtualpeople.core.labeler.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.delay
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.flow.toList

class SimpleIntegrationTest {

    @Test
    fun vidLabel() = runBlocking {
        // Step 1: Create temporary test paths
        val inputEventsPath = "temp/input"
        val outputEventsPath = "temp/output"
        val vidModelPath = "temp/model"

        // Step 2: Write out a test vid model (CompiledNode)
        val compiledNode = compiledNode {
            // Define the compiled node structure here
        }
        storageClient.writeBlob(vidModelPath, flowOf(ByteString.copyFrom(compiledNode.toByteArray())))

        // Step 3: Create a DataWatcherConfig
        val dataWatcherConfig = DataWatcherConfig.newBuilder().setSourcePathRegex(".*").build()

        // Step 4: Construct a DataWatcher
        val dataWatcher = DataWatcher(listOf(dataWatcherConfig))

        // Step 5: Construct a SubscribingStorageClient
        val underlyingClient = InMemoryStorageClient()
        val subscribingStorageClient = GcsSubscribingStorageClient(underlyingClient)

        // Step 6: Create a MesosRecordIoStorageClient
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(subscribingStorageClient)

        // Step 7: Create some test LabelerInput
        val labelerInput = labelerInput { eventId = eventId { id = "test-event" } }
        mesosRecordIoStorageClient.writeBlob(inputEventsPath, flowOf(ByteString.copyFrom(labelerInput.toByteArray())))

        // Step 8: Create a GooglePubSubEmulatorClient and start emulator
        val googlePubSubClient = GooglePubSubEmulatorClient()
        googlePubSubClient.startEmulator()

        // Step 9: Create CmmWork Vid Labeling Work item
        val cmmWork = CmmWork.newBuilder().setVidLabelingWork(
            CmmWork.VidLabelingWork.newBuilder()
                .setInputEventsPath(inputEventsPath)
                .setOutputEventsPath(outputEventsPath)
                .setVidModelPath(vidModelPath)
                .build()
        ).build()

        // Step 10: Pack the CmmWork into an Any
        val packedCmmWork = Any.pack(cmmWork)

        // Step 11: Create a CreateWorkItemRequest
        val createWorkItemRequest = CreateWorkItemRequest.newBuilder().setWorkItem(packedCmmWork).build()

        // Step 12: Send it to a workItemsService (mocked here)
        val workItemsService = mock<WorkItemsService> {}
        workItemsService.createWorkItem(createWorkItemRequest)

        // Step 13: Build a Subscriber to the GooglePubSubClient
        val subscriber = googlePubSubClient.buildSubscriber("test-project", "test-subscription") { message, consumer ->
            // Processing logic
            consumer.ack()
        }
        subscriber.startAsync().awaitRunning()

        // Step 14: Create a VidLabelerApp
        val vidLabelerApp = VidLabelerApp(storageClient, "test-queue", subscriber, CmmWork.parser())

        // Step 15: Wait for LabelerOutput
        withTimeout(30000) {
            while (true) {
                delay(1000)
                val outputBlob = mesosRecordIoStorageClient.getBlob(outputEventsPath)
                if (outputBlob != null) {
                    val outputs = outputBlob.read().toList()
                    if (outputs.isNotEmpty()) break
                }
            }
        }

        // Step 16: Parse the output events
        val outputBlob = mesosRecordIoStorageClient.getBlob(outputEventsPath)
        requireNotNull(outputBlob) { "Output blob should exist" }
        val outputs = outputBlob.read().toList().map { LabelerOutput.parseFrom(it) }

        // Assertions can be added here to validate the outputs
    }
}