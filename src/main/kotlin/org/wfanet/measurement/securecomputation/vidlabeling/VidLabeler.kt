package org.wfanet.measurement.securecomputation.teesdk

import com.google.protobuf.Parser
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.CmmWork
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import com.google.protobuf.ByteString
import kotlinx.coroutines.runBlocking
//import org.wfanet.virtualpeople.core.labeler.Labeler
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.CompiledNode
import kotlinx.coroutines.flow.flowOf
import org.wfanet.virtualpeople.common.LabelerInput

class VidLabelerApp(
  private val queueName: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<CmmWork>
) : BaseTeeApplication<CmmWork>(queueName = queueName, queueSubscriber = queueSubscriber, parser = parser) {

    private fun labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesosRecordIoStorageClient) = runBlocking {
        val CHUNK = 100
        val CONCURRENCY = 4

        val inputBlob = storageClient.getBlob(inputBlobKey) ?: error("Input blob not found.")
        val flowOfInputs = inputBlob.read().chunked(CHUNK)

        val flowOfOutputs = flowOfInputs.flatMapMerge(CONCURRENCY) { chunk ->
            chunk.map { byteString ->
                val input = LabelerInput.parseFrom(byteString)
                labeler.label(input).toByteString()
            }
        }

        val outputBlob = storageClient.writeBlob(outputBlobKey, flowOfOutputs)
        requireNotNull(outputBlob) { "Failed to write to output blob." }
    }

    override suspend fun runWork(work: CmmWork) {
        val inputEventsPath = work.vidLabelingWork.inputEventsPath
        val outputEventsPath = work.vidLabelingWork.outputEventsPath
        val vidModelPath = work.vidLabelingWork.vidModelPath

        val storageClient = InMemoryStorageClient()
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)

        val vidModelBlob = storageClient.getBlob(vidModelPath)
        requireNotNull(vidModelBlob) { "Blob should exist" }
        val compiledNodeBytes = vidModelBlob.read().toList().flatten()
        val compiledNode = CompiledNode.parseFrom(ByteString.copyFrom(compiledNodeBytes))

        val labeler = Labeler.build(compiledNode)

        labelPath(inputEventsPath, outputEventsPath, labeler, mesosRecordIoStorageClient)
    }
}
