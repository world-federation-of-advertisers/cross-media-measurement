package org.wfanet.measurement.securecomputation.vidlabeling

import org.wfanet.virtualpeople.common.LabelerInput
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.chunked
import org.wfanet.measurement.securecomputation.CmmWork
import com.google.protobuf.kotlin.toByteString
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import com.google.protobuf.Parser
import org.wfanet.measurement.storage.StorageClient
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.virtualpeople.core.labeler.Labeler
import kotlinx.coroutines.flow.reduce
import kotlinx.coroutines.runBlocking
import org.wfanet.virtualpeople.common.CompiledNode

class VidLabelerApp(
  private val storageClient: StorageClient,
  private val queueName: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<CmmWork>
) : BaseTeeApplication<CmmWork>(
  queueName = queueName,
  queueSubscriber = queueSubscriber,
  parser = parser
) {

    private suspend fun labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesosRecordIoStorageClient) {
        val CHUNK = 100
        val CONCURRENCY = 4

        val inputBlob = storageClient.getBlob(inputBlobKey)
        requireNotNull(inputBlob) { "Input blob does not exist" }

        val labeledData = inputBlob.read()
            .chunked(CHUNK)
            .flatMapMerge(CONCURRENCY) { chunk ->
                chunk.map { byteString ->
                    val labelerInput = LabelerInput.parseFrom(byteString)
                    val labelerOutput = labeler.label(labelerInput)
                    labelerOutput.toByteString()
                }.asFlow()
            }.toList()

        storageClient.writeBlob(outputBlobKey, labeledData.asFlow())
    }

    override suspend fun runWork(message: CmmWork) = runBlocking {
        val vidLabelingWork = message.vidLabelingWork
        val compiledNodeBlob = storageClient.getBlob(vidLabelingWork.vidModelPath)!!
        val compiledNodeBytes = compiledNodeBlob.read().reduce { acc, byteString -> acc.concat(byteString) }
        val compiledNode = CompiledNode.parseFrom(compiledNodeBytes)
        val labeler = Labeler.build(compiledNode)

        val storageClient = MesosRecordIoStorageClient(this@VidLabelerApp.storageClient)
        labelPath(vidLabelingWork.inputEventsPath, vidLabelingWork.outputEventsPath, labeler, storageClient)
    }
}
