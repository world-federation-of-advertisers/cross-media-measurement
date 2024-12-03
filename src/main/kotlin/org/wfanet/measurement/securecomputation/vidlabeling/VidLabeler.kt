package org.wfanet.measurement.securecomputation.vidlabeling

import org.wfanet.virtualpeople.common.LabelerInput
import com.google.protobuf.ByteString
import kotlinx.coroutines.flow.chunked
import org.wfanet.measurement.securecomputation.CmmWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig
import com.google.protobuf.kotlin.toByteString
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import com.google.protobuf.Parser
import org.wfanet.measurement.storage.StorageClient
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import kotlinx.coroutines.flow.reduce
import org.wfanet.virtualpeople.core.labeler.Labeler
import com.google.protobuf.Any
import org.wfanet.virtualpeople.common.CompiledNode

class VidLabelerApp(
  private val storageClient: StorageClient,
  private val queueName: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<DiscoveredWork>
) : BaseTeeApplication<DiscoveredWork>(
  queueName = queueName,
  queueSubscriber = queueSubscriber,
  parser = parser
) {

    private suspend fun labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesosRecordIoStorageClient) {
        val CHUNK = 1000 // Define appropriate chunk size
        val CONCURRENCY = 4 // Define appropriate concurrency level

        val inputBlob = storageClient.getBlob(inputBlobKey) ?: throw IllegalArgumentException("Input blob does not exist")
        val inputRecords = inputBlob.read()

        val outputFlow = inputRecords
            .chunked(CHUNK)
            .flatMapMerge(CONCURRENCY) { chunk ->
                chunk.map { byteString ->
                    val labelerInput = LabelerInput.parseFrom(byteString)
                    val labelerOutput = labeler.label(labelerInput)
                    labelerOutput.toByteString()
                }.asFlow()
            }

        storageClient.writeBlob(outputBlobKey, outputFlow)
    }

    override suspend fun runWork(message: DiscoveredWork) {
        val cmmWork = message.type.unpack(CmmWork::class.java)
        val vidLabelingWork = cmmWork.vidLabelingWork
        val vidModelBlob = storageClient.getBlob(vidLabelingWork.vidModelPath)!!
        val compiledNode = vidModelBlob.read().reduce { acc, byteString -> acc.concat(byteString) }
        val labeler = Labeler.build(CompiledNode.parseFrom(compiledNode))

        val inputBlobKey = message.path
        val outputBlobKey = vidLabelingWork.outputBasePath + inputBlobKey.removePrefix(vidLabelingWork.inputBasePath)

        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)

        labelPath(inputBlobKey, outputBlobKey, labeler, mesosRecordIoStorageClient)
    }
}
