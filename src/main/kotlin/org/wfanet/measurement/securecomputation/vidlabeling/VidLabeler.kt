package org.wfanet.measurement.securecomputation.teesdk

import com.google.protobuf.Parser
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.CmmWork
import com.google.protobuf.ByteString
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.wfanet.virtualpeople.core.labeler.Labeler
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import kotlinx.coroutines.flow.flattenMerge
import kotlinx.coroutines.flow.map

class VidLabelerApp(
  private val storageClient: StorageClient,
  private val queueName: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<CmmWork>
) : BaseTeeApplication<CmmWork>(queueName, queueSubscriber, parser) {

    private suspend fun labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesosRecordIoStorageClient) {
        val CHUNK = 100
        val CONCURRENCY = 4
    
        val inputBlob = storageClient.getBlob(inputBlobKey) ?: throw IllegalArgumentException("Input blob does not exist")
        val inputFlow = inputBlob.read()
    
        val labeledFlow = inputFlow.chunked(CHUNK).flatMapMerge(CONCURRENCY) { chunk ->
            flow {
                chunk.map { byteString ->
                    val labelerInput = LabelerInput.parseFrom(byteString)
                    val labelerOutput = labeler.label(labelerInput)
                    emit(ByteString.copyFrom(labelerOutput.toByteArray()))
                }
            }
        }
    
        withContext(Dispatchers.IO) {
            storageClient.writeBlob(outputBlobKey, labeledFlow)
        }
    }

    suspend fun runWork(work: CmmWork) {
        val vidModelPath = work.vidLabelingWork.vidModelPath
        val vidModelBlob = storageClient.getBlob(vidModelPath)
        requireNotNull(vidModelBlob) { "Vid model blob should exist" }
        val compiledNodeBytes = vidModelBlob.read().flattenMerge().toList()
        val compiledNode = CompiledNode.parseFrom(compiledNodeBytes)
        val labeler = Labeler.build(compiledNode)
        val inputEventsPath = work.vidLabelingWork.inputEventsPath
        val outputEventsPath = work.vidLabelingWork.outputEventsPath
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)
        labelPath(inputEventsPath, outputEventsPath, labeler, mesosRecordIoStorageClient)
    }
}