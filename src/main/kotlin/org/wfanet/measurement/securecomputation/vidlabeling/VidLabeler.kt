package org.wfanet.measurement.securecomputation.teesdk

import org.wfanet.measurement.common.rabbitmq.QueueClient
import com.google.protobuf.Parser
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CmmWork
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.virtualpeople.core.labeler.Labeler
import com.google.protobuf.ByteString
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.CompiledNode
import com.google.protobuf.InvalidProtocolBufferException
import java.io.IOException
import kotlin.coroutines.suspendCoroutine

class VidLabelerApp(private val queueName: String, private val queueClient: QueueClient, private val parser: Parser<CmmWork>) : BaseTeeApplication<CmmWork>(queueName = queueName, queueClient = queueClient, parser = parser) {

    private suspend fun labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesosRecordIoStorageClient) {
        val CHUNK = 1000
        val CONCURRENCY = 4
        
        val inputBlob = storageClient.getBlob(inputBlobKey) ?: error("Input blob not found")
        
        val inputFlow: Flow<ByteString> = inputBlob.read()
        val labeledFlow = inputFlow.chunked(CHUNK).flatMapMerge(CONCURRENCY) { chunk ->
            withContext(Dispatchers.Default) {
                chunk.map { byteString ->
                    val labelerInput = parseLabelerInput(byteString)
                    labeler.label(labelerInput).toByteString()
                }
            }
        }
        storageClient.writeBlob(outputBlobKey, flowOf(*labeledFlow.toList().toTypedArray()))
    }

    private fun parseLabelerInput(byteString: ByteString): LabelerInput {
        // Implement parsing logic here
        return LabelerInput.parseFrom(byteString)
    }

    suspend fun runWork(work: CmmWork) {
        val vidLabelingWork = work.vidLabelingWork
        val inputEventsPath = vidLabelingWork.inputEventsPath
        val outputEventsPath = vidLabelingWork.outputEventsPath
        val vidModelPath = vidLabelingWork.vidModelPath

        val storageClient = InMemoryStorageClient()
        val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)

        val compiledNodeBytes = suspendCoroutine<ByteArray> { cont ->
            try {
                val blob = storageClient.getBlob(vidModelPath)
                requireNotNull(blob) { "Blob should exist" }
                cont.resume(blob.read().toList().flatten().toByteArray())
            } catch (e: IOException) {
                cont.resumeWithException(e)
            }
        }

        val compiledNode = CompiledNode.parseFrom(compiledNodeBytes)
        val labeler = Labeler.build(compiledNode)

        labelPath(inputEventsPath, outputEventsPath, labeler, mesosRecordIoStorageClient)
    }
}