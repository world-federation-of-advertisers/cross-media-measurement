package org.wfanet.measurement.securecomputation.teesdk

import com.google.protobuf.Parser
import org.wfanet.measurement.queue.QueueClient
import org.wfanet.measurement.securecomputation.CmmWork
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.virtualpeople.core.labeler.Labeler
import com.google.protobuf.ByteString
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.measurement.storage.testing.InMemoryStorageClient
import org.wfanet.virtualpeople.common.CompiledNode

class VidLabelerApp(
  private val queueName: String,
  private val queueClient: QueueClient,
  private val parser: Parser<CmmWork>
) : BaseTeeApplication<CmmWork>(queueName = queueName, queueSubscriber = queueClient, parser = parser) {

    private fun labelPath(inputBlobKey: String, outputBlobKey: String, labeler: Labeler, storageClient: MesosRecordIoStorageClient) = runBlocking {
      val CHUNK = 100
      val CONCURRENCY = 4
      
      val inputBlob = storageClient.getBlob(inputBlobKey)
      requireNotNull(inputBlob) { "Input blob should exist" }
      
      val inputFlow: Flow<ByteString> = inputBlob.read()
      val outputFlow: Flow<LabelerOutput> = inputFlow
        .chunked(CHUNK)
        .flatMapMerge(concurrency = CONCURRENCY) { chunk ->
          chunk.map { byteString ->
            // Assuming a method to parse ByteString to LabelerInput
            val labelerInput = parseByteStringToLabelerInput(byteString)
            labeler.label(labelerInput)
          }
        }
      
      val outputBlob = storageClient.writeBlob(outputBlobKey, outputFlow.map { 
        // Assuming a method to convert LabelerOutput to ByteString
        it.toByteString() 
      })
      requireNotNull(outputBlob) { "Output blob should be created" }
    }
    
    private fun parseByteStringToLabelerInput(byteString: ByteString): LabelerInput {
      // Placeholder for parsing logic
      return LabelerInput.parseFrom(byteString)
    }

    suspend fun runWork(work: CmmWork) {
      val vidLabelingWork = work.vidLabelingWork
      val inputEventsPath = vidLabelingWork.inputEventsPath
      val outputEventsPath = vidLabelingWork.outputEventsPath
      val vidModelPath = vidLabelingWork.vidModelPath

      val inMemoryStorageClient = InMemoryStorageClient()
      val mesosRecordIoStorageClient = MesosRecordIoStorageClient(inMemoryStorageClient)

      val vidModelBlob = inMemoryStorageClient.getBlob(vidModelPath)
      requireNotNull(vidModelBlob) { "Blob should exist" }
      val compiledNodeByteString = vidModelBlob.read().toList().flatten()
      val compiledNode = CompiledNode.parseFrom(ByteString.copyFrom(compiledNodeByteString))

      val labeler = Labeler.build(compiledNode)

      labelPath(inputEventsPath, outputEventsPath, labeler, mesosRecordIoStorageClient)
    }
}
