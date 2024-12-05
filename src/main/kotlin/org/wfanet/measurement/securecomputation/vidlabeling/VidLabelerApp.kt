package org.wfanet.measurement.securecomputation.vidlabeling

import com.google.protobuf.Any
import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.chunked
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.reduce
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.CmmWork
import org.wfanet.measurement.securecomputation.DataWatcherConfig.DiscoveredWork
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.core.labeler.Labeler
import com.google.protobuf.kotlin.toByteStringUtf8

class VidLabelerApp(
  private val storageClient: StorageClient,
  private val queueName: String,
  private val queueSubscriber: QueueSubscriber,
  private val parser: Parser<Any>
) :
  BaseTeeApplication<Any>(
    queueName = queueName,
    queueSubscriber = queueSubscriber,
    parser = parser
  ) {

  private suspend fun labelPath(
    inputBlobKey: String,
    outputBlobKey: String,
    labeler: Labeler,
    storageClient: MesosRecordIoStorageClient
  ) {
    val CHUNK = 1000 // Define appropriate chunk size
    val CONCURRENCY = 4 // Define appropriate concurrency level

    val inputBlob =
      storageClient.getBlob(inputBlobKey)
        ?: throw IllegalArgumentException("Input blob does not exist")
    val inputRecords = inputBlob.read()

    val outputFlow =
      inputRecords
        //.chunked(CHUNK)
        //.flatMapMerge(CONCURRENCY) { chunk ->
          //chunk
            .map { byteString ->
              val labelerInput =
                LabelerInput.getDefaultInstance()
                  .newBuilderForType()
                  .apply {
                    TextFormat.Parser.newBuilder().build().merge(byteString.toStringUtf8(), this)
                  }
                  .build() as LabelerInput
              val labelerOutput: LabelerOutput = labeler.label(input = labelerInput)
              println("*********************************")
              println("VID Labeler: Processing Inputs")
              println("*********************************")
              labelerOutput.toString().toByteStringUtf8()
            }
            //.asFlow()
        //}

      storageClient.writeBlob(outputBlobKey, outputFlow)
  }

  override suspend fun runWork(message: Any) {
    println("*********************************")
    println("VID Labeler: runWork")
    println("*********************************")
    val discoveredWork = message.unpack(DiscoveredWork::class.java)
    val cmmWork = discoveredWork.type.unpack(CmmWork::class.java)
    val vidLabelingWork = cmmWork.vidLabelingWork
    val vidModelBlob = storageClient.getBlob(vidLabelingWork.vidModelPath)!!
    val modelData =
      vidModelBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
    val compiledNode: CompiledNode =
      CompiledNode.getDefaultInstance()
        .newBuilderForType()
        .apply { TextFormat.Parser.newBuilder().build().merge(modelData, this) }
        .build() as CompiledNode

    val labeler = Labeler.build(compiledNode)

    val inputBlobKey = discoveredWork.path
    val outputBlobKey =
      vidLabelingWork.outputBasePath + inputBlobKey.removePrefix(vidLabelingWork.inputBasePath)

    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)
    labelPath(inputBlobKey, outputBlobKey, labeler, mesosRecordIoStorageClient)
  }
}
