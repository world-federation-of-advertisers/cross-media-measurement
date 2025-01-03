/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.wfanet.measurement.securecomputation.vidlabeling

import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.toByteStringUtf8
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.reduce
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.datawatcher.v1alpha.DataWatcherConfig.TriggeredApp
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.TeeAppConfig
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication
import org.wfanet.measurement.storage.MesosRecordIoStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.virtualpeople.common.CompiledNode
import org.wfanet.virtualpeople.common.LabelerInput
import org.wfanet.virtualpeople.common.LabelerOutput
import org.wfanet.virtualpeople.core.labeler.Labeler

/*
 * TEE VID Labeling App.
 */
class VidLabelerApp(
  private val storageClient: StorageClient,
  queueName: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<TriggeredApp>
) :
  BaseTeeApplication<TriggeredApp>(
    subscriptionId = queueName,
    queueSubscriber = queueSubscriber,
    parser = parser
  ) {

  /*
   * Currently, labels events using a single thread. Consider using a different approach for faster labeling.
   * TODO: Read and write using serialized ByteString rather than TextProto once the MesosStorageClient bug is fixed.
   */
  private suspend fun labelPath(
    inputBlobKey: String,
    outputBlobKey: String,
    labeler: Labeler,
    storageClient: MesosRecordIoStorageClient
  ) {
    val inputBlob =
      storageClient.getBlob(inputBlobKey)
        ?: throw IllegalArgumentException("Input blob does not exist")
    val inputRecords = inputBlob.read()

    val outputFlow =
      inputRecords.map { byteString ->
        val labelerInput =
          LabelerInput.getDefaultInstance()
            .newBuilderForType()
            .apply { TextFormat.Parser.newBuilder().build().merge(byteString.toStringUtf8(), this) }
            .build() as LabelerInput
        val labelerOutput: LabelerOutput = labeler.label(input = labelerInput)
        labelerOutput.toString().toByteStringUtf8()
      }

    storageClient.writeBlob(outputBlobKey, outputFlow)
  }

  override suspend fun runWork(message: TriggeredApp) {
    val teeAppConfig = message.config.unpack(TeeAppConfig::class.java)
    assert(teeAppConfig.workTypeCase == TeeAppConfig.WorkTypeCase.VID_LABELING_CONFIG)
    val vidLabelingConfig = teeAppConfig.vidLabelingConfig
    val compiledNode: CompiledNode =
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (vidLabelingConfig.modelFormatCase) {
        TeeAppConfig.VidLabelingConfig.ModelFormatCase.MODEL_BLOB_TEXT_PROTO_PATH -> {
          val vidModelBlob = storageClient.getBlob(vidLabelingConfig.modelBlobTextProtoPath)!!
          val modelData =
            vidModelBlob.read().reduce { acc, byteString -> acc.concat(byteString) }.toStringUtf8()
          CompiledNode.getDefaultInstance()
            .newBuilderForType()
            .apply { TextFormat.Parser.newBuilder().build().merge(modelData, this) }
            .build() as CompiledNode
        }
        TeeAppConfig.VidLabelingConfig.ModelFormatCase.MODEL_BLOB_RIEGELI_PATH ->
          TODO("Currently Unsupported")
        TeeAppConfig.VidLabelingConfig.ModelFormatCase.MODEL_LINE -> TODO("Currently Unsupported")
        TeeAppConfig.VidLabelingConfig.ModelFormatCase.MODELFORMAT_NOT_SET ->
          throw Exception("Invalid model format: ${vidLabelingConfig.modelFormatCase}")
      }

    val labeler = Labeler.build(compiledNode)

    val inputBlobKey = message.path
    val outputBlobKey =
      vidLabelingConfig.outputBasePath + inputBlobKey.removePrefix(vidLabelingConfig.inputBasePath)

    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)
    labelPath(inputBlobKey, outputBlobKey, labeler, mesosRecordIoStorageClient)
  }
}
