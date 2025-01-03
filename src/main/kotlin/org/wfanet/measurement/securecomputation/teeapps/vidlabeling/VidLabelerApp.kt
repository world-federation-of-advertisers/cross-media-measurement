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

import com.google.protobuf.Any
import com.google.protobuf.Message
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

class VidLabelerApp<T : Message>(
  private val storageClient: StorageClient,
  queueName: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<T>
) :
  BaseTeeApplication<T>(
    subscriptionId = queueName,
    queueSubscriber = queueSubscriber,
    parser = parser
  ) {

  private suspend fun labelPath(
    inputBlobKey: String,
    outputBlobKey: String,
    labeler: Labeler,
    storageClient: MesosRecordIoStorageClient
  ) {
    val inputBlob =
      storageClient.getBlob(inputBlobKey)
        ?: throw IllegalArgumentException("Input blob $inputBlobKey does not exist")
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

  override suspend fun runWork(message: T) {
    // Currently, the Control Plane API only supports Any messages but BaseTeeApplication supports
    // TriggeredApp also
    val discoveredWork: TriggeredApp =
      if (message is Any) message.unpack(TriggeredApp::class.java)
      else if (message is TriggeredApp) message else throw Exception("Unsupported message type")
    val teeAppConfig = discoveredWork.config.unpack(TeeAppConfig::class.java)
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

    val inputBlobKey = discoveredWork.path
    val outputBlobKey =
      vidLabelingConfig.outputBasePath + inputBlobKey.removePrefix(vidLabelingConfig.inputBasePath)
    val mesosRecordIoStorageClient = MesosRecordIoStorageClient(storageClient)
    labelPath(inputBlobKey, outputBlobKey, labeler, mesosRecordIoStorageClient)
  }
}
