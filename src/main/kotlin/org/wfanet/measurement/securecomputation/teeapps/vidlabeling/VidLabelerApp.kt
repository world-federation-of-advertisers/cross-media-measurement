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
import com.google.protobuf.Parser
import com.google.protobuf.TextFormat
import com.google.protobuf.kotlin.toByteString
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.flatMapMerge
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.reduce
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.teeapps.v1alpha.TeeAppConfig
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
        .map { byteString ->
          val labelerInput =
            LabelerInput.getDefaultInstance()
              .newBuilderForType()
              .apply {
                TextFormat.Parser.newBuilder().build().merge(byteString.toStringUtf8(), this)
              }
              .build() as LabelerInput
          val labelerOutput: LabelerOutput = labeler.label(input = labelerInput)
          labelerOutput.toString().toByteStringUtf8()
        }

    storageClient.writeBlob(outputBlobKey, outputFlow)
  }

  override suspend fun runWork(message: Any) {
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
