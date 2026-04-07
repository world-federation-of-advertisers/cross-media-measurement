/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication

/**
 * TEE application for VID labeling that processes WorkItems from a Pub/Sub queue.
 *
 * Receives WorkItems containing [VidLabelerParams], resolves required dependencies (KMS clients,
 * storage config), and delegates to a VidLabeler for the actual labeling work.
 *
 * @param subscriptionId Pub/Sub subscription for VID labeling queue.
 * @param queueSubscriber handles Pub/Sub pull.
 * @param parser protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC stub for WorkItems service.
 * @param workItemAttemptsClient gRPC stub for WorkItemAttempts service.
 * @param rawImpressionsKmsClient decrypt-only KMS clients keyed by data provider resource name.
 * @param vidLabeledImpressionsKmsClient encrypt/decrypt KMS clients keyed by data provider resource
 *   name.
 * @param getStorageConfig builds a [StorageConfig] from [VidLabelerParams.StorageParams].
 */
class VidLabelerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val rawImpressionsKmsClient: Map<String, KmsClient>,
  private val vidLabeledImpressionsKmsClient: Map<String, KmsClient>,
  private val getStorageConfig: (VidLabelerParams.StorageParams) -> StorageConfig,
) :
  BaseTeeApplication(
    subscriptionId = subscriptionId,
    queueSubscriber = queueSubscriber,
    parser = parser,
    workItemsStub = workItemsClient,
    workItemAttemptsStub = workItemAttemptsClient,
  ) {

  override suspend fun runWork(message: Any) {
    val workItemParams = message.unpack(WorkItemParams::class.java)
    val vidLabelerParams = workItemParams.appParams.unpack(VidLabelerParams::class.java)

    val dataProvider = vidLabelerParams.dataProvider
    require(dataProvider.isNotEmpty()) { "data_provider must not be empty" }
    require(vidLabelerParams.hasRawImpressionsStorageParams()) {
      "raw_impressions_storage_params must be set"
    }
    require(vidLabelerParams.hasVidLabeledImpressionsStorageParams()) {
      "vid_labeled_impressions_storage_params must be set"
    }
    require(vidLabelerParams.inputBlobUrisCount > 0) { "input_blob_uris must not be empty" }

    val decryptKmsClient =
      requireNotNull(rawImpressionsKmsClient[dataProvider]) {
        "Decrypt KMS client not found for $dataProvider"
      }
    val encryptKmsClient =
      requireNotNull(vidLabeledImpressionsKmsClient[dataProvider]) {
        "Encrypt KMS client not found for $dataProvider"
      }

    val storageConfig = getStorageConfig(vidLabelerParams.rawImpressionsStorageParams)

    // TODO: Call VidLabeler.labelBatch() once VidLabeler implementation is available.
  }
}
