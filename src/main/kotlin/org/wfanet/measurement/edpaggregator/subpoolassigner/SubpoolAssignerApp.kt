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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication

/**
 * Phase-0 TEE application for the memoized VID assignment pipeline.
 *
 * The dispatcher publishes one [WorkItem] per fingerprint shard for each
 * (RawImpressionUpload, ModelLine) pair that has memoization enabled. Each
 * [SubpoolAssignerApp] instance processes the shard identified by [shardIndex]:
 * it reads the upload's raw impression files, filters to the impressions whose
 * fingerprint falls into its shard, drops impressions whose event timestamp
 * falls outside the model line's `[active_start_time, active_end_time)` window
 * (see `ModelLine.active_start_time` / `active_end_time` in the public API),
 * loads the compiled VID model from `ModelLineConfig.model_blob_path`, runs
 * the labeler in pool-emit mode to resolve a subpool per fingerprint, and
 * writes one [org.wfanet.measurement.edpaggregator.v1alpha.SubpoolFingerprints]
 * blob per subpool to the SubpoolMap storage.
 *
 * Per-shard pipeline state is reported back through the EDP Aggregator's
 * internal gRPC services: this app creates (or upserts) a
 * `RawImpressionUploadModelLine` row for the (RawImpressionUpload, ModelLine)
 * pair when it first runs, and updates its own `PoolAssignmentJob` row to
 * `POOL_ASSIGNMENT_SUCCEEDED` once the per-shard blobs are durable.
 *
 * The last shard to complete is responsible for merging the per-shard
 * per-subpool blobs into one cumulative blob per subpool, bin-packing
 * subpools into `RankerJob` rows, flipping
 * `RawImpressionUploadModelLineState` from `POOL_ASSIGNING` to `RANKING`,
 * and triggering Phase-1 (rank allocation) with one `WorkItem` per
 * `RankerJob`.
 *
 * @param subscriptionId The subscription ID for the queue subscriber.
 * @param queueSubscriber The [QueueSubscriber] instance for receiving work items.
 * @param parser The protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC client stub for [WorkItemsGrpcKt.WorkItemsCoroutineStub].
 * @param workItemAttemptsClient gRPC client stub for
 *   [WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub].
 * @param shardIndex The fingerprint shard id this VM is responsible for. The
 *   total shard count `N` lives in the per-WorkItem [VidLabelerParams]; this
 *   VM keeps only impressions where `fingerprint.hash() % N == shardIndex`.
 * @param kmsClients Per-DataProvider KMS clients used to wrap/unwrap DEKs
 *   for raw-impression and SubpoolFingerprints blobs.
 * @param getSubpoolMapStorageConfig Lambda to obtain the [StorageConfig] for
 *   reading and writing the per-subpool fingerprint blobs.
 * @param getRawImpressionsStorageConfig Lambda to obtain the [StorageConfig]
 *   for reading the raw-impression files uploaded by the EDP.
 */
class SubpoolAssignerApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  private val shardIndex: Int,
  private val kmsClients: MutableMap<String, KmsClient>,
  private val getSubpoolMapStorageConfig: (StorageParams) -> StorageConfig,
  private val getRawImpressionsStorageConfig: (StorageParams) -> StorageConfig,
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

    // TODO(@Marco-Premier): Implement Phase-0 pipeline:
    //   1. Resolve storage configs via getRawImpressionsStorageConfig and
    //      getSubpoolMapStorageConfig from vidLabelerParams storage params.
    //   2. Resolve the per-DataProvider KmsClient from kmsClients.
    //   3. For each entry in vidLabelerParams.modelLineConfigs, create (or
    //      upsert) the RawImpressionUploadModelLine row via the EDP Aggregator
    //      internal gRPC and load the model from
    //      ModelLineConfig.modelBlobPath into the labeler.
    //   4. Read raw-impression files for this upload from raw-impressions storage.
    //   5. Decrypt within the TEE; drop impressions whose event timestamp falls
    //      outside ModelLineConfig.[activeStartTime, activeEndTime).
    //   6. Compute SHA-256 fingerprints; keep only those where
    //      fingerprint.hash() % N == shardIndex (N from VidLabelerParams).
    //   7. Run the labeler in pool-emit mode; bucket fingerprints by subpool.
    //   8. Write per-shard per-subpool SubpoolFingerprints blobs (DEK-encrypted)
    //      to the SubpoolMap storage.
    //   9. Update this shard's PoolAssignmentJob row to POOL_ASSIGNMENT_SUCCEEDED
    //      via the EDP Aggregator internal gRPC.
    //  10. Last-shard-out: merge per-shard blobs into one cumulative blob per
    //      subpool, bin-pack subpools into RankerJob rows, flip
    //      RawImpressionUploadModelLineState POOL_ASSIGNING -> RANKING, and
    //      publish one WorkItem per RankerJob.
  }
}
