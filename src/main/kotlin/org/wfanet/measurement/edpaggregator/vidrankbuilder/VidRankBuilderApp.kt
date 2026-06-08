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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.crypto.tink.KmsClient
import com.google.protobuf.Any
import com.google.protobuf.Parser
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams.StorageParams
import org.wfanet.measurement.queue.QueueSubscriber
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.securecomputation.teesdk.BaseTeeApplication

/**
 * Phase-1 TEE application for the memoized VID assignment pipeline.
 *
 * One ranker VM per `RankerJob`. A `RankerJob` may cover one or more
 * subpools: the SubpoolAssigner's last-shard-out bin-packs subpools into
 * `RankerJob`s based on their fingerprint counts so that very small
 * subpools share a VM instead of each spinning up their own, while a single
 * very large subpool gets its own `RankerJob`. Within a subpool there is
 * no intra-subpool sharding — the entire subpool is owned by exactly one
 * `RankerJob`, and therefore one [VidRankBuilderApp] invocation. One
 * WorkItem is published per `RankerJob` row. The WorkItem's
 * [VidRankBuilderParams] carries the `(RawImpressionUpload, ModelLine,
 * RankerJobId)` triple plus the (subpool, Phase-0 blob URI) set this VM
 * ranks, supplied via [VidRankBuilderParams.subpool_map_blob_uris].
 *
 * Each VM:
 *  - loads the prior cumulative rank-index blob for each of its subpools
 *    from `vid_rank_map_storage_params` and rebuilds the in-heap
 *    `Bytes12IntMap` (12-byte fingerprint -> rank) and rank `BitSet`,
 *  - reads its subpools' `SubpoolFingerprints` blobs from
 *    `subpool_map_storage_params`,
 *  - prunes aged-out rank-index blobs per the retention policy and frees
 *    their ranks,
 *  - allocates ranks to new fingerprints, capping at `ranked_size` (overflow
 *    fingerprints fall back to the unranked path at Phase-2),
 *  - writes the new day-only and updated cumulative rank-index blobs
 *    (DEK-encrypted) back to `vid_rank_map_storage_params`,
 *  - flips its `RankerJob` row from `RANKING` to `RANKER_SUCCEEDED` via the
 *    EDP Aggregator internal gRPC.
 *
 * The last `RankerJob` to complete for a `(RawImpressionUpload, ModelLine)`
 * is responsible for flipping `RawImpressionUploadModelLineState` from
 * `RANKING` to `LABELING` and publishing one VidLabeler WorkItem per shard
 * (`total_shards` controls the fan-out).
 *
 * @param subscriptionId The subscription ID for the queue subscriber.
 * @param queueSubscriber The [QueueSubscriber] instance for receiving work items.
 * @param parser The protobuf [Parser] for [WorkItem] messages.
 * @param workItemsClient gRPC client stub for [WorkItemsGrpcKt.WorkItemsCoroutineStub].
 * @param workItemAttemptsClient gRPC client stub for
 *   [WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub].
 * @param kmsClients Per-DataProvider KMS clients used to wrap/unwrap DEKs
 *   for the rank-index blobs.
 * @param getRawImpressionStorageConfig Lambda to obtain the [StorageConfig]
 *   for reading raw-impression files (only consulted if Phase-1 ever needs
 *   to touch raw impressions; not used by the canonical Phase-1 design).
 * @param getSubpoolMapStorageConfig Lambda to obtain the [StorageConfig]
 *   for reading the per-(shard, subpool) `SubpoolFingerprints` blobs
 *   produced by the SubpoolAssigner.
 * @param getVidRankMapStorageConfig Lambda to obtain the [StorageConfig]
 *   for reading prior cumulative rank-index blobs and writing the new
 *   day-only + cumulative blobs.
 */
class VidRankBuilderApp(
  subscriptionId: String,
  queueSubscriber: QueueSubscriber,
  parser: Parser<WorkItem>,
  workItemsClient: WorkItemsGrpcKt.WorkItemsCoroutineStub,
  workItemAttemptsClient: WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub,
  // TODO(@Marco-Premier): wire EDP Aggregator internal gRPC service stubs
  //   needed by Phase-1 as they become available:
  //   - RawImpressionUpload
  //   - RawImpressionUploadModelLine
  //   - RankerJob
  //   - RankIndexBlob
  private val kmsClients: Map<String, KmsClient>,
  private val getRawImpressionStorageConfig: (StorageParams) -> StorageConfig,
  private val getSubpoolMapStorageConfig: (StorageParams) -> StorageConfig,
  private val getVidRankMapStorageConfig: (StorageParams) -> StorageConfig,
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
    val vidRankBuilderParams =
      workItemParams.appParams.unpack(VidRankBuilderParams::class.java)

    // TODO(@Marco-Premier): Implement Phase-1 pipeline:
    //   1. Resolve storage configs via getSubpoolMapStorageConfig and
    //      getVidRankMapStorageConfig from vidRankBuilderParams.
    //   2. Resolve the per-DataProvider KmsClient from kmsClients keyed by
    //      vidRankBuilderParams.dataProvider.
    //   3. Unwrap vidRankBuilderParams.encryptedSubpoolMapsDek via the
    //      DataProvider's KmsClient to obtain the plaintext DEK used by
    //      Phase-0 to encrypt every SubpoolFingerprints blob referenced in
    //      vidRankBuilderParams.subpoolMapBlobUrisMap.
    //   4. For each (subpoolId, blobUri) entry in
    //      vidRankBuilderParams.subpoolMapBlobUrisMap:
    //      a. Load the prior cumulative rank-index blob for this subpool
    //         from vid_rank_map_storage_params; decrypt and build the
    //         in-heap Bytes12IntMap and rank BitSet.
    //      b. Run the two-phase deletion pass (prune aged-out blobs whose
    //         MaxEventDate is older than RETENTION_DAYS; free their ranks).
    //      c. Read the SubpoolAssigner's merged SubpoolFingerprints blob
    //         at blobUri from subpool_map_storage_params; decrypt with the
    //         plaintext DEK from step 3.
    //      d. Allocate ranks to new fingerprints via
    //         bitSet.nextClearBit(cursor); cap at ranked_size.
    //      e. Write the new day-only and cumulative rank-index blobs
    //         (DEK-encrypted) to vid_rank_map_storage_params.
    //      f. Insert the new RankIndexBlob rows via the EDP Aggregator
    //         internal gRPC, stamping
    //         vidRankBuilderParams.maxEventDate on the day-only row(s)
    //         so the retention sweep can identify aged-out blobs.
    //   5. Flip this RankerJob row from RANKING to RANKER_SUCCEEDED via
    //      the EDP Aggregator internal gRPC, keyed by
    //      (vidRankBuilderParams.dataProvider,
    //       vidRankBuilderParams.rawImpressionUpload,
    //       vidRankBuilderParams.modelLine,
    //       vidRankBuilderParams.rankerJobId).
    //   6. Last-RankerJob-out for this (RawImpressionUpload, ModelLine):
    //      flip RawImpressionUploadModelLineState RANKING -> LABELING and
    //      publish one VidLabeler WorkItem per shard
    //      (vidRankBuilderParams.totalShards). The VidLabelerParams payload
    //      is constructed from the pass-through fields on this proto:
    //      rawImpressionStorageParams, vidLabeledImpressionsStorageParams,
    //      modelBlobPath, labelerInputFieldMapping, and
    //      eventTemplateFieldMapping.
    //
    // TODO(@Marco-Premier): runWork must be idempotent on Pub/Sub
    // redelivery. The Spanner commit (RankerJob -> RANKER_SUCCEEDED, new
    // RankIndexBlob rows) and the message ack are not atomic, so a crash
    // between them leads to redelivery with state already advanced. The
    // implementation must:
    //   - detect that this RankerJob is already RANKER_SUCCEEDED and treat
    //     it as a no-op for the rank-allocation steps, re-running only the
    //     last-RankerJob-out check (step 6) before acking,
    //   - tolerate partially-written RankIndexBlob rows from a prior
    //     attempt (e.g. resume rather than duplicate),
    //   - keep the BitSet / Bytes12IntMap rebuild deterministic across
    //     attempts so a redelivered run produces the same outcome.
    // Cover with tests that inject failures at: (a) after Spanner state
    // flip but before ack, (b) mid-blob-write, (c) after first subpool's
    // RankIndexBlob row insert but before second subpool's, (d) after
    // last-RankerJob-out Spanner flip but before Phase-2 WorkItem publish.
    // Each case must converge to the same final state on redelivery.
  }
}
