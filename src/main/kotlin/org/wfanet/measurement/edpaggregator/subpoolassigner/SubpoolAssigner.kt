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

import com.google.type.Date
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.telemetry.W3CTraceContext
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.rawimpressions.ParquetDigestedEvent
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.OutputBlobReference
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.OutputManifestRecordKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.commitRawImpressionUploadModelLineMergedOutputRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.outputManifestRecord
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.SelectedStorageClient

/**
 * Phase-0 per-WorkItem logic for the memoized VID assignment pipeline (analog of
 * `ResultsFulfiller`).
 *
 * For one (`RawImpressionUpload`, `ModelLine`, shard) WorkItem it:
 * 1. streams this shard's raw impressions, projects + active-window-filters them, and pool-emit
 *    labels each surviving fingerprint into [accumulator] (subpool -> fingerprint set),
 * 2. generates a per-shard DEK and writes one RecordIO `SubpoolFingerprints` blob per subpool plus
 *    a DEK blob, via [store],
 * 3. marks this shard's `PoolAssignmentJob` `SUCCEEDED`; the service atomically reports whether
 *    this was the last shard out and, if so, returns the discovered pool offsets,
 * 4. last-shard-out only: merges every shard's per-subpool blobs into one blob per subpool
 *    (streaming concat — shards partition the fingerprint space, so the slices are disjoint and
 *    need no dedup), deletes the temp per-shard blobs, then fans out Phase 1 — creates a
 *    `RankerJob` per subpool, publishes one `VidRankBuilder` WorkItem per `RankerJob`, and flips
 *    the parent `RawImpressionUploadModelLine` `POOL_ASSIGNING` -> `RANKING`.
 *
 * Idempotent on Pub/Sub redelivery at the per-shard granularity (a job already `SUCCEEDED` short-
 * circuits the mark). On failure the error propagates so the TEE framework nacks (Pub/Sub retries
 * -> dead-letter). Neither this worker nor the workload-agnostic DLQ listener changes the
 * `PoolAssignmentJob`; it remains available for explicit workload recovery.
 */
class SubpoolAssigner(
  private val rawImpressionSource: RawImpressionSource<ParquetDigestedEvent>,
  private val mapper: LabelerInputMapper,
  private val labeler: PoolEmitLabeler,
  private val activeWindow: ActiveWindow,
  private val store: SubpoolFingerprintsStore,
  private val kekUri: String,
  private val blobPrefix: String,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val rawImpressionUploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val poolAssignmentJob: String,
  private val shardIndex: Int,
  private val totalShards: Int,
  // Phase-1 (VidRankBuilder) fan-out config. The template carries the static `VidRankBuilderParams`
  // fields (all passed through from `SubpoolAssignerParams`); the per-RankerJob fields
  // (`encrypted_subpool_maps_dek`, `ranker_job`, `subpool_map_blob_uris`, `max_event_date`) are
  // filled in at fan-out time.
  private val vidRankBuilderQueue: String,
  private val vidRankBuilderParamsTemplate: VidRankBuilderParams,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
  private val outputManifestStore: OutputManifestStore,
  private val outputBlobPrefixUri: String,
  private val dispatchLabeling: suspend () -> Int,
  private val accumulator: SubpoolFingerprintsAccumulator = SubpoolFingerprintsAccumulator(),
  private val metrics: SubpoolAssignerMetrics = SubpoolAssignerMetrics(),
  private val traceContextProvider: () -> Map<String, String> = W3CTraceContext::inject,
  private val newAttemptId: () -> String = { UUID.randomUUID().toString() },
) {
  /**
   * @property subpoolCount number of non-empty subpools this shard wrote.
   * @property labeled events that routed to at least one subpool.
   * @property droppedOutsideWindow events dropped by the active-window filter.
   * @property unrouted events the labeler routed to no subpool.
   * @property lastShardOut whether this shard was the last to complete (and ran the merge).
   */
  data class Result(
    val subpoolCount: Int,
    val labeled: Long,
    val droppedOutsideWindow: Long,
    val unrouted: Long,
    val lastShardOut: Boolean,
    val outcome: String = "succeeded",
  )

  /**
   * Runs the full Phase-0 work for one shard. Any exception propagates so the TEE framework nacks
   * the message. Neither this worker nor the workload-agnostic DLQ listener changes the
   * `PoolAssignmentJob` state on retry exhaustion.
   */
  suspend fun assign(): Result =
    Tracing.traceSuspending(
      spanName = "edpa.vid_labeling.pool_assignment",
      attributes =
        Attributes.builder()
          .put(VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME, rawImpressionUpload)
          .put(VidLabelingTraceAttributes.MODEL_LINE_NAME, modelLine)
          .put(VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME, poolAssignmentJob)
          .put(VidLabelingTraceAttributes.PIPELINE_PHASE, "phase0")
          .put(XmmTraceAttributes.LIFECYCLE_STAGE, "pool_assignment")
          .put(XmmTraceAttributes.OUTCOME, "started")
          .build(),
    ) {
      runShard().also { result ->
        Span.current()
          .setAttribute(SUBPOOL_COUNT, result.subpoolCount.toLong())
          .setAttribute(LABELED_COUNT, result.labeled)
          .setAttribute(DROPPED_COUNT, result.droppedOutsideWindow)
          .setAttribute(UNROUTED_COUNT, result.unrouted)
          .setAttribute(LAST_SHARD_OUT, result.lastShardOut)
        Span.current().setAttribute(XmmTraceAttributes.OUTCOME, result.outcome)
        VidLabelingTraceLogging.log(
          logger,
          "edpa.vid_labeling.pool_assignment_completed",
          VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to rawImpressionUpload,
          VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
          VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME_STRING to poolAssignmentJob,
          VidLabelingTraceAttributes.PIPELINE_PHASE_STRING to "phase0",
          XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "pool_assignment",
          XmmTraceAttributes.OUTCOME_STRING to result.outcome,
        )
      }
    }

  private suspend fun runShard(): Result {
    // Gate on job state first: on Pub/Sub redelivery of an already-completed shard, skip the
    // expensive re-read/re-label/re-write (SUCCEEDED is set only after the per-shard write is
    // durable, so it is a reliable completion marker) and only recover the last-shard-out merge.
    val job =
      rpcThrottlers.metadataRead.onReady {
        poolAssignmentJobsStub.getPoolAssignmentJob(
          getPoolAssignmentJobRequest { name = poolAssignmentJob }
        )
      }
    if (job.state == PoolAssignmentJob.State.SUCCEEDED) {
      return recoverIfLastShardOut()
    }

    // 1. Read -> label -> accumulate.
    val sink = SubpoolAssignmentSink(mapper, labeler, accumulator, activeWindow, metrics)
    // Phase 0 has no entity keys; ignore the per-file footer metadata.
    rawImpressionSource.streamBlobs(openSink = { _, _ -> sink })

    // Every retry gets fresh keys. Only a complete manifest becomes authoritative.
    val attemptId = newAttemptId()
    val subpoolIds = accumulator.subpoolIds().toList()
    val dek = if (subpoolIds.isEmpty()) null else store.generateDek(kekUri)
    val records =
      try {
        coroutineScope {
          val uploads = Semaphore(SUBPOOL_UPLOAD_CONCURRENCY)
          subpoolIds
            .map { offset ->
              async {
                uploads.withPermit {
                  val key = shardSubpoolKey(shardIndex, offset) + "/attempt/" + attemptId
                  val blob =
                    try {
                      store.writeBlob(
                        key,
                        checkNotNull(dek),
                        offset,
                        accumulator.streamChunks(offset),
                      )
                    } catch (e: CancellationException) {
                      throw e
                    } catch (e: Exception) {
                      logPoolUnit(
                        Level.WARNING,
                        "edpa.vid_labeling.pool_assignment.shard_output_failed",
                        "pool_assignment_output",
                        "failed",
                        offset,
                        e,
                      )
                      throw e
                    }
                  Span.current()
                    .addEvent(
                      "edpa.vid_labeling.pool_assignment.shard_output",
                      Attributes.builder()
                        .put(POOL_OFFSET, offset)
                        .put(SHARD_INDEX, shardIndex.toLong())
                        .put(XmmTraceAttributes.OUTCOME, "written")
                        .build(),
                    )
                  val reference = outputManifestStore.reference(uriFor(key), blob)
                  accumulator.remove(offset)
                  logPoolUnit(
                    Level.INFO,
                    "edpa.vid_labeling.pool_assignment.shard_output",
                    "pool_assignment_output",
                    "written",
                    offset,
                  )
                  outputManifestRecord {
                    subpoolBlob =
                      OutputManifestRecordKt.subpoolBlob {
                        poolOffset = offset
                        this.blob = reference
                      }
                  }
                }
              }
            }
            .awaitAll()
        }
      } catch (e: BlobChangedException) {
        return recoverCollision(e)
      }
    val manifest =
      try {
        outputManifestStore.write(
          uriFor(shardSubpoolKey(shardIndex, 0L) + "/attempt/" + attemptId + "/manifest"),
          manifestHeader(
            OutputManifestRecord.Header.Phase.POOL_ASSIGNMENT,
            poolAssignmentJob,
            attemptId,
            dek,
          ),
          records.asFlow(),
        )
      } catch (e: BlobChangedException) {
        return recoverCollision(e)
      }
    val markResponse =
      try {
        rpcThrottlers.metadataWrite.onReady {
          poolAssignmentJobsStub.markPoolAssignmentJobSucceeded(
            markPoolAssignmentJobSucceededRequest {
              name = poolAssignmentJob
              etag = job.etag
              requestId = deterministicUuid("$poolAssignmentJob|succeeded")
              outputManifest = manifest
              if (dek != null) encryptedDek = dek
              poolOffsets += subpoolIds
              sink.maxTimestampUsec?.let { maxEventDate = usecToUtcDate(it) }
            }
          )
        }
      } catch (e: StatusException) {
        if (
          e.status.code != Status.Code.ABORTED && e.status.code != Status.Code.FAILED_PRECONDITION
        )
          throw e
        val selected = getJob()
        if (selected.state != PoolAssignmentJob.State.SUCCEEDED) throw e
        return recoverIfLastShardOut()
      }
    // The service may replay another attempt's original success. Recover using only elected state.
    val lastShardOut = markResponse.hasLastShardResult() || getJob().lastShard
    if (lastShardOut) {
      val parent = requireNotNull(getParent()) { "Pool-assignment parent is missing" }
      runLastShardOut(parent)
    }
    val subpoolCount = subpoolIds.size
    return summary(subpoolCount, sink, lastShardOut)
  }

  private suspend fun getJob(): PoolAssignmentJob =
    rpcThrottlers.metadataRead.onReady {
      poolAssignmentJobsStub.getPoolAssignmentJob(
        getPoolAssignmentJobRequest { name = poolAssignmentJob }
      )
    }

  private suspend fun recoverCollision(error: BlobChangedException): Result {
    val selected = getJob()
    // An incomplete job must retry with a fresh namespace, never acknowledge lost work.
    if (selected.state != PoolAssignmentJob.State.SUCCEEDED) throw error
    return Result(0, 0, 0, 0, false, "already_completed")
  }

  private suspend fun recoverIfLastShardOut(): Result {
    val parent = getParent() ?: return Result(0, 0, 0, 0, false, "already_completed")
    if (parent.state in NOOP_FANOUT_STATES) {
      return Result(0, 0, 0, 0, true, "already_completed")
    }
    val jobs = listShardJobs()
    if (jobs.isEmpty() || jobs.any { it.state != PoolAssignmentJob.State.SUCCEEDED }) {
      return Result(0, 0, 0, 0, false, "already_completed")
    }
    runLastShardOut(parent)
    return Result(0, 0, 0, 0, true, "recovered")
  }

  private fun uriFor(key: String): String {
    val root = SelectedStorageClient.parseBlobUri(outputBlobPrefixUri)
    val separator = if (root.scheme == "file") ":///" else "://"
    return root.scheme + separator + root.bucket + "/" + key
  }

  private fun manifestHeader(
    phase: OutputManifestRecord.Header.Phase,
    owner: String,
    attemptId: String,
    dek: EncryptedDek?,
  ) =
    OutputManifestRecordKt.header {
      this.phase = phase
      rawImpressionUpload = this@SubpoolAssigner.rawImpressionUpload
      this.owner = owner
      modelLines += modelLine
      this.attemptId = attemptId
      if (dek != null) encryptedDek = dek
    }

  private data class Publication(
    val header: OutputManifestRecord.Header,
    val blobs: Map<Long, OutputBlobReference>,
  )

  private suspend fun readPublication(
    reference: OutputBlobReference,
    owner: String,
    phase: OutputManifestRecord.Header.Phase,
  ): Publication {
    var header: OutputManifestRecord.Header? = null
    val blobs = mutableMapOf<Long, OutputBlobReference>()
    outputManifestStore.read(reference).collect { record ->
      if (record.hasHeader()) {
        check(
          record.header.owner == owner &&
            record.header.rawImpressionUpload == rawImpressionUpload &&
            record.header.phase == phase &&
            record.header.modelLinesList == listOf(modelLine)
        ) {
          "Pool output manifest scope does not match its owner"
        }
        header = record.header
      } else {
        check(record.hasSubpoolBlob()) { "Unexpected pool manifest record" }
        check(blobs.put(record.subpoolBlob.poolOffset, record.subpoolBlob.blob) == null) {
          "Duplicate subpool output"
        }
        outputManifestStore.readBlob(record.subpoolBlob.blob)
      }
    }
    return Publication(checkNotNull(header), blobs)
  }

  private suspend fun runLastShardOut(parent: RawImpressionUploadModelLine) =
    Tracing.traceSuspending(
      spanName = "edpa.vid_labeling.pool_assignment.finalize",
      attributes =
        Attributes.builder()
          .put(VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME, rawImpressionUpload)
          .put(VidLabelingTraceAttributes.MODEL_LINE_NAME, modelLine)
          .put(VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME, poolAssignmentJob)
          .put(XmmTraceAttributes.LIFECYCLE_STAGE, "pool_assignment_finalize")
          .put(XmmTraceAttributes.OUTCOME, "started")
          .build(),
    ) {
      val finalized = runLastShardOutInternal(parent)
      Span.current()
        .setAttribute(
          XmmTraceAttributes.OUTCOME,
          if (finalized) "succeeded" else "already_completed",
        )
    }

  private suspend fun runLastShardOutInternal(parent: RawImpressionUploadModelLine): Boolean {
    if (parent.state in NOOP_FANOUT_STATES) return false
    check(parent.state == RawImpressionUploadModelLine.State.POOL_ASSIGNING) {
      "Parent has not reached POOL_ASSIGNING; retry after the dispatcher transition"
    }
    val selectedParent = if (parent.hasMergedOutputManifest()) parent else commitMerge(parent)
    val selected =
      readPublication(
        selectedParent.mergedOutputManifest,
        selectedParent.name,
        OutputManifestRecord.Header.Phase.POOL_MERGE,
      )
    check(selected.blobs.keys == selectedParent.poolOffsetsList.toSet()) {
      "Selected merge does not cover the parent's subpools"
    }
    if (selected.blobs.isEmpty()) {
      fanOutEmptyLabeling(selectedParent)
      return true
    } else {
      return fanOutRanking(
        selectedParent,
        selectedParent.poolOffsetsList,
        selectedParent.maxEventDate,
        selected.header.encryptedDek,
        selected.blobs.mapValues { it.value.blobUri },
      )
    }
    // Selected shard files remain available for interrupted finalizers and audit; lifecycle owns
    // cleanup.
  }

  private suspend fun commitMerge(
    parent: RawImpressionUploadModelLine
  ): RawImpressionUploadModelLine {
    val jobs = listShardJobs()
    check(jobs.isNotEmpty() && jobs.all { it.state == PoolAssignmentJob.State.SUCCEEDED }) {
      "Cannot merge incomplete pool assignment"
    }
    val expectedShards = (0 until totalShards).toSet()
    val actualShards = jobs.map { it.shardIndex }.toSet()
    if (jobs.size != totalShards || actualShards != expectedShards) {
      val error = IllegalStateException("Pool assignment is missing a declared shard")
      logPoolUnit(
        Level.WARNING,
        "edpa.vid_labeling.pool_assignment.merge_failed",
        "pool_assignment_merge",
        "failed",
        parent.poolOffsetsList.firstOrNull(),
        error,
        failureShard = (expectedShards - actualShards).firstOrNull(),
      )
      throw error
    }
    val inputs = mutableMapOf<Long, MutableList<SubpoolFingerprintsStore.SubpoolBlob>>()
    for (job in jobs) {
      if (job.hasOutputManifest()) {
        val shard =
          readPublication(
            job.outputManifest,
            job.name,
            OutputManifestRecord.Header.Phase.POOL_ASSIGNMENT,
          )
        for ((offset, ref) in shard.blobs) {
          inputs
            .getOrPut(offset) { mutableListOf() }
            .add(
              SubpoolFingerprintsStore.SubpoolBlob(
                SelectedStorageClient.parseBlobUri(ref.blobUri).key,
                shard.header.encryptedDek,
                ref.blobGeneration,
              )
            )
        }
      } else {
        // Old shard layouts are sparse and carry no manifest; preserve their original key and AAD.
        for (offset in parent.poolOffsetsList) {
          val legacyKey =
            SubpoolFingerprintsStore.shardSubpoolKey(
              outputBlobPrefixUri,
              rawImpressionUpload,
              modelLine,
              job.shardIndex,
              offset,
            )
          inputs
            .getOrPut(offset) { mutableListOf() }
            .add(SubpoolFingerprintsStore.SubpoolBlob(legacyKey, job.encryptedDek))
        }
      }
    }
    check(inputs.keys == parent.poolOffsetsList.toSet()) {
      "Shard manifests disagree with parent pool offsets"
    }
    val dek =
      if (inputs.isEmpty()) null
      else {
        check(parent.hasEncryptedMergedDek()) { "Nonempty merge has no persisted encryption key" }
        parent.encryptedMergedDek
      }
    val attemptId = newAttemptId()
    val readSemaphore = Semaphore(MERGE_READ_CONCURRENCY)
    val records = coroutineScope {
      val writes = Semaphore(SUBPOOL_UPLOAD_CONCURRENCY)
      parent.poolOffsetsList
        .map { offset ->
          async {
            writes.withPermit {
              val key = mergedSubpoolKey(offset) + "/attempt/" + attemptId
              val blob =
                try {
                  store.mergeSubpool(
                    checkNotNull(inputs[offset]),
                    key,
                    checkNotNull(dek),
                    readSemaphore,
                  )
                } catch (e: CancellationException) {
                  throw e
                } catch (e: Exception) {
                  logPoolUnit(
                    Level.WARNING,
                    "edpa.vid_labeling.pool_assignment.merge_failed",
                    "pool_assignment_merge",
                    "failed",
                    offset,
                    e,
                    failureShard = null,
                  )
                  throw e
                }
              Span.current()
                .addEvent(
                  "edpa.vid_labeling.pool_assignment.merged_subpool",
                  Attributes.builder()
                    .put(POOL_OFFSET, offset)
                    .put(XmmTraceAttributes.OUTCOME, "written")
                    .build(),
                )
              logPoolUnit(
                Level.INFO,
                "edpa.vid_labeling.pool_assignment.merged_subpool",
                "pool_assignment_merge",
                "written",
                offset,
                failureShard = null,
              )
              outputManifestRecord {
                subpoolBlob =
                  OutputManifestRecordKt.subpoolBlob {
                    poolOffset = offset
                    this.blob = outputManifestStore.reference(uriFor(key), blob)
                  }
              }
            }
          }
        }
        .awaitAll()
    }
    val manifest =
      outputManifestStore.write(
        uriFor(mergedSubpoolKey(0L) + "/attempt/" + attemptId + "/manifest"),
        manifestHeader(OutputManifestRecord.Header.Phase.POOL_MERGE, parent.name, attemptId, dek),
        records.asFlow(),
      )
    return try {
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadModelLinesStub.commitRawImpressionUploadModelLineMergedOutput(
          commitRawImpressionUploadModelLineMergedOutputRequest {
            name = parent.name
            etag = parent.etag
            requestId = deterministicUuid("${parent.name}|merged-output")
            outputManifest = manifest
          }
        )
      }
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ABORTED && e.status.code != Status.Code.FAILED_PRECONDITION)
        throw e
      val current = getParent()
      if (current != null && current.hasMergedOutputManifest()) current else throw e
    }
  }

  private suspend fun fanOutEmptyLabeling(parent: RawImpressionUploadModelLine) {
    val count = dispatchLabeling()
    try {
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            name = parent.name
            etag = parent.etag
            requestId = RequestIds.forMarkRawImpressionUploadModelLineLabeling(parent.name)
          }
        )
      }
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ABORTED && e.status.code != Status.Code.FAILED_PRECONDITION)
        throw e
      if (
        getParent()?.state !in
          setOf(
            RawImpressionUploadModelLine.State.LABELING,
            RawImpressionUploadModelLine.State.COMPLETED,
          )
      )
        throw e
    }
    if (count == 0) {
      val labeled = checkNotNull(getParent())
      if (labeled.state == RawImpressionUploadModelLine.State.LABELING) {
        rpcThrottlers.metadataWrite.onReady {
          rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineCompleted(
            markRawImpressionUploadModelLineCompletedRequest {
              name = labeled.name
              etag = labeled.etag
              requestId = RequestIds.forMarkRawImpressionUploadModelLineCompleted(labeled.name)
            }
          )
        }
      }
    }
  }

  /**
   * Fans out Phase 1 for the merged subpools: creates a `RankerJob`, publishes a `VidRankBuilder`
   * WorkItem for it, and finally flips the parent `RawImpressionUploadModelLine` `POOL_ASSIGNING`
   * -> `RANKING`. The state flip is last so that `RANKING` is the durable marker that the whole
   * last-shard-out completed (see [recoverIfLastShardOut]).
   *
   * Every step is idempotent on re-run: `RankerJob` creation is keyed by a deterministic
   * `request_id`, WorkItem creation by a deterministic `work_item_id` (a duplicate is tolerated),
   * and the flip is a no-op once the parent has advanced past `POOL_ASSIGNING`.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#3998): bin-pack subpools into
   *   shared `RankerJob`s by fingerprint count so small subpools share a VM. For now this creates
   *   one `RankerJob` (and one `VidRankBuilder` VM) per subpool.
   */
  private suspend fun fanOutRanking(
    parent: RawImpressionUploadModelLine,
    poolOffsets: List<Long>,
    maxEventDate: Date,
    mergedDek: EncryptedDek,
    mergedBlobUris: Map<Long, String>,
  ): Boolean {
    for (poolOffset in poolOffsets) {
      val offsets = listOf(poolOffset)
      val rankerJob = createRankerJob(offsets)
      Span.current()
        .addEvent(
          "edpa.vid_labeling.pool_assignment.ranker_job",
          Attributes.builder()
            .put(VidLabelingTraceAttributes.RANKER_JOB_NAME, rankerJob.name)
            .put(XmmTraceAttributes.OUTCOME, "resolved")
            .build(),
        )
      VidLabelingTraceLogging.log(
        logger,
        "edpa.vid_labeling.pool_assignment.ranker_job",
        VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to rawImpressionUpload,
        VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
        VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME_STRING to poolAssignmentJob,
        VidLabelingTraceAttributes.RANKER_JOB_NAME_STRING to rankerJob.name,
        VidLabelingTraceAttributes.POOL_OFFSET_STRING to poolOffset.toString(),
        XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "pool_assignment_finalize",
        XmmTraceAttributes.OUTCOME_STRING to "resolved",
      )
      publishVidRankBuilderWorkItem(rankerJob, offsets, maxEventDate, mergedDek, mergedBlobUris)
    }

    val transitioned = markParentRanking(parent)
    Span.current()
      .addEvent(
        "edpa.vid_labeling.pool_assignment.parent_transition",
        Attributes.builder()
          .put(VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME, parent.name)
          .put(XmmTraceAttributes.OUTCOME, if (transitioned) "ranking" else "already_completed")
          .build(),
      )
    VidLabelingTraceLogging.log(
      logger,
      "edpa.vid_labeling.pool_assignment.parent_transition",
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to rawImpressionUpload,
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
      VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME_STRING to poolAssignmentJob,
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_MODEL_LINE_NAME_STRING to parent.name,
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "pool_assignment_finalize",
      XmmTraceAttributes.OUTCOME_STRING to if (transitioned) "ranking" else "already_completed",
    )
    logger.info(
      if (transitioned) {
        "Fanned out ${poolOffsets.size} RankerJob(s) for $modelLine; parent advanced to RANKING"
      } else {
        "Fanned out ${poolOffsets.size} RankerJob(s) for $modelLine; parent already advanced"
      }
    )
    return transitioned
  }

  /**
   * Creates (idempotently) a `RankerJob` for [poolOffsets] under this upload. The `request_id` is
   * derived deterministically from (model line, offsets) so a redelivered last-shard-out reuses the
   * existing row instead of creating a duplicate.
   */
  private suspend fun createRankerJob(poolOffsets: List<Long>): RankerJob =
    rpcThrottlers.metadataWrite.onReady {
      rankerJobsStub.createRankerJob(
        createRankerJobRequest {
          parent = rawImpressionUpload
          rankerJob = rankerJob {
            cmmsModelLine = modelLine
            this.poolOffsets += poolOffsets
          }
          requestId = rankerJobRequestId(poolOffsets)
        }
      )
    }

  /**
   * Publishes one `VidRankBuilder` WorkItem for [rankerJob], pointing it at the merged blobs for
   * [offsets] (resolved against `subpool_map_storage_params`) and the [mergedDek] that encrypts
   * them. Tolerates `ALREADY_EXISTS` so a redelivered last-shard-out is a no-op.
   */
  private suspend fun publishVidRankBuilderWorkItem(
    rankerJob: RankerJob,
    offsets: List<Long>,
    maxEventDate: Date,
    mergedDek: EncryptedDek,
    mergedBlobUris: Map<Long, String>,
  ) {
    val params =
      vidRankBuilderParamsTemplate.copy {
        encryptedSubpoolMapsDek = mergedDek
        this.rankerJob = rankerJob.name
        this.maxEventDate = maxEventDate
        offsets.forEach { subpoolMapBlobUris.put(it, checkNotNull(mergedBlobUris[it])) }
        offsets.forEach { subpoolRankedSizes.put(it, labeler.rankedSize(it)) }
      }
    val workItemId = WorkItemIds.forVidRankBuilder(rankerJob.name)
    var outcome = "created"
    try {
      rpcThrottlers.controlPlane.onReady {
        workItemsStub.createWorkItem(
          createWorkItemRequest {
            this.workItemId = workItemId
            workItem = workItem {
              queue = vidRankBuilderQueue
              workItemParams =
                workItemParams {
                    appParams = params.pack()
                    traceContext.putAll(traceContextProvider())
                  }
                  .pack()
            }
          }
        )
      }
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
      outcome = "already_exists"
      logger.warning(
        "WorkItem " +
          workItemId +
          " already exists (ALREADY_EXISTS); skipping as idempotent " +
          "re-run; verify this request_id is upload-scoped, else a cross-run collision would be " +
          "silently skipped here (same shape as the dispatcher ALREADY_EXISTS/BlobUri bug)"
      )
    }
    Span.current()
      .addEvent(
        "edpa.vid_labeling.pool_assignment.work_item",
        Attributes.builder()
          .put(XmmTraceAttributes.WORK_ITEM_NAME, "workItems/$workItemId")
          .put(VidLabelingTraceAttributes.RANKER_JOB_NAME, rankerJob.name)
          .put(XmmTraceAttributes.OUTCOME, outcome)
          .build(),
      )
    VidLabelingTraceLogging.log(
      logger,
      "edpa.vid_labeling.pool_assignment.work_item",
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to rawImpressionUpload,
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
      VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME_STRING to poolAssignmentJob,
      VidLabelingTraceAttributes.RANKER_JOB_NAME_STRING to rankerJob.name,
      XmmTraceAttributes.WORK_ITEM_NAME_STRING to "workItems/$workItemId",
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to "pool_assignment_finalize",
      XmmTraceAttributes.OUTCOME_STRING to outcome,
    )
  }

  /** Flips the parent `RawImpressionUploadModelLine` `POOL_ASSIGNING` -> `RANKING`. */
  private suspend fun markParentRanking(parent: RawImpressionUploadModelLine): Boolean {
    if (parent.state != RawImpressionUploadModelLine.State.POOL_ASSIGNING) return false
    try {
      // Pass the parent's etag for AIP-154 optimistic locking: the POOL_ASSIGNING -> RANKING flip
      // only lands if the row hasn't changed since getParent() read it, so a concurrent runner that
      // already advanced (or otherwise mutated) the parent loses the CAS with ABORTED rather than
      // racing on the state guard alone.
      rpcThrottlers.metadataWrite.onReady {
        rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            name = parent.name
            etag = parent.etag
            requestId = RequestIds.forMarkRawImpressionUploadModelLineRanking(parent.name)
          }
        )
      }
      return true
    } catch (e: StatusException) {
      if (
        e.status.code != Status.Code.FAILED_PRECONDITION && e.status.code != Status.Code.ABORTED
      ) {
        throw e
      }
      val current = getParent()
      if (current != null && current.state in NOOP_FANOUT_STATES) {
        logger.info(
          "markRawImpressionUploadModelLineRanking(${parent.name}) conflicted because the parent " +
            "is now ${current.state}; treating as done"
        )
        return false
      }
      throw e
    }
  }

  private suspend fun listShardJobs(): List<PoolAssignmentJob> {
    val jobs = mutableListOf<PoolAssignmentJob>()
    poolAssignmentJobsStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            poolAssignmentJobsStub.listPoolAssignmentJobs(
              listPoolAssignmentJobsRequest {
                parent = rawImpressionUpload
                filter = ListPoolAssignmentJobsRequestKt.filter { cmmsModelLine = modelLine }
                this.pageToken = pageToken
              }
            )
          }
        ResourceList(response.poolAssignmentJobsList, response.nextPageToken)
      }
      .collect { page -> jobs.addAll(page) }
    return jobs
  }

  /**
   * Returns the parent `RawImpressionUploadModelLine` for this (`RawImpressionUpload`,
   * `ModelLine`), located via `ListRawImpressionUploadModelLines` filtered by model line, or `null`
   * if absent. Carries the resource name (for the state flip), the `pool_offsets` union, the
   * reduced `max_event_date`, and the current pipeline state — all populated by the
   * last-shard-out's `MarkPoolAssignmentJobSucceeded`.
   */
  private suspend fun getParent(): RawImpressionUploadModelLine? {
    var parentRow: RawImpressionUploadModelLine? = null
    rawImpressionUploadModelLinesStub
      .listResources { pageToken: String ->
        val response =
          rpcThrottlers.metadataRead.onReady {
            rawImpressionUploadModelLinesStub.listRawImpressionUploadModelLines(
              listRawImpressionUploadModelLinesRequest {
                parent = rawImpressionUpload
                filter =
                  ListRawImpressionUploadModelLinesRequestKt.filter { cmmsModelLine = modelLine }
                this.pageToken = pageToken
              }
            )
          }
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .collect { page ->
        page.forEach { line -> if (line.cmmsModelLine == modelLine) parentRow = line }
      }
    return parentRow
  }

  /** Converts an epoch-microsecond timestamp to a UTC `google.type.Date`. */
  private fun usecToUtcDate(timestampUsec: Long): Date {
    val instant =
      Instant.ofEpochSecond(
        Math.floorDiv(timestampUsec, 1_000_000L),
        Math.floorMod(timestampUsec, 1_000_000L) * 1_000L,
      )
    val localDate = instant.atZone(ZoneOffset.UTC).toLocalDate()
    return date {
      year = localDate.year
      month = localDate.monthValue
      day = localDate.dayOfMonth
    }
  }

  /**
   * Deterministic UUID4 idempotency key for the `RankerJob` covering [poolOffsets], stable across
   * redeliveries of the same (upload, model line, offsets) so the server reuses an existing row
   * rather than duplicating.
   */
  private fun rankerJobRequestId(poolOffsets: List<Long>): String =
    deterministicUuid("$rawImpressionUpload|$modelLine|${poolOffsets.sorted().joinToString(",")}")

  /**
   * Derives a deterministic UUID4 from [seed], stable across redeliveries, for use as an AIP-155
   * `request_id`. Computed from an MD5 digest of the seed with the RFC-4122 version (4) and variant
   * bits forced, so it satisfies a field's `format = UUID4`.
   */
  private fun deterministicUuid(seed: String): String {
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  private fun summary(subpoolCount: Int, sink: SubpoolAssignmentSink, lastShardOut: Boolean) =
    Result(
      subpoolCount = subpoolCount,
      labeled = sink.labeled,
      droppedOutsideWindow = sink.droppedOutsideWindow,
      unrouted = sink.unrouted,
      lastShardOut = lastShardOut,
    )

  // Blob layout lives in SubpoolFingerprintsStore so Phase-1 reads the same keys; these bind this
  // run's (blobPrefix, upload, model line) context.
  private fun shardSubpoolKey(shard: Int, poolOffset: Long) =
    SubpoolFingerprintsStore.shardSubpoolKey(
      blobPrefix,
      rawImpressionUpload,
      modelLine,
      shard,
      poolOffset,
    )

  private fun mergedSubpoolKey(poolOffset: Long) =
    SubpoolFingerprintsStore.mergedSubpoolKey(
      blobPrefix,
      rawImpressionUpload,
      modelLine,
      poolOffset,
    )

  private fun logPoolUnit(
    level: Level,
    event: String,
    lifecycleStage: String,
    outcome: String,
    poolOffset: Long?,
    error: Throwable? = null,
    failureShard: Int? = shardIndex,
  ) {
    VidLabelingTraceLogging.log(
      logger,
      level,
      event,
      VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME_STRING to rawImpressionUpload,
      VidLabelingTraceAttributes.MODEL_LINE_NAME_STRING to modelLine,
      VidLabelingTraceAttributes.POOL_ASSIGNMENT_JOB_NAME_STRING to poolAssignmentJob,
      VidLabelingTraceAttributes.POOL_OFFSET_STRING to poolOffset?.toString(),
      VidLabelingTraceAttributes.SHARD_INDEX_STRING to failureShard?.toString(),
      XmmTraceAttributes.LIFECYCLE_STAGE_STRING to lifecycleStage,
      XmmTraceAttributes.OUTCOME_STRING to outcome,
      XmmTraceAttributes.ERROR_TYPE_STRING to error?.let(XmmTraceAttributes::errorType),
      XmmTraceAttributes.ERROR_CODE_STRING to error?.let(XmmTraceAttributes::errorCode),
    )
  }

  companion object {
    private val SUBPOOL_COUNT = AttributeKey.longKey("xmm.edpa.pool_assignment.subpool_count")
    private val LABELED_COUNT = AttributeKey.longKey("xmm.edpa.pool_assignment.labeled_count")
    private val DROPPED_COUNT = AttributeKey.longKey("xmm.edpa.pool_assignment.dropped_count")
    private val UNROUTED_COUNT = AttributeKey.longKey("xmm.edpa.pool_assignment.unrouted_count")
    private val LAST_SHARD_OUT = AttributeKey.booleanKey("xmm.edpa.pool_assignment.last_shard_out")
    private val POOL_OFFSET = AttributeKey.longKey("xmm.edpa.pool_offset")
    private val SHARD_INDEX = AttributeKey.longKey("xmm.edpa.shard_index")
    private val logger = Logger.getLogger(SubpoolAssigner::class.java.name)

    /** Default max total in-flight shard reads across all concurrent subpool merges. */
    private const val DEFAULT_MERGE_READ_CONCURRENCY = 16

    /**
     * Max total in-flight shard reads across all concurrent subpool merges, bounding merge memory.
     * Tunable via the `MERGE_READ_CONCURRENCY` env var; default [DEFAULT_MERGE_READ_CONCURRENCY].
     * Read once at class-load, so changing the env var inside a running container has no effect.
     */
    private val MERGE_READ_CONCURRENCY =
      (System.getenv("MERGE_READ_CONCURRENCY")?.toIntOrNull() ?: DEFAULT_MERGE_READ_CONCURRENCY)
        .coerceAtLeast(1)

    /**
     * Default max concurrent per-subpool blob uploads for one shard's flush. Measured not a
     * bottleneck: the flush was ~3s at 1B/16-shard, and it runs after the read stream fully drains,
     * so it never contends with GCS reads. 16 keeps the uploads parallel while still bounding the
     * concurrent GCS upload streams, so a high-subpool-count model can't open an unbounded number
     * at once.
     */
    private const val DEFAULT_SUBPOOL_UPLOAD_CONCURRENCY = 16

    /**
     * Max concurrent per-subpool blob uploads for one shard's flush. Tunable via the
     * `SUBPOOL_UPLOAD_CONCURRENCY` env var; default [DEFAULT_SUBPOOL_UPLOAD_CONCURRENCY]. Read once
     * at class-load, so changing the env var inside a running container has no effect (symmetric
     * with [MERGE_READ_CONCURRENCY]).
     */
    private val SUBPOOL_UPLOAD_CONCURRENCY =
      (System.getenv("SUBPOOL_UPLOAD_CONCURRENCY")?.toIntOrNull()
          ?: DEFAULT_SUBPOOL_UPLOAD_CONCURRENCY)
        .coerceAtLeast(1)

    /** Parent states for which a stale last-shard-out delivery must not publish more work. */
    private val NOOP_FANOUT_STATES =
      setOf(
        RawImpressionUploadModelLine.State.FAILED,
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.LABELING,
        RawImpressionUploadModelLine.State.COMPLETED,
      )
  }
}
