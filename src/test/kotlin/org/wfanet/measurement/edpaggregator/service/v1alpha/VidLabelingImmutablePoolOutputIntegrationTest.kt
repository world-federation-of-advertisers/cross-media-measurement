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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import org.junit.Test
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.storage.BlobChangedException
import org.wfanet.measurement.storage.SelectedStorageClient

internal class VidLabelingImmutablePoolOutputIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      immutableOutputPublication = true,
      initiallyVisibleModelLines = setOf(MEMOIZED_MODEL_LINE),
    )
  ) {
  private val getMethod =
    "wfa.measurement.edpaggregator.v1alpha.PoolAssignmentJobService/GetPoolAssignmentJob"
  private val markMethod =
    "wfa.measurement.edpaggregator.v1alpha.PoolAssignmentJobService/MarkPoolAssignmentJobSucceeded"

  private fun params(work: WorkItem): SubpoolAssignerParams =
    work.workItemParams
      .unpack(WorkItemParams::class.java)
      .appParams
      .unpack(SubpoolAssignerParams::class.java)

  private suspend fun prepare(folder: String): Pair<WorkItem, String> {
    withholdNextPoolAssignmentPublications()
    writeRawFile(folder, "input.parquet", listOf("person"))
    val generation = finalizeRawUpload(folder)
    workItemTransport.awaitIdle()
    return poolAssignmentWorkItems().single() to
      listUploads().single { it.doneBlobGeneration == generation }.name
  }

  private suspend fun job(work: WorkItem): PoolAssignmentJob =
    PoolAssignmentJobServiceCoroutineStub(edpaPublicServer.channel)
      .getPoolAssignmentJob(getPoolAssignmentJobRequest { name = params(work).poolAssignmentJob })

  @Test
  fun `competing pool workers select matching shard merge and ranker input versions`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("pool-race")
      val gate = responseGates.arm(getMethod)
      val attempts =
        List(2) { async(Dispatchers.Default) { runCatching { runPoolAssignerDirect(work) } } }
      gate.awaitBoth()
      assertThat(gate.messages.map { (it as PoolAssignmentJob).state })
        .containsExactly(PoolAssignmentJob.State.CREATED, PoolAssignmentJob.State.CREATED)
      assertThat(attempts.awaitAll().all { it.isSuccess }).isTrue()
      awaitPipelineIdle()
      verifySelectedInputs(work, upload)
    }

  @Test
  fun `precommit manifest collision cannot mark success and a fresh attempt recovers`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("pool-orphan")
      val id = "00000000-0000-4000-8000-000000000099"
      forcedPoolAttemptId = id
      val p = params(work)
      val orphanKey =
        SubpoolFingerprintsStore.shardSubpoolKey(
          SelectedStorageClient.parseBlobUri(p.subpoolMapStorageParams.blobPrefix).key,
          p.rawImpressionUpload,
          p.modelLine,
          p.shardIndex,
          0L,
        ) + "/attempt/" + id + "/manifest"
      mapStorage.writeBlob(orphanKey, ByteString.copyFromUtf8("orphan"))
      val original = mapStorage.getFreshnessToken(orphanKey)
      val mutations = responseGates.callCount(markMethod)
      assertFailsWith<BlobChangedException> { runPoolAssignerDirect(work) }
      assertThat(job(work).state).isEqualTo(PoolAssignmentJob.State.CREATED)
      assertThat(responseGates.callCount(markMethod)).isEqualTo(mutations)
      assertThat(mapStorage.getFreshnessToken(orphanKey)).isEqualTo(original)
      forcedPoolAttemptId = null
      runPoolAssignerDirect(work)
      awaitPipelineIdle()
      assertThat(job(work).state).isEqualTo(PoolAssignmentJob.State.SUCCEEDED)
      assertThat(listModelLines(upload).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("person")
      assertThat(mapStorage.getFreshnessToken(orphanKey)).isEqualTo(original)
    }

  @Test
  fun `empty pool output fans out normal labeling jobs and publishes successful empty output`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("empty-pool")
      val outer = work.workItemParams.unpack(WorkItemParams::class.java)
      val modified =
        params(work)
          .toBuilder()
          .setActiveStartTime(
            EVENT_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
          )
          .build()
      val delayedWindow =
        work
          .toBuilder()
          .setWorkItemParams(outer.toBuilder().setAppParams(modified.pack()).build().pack())
          .build()
      runPoolAssignerDirect(delayedWindow)
      awaitPipelineIdle()
      val selectedJob = job(work)
      assertThat(selectedJob.hasEncryptedDek()).isFalse()
      assertThat(selectedJob.lastShard).isTrue()
      assertThat(rankBuilderWorkItems()).isEmpty()
      assertThat(listRankIndexBlobs(upload)).isEmpty()
      assertThat(vidLabelerWorkItems()).hasSize(1)
      assertThat(listMetadata()).isEmpty()
      assertThat(listModelLines(upload).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listAvailabilityTasks(upload).single().state).isEqualTo(WorkItem.State.SUCCEEDED)
    }

  @Test
  fun `lost pool completion reply reuses the selected shard and completes its finalizer`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("lost-shard-reply")
      edpaBeforeCallFaults.armAfterCommit(markMethod)
      assertThat(runCatching { runPoolAssignerDirect(work) }.isFailure).isTrue()
      val selected = job(work).outputManifest
      assertThat(job(work).state).isEqualTo(PoolAssignmentJob.State.SUCCEEDED)
      runPoolAssignerDirect(work)
      awaitPipelineIdle()
      assertThat(job(work).outputManifest).isEqualTo(selected)
      assertThat(listModelLines(upload).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("person")
    }

  @Test
  fun `lost merge selection reply reuses the selected merge before ranker publication`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("lost-merge-reply")
      edpaBeforeCallFaults.armAfterCommit(
        "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/CommitRawImpressionUploadModelLineMergedOutput"
      )
      assertThat(runCatching { runPoolAssignerDirect(work) }.isFailure).isTrue()
      val selected = listModelLines(upload).single().mergedOutputManifest
      assertThat(selected.blobGeneration).isGreaterThan(0L)
      runPoolAssignerDirect(work)
      awaitPipelineIdle()
      assertThat(listModelLines(upload).single().mergedOutputManifest).isEqualTo(selected)
      assertThat(rankBuilderWorkItems()).hasSize(1)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("person")
    }

  private suspend fun verifySelectedInputs(work: WorkItem, upload: String) {
    val selectedJob = job(work)
    val store = OutputManifestStore(mapStorage, params(work).subpoolMapStorageParams.blobPrefix)
    val shardRecords = store.read(selectedJob.outputManifest).toList()
    assertThat(shardRecords.first().header.encryptedDek).isEqualTo(selectedJob.encryptedDek)
    val fingerprints = SubpoolFingerprintsStore(mapStorage, kmsClient)
    for (entry in shardRecords.drop(1)) {
      val ref = entry.subpoolBlob.blob
      val records =
        fingerprints
          .readBlob(
            SelectedStorageClient.parseBlobUri(ref.blobUri).key,
            shardRecords.first().header.encryptedDek,
            ref.blobGeneration,
          )
          .toList()
      assertThat(records.sumOf { it.fingerprints.size() / 12 }).isEqualTo(1)
    }
    val parent = listModelLines(upload).single()
    val mergeRecords = store.read(parent.mergedOutputManifest).toList()
    val merged = mergeRecords.drop(1).associate { it.subpoolBlob.poolOffset to it.subpoolBlob.blob }
    val rankParams =
      rankBuilderWorkItems()
        .single()
        .workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidRankBuilderParams::class.java)
    assertThat(rankParams.subpoolMapBlobUrisMap)
      .containsExactlyEntriesIn(merged.mapValues { it.value.blobUri })
    assertThat(rankParams.encryptedSubpoolMapsDek)
      .isEqualTo(mergeRecords.first().header.encryptedDek)
    for (entry in mergeRecords.drop(1)) {
      val ref = entry.subpoolBlob.blob
      val records =
        fingerprints
          .readBlob(
            SelectedStorageClient.parseBlobUri(ref.blobUri).key,
            mergeRecords.first().header.encryptedDek,
            ref.blobGeneration,
          )
          .toList()
      assertThat(records.sumOf { it.fingerprints.size() / 12 }).isEqualTo(1)
    }
    assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId }).containsExactly("person")
    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet()).containsExactly(upload)
  }

  @Test
  fun `stale pool worker colliding with a successful winner makes no success mutation`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("successful-pool-collision")
      forcedPoolAttemptId = "00000000-0000-4000-8000-000000000099"
      val before = responseGates.callCount(markMethod)
      val gate = responseGates.arm(getMethod, holdSecond = true)
      val attempts =
        List(2) { async(Dispatchers.Default) { runCatching { runPoolAssignerDirect(work) } } }
      gate.awaitBoth()
      assertThat(gate.messages.map { (it as PoolAssignmentJob).state })
        .containsExactly(PoolAssignmentJob.State.CREATED, PoolAssignmentJob.State.CREATED)
      val first = select { attempts.forEach { it.onAwait { result -> result } } }
      assertThat(first.isSuccess).isTrue()
      awaitPipelineIdle()
      val selected = job(work)
      val parent = listModelLines(upload).single()
      val generations = outputGenerations(MEMOIZED_MODEL_LINE)
      gate.releaseSecond()
      assertThat(attempts.awaitAll().all { it.isSuccess }).isTrue()
      assertThat(responseGates.callCount(markMethod)).isEqualTo(before + 1)
      assertThat(job(work)).isEqualTo(selected)
      assertThat(listModelLines(upload).single()).isEqualTo(parent)
      assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).containsExactlyEntriesIn(generations)
      forcedPoolAttemptId = null
      verifySelectedInputs(work, upload)
    }

  @Test
  fun `complete shard manifest before selection remains orphaned after a fresh retry`() =
    runBlocking<Unit> { recoverBeforeSelection("shard") }

  @Test
  fun `complete merge manifest before selection remains orphaned after a fresh retry`() =
    runBlocking<Unit> { recoverBeforeSelection("merge") }

  private suspend fun recoverBeforeSelection(phase: String) {
    val (work, upload) = prepare("unselected-pool-" + phase)
    val p = params(work)
    val root = SelectedStorageClient.parseBlobUri(p.subpoolMapStorageParams.blobPrefix)
    val id = "00000000-0000-4000-8000-000000000099"
    forcedPoolAttemptId = id
    edpaBeforeCallFaults.arm(
      if (phase == "shard") markMethod
      else
        "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/CommitRawImpressionUploadModelLineMergedOutput"
    )
    assertThat(runCatching { runPoolAssignerDirect(work) }.isFailure).isTrue()
    val parent = listModelLines(upload).single()
    assertThat(parent.hasMergedOutputManifest()).isFalse()
    if (phase == "shard") {
      assertThat(job(work).state).isEqualTo(PoolAssignmentJob.State.CREATED)
      assertThat(job(work).hasOutputManifest()).isFalse()
    } else {
      assertThat(job(work).state).isEqualTo(PoolAssignmentJob.State.SUCCEEDED)
    }
    val key =
      (if (phase == "shard")
        SubpoolFingerprintsStore.shardSubpoolKey(
          root.key,
          p.rawImpressionUpload,
          p.modelLine,
          p.shardIndex,
          0L,
        )
      else
        SubpoolFingerprintsStore.mergedSubpoolKey(
          root.key,
          p.rawImpressionUpload,
          p.modelLine,
          0L,
        )) + "/attempt/" + id + "/manifest"
    val blob = checkNotNull(mapStorage.getBlob(key))
    val generation = mapStorage.getFreshnessToken(key)
    val store = OutputManifestStore(mapStorage, p.subpoolMapStorageParams.blobPrefix)
    val uri = root.scheme + "://" + root.bucket + "/" + key
    val orphan = store.reference(uri, blob)
    assertThat(store.read(orphan).toList().first().header.attemptId).isEqualTo(id)
    assertThat(rankBuilderWorkItems()).isEmpty()
    assertThat(listMetadata()).isEmpty()
    forcedPoolAttemptId = null
    runPoolAssignerDirect(work)
    awaitPipelineIdle()
    val selected =
      if (phase == "shard") job(work).outputManifest
      else listModelLines(upload).single().mergedOutputManifest
    assertThat(selected).isNotEqualTo(orphan)
    assertThat(mapStorage.getFreshnessToken(key)).isEqualTo(generation)
    verifySelectedInputs(work, upload)
  }
}
