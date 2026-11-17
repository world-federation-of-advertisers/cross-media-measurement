/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingConcurrencyIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `same memoized line serializes while a direct line proceeds independently`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    withholdNextPoolAssignmentPublications()
    writeRawFile("serial-a", "input.parquet", listOf("memo-a"))
    val firstGeneration = finalizeRawUpload("serial-a")
    workItemTransport.awaitIdle()

    setVisibleModelLines(DIRECT_MODEL_LINE)
    writeRawFile("parallel-direct", "input.parquet", listOf("direct"))
    val directGeneration = finalizeRawUpload("parallel-direct")
    awaitPipelineIdle()

    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    writeRawFile("serial-b", "input.parquet", listOf("memo-b"))
    val secondGeneration = finalizeRawUpload("serial-b")
    workItemTransport.awaitIdle()

    val uploads = listUploads().associateBy { it.doneBlobGeneration }
    val first = uploads.getValue(firstGeneration)
    val direct = uploads.getValue(directGeneration)
    val second = uploads.getValue(secondGeneration)
    assertThat(listModelLines(first.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    assertThat(listModelLines(direct.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(listModelLines(second.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.CREATED)

    republishQueuedPoolAssignments()
    drainSequencer()

    assertThat(listModelLines(first.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(listModelLines(second.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
  }

  @Test
  fun `duplicate phase zero delivery converges once`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    duplicateNextPoolAssignmentDelivery()

    runSingleUpload("duplicate-phase-0")

    assertThat(poolAssignmentWorkItems()).hasSize(1)
    assertThat(workItemTransport.deliveryCount(poolAssignmentWorkItems().single().name))
      .isAtLeast(2)
  }

  @Test
  fun `duplicate phase one delivery converges once`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    duplicateNextRankBuilderDelivery()

    runSingleUpload("duplicate-phase-1")

    assertThat(rankBuilderWorkItems()).hasSize(1)
    assertThat(workItemTransport.deliveryCount(rankBuilderWorkItems().single().name)).isAtLeast(2)
  }

  @Test
  fun `duplicate phase two delivery converges once`() = runBlocking {
    setVisibleModelLines(DIRECT_MODEL_LINE)
    duplicateNextVidLabelerDelivery()

    runSingleUpload("duplicate-phase-2")

    assertThat(vidLabelerWorkItems()).hasSize(1)
    assertThat(workItemTransport.deliveryCount(vidLabelerWorkItems().single().name)).isAtLeast(2)
  }

  @Test
  fun `two rank builders racing commit one cumulative snapshot`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    withholdNextRankBuilderPublications()
    writeRawFile("rank-commit-race", "input.parquet", listOf("person"))
    val generation = finalizeRawUpload("rank-commit-race")
    workItemTransport.awaitIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }
    val rankWorkItem = rankBuilderWorkItems().single()

    val results =
      List(2) { async(Dispatchers.Default) { runCatching { runRankBuilderDirect(rankWorkItem) } } }
        .awaitAll()

    assertThat(results.all { it.isSuccess }).isTrue()
    workItemTransport.awaitIdle()
    val rankRows = listRankIndexBlobs(upload.name)
    assertThat(
        rankRows.count {
          it.blobType ==
            org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob.BlobType.SNAPSHOT
        }
      )
      .isEqualTo(1)
    assertThat(
        rankRows.count {
          it.blobType ==
            org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob.BlobType.DAY_ONLY
        }
      )
      .isEqualTo(1)
    assertThat(vidLabelerWorkItems()).hasSize(1)
    assertThat(listMetadata()).hasSize(1)
  }

  @Test
  fun `phase zero retries after a transient model read failure`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    withholdNextPoolAssignmentPublications()
    writeRawFile("retry-phase-0", "input.parquet", listOf("person"))
    finalizeRawUpload("retry-phase-0")
    workItemTransport.awaitIdle()
    val modelKey = blobKey(modelBlobUri)
    val validModel = checkNotNull(fileStorage.getBlob(modelKey)).read().flatten()
    fileStorage.writeBlob(modelKey, flowOf(ByteString.copyFromUtf8("invalid model")))
    workItemTransport.beforeNextRedelivery { fileStorage.writeBlob(modelKey, flowOf(validModel)) }

    republishQueuedPoolAssignments()
    awaitPipelineIdle()

    assertThat(poolAssignmentWorkItems()).hasSize(1)
    assertThat(workItemTransport.attemptCount(poolAssignmentWorkItems().single().name)).isEqualTo(2)
    assertThat(listMetadata()).hasSize(1)
  }

  @Test
  fun `phase two retries after a transient model read failure`() = runBlocking {
    setVisibleModelLines(DIRECT_MODEL_LINE)
    withholdNextVidLabelerPublications()
    writeRawFile("retry-phase-2", "input.parquet", listOf("person"))
    finalizeRawUpload("retry-phase-2")
    workItemTransport.awaitIdle()
    val modelKey = blobKey(modelBlobUri)
    val validModel = checkNotNull(fileStorage.getBlob(modelKey)).read().flatten()
    fileStorage.writeBlob(modelKey, flowOf(ByteString.copyFromUtf8("invalid model")))
    workItemTransport.beforeNextRedelivery { fileStorage.writeBlob(modelKey, flowOf(validModel)) }

    republishQueuedVidLabelers()
    awaitPipelineIdle()

    assertThat(vidLabelerWorkItems()).hasSize(1)
    assertThat(workItemTransport.attemptCount(vidLabelerWorkItems().single().name)).isEqualTo(2)
    assertThat(listMetadata()).hasSize(1)
  }

  @Test
  fun `monitor repairs a completed phase zero child whose parent transition was lost`() =
    runBlocking {
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextPoolAssignmentPublications()
      withholdNextRankBuilderPublications()

      writeRawFile("monitor-phase-0", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("monitor-phase-0")
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/" +
          "MarkRawImpressionUploadModelLineRanking"
      )
      holdNextPoolAssignmentRedelivery()
      republishQueuedPoolAssignments()
      awaitHeldPoolAssignmentRedelivery()

      delay(10L)
      val result = buildVidLabelingMonitor().runHealth()

      assertThat(result.recoveredTransitions).isEqualTo(1)
      withTimeout(5_000L) {
        while (
          listModelLines(upload.name).single().state != RawImpressionUploadModelLine.State.RANKING
        ) {
          delay(10L)
        }
      }
      releaseHeldPoolAssignmentRedelivery()
      republishQueuedRankBuilders()
      awaitPipelineIdle()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listMetadata()).hasSize(1)
    }

  @Test
  fun `monitor repairs a completed phase one child whose parent transition was lost`() =
    runBlocking {
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextRankBuilderPublications()
      withholdNextVidLabelerPublications()

      writeRawFile("monitor-phase-1", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("monitor-phase-1")
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      withTimeout(5_000L) {
        while (
          listModelLines(upload.name).single().state != RawImpressionUploadModelLine.State.RANKING
        ) {
          delay(10L)
        }
      }
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/" +
          "MarkRawImpressionUploadModelLineLabeling"
      )
      holdNextRankBuilderRedelivery()
      republishQueuedRankBuilders()
      awaitHeldRankBuilderRedelivery()

      delay(10L)
      val result = buildVidLabelingMonitor().runHealth()

      assertThat(result.recoveredTransitions).isEqualTo(1)
      withTimeout(5_000L) {
        while (
          listModelLines(upload.name).single().state != RawImpressionUploadModelLine.State.LABELING
        ) {
          delay(10L)
        }
      }
      releaseHeldRankBuilderRedelivery()
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listMetadata()).hasSize(1)
    }

  @Test
  fun `monitor repairs a completed phase two child whose parent transition was lost`() =
    runBlocking {
      setVisibleModelLines(DIRECT_MODEL_LINE)
      withholdNextVidLabelerPublications()
      withholdNextAvailabilityPublications()

      writeRawFile("monitor-phase-2", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("monitor-phase-2")
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/" +
          "MarkRawImpressionUploadModelLineAvailabilitySyncing"
      )
      holdNextVidLabelerRedelivery()
      republishQueuedVidLabelers()
      awaitHeldVidLabelerRedelivery()

      delay(10L)
      val result = buildVidLabelingMonitor().runHealth()

      assertThat(result.recoveredTransitions).isEqualTo(1)
      withTimeout(5_000L) {
        while (
          listModelLines(upload.name).single().state !=
            RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING
        ) {
          delay(10L)
        }
      }
      releaseHeldVidLabelerRedelivery()
      republishQueuedAvailability()
      awaitPipelineIdle()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listMetadata()).hasSize(1)
    }

  @Test
  fun `exhausted phase zero work is failed repaired and retried to completion`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    withholdNextPoolAssignmentPublications()
    writeRawFile("operator-retry", "input.parquet", listOf("person"))
    val generation = finalizeRawUpload("operator-retry")
    workItemTransport.awaitIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }
    val modelKey = blobKey(modelBlobUri)
    val validModel = checkNotNull(fileStorage.getBlob(modelKey)).read().flatten()
    fileStorage.writeBlob(modelKey, flowOf(ByteString.copyFromUtf8("invalid model")))

    republishQueuedPoolAssignments()
    workItemTransport.awaitIdle(allowFailures = true)

    assertThat(poolAssignmentWorkItems().single().state).isEqualTo(WorkItem.State.FAILED)
    fileStorage.writeBlob(modelKey, flowOf(validModel))
    assertThat(buildDispatchFailer().failUpload(upload.name, "model repaired")).hasSize(1)
    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.FAILED)

    val retrier = buildFailedDispatchRetrier()
    edpaBeforeCallFaults.armAfterCommit(
      "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/" +
        "MarkRawImpressionUploadModelLinePoolAssigning"
    )
    assertThat(runCatching { retrier.retryFailed(upload.name, MEMOIZED_MODEL_LINE) }.isFailure)
      .isTrue()
    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)

    val concurrentRetries =
      List(2) {
          async(Dispatchers.Default) {
            runCatching { retrier.retryFailed(upload.name, MEMOIZED_MODEL_LINE) }
          }
        }
        .awaitAll()
    assertThat(concurrentRetries.all { it.isSuccess }).isTrue()
    val retryResults = concurrentRetries.map { it.getOrThrow() }
    assertThat(retryResults.map { it.newState }.toSet())
      .containsExactly(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    assertThat(retryResults.sumOf { it.workItemsRepublished }).isEqualTo(1)
    workItemTransport.awaitIdle(allowFailures = true)

    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(listMetadata()).hasSize(1)
    val repeated = retrier.retryFailed(upload.name, MEMOIZED_MODEL_LINE)
    assertThat(repeated.wasAlreadyStarted).isTrue()
    assertThat(repeated.workItemsRepublished).isEqualTo(0)
  }

  private suspend fun runSingleUpload(folder: String) {
    writeRawFile(folder, "input.parquet", listOf("person"))
    val generation = finalizeRawUpload(folder)
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }
    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(1)
  }
}
