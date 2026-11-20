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
import io.grpc.Status
import io.grpc.StatusException
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingRegistrationBoundaryIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `late duplicate rank registration retries without undoing completed phase one`() =
    runBlocking<Unit> { verifyStrictDuplicate(0) }

  @Test
  fun `duplicate rank registration retries after winner crashes before Mark`() =
    runBlocking<Unit> { verifyStrictDuplicate(1) }

  @Test
  fun `duplicate rank registration recovers winner Mark before handoff`() =
    runBlocking<Unit> { verifyStrictDuplicate(2) }

  private suspend fun verifyStrictDuplicate(crashStage: Int) =
    withTimeout(90_000L) {
      visibleModelLineNames.clear()
      visibleModelLineNames += MEMOIZED_MODEL_LINE
      withholdNextRankBuilderPublications()
      withholdNextVidLabelerPublications()
      writeRawFile("strict-race-$crashStage", "input.parquet", listOf("strict-person"))
      val generation = finalizeRawUpload("strict-race-$crashStage")
      awaitPipelineIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      val item = rankBuilderWorkItems().single()
      seedCompletedRankerSiblings(item)
      val before = rankerStatesFor(item)
      assertThat(before).hasSize(3)
      assertThat(before.values.count { it == "SUCCEEDED" }).isEqualTo(2)
      assertThat(before.values.count { it == "CREATED" }).isEqualTo(1)

      val held =
        edpaBeforeCallFaults.holdNextCall(
          "wfa.measurement.edpaggregator.v1alpha.RankIndexBlobService/BatchCreateRankIndexBlobs"
        )
      republishQueuedRankBuilders() // D is a real SDK execution, paused after its encrypted file
      // writes.
      held.held.await()
      assertThat(listRankIndexBlobs(upload.name)).isEmpty()
      try {
        val markMethod =
          "wfa.measurement.edpaggregator.v1alpha.RankerJobService/MarkRankerJobSucceeded"
        when (crashStage) {
          1 -> edpaBeforeCallFaults.arm(markMethod)
          2 -> edpaBeforeCallFaults.armAfterCommit(markMethod)
        }
        if (crashStage == 0) {
          val winner = runRankBuilderDirect(item) // C is another real ranker body for the same job.
          assertThat(winner.lastJobOut).isTrue()
        } else {
          val lostResponse = assertFailsWith<StatusException> { runRankBuilderDirect(item) }
          assertThat(lostResponse.status.code).isEqualTo(Status.Code.UNAVAILABLE)
        }
        assertThat(held.requests).hasSize(2)
        val registrationRequests =
          held.requests.map {
            it as org.wfanet.measurement.edpaggregator.v1alpha.BatchCreateRankIndexBlobsRequest
          }
        val loserRequest = registrationRequests[0]
        val winnerRequest = registrationRequests[1]
        assertThat(loserRequest.requestsList.map { it.requestId })
          .containsExactlyElementsIn(winnerRequest.requestsList.map { it.requestId })
          .inOrder()
        assertThat(loserRequest.requestsList.map { it.rankIndexBlob.blobUri })
          .isNotEqualTo(winnerRequest.requestsList.map { it.rankIndexBlob.blobUri })
        assertThat(loserRequest.requestsList.map { it.rankIndexBlob.encryptedDek })
          .isNotEqualTo(winnerRequest.requestsList.map { it.rankIndexBlob.encryptedDek })
        val winningBlobs = listRankIndexBlobs(upload.name)
        assertThat(winningBlobs).hasSize(2)
        val statesAfterC = rankerStatesFor(item)
        assertThat(statesAfterC.values.count { it == "SUCCEEDED" })
          .isEqualTo(if (crashStage == 1) 2 else 3)
        assertThat(vidLabelerWorkItems()).hasSize(if (crashStage == 0) 1 else 0)
        assertThat(listModelLines(upload.name).single().state)
          .isEqualTo(
            if (crashStage == 0) RawImpressionUploadModelLine.State.LABELING
            else RawImpressionUploadModelLine.State.RANKING
          )

        held.release() // Registration must reject D's different URI/DEK under the same
        // IDs.
        awaitPipelineIdle()
        assertThat(rankerStatesFor(item).values)
          .containsExactly("SUCCEEDED", "SUCCEEDED", "SUCCEEDED")
        assertThat(rankBuilderWorkItems().single().state).isEqualTo(WorkItem.State.SUCCEEDED)
        assertThat(workItemTransport.attemptErrors(item.name)).isEmpty()
        assertThat(workItemTransport.attemptCount(item.name)).isEqualTo(1)
        assertThat(vidLabelerWorkItems()).hasSize(1)
        assertThat(listModelLines(upload.name).single().state)
          .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
        assertThat(listRankIndexBlobs(upload.name)).containsExactlyElementsIn(winningBlobs)

        republishQueuedVidLabelers()
        awaitPipelineIdle()
        assertThat(vidLabelerWorkItems().single().state).isEqualTo(WorkItem.State.SUCCEEDED)
        assertThat(listModelLines(upload.name).single().state)
          .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
        assertThat(listMetadata()).hasSize(1)
      } finally {
        held.release()
      }
    }

  @Test
  fun `one hundred one files cross registration and metadata batch boundaries without loss`() =
    runBlocking {
      val template = writeRawFile("file-boundary", "input-000.parquet", listOf("boundary-person"))
      val bytes = checkNotNull(fileStorage.getBlob(blobKey(template))).read().flatten()
      for (index in 1..100) {
        fileStorage.writeBlob(
          "$rootKey/raw/file-boundary/input-${index.toString().padStart(3, '0')}.parquet",
          flowOf(bytes),
        )
      }

      val generation = finalizeRawUpload("file-boundary")
      awaitPipelineIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }

      assertThat(listUploadFiles(upload.name)).hasSize(101)
      assertThat(listModelLines(upload.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listMetadata()).hasSize(202)
      assertThat(listAvailabilityTasks(upload.name)).hasSize(2)
      assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
        .containsExactly(WorkItem.State.SUCCEEDED)
      Unit
    }

  @Test
  fun `registration resumes after the second file batch fails`() = runBlocking {
    val template =
      writeRawFile("partial-file-batch", "input-000.parquet", listOf("boundary-person"))
    val bytes = checkNotNull(fileStorage.getBlob(blobKey(template))).read().flatten()
    for (index in 1..100) {
      fileStorage.writeBlob(
        "$rootKey/raw/partial-file-batch/input-${index.toString().padStart(3, '0')}.parquet",
        flowOf(bytes),
      )
    }
    edpaBeforeCallFaults.arm(
      "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadFileService/" +
        "BatchCreateRawImpressionUploadFiles",
      skip = 1,
    )

    assertThat(runCatching { finalizeRawUpload("partial-file-batch") }.isFailure).isTrue()
    val doneUri = "$rawPrefix/partial-file-batch/done"
    val generation = generationOf(doneUri)
    val partial = listUploads().single { it.doneBlobGeneration == generation }
    assertThat(partial.registrationComplete).isFalse()
    assertThat(listUploadFiles(partial.name)).hasSize(100)

    rawWatcher.receivePath(
      doneUri,
      mapOf(
        org.wfanet.measurement.securecomputation.datawatcher.DataWatcher.GENERATION_METADATA_KEY to
          generation.toString()
      ),
    )
    awaitPipelineIdle()

    val completed = listUploads().single { it.doneBlobGeneration == generation }
    assertThat(completed.registrationComplete).isTrue()
    assertThat(listUploadFiles(completed.name)).hasSize(101)
    assertThat(listMetadata()).hasSize(202)
  }
}

internal class VidLabelingShardBoundaryIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      initiallyVisibleModelLines = setOf(MEMOIZED_MODEL_LINE),
      numberOfShards = 51,
    )
  ) {
  @Test
  fun `fifty one phase zero shards cross the batch boundary without loss`() = runBlocking {
    writeRawFile("shard-boundary", "input.parquet", listOf("person"))
    val generation = finalizeRawUpload("shard-boundary")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    assertThat(poolAssignmentWorkItems()).hasSize(51)
    assertThat(poolAssignmentWorkItems().map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(listMetadata()).hasSize(1)
  }
}

internal class VidLabelingFileBatchBoundaryIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      initiallyVisibleModelLines = setOf(DIRECT_MODEL_LINE),
      maxFileBatchSizeBytes = 1L,
    )
  ) {
  @Test
  fun `fifty one phase two file batches cross the batch boundary without loss`() = runBlocking {
    val template = writeRawFile("job-boundary", "input-00.parquet", listOf("person"))
    val bytes = checkNotNull(fileStorage.getBlob(blobKey(template))).read().flatten()
    for (index in 1..50) {
      fileStorage.writeBlob(
        "$rootKey/raw/job-boundary/input-${index.toString().padStart(2, '0')}.parquet",
        flowOf(bytes),
      )
    }

    val generation = finalizeRawUpload("job-boundary")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    assertThat(listUploadFiles(upload.name)).hasSize(51)
    assertThat(vidLabelerWorkItems()).hasSize(51)
    assertThat(vidLabelerWorkItems().map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(51)
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
  }
}

internal class VidLabelingModelLineBoundaryIntegrationTest :
  VidLabelingPipelineTestHarness(modelLineBoundaryConfig()) {
  @Test
  fun `fifty one direct model lines cross registration batching and share one labeling job`() =
    runBlocking {
      writeRawFile("model-line-boundary", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("model-line-boundary")
      awaitPipelineIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }

      assertThat(listModelLines(upload.name)).hasSize(51)
      assertThat(listModelLines(upload.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(vidLabelerWorkItems()).hasSize(1)
      assertThat(listAvailabilityTasks(upload.name)).hasSize(51)
      assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
        .containsExactly(WorkItem.State.SUCCEEDED)
      assertThat(listMetadata()).hasSize(51)
    }

  @Test
  fun `registration resumes after the second model line batch fails`() = runBlocking {
    writeRawFile("partial-model-lines", "input.parquet", listOf("person"))
    edpaBeforeCallFaults.arm(
      "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/" +
        "BatchCreateRawImpressionUploadModelLines",
      skip = 1,
    )

    assertThat(runCatching { finalizeRawUpload("partial-model-lines") }.isFailure).isTrue()
    val doneUri = "$rawPrefix/partial-model-lines/done"
    val generation = generationOf(doneUri)
    val partial = listUploads().single { it.doneBlobGeneration == generation }
    assertThat(partial.registrationComplete).isFalse()
    assertThat(listModelLines(partial.name)).hasSize(50)

    rawWatcher.receivePath(
      doneUri,
      mapOf(
        org.wfanet.measurement.securecomputation.datawatcher.DataWatcher.GENERATION_METADATA_KEY to
          generation.toString()
      ),
    )
    awaitPipelineIdle()

    val completed = listUploads().single { it.doneBlobGeneration == generation }
    assertThat(completed.registrationComplete).isTrue()
    assertThat(listModelLines(completed.name)).hasSize(51)
    assertThat(listMetadata()).hasSize(51)
  }
}

private const val BOUNDARY_MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"

private fun modelLineBoundaryConfig(): PipelineHarnessConfig {
  val activeStart = EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant()
  val activeEnd = EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant()
  val extras =
    (1..50).map { index ->
      ModelLineFixture(
        "$BOUNDARY_MODEL_SUITE/modelLines/direct-boundary-$index",
        "$BOUNDARY_MODEL_SUITE/modelReleases/direct-boundary-$index",
        memoized = false,
        activeStart = activeStart,
        activeEnd = activeEnd,
      )
    }
  return PipelineHarnessConfig(
    extraModelLines = extras,
    initiallyVisibleModelLines = extras.mapTo(mutableSetOf(DIRECT_MODEL_LINE)) { it.name },
  )
}
