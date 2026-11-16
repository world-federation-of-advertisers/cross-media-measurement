// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

/** Verifies checkpoint continuity through a real partial rank RPC failure and operator retry. */
internal class VidLabelingPartialRankFailureIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      initiallyVisibleModelLines = setOf(MEMOIZED_MODEL_LINE),
      errorIfGapsExist = true,
      redeliveryDelayMillis = 1_000L,
      maxDeliveryAttempts = 5,
      initialToday = EVENT_DATE.plusDays(9),
      dispatcherClockUsesToday = true,
    )
  ) {
  @Test
  fun `partial rank checkpoint survives exhausted delivery and blocks later computation until retry`() =
    runBlocking<Unit> {
      for (day in 1..3) assertCompleted(uploadDay(day))
      val fifth = uploadDay(5)
      assertThat(listModelLines(fifth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)

      // Finish Phase 0 before delivering Phase 1. This keeps the five deliveries focused on
      // the rank RPC fault rather than the independently valid Phase-0 transition race.
      withholdNextRankBuilderPublications(count = 2)
      writeRawEvents(
        "day-4",
        "two-pools.parquet",
        listOf(
          RawEventFixture("fourth-male", "fourth-male", date(4), gender = "MALE"),
          RawEventFixture("fourth-female", "fourth-female", date(4), gender = "FEMALE"),
        ),
      )
      val fourthGeneration = finalizeRawUpload("day-4")
      awaitPipelineIdle(allowFailures = true)
      val fourth = listUploads().single { it.doneBlobGeneration == fourthGeneration }
      assertThat(listModelLines(fourth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.RANKING)
      assertThat(listRankIndexBlobs(fourth.name)).isEmpty()
      val queuedRankTasks =
        rankBuilderWorkItems().filter { item ->
          item.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(VidRankBuilderParams::class.java)
            .rankerJob
            .startsWith("${fourth.name}/rankerJobs/")
        }
      assertThat(queuedRankTasks).hasSize(2)
      assertThat(queuedRankTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.QUEUED)
      for (task in queuedRankTasks) {
        val params =
          task.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(VidRankBuilderParams::class.java)
        assertThat(params.subpoolMapBlobUrisCount).isEqualTo(1)
      }
      val firstRankTask = queuedRankTasks.first()
      val secondRankTask = queuedRankTasks.last()
      workItemTransport.republishWorkItem(firstRankTask.name)
      awaitPipelineIdle(allowFailures = true)
      assertThat(rankBuilderWorkItems().single { it.name == firstRankTask.name }.state)
        .isEqualTo(WorkItem.State.SUCCEEDED)
      assertThat(workItemTransport.attemptCount(firstRankTask.name)).isEqualTo(1)
      val firstPoolBlobs = listRankIndexBlobs(fourth.name)
      assertThat(firstPoolBlobs.map { it.blobType })
        .containsExactly(RankIndexBlob.BlobType.SNAPSHOT, RankIndexBlob.BlobType.DAY_ONLY)

      // Phase 0 currently creates one rank WorkItem per pool. Let the first pool finish, then
      // fail the second pool's atomic SNAPSHOT+DAY_ONLY batch on each of its five deliveries.
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.RankIndexBlobService/BatchCreateRankIndexBlobs",
        count = 5,
      )
      workItemTransport.republishWorkItem(secondRankTask.name)
      awaitPipelineIdle(allowFailures = true)
      val partialBlobs = listRankIndexBlobs(fourth.name)
      assertThat(partialBlobs.map { it.blobType })
        .containsExactly(RankIndexBlob.BlobType.SNAPSHOT, RankIndexBlob.BlobType.DAY_ONLY)
      val retainedSnapshot = partialBlobs.single { it.blobType == RankIndexBlob.BlobType.SNAPSHOT }
      assertThat(partialBlobs.map { it.poolOffset }.toSet()).hasSize(1)
      val retainedEntries = rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT)
      assertThat(partialBlobs).containsExactlyElementsIn(firstPoolBlobs)
      val failedTask = rankBuilderWorkItems().single { it.name == secondRankTask.name }
      assertThat(failedTask.state).isEqualTo(WorkItem.State.FAILED)
      assertThat(workItemTransport.attemptCount(failedTask.name)).isEqualTo(5)
      assertThat(workItemTransport.deliveryCount(failedTask.name)).isEqualTo(5)
      assertThat(listModelLines(fourth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.RANKING)

      buildVidLabelingMonitor().runHealth()
      assertThat(listModelLines(fourth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.FAILED)
      assertThat(listModelLines(fourth.name).single().failureReason)
        .isEqualTo(RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE)
      val sixth = uploadDay(6)
      assertThat(listModelLines(sixth.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.CREATED)
      assertThat(listRankIndexBlobs(sixth.name)).isEmpty()
      assertThat(listRankIndexBlobs(fourth.name)).containsExactlyElementsIn(partialBlobs)

      edpaBeforeCallFaults.clear()
      val retry = buildFailedDispatchRetrier().retryFailed(fourth.name, MEMOIZED_MODEL_LINE)
      assertThat(retry.newState).isEqualTo(RawImpressionUploadModelLine.State.RANKING)
      assertThat(retry.workItemsRepublished).isEqualTo(1)
      assertThat(
          rankBuilderWorkItems().filter { item ->
            item.workItemParams
              .unpack(WorkItem.WorkItemParams::class.java)
              .appParams
              .unpack(VidRankBuilderParams::class.java)
              .rankerJob ==
              firstRankTask.workItemParams
                .unpack(WorkItem.WorkItemParams::class.java)
                .appParams
                .unpack(VidRankBuilderParams::class.java)
                .rankerJob
          }
        )
        .hasSize(1)
      awaitPipelineIdle(allowFailures = true)
      for (tick in 1..8) {
        assertThat(buildVidLabelingMonitor().runDispatch().dispatchError).isFalse()
        awaitPipelineIdle(allowFailures = true)
      }

      assertCompleted(fourth)
      assertCompleted(fifth)
      assertCompleted(sixth)
      val completedBlobs = listRankIndexBlobs(fourth.name)
      val snapshots = completedBlobs.filter { it.blobType == RankIndexBlob.BlobType.SNAPSHOT }
      assertThat(snapshots).hasSize(2)
      assertThat(snapshots.map { it.poolOffset }.toSet()).hasSize(2)
      assertThat(completedBlobs.single { it.name == retainedSnapshot.name })
        .isEqualTo(retainedSnapshot)
      assertThat(
          rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT).filter {
            it.poolOffset == retainedSnapshot.poolOffset
          }
        )
        .containsExactlyElementsIn(retainedEntries)
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.DAY_ONLY).map { it.digest })
        .containsAtLeast(digest("fourth-male"), digest("fourth-female"))
      assertAvailabilityPublished(setOf(MEMOIZED_MODEL_LINE), (1..6).map(::date).toSet())
    }

  private fun date(day: Int) = EVENT_DATE.plusDays(day - 1L)

  private suspend fun uploadDay(day: Int): RawImpressionUpload {
    writeRawFile("day-$day", "input.parquet", listOf("person-$day", "stable-person"), date(day))
    val generation = finalizeRawUpload("day-$day")
    awaitPipelineIdle(allowFailures = true)
    return listUploads().single { it.doneBlobGeneration == generation }
  }

  private suspend fun assertCompleted(upload: RawImpressionUpload) {
    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
  }
}
