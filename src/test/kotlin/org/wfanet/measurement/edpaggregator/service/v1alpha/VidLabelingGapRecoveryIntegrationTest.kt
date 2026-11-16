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
import java.time.LocalDate
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest

/** Exercises provider uploads with real pipeline processing; dates need not arrive in order. */
internal abstract class VidLabelingGapRecoveryHarness :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      errorIfGapsExist = true,
      redeliveryDelayMillis = 1_000L,
      maxDeliveryAttempts = 5,
      initialToday = EVENT_DATE.plusDays(9),
      dispatcherClockUsesToday = true,
    )
  ) {
  protected fun date(day: Int): LocalDate = EVENT_DATE.plusDays(day - 1L)

  protected suspend fun seedPublishedDays() {
    for (day in 1..3) assertCompletedForBothPaths(uploadDay(day))
  }

  protected suspend fun uploadDay(day: Int, folder: String = "day-$day"): RawImpressionUpload {
    // All provider dates are historical; late arrivals do not move the worker clock backward.
    setToday(EVENT_DATE.plusDays(9))
    writeRawFile(folder, "input.parquet", listOf("person-$day", "stable-person"), date(day))
    val generation = finalizeRawUpload(folder)
    awaitPipelineIdle(allowFailures = true)
    return listUploads().single { it.doneBlobGeneration == generation }
  }

  protected suspend fun assertPending(upload: RawImpressionUpload) {
    assertThat(listModelLines(upload.name).map { it.state }.toSet())
      .containsExactly(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
  }

  protected suspend fun recoverPending() {
    repeat(8) {
      val result = buildVidLabelingMonitor().runDispatch()
      assertThat(result.dispatchError).isFalse()
      awaitPipelineIdle(allowFailures = true)
    }
  }
}

internal class VidLabelingGapRecoveryIntegrationTest : VidLabelingGapRecoveryHarness() {
  @Test
  fun `late missing day unblocks availability without labeling the later day again`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val later = uploadDay(5)
      assertPending(later)
      val artifacts = snapshotOutputArtifacts(listMetadata())
      val labelingCount = vidLabelerWorkItems().size
      val originalTasks = listAvailabilityTasks(later.name).associateBy { it.name }
      assertThat(originalTasks.values.map { it.state }.toSet())
        .containsExactly(WorkItem.State.FAILED)

      val fourth = uploadDay(4)
      val laterDigest = digest("person-5-0")
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT).map { it.digest })
        .contains(laterDigest)
      recoverPending()

      assertCompletedForBothPaths(later)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..5).map(::date).toSet(),
      )
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
      assertThat(vidLabelerWorkItems()).hasSize(labelingCount + 2)
      assertThat(
          readLabeledPeople(MEMOIZED_MODEL_LINE)
            .filter { it.personId == "stable-person" }
            .map { it.vid }
            .toSet()
        )
        .hasSize(1)
      for (task in listAvailabilityTasks(later.name)) {
        val original = originalTasks.getValue(task.name)
        assertThat(task.workItem.generation).isGreaterThan(original.workItem.generation)
        assertThat(task.doneBlobGeneration).isEqualTo(original.doneBlobGeneration)
        assertThat(task.doneBlobUri).isEqualTo(original.doneBlobUri)
      }
    }

  @Test
  fun `multiple pending days recover when the missing day arrives`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val later = (5..7).map { uploadDay(it).also { upload -> assertPending(upload) } }
      val artifacts = snapshotOutputArtifacts(listMetadata())
      uploadDay(4)
      recoverPending()
      for (upload in later) assertCompletedForBothPaths(upload)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..7).map(::date).toSet(),
      )
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
    }

  @Test
  fun `two missing days can arrive in either order while publication remains strict`() =
    runBlocking<Unit> {
      uploadDay(1)
      uploadDay(2)
      val later = uploadDay(5)
      assertPending(later)
      val before = dataProvidersService.requests.toList()
      val fourth = uploadDay(4)
      assertPending(fourth)
      recoverPending()
      assertThat(dataProvidersService.requests).containsExactlyElementsIn(before).inOrder()
      uploadDay(3)
      recoverPending()
      assertCompletedForBothPaths(later)
      assertCompletedForBothPaths(fourth)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..5).map(::date).toSet(),
      )
    }

  @Test
  fun `same date append with a new input URI does not overwrite a pending output`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val later = uploadDay(5, folder = "append")
      assertPending(later)
      val artifacts = snapshotOutputArtifacts(listMetadata())
      writeRawFile("append", "second.parquet", listOf("new-person"), date(5))
      val generation = finalizeRawUpload("append")
      awaitPipelineIdle(allowFailures = true)
      val append = listUploads().single { it.doneBlobGeneration == generation }
      assertPending(append)
      assertThat(listUploadFiles(append.name)).hasSize(1)
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
      uploadDay(4)
      recoverPending()
      assertCompletedForBothPaths(later)
      assertCompletedForBothPaths(append)
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in artifacts })
        .isEqualTo(artifacts)
    }

  @Test
  fun `availability retry keeps working after repeated failures while a gap remains`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val later = uploadDay(5)
      val originals = listAvailabilityTasks(later.name).associateBy { it.name }
      repeat(6) {
        assertThat(buildVidLabelingMonitor().runDispatch().dispatchError).isFalse()
        awaitPipelineIdle(allowFailures = true)
        assertPending(later)
      }
      uploadDay(4)
      recoverPending()
      assertCompletedForBothPaths(later)
      for (task in listAvailabilityTasks(later.name)) {
        assertThat(task.workItem.generation)
          .isGreaterThan(originals.getValue(task.name).workItem.generation)
      }
    }

  @Test
  fun `later done cannot publish across an earlier done whose metadata task is queued`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val before = dataProvidersService.requests.toList()
      withholdNextAvailabilityPublications(count = 2)
      val fourth = uploadDay(4)
      assertPending(fourth)
      assertThat(listAvailabilityTasks(fourth.name).map { it.state }.toSet())
        .containsExactly(WorkItem.State.QUEUED)
      val fifth = uploadDay(5)
      assertPending(fifth)
      assertThat(dataProvidersService.requests).containsExactlyElementsIn(before).inOrder()
      republishQueuedAvailability()
      awaitPipelineIdle(allowFailures = true)
      recoverPending()
      assertCompletedForBothPaths(fourth)
      assertCompletedForBothPaths(fifth)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..5).map(::date).toSet(),
      )
    }
}

internal class VidLabelingGapHealingIntegrationTest : VidLabelingGapRecoveryHarness() {
  @Test
  fun `approved correction of pending later day permits missing ordinary day after fence release`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val later = uploadDay(5)
      assertPending(later)
      writeRawFile("day-5", "input.parquet", listOf("corrected-person"), date(5))
      finalizeRawUpload("day-5")
      workItemTransport.awaitIdle(allowFailures = true)
      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
      )
      repeat(4) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        awaitPipelineIdle(allowFailures = true)
      }
      val replacement = listUploads().single { it.replacesRawImpressionUpload == later.name }
      assertPending(replacement)
      val fourth = uploadDay(4)
      repeat(8) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        recoverPending()
      }
      assertThat(listHealingOperations().single().state)
        .isEqualTo(UploadHealingOperation.State.COMPLETE)
      assertCompletedForBothPaths(replacement)
      assertCompletedForBothPaths(fourth)
      assertThat(listMetadata().map { it.rawImpressionUpload }).doesNotContain(later.name)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..5).map(::date).toSet(),
      )
    }

  @Test
  fun `ordinary missing day deferred during approval resumes after healing releases fence`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val later = uploadDay(5)
      assertPending(later)
      writeRawFile("day-5", "input.parquet", listOf("corrected-person"), date(5))
      finalizeRawUpload("day-5")
      workItemTransport.awaitIdle(allowFailures = true)
      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
      )
      val fourth = uploadDay(4)
      assertThat(listUploads().single { it.name == fourth.name }.processingDeferred).isTrue()
      assertThat(listModelLines(fourth.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.CREATED)
      repeat(4) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        awaitPipelineIdle(allowFailures = true)
      }
      val replacement = listUploads().single { it.replacesRawImpressionUpload == later.name }
      assertPending(replacement)
      repeat(8) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        recoverPending()
      }
      assertThat(listHealingOperations().single().state)
        .isEqualTo(UploadHealingOperation.State.COMPLETE)
      assertCompletedForBothPaths(replacement)
      assertCompletedForBothPaths(fourth)
      assertThat(listMetadata().map { it.rawImpressionUpload }).doesNotContain(later.name)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..5).map(::date).toSet(),
      )
    }

  @Test
  fun `removing pending later day leaves its old done markers unable to extend availability`() =
    runBlocking<Unit> {
      seedPublishedDays()
      withholdNextAvailabilityPublications(count = 2)
      val later = uploadDay(5)
      assertPending(later)
      val oldTasks = listAvailabilityTasks(later.name)
      assertThat(oldTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.QUEUED)
      writeRawFile("day-5", "input.parquet", listOf("discarded-revision"), date(5))
      finalizeRawUpload("day-5")
      workItemTransport.awaitIdle(allowFailures = true)
      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
      )
      advanceHealingUntilComplete(controller, draft.name)
      assertThat(listModelLines(later.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.FAILED)
      assertThat(listModelLines(later.name).map { it.failureReason }.toSet())
        .containsExactly(RawImpressionUploadModelLine.FailureReason.EVICTED_OUTPUT)
      assertThat(listMetadata().map { it.rawImpressionUpload }).doesNotContain(later.name)
      val fourth = uploadDay(4)
      recoverPending()
      assertCompletedForBothPaths(fourth)
      val requests = dataProvidersService.requests.toList()
      for (task in oldTasks) processAvailabilityTask(task)
      assertThat(listAvailabilityTasks(later.name).map { it.state }.toSet())
        .containsExactly(WorkItem.State.SUCCEEDED)
      recoverPending()
      assertThat(listMetadata().map { it.rawImpressionUpload }).doesNotContain(later.name)
      assertThat(dataProvidersService.requests.last().dataAvailabilityIntervalsList)
        .containsExactlyElementsIn(requests.last().dataAvailabilityIntervalsList)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..4).map(::date).toSet(),
      )
    }

  @Test
  fun `correcting pending D5 drains active older D4 and rebuilds its consumed snapshot`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val fifth = uploadDay(5)
      assertPending(fifth)
      withholdNextVidLabelerPublications(count = 2)
      val fourth = uploadDay(4)
      assertThat(listModelLines(fourth.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.LABELING)
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT).map { it.digest })
        .contains(digest("person-5-0"))
      writeRawFile("day-5", "input.parquet", listOf("corrected-person"), date(5))
      finalizeRawUpload("day-5")
      workItemTransport.awaitIdle(allowFailures = true)
      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
      )
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      assertThat(listHealingOperations().single().state)
        .isEqualTo(UploadHealingOperation.State.DRAINING)
      republishQueuedVidLabelers()
      awaitPipelineIdle(allowFailures = true)
      for (tick in 1..20) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        assertThat(buildVidLabelingMonitor().runDispatch().dispatchError).isFalse()
        awaitPipelineIdle(allowFailures = true)
        if (listHealingOperations().single().state == UploadHealingOperation.State.COMPLETE) break
      }
      assertThat(listHealingOperations().single().state)
        .isEqualTo(UploadHealingOperation.State.COMPLETE)
      recoverPending()
      val replacement = listUploads().single { it.replacesRawImpressionUpload == fifth.name }
      val rebuiltFourth = listUploads().single { it.replacesRawImpressionUpload == fourth.name }
      assertCompletedForBothPaths(replacement)
      assertThat(
          listModelLines(rebuiltFourth.name)
            .single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }
            .state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      val rebuilt = rankEntries(rebuiltFourth, RankIndexBlob.BlobType.SNAPSHOT).map { it.digest }
      assertThat(rebuilt).doesNotContain(digest("person-5-0"))
      assertThat(rebuilt).contains(digest("corrected-person-0"))
      assertThat(rebuilt).contains(digest("person-4-0"))
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..5).map(::date).toSet(),
      )
    }

  @Test
  fun `correcting D6 uses unaffected waiting D5 history and permits the missing D4`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val fifth = uploadDay(5)
      val sixth = uploadDay(6)
      assertPending(fifth)
      assertPending(sixth)
      val oldArtifacts =
        snapshotOutputArtifacts(listMetadata().filter { it.rawImpressionUpload == fifth.name })
      val oldSnapshots = listRankIndexBlobs(fifth.name)
      writeRawFile("day-6", "input.parquet", listOf("corrected-sixth"), date(6))
      finalizeRawUpload("day-6")
      workItemTransport.awaitIdle(allowFailures = true)
      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
      )
      repeat(6) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        awaitPipelineIdle(allowFailures = true)
      }
      assertThat(listHealingOperations().single().state)
        .isAnyOf(
          UploadHealingOperation.State.AVAILABILITY_HANDOFF_PENDING,
          UploadHealingOperation.State.COMPLETE,
        )
      val replacement = listUploads().single { it.replacesRawImpressionUpload == sixth.name }
      assertPending(replacement)
      uploadDay(4)
      for (tick in 1..12) {
        assertThat(controller.run().failedDataProviders).isEqualTo(0)
        assertThat(buildVidLabelingMonitor().runDispatch().dispatchError).isFalse()
        awaitPipelineIdle(allowFailures = true)
        if (
          listHealingOperations().single().state == UploadHealingOperation.State.COMPLETE &&
            listModelLines(fifth.name).all {
              it.state == RawImpressionUploadModelLine.State.COMPLETED
            }
        )
          break
      }
      assertCompletedForBothPaths(fifth)
      assertCompletedForBothPaths(replacement)
      assertThat(snapshotOutputArtifacts(listMetadata()).filterKeys { it in oldArtifacts })
        .isEqualTo(oldArtifacts)
      assertThat(listRankIndexBlobs(fifth.name)).containsExactlyElementsIn(oldSnapshots)
      assertAvailabilityPublished(
        setOf(DIRECT_MODEL_LINE, MEMOIZED_MODEL_LINE),
        (1..6).map(::date).toSet(),
      )
    }
}

internal class VidLabelingGapFailureIntegrationTest : VidLabelingGapRecoveryHarness() {
  @Test
  fun `failed labeling with a retained rank snapshot holds later ranking until operator retry`() =
    runBlocking<Unit> {
      seedPublishedDays()
      val fifth = uploadDay(5)
      assertPending(fifth)
      withholdNextVidLabelerPublications(count = 2)
      val fourth = uploadDay(4)
      val memoizedTask =
        vidLabelerWorkItems().single { item ->
          val params =
            item.workItemParams
              .unpack(WorkItem.WorkItemParams::class.java)
              .appParams
              .unpack(VidLabelerParams::class.java)
          params.vidLabelingJob.startsWith("${fourth.name}/vidLabelingJobs/") &&
            MEMOIZED_MODEL_LINE in params.modelLinesList
        }
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT)).isNotEmpty()
      workItemsStub.failWorkItem(
        failWorkItemRequest {
          name = memoizedTask.name
          expectedWorkItemGeneration = memoizedTask.generation
        }
      )
      republishQueuedVidLabelers()
      awaitPipelineIdle(allowFailures = true)
      buildVidLabelingMonitor().runHealth()
      assertThat(
          listModelLines(fourth.name).single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }.state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.FAILED)
      val snapshots = rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT)
      val sixth = uploadDay(6)
      assertThat(
          listModelLines(sixth.name).single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }.state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.CREATED)
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT))
        .containsExactlyElementsIn(snapshots)
      assertThat(
          buildFailedDispatchRetrier()
            .retryFailed(fourth.name, MEMOIZED_MODEL_LINE)
            .workItemsRepublished
        )
        .isEqualTo(1)
      awaitPipelineIdle(allowFailures = true)
      recoverPending()
      assertCompletedForBothPaths(fourth)
      assertCompletedForBothPaths(fifth)
      assertCompletedForBothPaths(sixth)
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT))
        .containsExactlyElementsIn(snapshots)
    }

  @Test
  fun `a failed direct sibling does not hide the memoized availability owner or its snapshot`() =
    runBlocking<Unit> {
      seedPublishedDays()
      withholdNextVidLabelerPublications(count = 2)
      val fifth = uploadDay(5)
      val tasks =
        vidLabelerWorkItems().filter { item ->
          item.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(VidLabelerParams::class.java)
            .vidLabelingJob
            .startsWith("${fifth.name}/vidLabelingJobs/")
        }
      val direct =
        tasks.single { item ->
          DIRECT_MODEL_LINE in
            item.workItemParams
              .unpack(WorkItem.WorkItemParams::class.java)
              .appParams
              .unpack(VidLabelerParams::class.java)
              .modelLinesList
        }
      val memo = tasks.single { it.name != direct.name }
      workItemsStub.failWorkItem(
        failWorkItemRequest {
          name = direct.name
          expectedWorkItemGeneration = direct.generation
        }
      )
      workItemTransport.republishWorkItem(memo.name)
      awaitPipelineIdle(allowFailures = true)
      buildVidLabelingMonitor().runHealth()
      assertThat(listUploads().single { it.name == fifth.name }.state)
        .isEqualTo(RawImpressionUpload.State.FAILED)
      assertThat(
          listModelLines(fifth.name).single { it.cmmsModelLine == MEMOIZED_MODEL_LINE }.state
        )
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      val fourth = uploadDay(4)
      assertThat(rankEntries(fourth, RankIndexBlob.BlobType.SNAPSHOT).map { it.digest })
        .contains(digest("person-5-0"))
      assertThat(
          buildFailedDispatchRetrier()
            .retryFailed(fifth.name, DIRECT_MODEL_LINE)
            .workItemsRepublished
        )
        .isEqualTo(1)
      awaitPipelineIdle(allowFailures = true)
      recoverPending()
      assertCompletedForBothPaths(fourth)
      assertCompletedForBothPaths(fifth)
    }
}
