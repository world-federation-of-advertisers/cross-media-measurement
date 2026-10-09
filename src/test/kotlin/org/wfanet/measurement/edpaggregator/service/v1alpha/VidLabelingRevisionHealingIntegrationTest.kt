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
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingRevisionHealingIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `obsolete availability delivery cannot restore evicted metadata`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("stale-person"))
    val originalDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalDoneGeneration }
    assertCompletedForBothPaths(original)
    val originalTasks = listAvailabilityTasks(original.name)
    assertThat(originalTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(2)

    writeRawFile("day-1", "input.parquet", listOf("corrected-person"))
    val correctionDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val correction = listUploads().single { it.doneBlobGeneration == correctionDoneGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    approveHealingOperation(
      listHealingOperations().single(),
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
    )

    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    workItemTransport.awaitIdle()
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Corrected revision replay failed", it)
    }
    val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
    val replacementTasks = listAvailabilityTasks(replacement.name)
    assertThat(replacementTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.QUEUED)
    assertThat(listMetadata()).isEmpty()

    val fencedTask = replacementTasks.first()
    assertThat(runCatching { processAvailabilityTask(fencedTask) }.isFailure).isTrue()
    val blockedTask = listAvailabilityTasks(replacement.name).single { it.name == fencedTask.name }
    assertThat(blockedTask.state).isEqualTo(WorkItem.State.RUNNING)
    assertThat(blockedTask.attemptCount).isEqualTo(1)
    assertThat(listMetadata()).isEmpty()

    for (task in originalTasks) {
      workItemTransport.redeliverWorkItem(task.name)
    }
    workItemTransport.awaitIdle(allowFailures = true)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).isEmpty()

    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    workItemTransport.redeliverWorkItem(fencedTask.name)
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    assertCompletedForBothPaths(replacement)
    assertThat(listAvailabilityTasks(replacement.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet())
      .containsExactly(replacement.name)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("corrected-person"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("corrected-person"))
  }

  @Test
  fun `no-replacement correction evicts metadata without rewriting WorkItems`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("stale-person"))
    val originalDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalDoneGeneration }
    assertCompletedForBothPaths(original)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(2)

    writeRawFile("day-1", "input.parquet", listOf("replacement-person"))
    val correctionDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val correction = listUploads().single { it.doneBlobGeneration == correctionDoneGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
    assertThat(listModelLines(correction.name)).isEmpty()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    approveHealingOperation(
      draft,
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
    )

    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    val completed = listHealingOperations().single()
    assertThat(completed.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).isEmpty()
    assertThat(listMetadata(showDeleted = true).map { it.state }.toSet())
      .containsExactly(ImpressionMetadata.State.DELETED)
    Unit
  }
}
