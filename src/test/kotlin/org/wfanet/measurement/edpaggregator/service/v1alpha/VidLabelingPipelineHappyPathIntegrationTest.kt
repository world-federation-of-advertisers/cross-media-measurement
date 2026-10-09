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
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

internal class VidLabelingPipelineHappyPathIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `raw uploads run through both pipelines and data availability`() = runBlocking {
    workItemTransport.forceNextAcknowledgementRedelivery()
    workItemTransport.duplicateNextDelivery(DataAvailabilitySyncWorkItems.QUEUE)
    val initialFile = writeRawFile("day-1", "initial.parquet", listOf("person-1"))
    val firstDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()

    val initialUploads = listUploads()
    assertThat(initialUploads).hasSize(1)
    assertThat(initialUploads.single().doneBlobGeneration).isEqualTo(firstDoneGeneration)
    assertThat(listUploadFiles(initialUploads.single().name).single().blobGeneration)
      .isEqualTo(generationOf(initialFile))
    assertCompletedForBothPaths(initialUploads.single())
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1"))
    assertThat(workItemTransport.forcedRetryCount).isEqualTo(1)
    assertThat(workItemTransport.forcedRetryWorkItemNames).hasSize(1)

    val workItemsAfterInitial = workItemTransport.publishedCount
    val metadataAfterInitial = listMetadata()
    val memoizedOutputsAfterInitial = outputGenerations(MEMOIZED_MODEL_LINE)
    val directOutputsAfterInitial = outputGenerations(DIRECT_MODEL_LINE)
    val rankIndexesAfterInitial = listRankIndexBlobs(initialUploads.single().name)
    assertThat(metadataAfterInitial).hasSize(2)
    assertThat(memoizedOutputsAfterInitial).hasSize(1)
    assertThat(directOutputsAfterInitial).hasSize(1)
    assertThat(rankIndexesAfterInitial).hasSize(2)
    val initialTasks = listAvailabilityTasks(initialUploads.single().name)
    assertThat(initialTasks).hasSize(2)
    assertThat(initialTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(initialTasks.map { it.attemptCount }.toSet()).containsExactly(1)
    assertAvailabilityTaskIdentities(initialUploads.single(), EVENT_DATE)
    assertThat(initialTasks.any { workItemTransport.deliveryCount(it.name) >= 2 }).isTrue()
    assertThat(externalAvailabilityDeliveries.get()).isEqualTo(0)

    rawWatcher.receivePath(
      "$rawPrefix/day-1/done",
      mapOf(DataWatcher.GENERATION_METADATA_KEY to firstDoneGeneration.toString()),
    )
    awaitPipelineIdle()
    assertThat(listUploads()).hasSize(1)
    assertThat(workItemTransport.publishedCount).isEqualTo(workItemsAfterInitial)
    assertThat(listMetadata()).containsExactlyElementsIn(metadataAfterInitial)
    assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(memoizedOutputsAfterInitial)
    assertThat(outputGenerations(DIRECT_MODEL_LINE)).isEqualTo(directOutputsAfterInitial)
    assertThat(listRankIndexBlobs(initialUploads.single().name))
      .containsExactlyElementsIn(rankIndexesAfterInitial)

    val additiveFile = writeRawFile("day-1", "additional.parquet", listOf("person-2"))
    val additiveFileGeneration = generationOf(additiveFile)
    workItemTransport.duplicateNextDelivery(DataAvailabilitySyncWorkItems.QUEUE)
    val additiveDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val revisions = listUploads().filter { it.doneBlobUri == "$rawPrefix/day-1/done" }
    assertThat(revisions).hasSize(2)
    val additive = revisions.single { it.doneBlobGeneration == additiveDoneGeneration }
    assertThat(additive.replacesRawImpressionUpload).isEqualTo(initialUploads.single().name)
    assertThat(listUploadFiles(additive.name).map { it.blobUri to it.blobGeneration })
      .containsExactly(additiveFile to additiveFileGeneration)
    val additiveTasks = listAvailabilityTasks(additive.name)
    assertThat(additiveTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.SUCCEEDED)
    assertAvailabilityTaskIdentities(additive, EVENT_DATE)
    assertThat(additiveTasks.any { workItemTransport.deliveryCount(it.name) >= 2 }).isTrue()

    val independentFile =
      writeRawFile("day-1/advertiser-a", "independent.parquet", listOf("person-3"))
    val independentFileGeneration = generationOf(independentFile)
    val independentDoneGeneration = finalizeRawUpload("day-1/advertiser-a")
    awaitPipelineIdle()
    val independent =
      listUploads().single {
        it.doneBlobUri == "$rawPrefix/day-1/advertiser-a/done" &&
          it.doneBlobGeneration == independentDoneGeneration
      }
    assertThat(independent.replacesRawImpressionUpload).isEmpty()
    assertThat(listUploadFiles(independent.name).map { it.blobUri to it.blobGeneration })
      .containsExactly(independentFile to independentFileGeneration)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1", "person-2", "person-3"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1", "person-2", "person-3"))
    assertAvailabilityPublished(setOf(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE), setOf(EVENT_DATE))
    assertThat(externalAvailabilityDeliveries.get()).isEqualTo(0)
    assertEveryRegisteredRawGenerationWasRead()
  }
}
