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
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.tools.ModelLineBackfiller
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingNormalProcessingIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `rapid done markers serialize the memoized line oldest first`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    withholdNextPoolAssignmentPublications()

    val generations = mutableListOf<Long>()
    repeat(3) { index ->
      val folder = "rapid/day-${index + 1}"
      writeRawFile(
        folder,
        "input.parquet",
        listOf("person-${index + 1}"),
        EVENT_DATE.plusDays(index.toLong()),
      )
      generations += finalizeRawUpload(folder)
    }
    workItemTransport.awaitIdle()

    val uploadsByGeneration = listUploads().associateBy { it.doneBlobGeneration }
    val uploads = generations.map { uploadsByGeneration.getValue(it) }
    assertThat(listModelLines(uploads[0].name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    assertThat(listModelLines(uploads[1].name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.CREATED)
    assertThat(listModelLines(uploads[2].name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.CREATED)

    republishQueuedPoolAssignments()
    drainSequencer()

    for (upload in uploads) {
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listAvailabilityTasks(upload.name).single().state)
        .isEqualTo(WorkItem.State.SUCCEEDED)
    }
  }

  @Test
  fun `upload with no active line completes after real model line backfill`() = runBlocking {
    setVisibleModelLines()
    writeRawFile("backfill-line", "input.parquet", listOf("person"))
    val generation = finalizeRawUpload("backfill-line")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    assertThat(listModelLines(upload.name)).isEmpty()
    assertThat(listAvailabilityTasks(upload.name)).isEmpty()
    assertThat(listMetadata()).isEmpty()

    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    ModelLineBackfiller(modelLineRowsStub).backfill(MEMOIZED_MODEL_LINE, listOf(upload.name))
    val dispatch = buildVidLabelingMonitor().runDispatch()
    assertThat(dispatch.dispatchError).isFalse()
    assertThat(dispatch.dispatchedUpload).isEqualTo(upload.name)
    awaitPipelineIdle()

    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId }).containsExactly("person")
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
  }
}
