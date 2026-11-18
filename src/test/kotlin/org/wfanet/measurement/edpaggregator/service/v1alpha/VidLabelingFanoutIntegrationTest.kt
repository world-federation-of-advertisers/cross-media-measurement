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
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingFanoutIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(numberOfShards = 2, rankStripes = 2, maxFileBatchSizeBytes = 1L)
  ) {
  @Test
  fun `multiple shards subpools and file batches reconcile once`() = runBlocking {
    val demographics =
      listOf(
        "MALE" to "YEARS_18_TO_34",
        "MALE" to "YEARS_35_TO_54",
        "FEMALE" to "YEARS_18_TO_34",
        "FEMALE" to "YEARS_55_PLUS",
      )
    val people = mutableSetOf<String>()
    for ((fileIndex, demographic) in demographics.withIndex()) {
      val filePeople = (0 until 3).map { "person-$fileIndex-$it" }
      people += filePeople
      writeRawFile(
        "fanout",
        "input-$fileIndex.parquet",
        filePeople,
        EVENT_DATE,
        gender = demographic.first,
        ageGroup = demographic.second,
      )
    }

    val generation = finalizeRawUpload("fanout")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    assertCompletedForBothPaths(upload)
    assertThat(poolAssignmentWorkItems()).hasSize(2)
    assertThat(rankBuilderWorkItems().size).isAtLeast(2)
    assertThat(vidLabelerWorkItems().size).isAtLeast(4)
    assertThat(listUploadFiles(upload.name)).hasSize(4)
    assertThat(listMetadata()).hasSize(8)
    assertThat(listAvailabilityTasks(upload.name)).hasSize(2)
    assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    val memoized = readLabeledPeople(MEMOIZED_MODEL_LINE)
    val direct = readLabeledPeople(DIRECT_MODEL_LINE)
    assertThat(memoized.map { it.personId }).containsExactlyElementsIn(people)
    assertThat(direct.map { it.personId }).containsExactlyElementsIn(people)
    assertThat(memoized.all { it.vid in 10_000L..10_599L }).isTrue()
    assertThat(direct.all { it.vid in 10_000L..10_599L }).isTrue()
    assertAvailabilityPublished(setOf(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE), setOf(EVENT_DATE))
  }

  @Test
  fun `one fingerprint is ranked independently in every routed subpool`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    val contexts =
      listOf(
        "MALE" to "YEARS_18_TO_34",
        "MALE" to "YEARS_35_TO_54",
        "MALE" to "YEARS_55_PLUS",
        "FEMALE" to "YEARS_18_TO_34",
        "FEMALE" to "YEARS_35_TO_54",
        "FEMALE" to "YEARS_55_PLUS",
      )
    writeRawEvents(
      "multi-subpool",
      "input.parquet",
      contexts.mapIndexed { index, (gender, ageGroup) ->
        RawEventFixture(
          eventId = "shared-event",
          personId = "shared-person-$index",
          eventDate = EVENT_DATE,
          gender = gender,
          ageGroup = ageGroup,
        )
      },
    )
    val generation = finalizeRawUpload("multi-subpool")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    val entries =
      rankEntries(upload, RankIndexBlob.BlobType.DAY_ONLY).filter {
        it.digest == digest("shared-event")
      }
    assertThat(entries.map { it.poolOffset }.toSet().size).isAtLeast(2)
    assertThat(entries.groupBy { it.poolOffset }.values.all { it.size == 1 }).isTrue()
    assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE)).hasSize(contexts.size)
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
  }
}
