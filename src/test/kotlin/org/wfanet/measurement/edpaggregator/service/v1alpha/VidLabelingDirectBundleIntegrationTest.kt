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
import java.time.ZoneOffset
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingDirectBundleIntegrationTest :
  VidLabelingPipelineTestHarness(directBundleConfig()) {
  @Test
  fun `two direct lines share labeling jobs and finish availability independently`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE)
    val input = writeRawFile("direct-bundle", "input.parquet", listOf("person"))
    val generation = finalizeRawUpload("direct-bundle")
    awaitPipelineIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }

    assertThat(vidLabelerWorkItems()).hasSize(1)
    val firstPassTasks = listAvailabilityTasks(upload.name)
    assertThat(firstPassTasks).hasSize(2)
    assertThat(firstPassTasks.count { it.state == WorkItem.State.SUCCEEDED }).isEqualTo(1)
    assertThat(firstPassTasks.count { it.state == WorkItem.State.QUEUED }).isEqualTo(1)
    assertThat(listMetadata()).hasSize(1)
    val outputGenerationsBeforeRetry =
      mapOf(
        DIRECT_MODEL_LINE to labeledOutputGeneration(input, DIRECT_MODEL_LINE, EVENT_DATE),
        SECOND_DIRECT_MODEL_LINE to
          labeledOutputGeneration(input, SECOND_DIRECT_MODEL_LINE, EVENT_DATE),
      )
    val labelingWorkItemsBeforeRetry = vidLabelerWorkItems()

    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata().map { it.modelLine })
      .containsExactly(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE)
    assertThat(vidLabelerWorkItems()).containsExactlyElementsIn(labelingWorkItemsBeforeRetry)
    assertThat(labeledOutputGeneration(input, DIRECT_MODEL_LINE, EVENT_DATE))
      .isEqualTo(outputGenerationsBeforeRetry.getValue(DIRECT_MODEL_LINE))
    assertThat(labeledOutputGeneration(input, SECOND_DIRECT_MODEL_LINE, EVENT_DATE))
      .isEqualTo(outputGenerationsBeforeRetry.getValue(SECOND_DIRECT_MODEL_LINE))
  }
}

private const val MODEL_SUITE = "modelProviders/mp1/modelSuites/ms1"
private const val SECOND_DIRECT_MODEL_LINE = "$MODEL_SUITE/modelLines/direct-2"

private fun directBundleConfig(): PipelineHarnessConfig {
  val secondLine =
    ModelLineFixture(
      SECOND_DIRECT_MODEL_LINE,
      "$MODEL_SUITE/modelReleases/direct-2",
      memoized = false,
      activeStart = EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
      activeEnd = EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant(),
    )
  return PipelineHarnessConfig(
    extraModelLines = listOf(secondLine),
    initiallyVisibleModelLines = setOf(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE),
  )
}
