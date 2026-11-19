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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams

internal class VidLabelingImmutableOutputBundleIntegrationTest :
  VidLabelingPipelineTestHarness(directBundleConfig()) {
  @Test
  fun `two direct lines share labeling jobs and finish availability independently`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE)
    writeRawFile("direct-bundle", "input.parquet", listOf("person"))
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
        DIRECT_MODEL_LINE to selectedOutputGeneration(DIRECT_MODEL_LINE),
        SECOND_DIRECT_MODEL_LINE to selectedOutputGeneration(SECOND_DIRECT_MODEL_LINE),
      )
    val labelingWorkItemsBeforeRetry = vidLabelerWorkItems()

    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata().map { it.modelLine })
      .containsExactly(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE)
    assertThat(vidLabelerWorkItems()).containsExactlyElementsIn(labelingWorkItemsBeforeRetry)
    assertThat(selectedOutputGeneration(DIRECT_MODEL_LINE))
      .isEqualTo(outputGenerationsBeforeRetry.getValue(DIRECT_MODEL_LINE))
    assertThat(selectedOutputGeneration(SECOND_DIRECT_MODEL_LINE))
      .isEqualTo(outputGenerationsBeforeRetry.getValue(SECOND_DIRECT_MODEL_LINE))
  }

  @Test
  fun `competing bundled labelers preserve selected pairs for both model lines`() =
    runBlocking<Unit> {
      withholdNextVidLabelerPublications()
      writeRawFile("bundled-output-race", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("bundled-output-race")
      workItemTransport.awaitIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      val work = vidLabelerWorkItems().single()
      val gate =
        responseGates.arm(
          "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/GetVidLabelingJob"
        )
      val attempts =
        List(2) { async(Dispatchers.Default) { runCatching { runVidLabelerDirect(work) } } }
      gate.awaitBoth()
      assertThat(gate.messages.map { (it as VidLabelingJob).state })
        .containsExactly(VidLabelingJob.State.CREATED, VidLabelingJob.State.CREATED)
      assertThat(attempts.awaitAll().all { it.isSuccess }).isTrue()
      awaitPipelineIdle()
      assertThat(listMetadata().map { it.rawImpressionUpload }.toSet()).containsExactly(upload.name)
      assertThat(listMetadata().map { it.modelLine })
        .containsExactly(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE)
      for (model in listOf(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE)) {
        assertThat(selectedOutputGeneration(model)).isGreaterThan(0L)
        assertThat(readLabeledPeople(model).map { it.personId }).containsExactly("person")
      }
    }

  private suspend fun selectedOutputGeneration(model: String): Long {
    val work = vidLabelerWorkItems().single()
    val params =
      work.workItemParams
        .unpack(WorkItemParams::class.java)
        .appParams
        .unpack(VidLabelerParams::class.java)
    val job =
      VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
        .getVidLabelingJob(getVidLabelingJobRequest { name = params.vidLabelingJob })
    val records =
      OutputManifestStore(
          fileStorage,
          params.vidLabeledImpressionsStorageParams.impressionsBlobPrefix,
        )
        .read(job.outputManifest)
        .toList()
    assertThat(records.first().header.owner).isEqualTo(job.name)
    assertThat(records.first().header.modelLinesList)
      .containsExactly(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE)
    assertThat(records.filter { it.hasFileCompleted() }.map { it.fileCompleted.modelLine })
      .containsExactly(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE)
    val entry =
      records.single { it.hasLabeledBlob() && it.labeledBlob.modelLine == model }.labeledBlob
    assertThat(fileStorage.getFreshnessToken(blobKey(entry.impressions.blobUri)))
      .isEqualTo(entry.impressions.blobGeneration.toString())
    assertThat(fileStorage.getFreshnessToken(blobKey(entry.metadata.blobUri)))
      .isEqualTo(entry.metadata.blobGeneration.toString())
    return entry.impressions.blobGeneration
  }

  companion object {
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
        immutableOutputPublication = true,
        extraModelLines = listOf(secondLine),
        initiallyVisibleModelLines = setOf(DIRECT_MODEL_LINE, SECOND_DIRECT_MODEL_LINE),
      )
    }
  }
}
