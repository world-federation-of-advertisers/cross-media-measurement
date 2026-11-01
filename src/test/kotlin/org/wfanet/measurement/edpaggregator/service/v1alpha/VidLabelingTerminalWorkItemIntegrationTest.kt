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
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest

@RunWith(Parameterized::class)
internal class VidLabelingTerminalWorkItemIntegrationTest(private val phase: Phase) :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(initiallyVisibleModelLines = setOf(phase.modelLine))
  ) {
  @Test
  fun `monitor reconciles a terminal phase failure and operator retry restores progress`() =
    runBlocking {
      withholdPhasePublication()
      writeRawFile("terminal-${phase.name.lowercase()}", "input.parquet", listOf("recovered"))
      val generation = finalizeRawUpload("terminal-${phase.name.lowercase()}")
      workItemTransport.awaitIdle()
      val upload = listUploads().single { it.doneBlobGeneration == generation }
      val original = phaseWorkItems().single()
      assertJobState(upload.name, JobState.CREATED)

      workItemsStub.failWorkItem(
        failWorkItemRequest {
          name = original.name
          expectedWorkItemGeneration = original.generation
        }
      )
      assertThat(phaseWorkItems().single().state).isEqualTo(WorkItem.State.FAILED)

      buildVidLabelingMonitor().runHealth()

      assertJobState(upload.name, JobState.FAILED)
      val failedModelLine = listModelLines(upload.name).single()
      assertThat(failedModelLine.state).isEqualTo(RawImpressionUploadModelLine.State.FAILED)
      assertThat(failedModelLine.failureAttemptId).isNotEmpty()

      val retry = buildFailedDispatchRetrier().retryFailed(upload.name, phase.modelLine)
      assertThat(retry.workItemsRepublished).isEqualTo(1)
      workItemTransport.awaitIdle()

      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertJobState(upload.name, JobState.SUCCEEDED)
      assertThat(phaseWorkItems().map { it.state })
        .containsExactly(WorkItem.State.FAILED, WorkItem.State.SUCCEEDED)
      assertThat(phaseWorkItems().map { it.name }.toSet()).hasSize(2)

      setToday(EVENT_DATE.plusDays(1))
      writeRawFile(
        "later-${phase.name.lowercase()}",
        "input.parquet",
        listOf("later"),
        EVENT_DATE.plusDays(1),
      )
      val laterGeneration = finalizeRawUpload("later-${phase.name.lowercase()}")
      awaitPipelineIdle()
      val laterUpload = listUploads().single { it.doneBlobGeneration == laterGeneration }
      assertThat(listModelLines(laterUpload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    }

  private fun withholdPhasePublication() {
    when (phase) {
      Phase.SUBPOOL_ASSIGNER -> withholdNextPoolAssignmentPublications()
      Phase.VID_RANK_BUILDER -> withholdNextRankBuilderPublications()
      Phase.VID_LABELER -> withholdNextVidLabelerPublications()
    }
  }

  private fun phaseWorkItems(): List<WorkItem> =
    when (phase) {
      Phase.SUBPOOL_ASSIGNER -> poolAssignmentWorkItems()
      Phase.VID_RANK_BUILDER -> rankBuilderWorkItems()
      Phase.VID_LABELER -> vidLabelerWorkItems()
    }

  private suspend fun assertJobState(upload: String, expected: JobState) {
    when (phase) {
      Phase.SUBPOOL_ASSIGNER -> {
        val response =
          PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
              edpaPublicServer.channel
            )
            .listPoolAssignmentJobs(
              listPoolAssignmentJobsRequest {
                parent = upload
                filter = ListPoolAssignmentJobsRequestKt.filter { cmmsModelLine = phase.modelLine }
              }
            )
        assertThat(response.poolAssignmentJobsList.single().state)
          .isEqualTo(
            when (expected) {
              JobState.CREATED -> PoolAssignmentJob.State.CREATED
              JobState.FAILED -> PoolAssignmentJob.State.FAILED
              JobState.SUCCEEDED -> PoolAssignmentJob.State.SUCCEEDED
            }
          )
      }
      Phase.VID_RANK_BUILDER -> {
        val response =
          RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(edpaPublicServer.channel)
            .listRankerJobs(
              listRankerJobsRequest {
                parent = upload
                filter = ListRankerJobsRequestKt.filter { cmmsModelLine = phase.modelLine }
              }
            )
        assertThat(response.rankerJobsList.single().state)
          .isEqualTo(
            when (expected) {
              JobState.CREATED -> RankerJob.State.CREATED
              JobState.FAILED -> RankerJob.State.FAILED
              JobState.SUCCEEDED -> RankerJob.State.SUCCEEDED
            }
          )
      }
      Phase.VID_LABELER -> {
        val response =
          VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
            .listVidLabelingJobs(
              listVidLabelingJobsRequest {
                parent = upload
                filter = ListVidLabelingJobsRequestKt.filter { cmmsModelLine = phase.modelLine }
              }
            )
        assertThat(response.vidLabelingJobsList.single().state)
          .isEqualTo(
            when (expected) {
              JobState.CREATED -> VidLabelingJob.State.CREATED
              JobState.FAILED -> VidLabelingJob.State.FAILED
              JobState.SUCCEEDED -> VidLabelingJob.State.SUCCEEDED
            }
          )
      }
    }
  }

  internal enum class Phase(val modelLine: String) {
    SUBPOOL_ASSIGNER(MEMOIZED_MODEL_LINE),
    VID_RANK_BUILDER(MEMOIZED_MODEL_LINE),
    VID_LABELER(DIRECT_MODEL_LINE),
  }

  private enum class JobState {
    CREATED,
    FAILED,
    SUCCEEDED,
  }

  companion object {
    @JvmStatic
    @Parameterized.Parameters(name = "{0}")
    fun phases(): List<Array<Any>> = Phase.entries.map { arrayOf(it) }
  }
}
