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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.acquireDataAvailabilitySyncLeaseRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingHealingHandoffIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      extraModelLines =
        listOf(
          ModelLineFixture(
            THIRD_MODEL_LINE,
            THIRD_RELEASE,
            memoized = false,
            EVENT_DATE.minusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant(),
            EVENT_DATE.plusDays(10).atStartOfDay(ZoneOffset.UTC).toInstant(),
          )
        )
    )
  ) {
  @Test
  fun `healing publishes one availability handoff per model line only after fence release`() =
    runBlocking {
      writeRawFile("three-line-healing", "input.parquet", listOf("original"))
      val originalGeneration = finalizeRawUpload("three-line-healing")
      awaitPipelineIdle()
      val original = listUploads().single { it.doneBlobGeneration == originalGeneration }

      writeRawFile("three-line-healing", "input.parquet", listOf("corrected"))
      finalizeRawUpload("three-line-healing")
      workItemTransport.awaitIdle()

      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
      )

      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      workItemTransport.awaitIdle()
      endpointFailure.getAndSet(null)?.let {
        throw AssertionError("Three-line replacement replay failed", it)
      }
      val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
      assertThat(listModelLines(replacement.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertThat(listAvailabilityTasks(replacement.name)).isEmpty()
      assertThat(listHealingOperations().single().state)
        .isNotEqualTo(UploadHealingOperation.State.COMPLETE)

      val leaseStub =
        DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub(
          edpaPublicServer.channel
        )
      val fenced =
        assertFailsWith<StatusException> {
          leaseStub.acquireDataAvailabilitySyncLease(
            acquireDataAvailabilitySyncLeaseRequest {
              name =
                "dataProviders/dp1/dataAvailabilitySyncLeases/" +
                  "11111111-1111-4111-8111-111111111111"
              requestId = "22222222-2222-4222-8222-222222222222"
            }
          )
        }
      assertThat(fenced.status.code).isEqualTo(Status.Code.UNAVAILABLE)

      withholdNextAvailabilityPublications(count = 3)
      assertThat(controller.run().failedDataProviders).isEqualTo(0)

      assertThat(listHealingOperations().single().state)
        .isEqualTo(UploadHealingOperation.State.COMPLETE)
      val queued = listAvailabilityTasks(replacement.name)
      assertThat(queued).hasSize(3)
      assertThat(queued.map { it.cmmsModelLine })
        .containsExactly(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE, THIRD_MODEL_LINE)
      assertThat(queued.map { it.state }.toSet()).containsExactly(WorkItem.State.QUEUED)

      republishQueuedAvailability()
      awaitPipelineIdle()

      assertThat(listAvailabilityTasks(replacement.name).map { it.state }.toSet())
        .containsExactly(WorkItem.State.SUCCEEDED)
      assertThat(listModelLines(replacement.name).map { it.state }.toSet())
        .containsExactly(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listUploads().single { it.name == replacement.name }.state)
        .isEqualTo(RawImpressionUpload.State.COMPLETED)
      assertThat(listMetadata().map { it.modelLine })
        .containsExactly(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE, THIRD_MODEL_LINE)
      Unit
    }

  @Test
  fun `draining waits for an active lease then publishes healing and resumes a later upload`() =
    runBlocking {
      setVisibleModelLines(DIRECT_MODEL_LINE)
      writeRawFile("draining/day-1", "input.parquet", listOf("original"), EVENT_DATE)
      val originalGeneration = finalizeRawUpload("draining/day-1")
      awaitPipelineIdle()
      val original = listUploads().single { it.doneBlobGeneration == originalGeneration }

      setToday(EVENT_DATE.plusDays(1))
      withholdNextAvailabilityPublications()
      writeRawFile(
        "draining/day-2",
        "input.parquet",
        listOf("active-lease"),
        EVENT_DATE.plusDays(1),
      )
      val activeGeneration = finalizeRawUpload("draining/day-2")
      workItemTransport.awaitIdle()
      val activeUpload = listUploads().single { it.doneBlobGeneration == activeGeneration }
      val activeTask = listAvailabilityTasks(activeUpload.name).single()
      metadataStorage.pauseNextSyncStart()
      val activeSync =
        async(Dispatchers.Default) { runCatching { processAvailabilityTask(activeTask) } }
      metadataStorage.awaitPausedSyncStart()

      writeRawFile("draining/day-1", "input.parquet", listOf("corrected"), EVENT_DATE)
      finalizeRawUpload("draining/day-1")
      workItemTransport.awaitIdle()
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
      assertThat(listAvailabilityTasks(activeUpload.name).single().state)
        .isEqualTo(WorkItem.State.RUNNING)

      setToday(EVENT_DATE.plusDays(2))
      writeRawFile(
        "draining/day-3",
        "input.parquet",
        listOf("queued-later"),
        EVENT_DATE.plusDays(2),
      )
      val queuedGeneration = finalizeRawUpload("draining/day-3")
      workItemTransport.awaitIdle()
      val queuedUpload = listUploads().single { it.doneBlobGeneration == queuedGeneration }
      assertThat(listModelLines(queuedUpload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.CREATED)

      metadataStorage.releasePausedSyncStart()
      assertThat(activeSync.await().isSuccess).isTrue()
      assertThat(listAvailabilityTasks(activeUpload.name).single().state)
        .isEqualTo(WorkItem.State.SUCCEEDED)

      withholdNextAvailabilityPublications()
      advanceHealingUntilComplete(controller, draft.name)
      val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
      assertThat(listAvailabilityTasks(replacement.name).single().state)
        .isEqualTo(WorkItem.State.QUEUED)

      republishQueuedAvailability()
      awaitPipelineIdle()
      val resumed = buildVidLabelingMonitor().runDispatch()
      assertThat(resumed.dispatchedUpload).isEqualTo(queuedUpload.name)
      awaitPipelineIdle()

      assertThat(listModelLines(replacement.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listModelLines(queuedUpload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
      assertThat(listAvailabilityTasks(replacement.name).single().state)
        .isEqualTo(WorkItem.State.SUCCEEDED)
      assertThat(listAvailabilityTasks(queuedUpload.name).single().state)
        .isEqualTo(WorkItem.State.SUCCEEDED)
      Unit
    }

  companion object {
    private const val THIRD_MODEL_LINE =
      "modelProviders/mp1/modelSuites/ms1/modelLines/direct-secondary"
    private const val THIRD_RELEASE =
      "modelProviders/mp1/modelSuites/ms1/modelReleases/direct-secondary"
  }
}
