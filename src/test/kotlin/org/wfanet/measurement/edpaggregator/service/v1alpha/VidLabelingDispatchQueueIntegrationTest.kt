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
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.edpaggregator.VidLabelingDispatchWorkItems
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpc
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatchWorkItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.retryWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

class VidLabelingDispatchQueueIntegrationTest
private constructor(private val leaseClock: MutableClock) :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(queuedDispatch = true, workItemClock = leaseClock)
  ) {
  constructor() : this(MutableClock())

  private class MutableClock : Clock() {
    private val now = AtomicReference(Instant.now())

    override fun getZone(): ZoneId = ZoneOffset.UTC

    override fun withZone(zone: ZoneId): Clock = this

    override fun instant(): Instant = now.get()

    fun advance(duration: Duration) {
      now.updateAndGet { it.plus(duration) }
    }
  }

  private fun dispatchTasks() =
    workItemTransport.workItemsForQueue(VidLabelingDispatchWorkItems.QUEUE)

  private fun downstreamIdentities(): Set<String> {
    assertThat(poolAssignmentWorkItems()).hasSize(1)
    assertThat(rankBuilderWorkItems()).hasSize(1)
    assertThat(vidLabelerWorkItems()).hasSize(2)
    val tasks = poolAssignmentWorkItems() + rankBuilderWorkItems() + vidLabelerWorkItems()
    assertThat(tasks.map { it.name }.toSet()).hasSize(4)
    assertThat(tasks.all { it.state == WorkItem.State.SUCCEEDED }).isTrue()
    return tasks.map { it.name }.toSet()
  }

  @Test
  fun `queued duplicate notifications label once and publish both model lines`() =
    runBlocking<Unit> {
      workItemTransport.duplicateNextDelivery(VidLabelingDispatchWorkItems.QUEUE)
      writeRawFile("day", "input.parquet", listOf("person"))
      finalizeRawUpload("day")
      awaitPipelineIdle()
      assertThat(listUploads()).hasSize(1)
      assertCompletedForBothPaths(listUploads().single())
      assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person"))
      assertReadablePeople(DIRECT_MODEL_LINE, setOf("person"))
      assertThat(dispatchTasks()).hasSize(1)
      assertThat(dispatchTasks().single().state).isEqualTo(WorkItem.State.SUCCEEDED)
      assertThat(workItemTransport.deliveryCount(dispatchTasks().single().name)).isAtLeast(2)
      assertThat(listMetadata()).hasSize(2)
      val identities = downstreamIdentities()
      dispatchWorkItemProcessor.process(dispatchTasks().single())
      awaitPipelineIdle()
      assertThat(downstreamIdentities()).containsExactlyElementsIn(identities)
    }

  @Test
  fun `lost registration response retries the same upload without duplicate jobs`() =
    runBlocking<Unit> {
      edpaBeforeCallFaults.armAfterCommit(
        RawImpressionUploadServiceGrpc.getCreateRawImpressionUploadMethod().fullMethodName
      )
      writeRawFile("day", "input.parquet", listOf("person"))
      finalizeRawUpload("day")
      awaitPipelineIdle()
      assertThat(listUploads()).hasSize(1)
      assertCompletedForBothPaths(listUploads().single())
      assertThat(listUploadFiles(listUploads().single().name)).hasSize(1)
      assertThat(workItemTransport.deliveryCount(dispatchTasks().single().name)).isAtLeast(2)
      assertThat(listMetadata()).hasSize(2)
      val identities = downstreamIdentities()
      dispatchWorkItemProcessor.process(dispatchTasks().single())
      awaitPipelineIdle()
      assertThat(downstreamIdentities()).containsExactlyElementsIn(identities)
    }

  @Test
  fun `expired attempt is replaced and late completion is rejected`() =
    runBlocking<Unit> {
      workItemTransport.dropNextPublications(VidLabelingDispatchWorkItems.QUEUE)
      writeRawFile("day", "input.parquet", listOf("person"))
      finalizeRawUpload("day")
      val task = dispatchTasks().single()
      val abandoned =
        workItemAttemptsStub.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            parent = task.name
            workItemAttemptId = "abandoned"
            expectedWorkItemGeneration = task.generation
            supportsAttemptLease = true
          }
        )
      leaseClock.advance(Duration.ofMinutes(6))
      dispatchWorkItemProcessor.process(task)
      awaitPipelineIdle()
      assertCompletedForBothPaths(listUploads().single())
      assertFailsWith<io.grpc.StatusException> {
        workItemAttemptsStub.completeWorkItemAttempt(
          completeWorkItemAttemptRequest { name = abandoned.name }
        )
      }
      assertThat(dispatchTasks().single().state).isEqualTo(WorkItem.State.SUCCEEDED)
      assertThat(listMetadata()).hasSize(2)
    }

  @Test
  fun `retrying an older marker and redelivering an older execution preserve outputs`() =
    runBlocking<Unit> {
      writeRawFile("day", "input.parquet", listOf("person"))
      finalizeRawUpload("day")
      awaitPipelineIdle()
      val old = dispatchTasks().single()
      writeRawFile("day", "added.parquet", listOf("other"))
      finalizeRawUpload("day")
      awaitPipelineIdle()
      val metadata = listMetadata()
      val downstream =
        poolAssignmentWorkItems().size + rankBuilderWorkItems().size + vidLabelerWorkItems().size
      workItemTransport.dropNextPublications(VidLabelingDispatchWorkItems.QUEUE)
      val staged =
        workItemsStub.createWorkItem(
          createWorkItemRequest {
            workItemId = "stale-marker"
            workItem = workItem {
              queue = VidLabelingDispatchWorkItems.QUEUE
              workItemParams = old.workItemParams
            }
          }
        )
      workItemAttemptsStub.createWorkItemAttempt(
        createWorkItemAttemptRequest {
          parent = staged.name
          workItemAttemptId = "failed-before-retry"
          expectedWorkItemGeneration = staged.generation
          supportsAttemptLease = true
        }
      )
      workItemsStub.failWorkItem(
        failWorkItemRequest {
          name = staged.name
          expectedWorkItemGeneration = staged.generation
        }
      )
      workItemsStub.retryWorkItem(
        retryWorkItemRequest {
          name = staged.name
          expectedWorkItemGeneration = staged.generation
        }
      )
      awaitPipelineIdle()
      dispatchWorkItemProcessor.process(staged)
      assertThat(listUploads()).hasSize(2)
      assertThat(listMetadata()).containsExactlyElementsIn(metadata)
      assertThat(
          poolAssignmentWorkItems().size + rankBuilderWorkItems().size + vidLabelerWorkItems().size
        )
        .isEqualTo(downstream)
      assertThat(dispatchTasks().single { it.name == staged.name }.generation).isEqualTo(2L)
    }

  @Test
  fun `approved correction creates a separate replay task for the quarantined generation`() =
    runBlocking<Unit> {
      writeRawFile("day", "input.parquet", listOf("original"))
      finalizeRawUpload("day")
      awaitPipelineIdle()
      writeRawFile("day", "input.parquet", listOf("corrected"))
      val generation = finalizeRawUpload("day")
      awaitPipelineIdle()
      assertThat(listUploads().single { it.doneBlobGeneration == generation }.state)
        .isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
      val controller = buildHealingController()
      controller.run()
      val draft = listHealingOperations().single()
      approveHealingOperation(
        draft,
        draft.rawImpressionUploadCorrectionCandidatesList.associateWith {
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE
        },
      )
      workItemTransport.dropNextPublications(VidLabelingDispatchWorkItems.QUEUE)
      controller.run()
      val sameGeneration =
        dispatchTasks().filter {
          it.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(VidLabelingDispatchWorkItemParams::class.java)
            .source
            .generation == generation
        }
      assertThat(sameGeneration).hasSize(2)
      assertThat(sameGeneration.map { it.name }.toSet()).hasSize(2)
      assertThat(
          sameGeneration
            .map {
              it.workItemParams
                .unpack(WorkItem.WorkItemParams::class.java)
                .appParams
                .unpack(VidLabelingDispatchWorkItemParams::class.java)
                .recovery
                .sourceUpload
                .isNotEmpty()
            }
            .toSet()
        )
        .containsExactly(false, true)
      assertThat(listHealingOperations().single().state)
        .isNotEqualTo(UploadHealingOperation.State.COMPLETE)
      workItemTransport.republishQueuedWorkItems(VidLabelingDispatchWorkItems.QUEUE)
      awaitPipelineIdle()
      advanceHealingUntilComplete(controller, draft.name)
      assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("corrected"))
      assertReadablePeople(DIRECT_MODEL_LINE, setOf("corrected"))
    }

  @Test
  fun `legacy delivery and queued delivery share registration and downstream identities`() =
    runBlocking<Unit> {
      workItemTransport.dropNextPublications(VidLabelingDispatchWorkItems.QUEUE)
      writeRawFile("day", "input.parquet", listOf("person"))
      val generation = finalizeRawUpload("day")
      val request =
        java.net.http.HttpRequest.newBuilder(
            java.net.URI("http://localhost:${httpServer.address.port}/raw")
          )
          .header("X-DataWatcher-Path", "$rawPrefix/day/done")
          .header("X-DataWatcher-Generation", generation.toString())
          .POST(java.net.http.HttpRequest.BodyPublishers.ofString("{}"))
          .build()
      assertThat(
          java.net.http.HttpClient.newHttpClient()
            .send(request, java.net.http.HttpResponse.BodyHandlers.ofString())
            .statusCode()
        )
        .isEqualTo(200)
      awaitPipelineIdle()
      val legacyIdentities = downstreamIdentities()
      workItemTransport.republishQueuedWorkItems(VidLabelingDispatchWorkItems.QUEUE)
      awaitPipelineIdle()
      assertThat(listUploads()).hasSize(1)
      assertCompletedForBothPaths(listUploads().single())
      assertThat(listMetadata()).hasSize(2)
      assertThat(downstreamIdentities()).containsExactlyElementsIn(legacyIdentities)
    }
}
