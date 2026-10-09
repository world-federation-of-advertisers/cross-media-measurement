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
import com.google.protobuf.ByteString
import java.time.ZoneOffset
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityBlobs
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.blobDetails
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem

internal class VidLabelingAvailabilityIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `missing WorkItem publication is retried without relabeling`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()

    assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.QUEUED)
    assertThat(listMetadata()).isEmpty()

    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(2)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1"))
  }

  @Test
  fun `lost WorkItem publication response is idempotent`() =
    runBlocking<Unit> {
      workItemTransport.loseNextPublicationResponse(DataAvailabilitySyncWorkItems.QUEUE)
      writeRawFile("day-1", "input.parquet", listOf("person-1"))
      finalizeRawUpload("day-1")
      awaitPipelineIdle()
      val upload = listUploads().single()

      assertThat(listAvailabilityTasks(upload.name).map { it.state }.toSet())
        .containsExactly(WorkItem.State.SUCCEEDED)
      assertThat(workItemTransport.lostResponseWorkItemNames()).hasSize(1)
      assertThat(listMetadata()).hasSize(2)
    }

  @Test
  fun `WorkItem failure before metadata creation recovers by redelivery`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    val inputFile = writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()
    val directTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    val directSidecarKey = blobKey(sidecarUri(inputFile, DIRECT_MODEL_LINE, EVENT_DATE))
    val validSidecar = checkNotNull(fileStorage.getBlob(directSidecarKey)).read().flatten()
    fileStorage.writeBlob(directSidecarKey, flowOf(ByteString.copyFromUtf8("invalid sidecar")))
    workItemTransport.beforeNextRedelivery {
      fileStorage.writeBlob(directSidecarKey, flowOf(validSidecar))
    }
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    val recoveredTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    assertThat(recoveredTask.state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(recoveredTask.attemptCount).isEqualTo(2)
    assertThat(listMetadata().any { it.modelLine == DIRECT_MODEL_LINE }).isTrue()
  }

  @Test
  fun `WorkItem is terminally failed after redelivery exhaustion`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    val inputFile = writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()
    val directTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    val sidecarKey = blobKey(sidecarUri(inputFile, DIRECT_MODEL_LINE, EVENT_DATE))
    fileStorage.writeBlob(sidecarKey, flowOf(ByteString.copyFromUtf8("invalid sidecar")))

    workItemTransport.republishWorkItem(directTask.name)
    workItemTransport.awaitIdle(allowFailures = true)

    val failedTask = listAvailabilityTasks(upload.name).single { it.name == directTask.name }
    assertThat(failedTask.state).isEqualTo(WorkItem.State.FAILED)
    assertThat(failedTask.attemptCount).isEqualTo(3)
    assertThat(listMetadata().none { it.modelLine == DIRECT_MODEL_LINE }).isTrue()
  }

  @Test
  fun `WorkItem with only stale sidecars retries after its output is restored`() = runBlocking {
    writeRawFile("day-1", "initial.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    awaitPipelineIdle()

    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    val additiveFile = writeRawFile("day-1", "additive.parquet", listOf("person-2"))
    val additiveDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val additiveUpload = listUploads().single { it.doneBlobGeneration == additiveDoneGeneration }
    val directTask =
      listAvailabilityTasks(additiveUpload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    val sidecarKey = blobKey(sidecarUri(additiveFile, DIRECT_MODEL_LINE, EVENT_DATE))
    val sidecarBytes = checkNotNull(fileStorage.getBlob(sidecarKey)).read().flatten()
    checkNotNull(fileStorage.getBlob(sidecarKey)).delete()
    workItemTransport.beforeNextRedelivery {
      fileStorage.writeBlob(sidecarKey, flowOf(sidecarBytes))
    }
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    val recoveredTask =
      listAvailabilityTasks(additiveUpload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    assertThat(recoveredTask.state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(recoveredTask.attemptCount).isEqualTo(2)
    assertThat(
        listMetadata().any {
          it.rawImpressionUpload == additiveUpload.name && it.modelLine == DIRECT_MODEL_LINE
        }
      )
      .isTrue()
  }

  @Test
  fun `overlapping WorkItem delivery does not reclaim a running attempt`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val upload = listUploads().single()
    val directTask =
      listAvailabilityTasks(upload.name).single { it.cmmsModelLine == DIRECT_MODEL_LINE }
    metadataStorage.pauseNextSyncStart()

    val firstDelivery = async(Dispatchers.Default) { processAvailabilityTask(directTask) }
    metadataStorage.awaitPausedSyncStart()
    val duplicateResult =
      async(Dispatchers.Default) { runCatching { processAvailabilityTask(directTask) } }
    val running = listAvailabilityTasks(upload.name).single { it.name == directTask.name }
    assertThat(running.state).isEqualTo(WorkItem.State.RUNNING)
    assertThat(running.attemptCount).isEqualTo(1)

    metadataStorage.releasePausedSyncStart()
    firstDelivery.await()
    assertThat(duplicateResult.await().isSuccess).isTrue()
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    val succeeded = listAvailabilityTasks(upload.name).single { it.name == directTask.name }
    assertThat(succeeded.state).isEqualTo(WorkItem.State.SUCCEEDED)
    assertThat(succeeded.attemptCount).isEqualTo(1)
    val doneKey = blobKey(succeeded.doneBlobUri)
    val doneBlob = checkNotNull(metadataStorage.getBlob(doneKey))
    assertThat(DataAvailabilityBlobs.isSynced(doneBlob)).isTrue()
    assertThat(DataAvailabilityBlobs.isDataAvailabilityPublished(doneBlob)).isTrue()
  }

  @Test
  fun `external watched path uses DataWatcher while internal output uses WorkItems`() =
    runBlocking {
      val internalDoneKey =
        blobKey(LabeledImpressionsBlobKeys.forDoneUri(outputPrefix, DIRECT_MODEL_LINE, EVENT_DATE))
      outputEventStorage.writeBlob(internalDoneKey, flowOf(ByteString.EMPTY))
      assertThat(externalAvailabilityDeliveries.get()).isEqualTo(0)

      val externalDataUri =
        "$externalOutputPrefix/model-line/direct/$EVENT_DATE/external-output.riegeli"
      val externalSidecarUri = "$externalDataUri.metadata.binpb"
      outputEventStorage.writeBlob(blobKey(externalDataUri), flowOf(ByteString.EMPTY))
      outputEventStorage.writeBlob(
        blobKey(externalSidecarUri),
        flowOf(
          blobDetails {
              blobUri = externalDataUri
              eventGroupReferenceId = "external-event-group"
              modelLine = DIRECT_MODEL_LINE
              interval =
                com.google.type.interval {
                  startTime = EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
                  endTime =
                    EVENT_DATE.plusDays(1).atStartOfDay(ZoneOffset.UTC).toInstant().toProtoTime()
                }
            }
            .toByteString()
        ),
      )
      val externalDoneKey = blobKey("$externalOutputPrefix/model-line/direct/$EVENT_DATE/done")
      outputEventStorage.writeBlob(externalDoneKey, flowOf(ByteString.EMPTY))
      endpointFailure.getAndSet(null)?.let {
        throw AssertionError("External finalized-object delivery failed", it)
      }

      assertThat(externalAvailabilityDeliveries.get()).isEqualTo(1)
      val metadata = listMetadata().single()
      assertThat(metadata.modelLine).isEqualTo(DIRECT_MODEL_LINE)
      assertThat(metadata.rawImpressionUpload).isEmpty()
      assertThat(metadata.state).isEqualTo(ImpressionMetadata.State.ACTIVE)
    }
}
