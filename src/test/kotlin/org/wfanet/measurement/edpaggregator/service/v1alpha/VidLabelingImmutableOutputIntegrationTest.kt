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
import java.io.File
import kotlin.test.assertFailsWith
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.selects.select
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.rawimpressions.OutputManifestStore
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventReader
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeler.LabelingOutputAttempt
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem.WorkItemParams
import org.wfanet.measurement.storage.BlobChangedException

internal class VidLabelingImmutableOutputIntegrationTest :
  VidLabelingPipelineTestHarness(PipelineHarnessConfig(immutableOutputPublication = true)) {
  private val getMethod =
    "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/GetVidLabelingJob"
  private val markMethod =
    "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/MarkVidLabelingJobSucceeded"

  private fun params(work: WorkItem): VidLabelerParams =
    work.workItemParams
      .unpack(WorkItemParams::class.java)
      .appParams
      .unpack(VidLabelerParams::class.java)

  private suspend fun prepare(folder: String, model: String): Pair<WorkItem, String> {
    setVisibleModelLines(model)
    withholdNextVidLabelerPublications()
    writeRawFile(folder, "input.parquet", listOf("person"))
    val generation = finalizeRawUpload(folder)
    workItemTransport.awaitIdle()
    val upload = listUploads().single { it.doneBlobGeneration == generation }.name
    return vidLabelerWorkItems().single { params(it).vidLabelingJob.startsWith(upload + "/") } to
      upload
  }

  private suspend fun job(work: WorkItem): VidLabelingJob =
    VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
      .getVidLabelingJob(getVidLabelingJobRequest { name = params(work).vidLabelingJob })

  /** Reads the exact selected sidecar and decrypts its data through the fulfillment reader. */
  private suspend fun verifySelected(work: WorkItem, upload: String, model: String) {
    val selected = job(work)
    assertThat(selected.state).isEqualTo(VidLabelingJob.State.SUCCEEDED)
    val records =
      OutputManifestStore(
          fileStorage,
          params(work).vidLabeledImpressionsStorageParams.impressionsBlobPrefix,
        )
        .read(selected.outputManifest)
        .toList()
    assertThat(records.first().header.owner).isEqualTo(selected.name)
    assertThat(records.first().header.rawImpressionUpload).isEqualTo(upload)
    assertThat(records.first().header.modelLinesList).containsExactly(model)
    val entry = records.single { it.hasLabeledBlob() }.labeledBlob
    assertThat(entry.rawImpressionUploadFile)
      .isEqualTo(selected.rawImpressionUploadFilesList.single())
    assertThat(records.count { it.hasFileCompleted() }).isEqualTo(1)
    assertThat(entry.impressions.blobGeneration).isGreaterThan(0L)
    assertThat(entry.metadata.blobGeneration).isGreaterThan(0L)
    assertThat(fileStorage.getFreshnessToken(blobKey(entry.metadata.blobUri)))
      .isEqualTo(entry.metadata.blobGeneration.toString())
    assertThat(fileStorage.getFreshnessToken(blobKey(entry.impressions.blobUri)))
      .isEqualTo(entry.impressions.blobGeneration.toString())
    val details =
      BlobDetails.parseFrom(
        checkNotNull(fileStorage.getBlob(blobKey(entry.metadata.blobUri))).read().flatten()
      )
    assertThat(details.blobUri).isEqualTo(entry.impressions.blobUri)
    val events =
      StorageEventReader(
          blobGeneration = entry.impressions.blobGeneration,
          blobDetails = details,
          kmsClient = kmsClient,
          impressionsStorageConfig = StorageConfig(rootDirectory = File("/")),
          descriptor = pipelineConfig.eventDescriptor,
        )
        .readEvents()
        .toList()
        .flatten()
    assertThat(
        events.flatMap { it.entityKeys }.filter { it.entityType == "person" }.map { it.entityId }
      )
      .containsExactly("person")
    val row = listMetadata().single { it.rawImpressionUpload == upload && it.modelLine == model }
    assertThat(row.blobUri).isEqualTo(entry.metadata.blobUri)
    assertThat(row.metadataBlobGeneration).isEqualTo(entry.metadata.blobGeneration)
    assertThat(row.impressionsBlobGeneration).isEqualTo(entry.impressions.blobGeneration)
    assertThat(listModelLines(upload).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
  }

  @Test
  fun `competing memoized labelers select one matched immutable output pair`() =
    runBlocking<Unit> { race("memoized-output-race", MEMOIZED_MODEL_LINE) }

  @Test
  fun `competing direct labelers select one matched immutable output pair`() =
    runBlocking<Unit> { race("direct-output-race", DIRECT_MODEL_LINE) }

  private suspend fun race(folder: String, model: String) =
    kotlinx.coroutines.coroutineScope {
      val (work, upload) = prepare(folder, model)
      val gate = responseGates.arm(getMethod)
      val attempts =
        List(2) { async(Dispatchers.Default) { runCatching { runVidLabelerDirect(work) } } }
      gate.awaitBoth()
      assertThat(gate.messages.map { (it as VidLabelingJob).state })
        .containsExactly(VidLabelingJob.State.CREATED, VidLabelingJob.State.CREATED)
      assertThat(attempts.awaitAll().all { it.isSuccess }).isTrue()
      awaitPipelineIdle()
      verifySelected(work, upload, model)
      val selected = job(work).outputManifest
      val generations = outputGenerations(model)
      runVidLabelerDirect(work)
      awaitPipelineIdle()
      assertThat(job(work).outputManifest).isEqualTo(selected)
      assertThat(outputGenerations(model)).containsExactlyEntriesIn(generations)
    }

  @Test
  fun `stale competing worker colliding with successful output makes no success mutation`() =
    runBlocking<Unit> {
      for ((index, model) in listOf(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE).withIndex()) {
        val (work, upload) = prepare("successful-collision-$index", model)
        forcedLabelingAttemptId = "00000000-0000-4000-8000-000000000099"
        val before = responseGates.callCount(markMethod)
        val gate = responseGates.arm(getMethod, holdSecond = true)
        val attempts =
          List(2) { async(Dispatchers.Default) { runCatching { runVidLabelerDirect(work) } } }
        gate.awaitBoth()
        assertThat(gate.messages.map { (it as VidLabelingJob).state })
          .containsExactly(VidLabelingJob.State.CREATED, VidLabelingJob.State.CREATED)
        val first = select { attempts.forEach { it.onAwait { result -> result } } }
        assertThat(first.isSuccess).isTrue()
        awaitPipelineIdle()
        val selected = job(work).outputManifest
        val generations = outputGenerations(model)
        gate.releaseSecond()
        assertThat(attempts.awaitAll().all { it.isSuccess }).isTrue()
        assertThat(responseGates.callCount(markMethod)).isEqualTo(before + 1)
        assertThat(job(work).outputManifest).isEqualTo(selected)
        assertThat(outputGenerations(model)).containsExactlyEntriesIn(generations)
        forcedLabelingAttemptId = null
        verifySelected(work, upload, model)
      }
    }

  @Test
  fun `incomplete data sidecar and manifest collisions preserve orphan bytes and retry freshly`() =
    runBlocking<Unit> {
      val cases =
        listOf(MEMOIZED_MODEL_LINE, DIRECT_MODEL_LINE).flatMap { model ->
          listOf("data", "metadata", "manifest").map { model to it }
        }
      for ((index, case) in cases.withIndex()) {
        val (model, boundary) = case
        val (work, upload) = prepare("incomplete-output-$index", model)
        val p = params(work)
        val current = job(work)
        val file = listUploadFiles(upload).single()
        val id = "00000000-0000-4000-8000-000000000099"
        forcedLabelingAttemptId = id
        val root = p.vidLabeledImpressionsStorageParams.impressionsBlobPrefix
        val uri =
          LabelingOutputAttempt(fileStorage, root, current.name, p.modelLinesList, id).use { attempt
            ->
            LabeledImpressionsBlobKeys.forAttemptInputUri(
              root,
              file.blobUri,
              model,
              EVENT_DATE,
              attempt.path(file.name),
            )
          }
        val orphanUri =
          when (boundary) {
            "data" -> uri
            "metadata" -> uri + ".metadata.binpb"
            else ->
              root.trimEnd('/') +
                "/.attempts/" +
                upload.substringAfterLast('/') +
                "/" +
                current.name.substringAfterLast('/') +
                "/" +
                id +
                "/manifest"
          }
        val key = blobKey(orphanUri)
        val bytes = ByteString.copyFromUtf8("orphan")
        fileStorage.writeBlob(key, bytes)
        val original = fileStorage.getFreshnessToken(key)
        val marks = responseGates.callCount(markMethod)
        assertFailsWith<BlobChangedException> { runVidLabelerDirect(work) }
        assertThat(job(work).state).isEqualTo(VidLabelingJob.State.CREATED)
        assertThat(responseGates.callCount(markMethod)).isEqualTo(marks)
        assertThat(fileStorage.getFreshnessToken(key)).isEqualTo(original)
        assertThat(checkNotNull(fileStorage.getBlob(key)).read().flatten()).isEqualTo(bytes)
        forcedLabelingAttemptId = null
        runVidLabelerDirect(work)
        awaitPipelineIdle()
        verifySelected(work, upload, model)
        assertThat(fileStorage.getFreshnessToken(key)).isEqualTo(original)
      }
    }

  @Test
  fun `crash after complete manifest before selection leaves only orphan output`() =
    runBlocking<Unit> {
      val (work, upload) = prepare("before-label-selection", DIRECT_MODEL_LINE)
      forcedLabelingAttemptId = "00000000-0000-4000-8000-000000000099"
      edpaBeforeCallFaults.arm(markMethod)
      assertThat(runCatching { runVidLabelerDirect(work) }.isFailure).isTrue()
      assertThat(job(work).state).isEqualTo(VidLabelingJob.State.CREATED)
      assertThat(job(work).hasOutputManifest()).isFalse()
      assertThat(listMetadata()).isEmpty()
      forcedLabelingAttemptId = null
      runVidLabelerDirect(work)
      awaitPipelineIdle()
      verifySelected(work, upload, DIRECT_MODEL_LINE)
    }

  @Test
  fun `lost selection and parent replies reuse committed outputs and done generation`() =
    runBlocking<Unit> {
      for ((index, method) in
        listOf(
            markMethod,
            "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/MarkRawImpressionUploadModelLineAvailabilitySyncing",
          )
          .withIndex()) {
        val (work, upload) = prepare("after-label-selection-$index", DIRECT_MODEL_LINE)
        edpaBeforeCallFaults.armAfterCommit(method)
        assertThat(runCatching { runVidLabelerDirect(work) }.isFailure).isTrue()
        val selected = job(work).outputManifest
        assertThat(selected.blobGeneration).isGreaterThan(0L)
        runVidLabelerDirect(work)
        awaitPipelineIdle()
        val generations = outputGenerations(DIRECT_MODEL_LINE)
        val row = listMetadata().single { it.rawImpressionUpload == upload }
        val doneGeneration = row.outputDoneBlobGeneration
        runVidLabelerDirect(work)
        awaitPipelineIdle()
        assertThat(job(work).outputManifest).isEqualTo(selected)
        assertThat(outputGenerations(DIRECT_MODEL_LINE)).containsExactlyEntriesIn(generations)
        assertThat(
            listMetadata().single { it.rawImpressionUpload == upload }.outputDoneBlobGeneration
          )
          .isEqualTo(doneGeneration)
        verifySelected(work, upload, DIRECT_MODEL_LINE)
      }
    }

  @Test
  fun `lost availability publication recovers selected output without relabeling`() =
    runBlocking<Unit> {
      workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
      val (work, upload) = prepare("lost-output-publication", DIRECT_MODEL_LINE)
      runVidLabelerDirect(work)
      workItemTransport.awaitIdle()
      val selected = job(work).outputManifest
      assertThat(listMetadata()).isEmpty()
      workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
      awaitPipelineIdle()
      assertThat(job(work).outputManifest).isEqualTo(selected)
      verifySelected(work, upload, DIRECT_MODEL_LINE)
    }
}
