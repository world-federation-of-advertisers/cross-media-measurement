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
import io.grpc.Status
import java.nio.file.Files
import java.nio.file.attribute.FileTime
import java.time.Duration
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.DIRECT_MODEL_LINE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcher

internal class VidLabelingRevisionHealingIntegrationTest : VidLabelingPipelineTestHarness() {
  @Test
  fun `unchanged done rewrite is a no-op`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val original = listUploads().single()
    val metadata = listMetadata()
    val memoizedOutputs = outputGenerations(MEMOIZED_MODEL_LINE)
    val directOutputs = outputGenerations(DIRECT_MODEL_LINE)

    finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()

    assertThat(listUploads()).containsExactly(original)
    assertThat(listCorrectionCandidates()).isEmpty()
    assertThat(listMetadata()).containsExactlyElementsIn(metadata)
    assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(memoizedOutputs)
    assertThat(outputGenerations(DIRECT_MODEL_LINE)).isEqualTo(directOutputs)
  }

  @Test
  fun `overwrite is quarantined before labeling work`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("person-1"))
    val originalGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalGeneration }

    writeRawFile("day-1", "input.parquet", listOf("corrected-person"))
    val correctionGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()

    val correction = listUploads().single { it.doneBlobGeneration == correctionGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
    assertThat(correction.replacesRawImpressionUpload).isEqualTo(original.name)
    assertThat(listModelLines(correction.name)).isEmpty()
    assertThat(listAvailabilityTasks(correction.name)).isEmpty()
    assertThat(listCorrectionCandidates().single().classification)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.Classification.EDITED)
  }

  @Test
  fun `empty removal revision is quarantined before labeling work`() = runBlocking {
    val inputFile = writeRawFile("day-1", "input.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    awaitPipelineIdle()
    checkNotNull(fileStorage.getBlob(blobKey(inputFile))).delete()

    val correctionGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()

    val correction = listUploads().single { it.doneBlobGeneration == correctionGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
    assertThat(listUploadFiles(correction.name)).isEmpty()
    assertThat(listModelLines(correction.name)).isEmpty()
    assertThat(listCorrectionCandidates().single().classification)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.Classification.REMOVED)
  }

  @Test
  fun `late raw file waits for a new done generation`() = runBlocking {
    writeRawFile("day-1", "initial.parquet", listOf("person-1"))
    finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val originalUploads = listUploads()

    writeRawFile("day-1", "late.parquet", listOf("person-2"))
    workItemTransport.awaitIdle()
    assertThat(listUploads()).containsExactlyElementsIn(originalUploads)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1"))

    val nextGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()

    val append = listUploads().single { it.doneBlobGeneration == nextGeneration }
    assertThat(append.replacesRawImpressionUpload).isEqualTo(originalUploads.single().name)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("person-1", "person-2"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("person-1", "person-2"))
  }

  @Test
  fun `raw object overwritten after registration fails before using the newer generation`() =
    runBlocking {
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextPoolAssignmentPublications()
      val input = writeRawFile("registered-generation", "input.parquet", listOf("registered"))
      val doneGeneration = finalizeRawUpload("registered-generation")
      workItemTransport.awaitIdle()
      val upload = listUploads().single { it.doneBlobGeneration == doneGeneration }
      val registeredGeneration = listUploadFiles(upload.name).single().blobGeneration

      writeRawFile("registered-generation", "input.parquet", listOf("newer-overwrite"))
      assertThat(generationOf(input)).isNotEqualTo(registeredGeneration)
      republishQueuedPoolAssignments()
      workItemTransport.awaitIdle(allowFailures = true)

      assertThat(poolAssignmentWorkItems().single().state).isEqualTo(WorkItem.State.FAILED)
      assertThat(rankBuilderWorkItems()).isEmpty()
      assertThat(vidLabelerWorkItems()).isEmpty()
      assertThat(listAvailabilityTasks(upload.name)).isEmpty()
      assertThat(listMetadata()).isEmpty()
    }

  @Test
  fun `registration resumes after failures at every persisted boundary`() = runBlocking {
    val boundaries =
      listOf(
        "files" to
          "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadFileService/" +
            "BatchCreateRawImpressionUploadFiles",
        "model-lines" to
          "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/" +
            "BatchCreateRawImpressionUploadModelLines",
        "completion" to
          "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadService/" +
            "MarkRawImpressionUploadRegistrationComplete",
      )
    for ((folder, fullMethodName) in boundaries) {
      writeRawFile("registration-$folder", "input.parquet", listOf("person-$folder"))
      edpaBeforeCallFaults.arm(fullMethodName)

      val firstDelivery = runCatching { finalizeRawUpload("registration-$folder") }

      assertThat(firstDelivery.isFailure).isTrue()
      val doneUri = "$rawPrefix/registration-$folder/done"
      val generation = generationOf(doneUri)
      val partial =
        listUploads().single { it.doneBlobUri == doneUri && it.doneBlobGeneration == generation }
      assertThat(partial.registrationComplete).isFalse()

      rawWatcher.receivePath(
        doneUri,
        mapOf(DataWatcher.GENERATION_METADATA_KEY to generation.toString()),
      )
      awaitPipelineIdle()

      val revisions =
        listUploads().filter { it.doneBlobUri == doneUri && it.doneBlobGeneration == generation }
      assertThat(revisions).hasSize(1)
      assertThat(revisions.single().registrationComplete).isTrue()
      assertCompletedForBothPaths(revisions.single())
    }
  }

  @Test
  fun `lost registration completion response resumes without a duplicate upload`() = runBlocking {
    writeRawFile("lost-registration-response", "input.parquet", listOf("person"))
    edpaBeforeCallFaults.armAfterCommit(
      "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadService/" +
        "MarkRawImpressionUploadRegistrationComplete"
    )

    assertThat(runCatching { finalizeRawUpload("lost-registration-response") }.isFailure).isTrue()
    val doneUri = "$rawPrefix/lost-registration-response/done"
    val generation = generationOf(doneUri)
    val committed = listUploads().single { it.doneBlobGeneration == generation }
    assertThat(committed.registrationComplete).isTrue()
    assertThat(listUploadFiles(committed.name)).hasSize(1)
    assertThat(listModelLines(committed.name)).hasSize(2)
    assertThat(poolAssignmentWorkItems()).isEmpty()
    assertThat(vidLabelerWorkItems()).isEmpty()

    rawWatcher.receivePath(
      doneUri,
      mapOf(DataWatcher.GENERATION_METADATA_KEY to generation.toString()),
    )
    workItemTransport.awaitIdle()

    assertThat(listModelLines(committed.name).map { it.state }.toSet())
      .containsExactly(RawImpressionUploadModelLine.State.CREATED)
    val monitorDispatch = buildVidLabelingMonitor().runDispatch()
    assertThat(monitorDispatch.dispatchError).isFalse()
    assertThat(monitorDispatch.dispatchedUpload).isEqualTo(committed.name)
    awaitPipelineIdle()

    assertThat(listUploads().filter { it.doneBlobGeneration == generation }).hasSize(1)
    assertCompletedForBothPaths(listUploads().single { it.doneBlobGeneration == generation })
    assertThat(listMetadata()).hasSize(2)
  }

  @Test
  fun `historical days and advertiser directories remain independent`() = runBlocking {
    val uploads =
      listOf(
        Triple("main/day-1", EVENT_DATE, "stable-person"),
        Triple("main/day-3", EVENT_DATE.plusDays(2), "stable-person"),
        Triple("main/day-2", EVENT_DATE.plusDays(1), "stable-person"),
        Triple("main/day-2/advertiser-a", EVENT_DATE.plusDays(1), "advertiser-person"),
      )
    for ((folder, date, person) in uploads) {
      writeRawFile(folder, "input.parquet", listOf(person), date)
      finalizeRawUpload(folder)
      awaitPipelineIdle()
    }

    assertThat(listUploads().map { it.doneBlobUri }.toSet())
      .containsExactlyElementsIn(uploads.map { "$rawPrefix/${it.first}/done" })
    assertThat(listUploads().all { it.replacesRawImpressionUpload.isEmpty() }).isTrue()
    assertThat(listMetadata()).hasSize(8)
    assertThat(
        readLabeledPeople(MEMOIZED_MODEL_LINE)
          .filter { it.personId == "stable-person" }
          .map { it.vid }
          .toSet()
      )
      .hasSize(1)
  }

  @Test
  fun `all mixed manifest combinations are quarantined atomically`() = runBlocking {
    val folders = listOf("add-edit", "add-remove", "edit-remove", "all-four")
    for (folder in folders) {
      writeRawFile(folder, "a.parquet", listOf("$folder-a"))
      writeRawFile(folder, "b.parquet", listOf("$folder-b"))
      if (folder == "all-four") {
        writeRawFile(folder, "retained.parquet", listOf("$folder-retained"))
      }
      finalizeRawUpload(folder)
      awaitPipelineIdle()
    }

    writeRawFile("add-edit", "a.parquet", listOf("add-edit-a-new"))
    writeRawFile("add-edit", "added.parquet", listOf("add-edit-added"))

    checkNotNull(fileStorage.getBlob(blobKey("gs://$fileBucket/$rootKey/raw/add-remove/a.parquet")))
      .delete()
    writeRawFile("add-remove", "added.parquet", listOf("add-remove-added"))

    writeRawFile("edit-remove", "a.parquet", listOf("edit-remove-a-new"))
    checkNotNull(
        fileStorage.getBlob(blobKey("gs://$fileBucket/$rootKey/raw/edit-remove/b.parquet"))
      )
      .delete()

    writeRawFile("all-four", "a.parquet", listOf("all-four-a-new"))
    checkNotNull(fileStorage.getBlob(blobKey("gs://$fileBucket/$rootKey/raw/all-four/b.parquet")))
      .delete()
    writeRawFile("all-four", "added.parquet", listOf("all-four-added"))

    for (folder in folders) {
      val generation = finalizeRawUpload(folder)
      workItemTransport.awaitIdle()
      val correction = listUploads().single { it.doneBlobGeneration == generation }
      assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
      assertThat(listModelLines(correction.name)).isEmpty()
    }

    val candidates = listCorrectionCandidates()
    assertThat(candidates).hasSize(4)
    assertThat(candidates.map { it.classification }.toSet())
      .containsExactly(RawImpressionUploadCorrectionCandidate.Classification.MIXED)
    val differencesByFolder =
      candidates.associate { candidate ->
        val upload = listUploads().single { it.name == candidate.rawImpressionUpload }
        val folder = upload.doneBlobUri.substringAfter("$rawPrefix/").substringBeforeLast("/done")
        folder to
          getCorrectionCandidate(candidate.name).manifestDifferencesList.map { it.type }.toSet()
      }
    assertThat(differencesByFolder.getValue("add-edit"))
      .containsExactly(
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.ADDED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED,
      )
    assertThat(differencesByFolder.getValue("add-remove"))
      .containsExactly(
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.ADDED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.REMOVED,
      )
    assertThat(differencesByFolder.getValue("edit-remove"))
      .containsExactly(
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.REMOVED,
      )
    assertThat(differencesByFolder.getValue("all-four"))
      .containsExactly(
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.ADDED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EDITED,
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.REMOVED,
      )
    Unit
  }

  @Test
  fun `moving an object to another event date is one linked correction`() = runBlocking {
    writeRawFile("date-move", "input.parquet", listOf("person"), EVENT_DATE)
    finalizeRawUpload("date-move")
    awaitPipelineIdle()

    writeRawFile("date-move", "input.parquet", listOf("person"), EVENT_DATE.plusDays(1))
    val correctionGeneration = finalizeRawUpload("date-move")
    workItemTransport.awaitIdle()

    val correction = listUploads().single { it.doneBlobGeneration == correctionGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
    val candidate = getCorrectionCandidate(listCorrectionCandidates().single().name)
    assertThat(candidate.classification)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.Classification.MIXED)
    assertThat(candidate.manifestDifferencesList.map { it.type })
      .containsExactly(
        RawImpressionUploadCorrectionCandidate.ManifestDifference.Type.EVENT_DATE_CHANGED
      )
    assertThat(listUploadFiles(correction.name).single().eventDate)
      .isEqualTo(
        com.google.type.date {
          year = EVENT_DATE.plusDays(1).year
          month = EVENT_DATE.plusDays(1).monthValue
          day = EVENT_DATE.plusDays(1).dayOfMonth
        }
      )
  }

  @Test
  fun `duplicate correction delivery is idempotent and a newer correction supersedes it`() =
    runBlocking {
      writeRawFile("supersession", "input.parquet", listOf("original"))
      finalizeRawUpload("supersession")
      awaitPipelineIdle()

      writeRawFile("supersession", "input.parquet", listOf("correction-1"))
      val firstCorrectionGeneration = finalizeRawUpload("supersession")
      workItemTransport.awaitIdle()
      val uploadsAfterFirst = listUploads()
      val firstCandidate = listCorrectionCandidates().single()

      rawWatcher.receivePath(
        "$rawPrefix/supersession/done",
        mapOf(DataWatcher.GENERATION_METADATA_KEY to firstCorrectionGeneration.toString()),
      )
      workItemTransport.awaitIdle()
      assertThat(listUploads()).containsExactlyElementsIn(uploadsAfterFirst)
      assertThat(listCorrectionCandidates()).containsExactly(firstCandidate)

      writeRawFile("supersession", "input.parquet", listOf("correction-2"))
      finalizeRawUpload("supersession")
      workItemTransport.awaitIdle()

      val candidates = listCorrectionCandidates()
      assertThat(candidates).hasSize(2)
      val superseded = candidates.single { it.name == firstCandidate.name }
      val pending = candidates.single { it.name != firstCandidate.name }
      assertThat(superseded.state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.SUPERSEDED)
      assertThat(superseded.supersedingRawImpressionUploadCorrectionCandidate)
        .isEqualTo(pending.name)
      assertThat(pending.state).isEqualTo(RawImpressionUploadCorrectionCandidate.State.PENDING)
    }

  @Test
  fun `healthy no-op after quarantine resolves the candidate and releases the fence`() =
    runBlocking {
      val input = writeRawFile("healthy-resolution", "input.parquet", listOf("original"))
      val originalBytes = checkNotNull(fileStorage.getBlob(blobKey(input))).read().flatten()
      val originalGeneration = generationOf(input)
      finalizeRawUpload("healthy-resolution")
      awaitPipelineIdle()

      writeRawFile("healthy-resolution", "input.parquet", listOf("incorrect"))
      finalizeRawUpload("healthy-resolution")
      workItemTransport.awaitIdle()
      assertThat(listCorrectionCandidates().single().state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.PENDING)

      fileStorage.writeBlob(blobKey(input), flowOf(originalBytes))
      Files.setLastModifiedTime(
        java.nio.file.Paths.get("/$fileBucket/${blobKey(input)}"),
        FileTime.fromMillis(originalGeneration),
      )
      assertThat(generationOf(input)).isEqualTo(originalGeneration)
      finalizeRawUpload("healthy-resolution")
      workItemTransport.awaitIdle()

      assertThat(listUploads()).hasSize(2)
      assertThat(listCorrectionCandidates().single().state)
        .isEqualTo(RawImpressionUploadCorrectionCandidate.State.SUPERSEDED)

      writeRawFile("after-resolution", "input.parquet", listOf("new-person"))
      val nextGeneration = finalizeRawUpload("after-resolution")
      awaitPipelineIdle()
      val next = listUploads().single { it.doneBlobGeneration == nextGeneration }
      assertThat(next.processingDeferred).isFalse()
      assertCompletedForBothPaths(next)
    }

  @Test
  fun `one plan applies one correction and removes another without replacement`() = runBlocking {
    for (folder in listOf("plan-apply", "plan-remove")) {
      writeRawFile(folder, "input.parquet", listOf("$folder-original"))
      finalizeRawUpload(folder)
      awaitPipelineIdle()
    }
    for (folder in listOf("plan-apply", "plan-remove")) {
      writeRawFile(folder, "input.parquet", listOf("$folder-corrected"))
      finalizeRawUpload(folder)
      workItemTransport.awaitIdle()
    }
    val candidateUploads =
      listCorrectionCandidates().associateBy { candidate ->
        listUploads()
          .single { it.name == candidate.rawImpressionUpload }
          .doneBlobUri
          .substringAfter("$rawPrefix/")
          .substringBeforeLast("/done")
      }
    assertThat(candidateUploads.keys).containsExactly("plan-apply", "plan-remove")

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    assertThat(draft.rawImpressionUploadCorrectionCandidatesList)
      .containsExactlyElementsIn(candidateUploads.values.map { it.name })
    approveHealingOperation(
      draft,
      mapOf(
        candidateUploads.getValue("plan-apply").name to
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
        candidateUploads.getValue("plan-remove").name to
          RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
      ),
    )

    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    workItemTransport.awaitIdle()
    endpointFailure.getAndSet(null)?.let { throw AssertionError("Correction replay failed", it) }
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    val completedCandidates = listCorrectionCandidates().associateBy { it.name }
    assertThat(completedCandidates.values.map { it.state }.toSet())
      .containsExactly(RawImpressionUploadCorrectionCandidate.State.COMPLETE)
    assertThat(completedCandidates.getValue(candidateUploads.getValue("plan-apply").name).decision)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)
    assertThat(completedCandidates.getValue(candidateUploads.getValue("plan-remove").name).decision)
      .isEqualTo(
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
      )
    assertThat(listMetadata()).hasSize(2)
    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet())
      .containsExactly(candidateUploads.getValue("plan-apply").rawImpressionUpload)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("plan-apply-corrected"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("plan-apply-corrected"))
  }

  @Test
  fun `queued old availability work becomes obsolete during correction healing`() = runBlocking {
    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    writeRawFile("queued-obsolete", "input.parquet", listOf("old-person"))
    finalizeRawUpload("queued-obsolete")
    workItemTransport.awaitIdle()
    val original = listUploads().single()
    val originalTasks = listAvailabilityTasks(original.name)
    assertThat(originalTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.QUEUED)

    writeRawFile("queued-obsolete", "input.parquet", listOf("corrected-person"))
    finalizeRawUpload("queued-obsolete")
    workItemTransport.awaitIdle()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    approveHealingOperation(
      draft,
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
    )

    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    workItemTransport.awaitIdle()
    endpointFailure.getAndSet(null)?.let { throw AssertionError("Correction replay failed", it) }
    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    for (task in originalTasks) {
      workItemTransport.redeliverWorkItem(task.name)
    }
    workItemTransport.awaitIdle(allowFailures = true)
    assertThat(listMetadata()).isEmpty()
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.FAILED)

    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle(allowFailures = true)
    val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
    assertThat(listAvailabilityTasks(replacement.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet())
      .containsExactly(replacement.name)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("corrected-person"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("corrected-person"))
  }

  @Test
  fun `middle day correction rebuilds every later memoized snapshot`() = runBlocking {
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    val originalsByDate = linkedMapOf<java.time.LocalDate, RawImpressionUpload>()
    for ((offset, uniquePerson) in listOf("day-1", "day-2", "day-3").withIndex()) {
      val date = EVENT_DATE.plusDays(offset.toLong())
      setToday(date)
      writeRawFile(
        "cascade/$uniquePerson",
        "input.parquet",
        listOf("stable-person", uniquePerson),
        date,
      )
      val generation = finalizeRawUpload("cascade/$uniquePerson")
      awaitPipelineIdle()
      originalsByDate[date] = listUploads().single { it.doneBlobGeneration == generation }
    }
    val originalRankRows =
      originalsByDate.mapValues { (_, upload) -> listRankIndexBlobs(upload.name) }

    setToday(EVENT_DATE.plusDays(2))
    writeRawFile(
      "cascade/day-2",
      "input.parquet",
      listOf("stable-person", "corrected-day-2"),
      EVENT_DATE.plusDays(1),
    )
    finalizeRawUpload("cascade/day-2")
    workItemTransport.awaitIdle()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    approveHealingOperation(
      draft,
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
    )

    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    repeat(10) {
      if (listHealingOperations().single().state == UploadHealingOperation.State.COMPLETE) {
        return@repeat
      }
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      workItemTransport.awaitIdle()
      endpointFailure.getAndSet(null)?.let { throw AssertionError("Cascade replay failed", it) }
    }
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)

    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    val day1 = originalsByDate.getValue(EVENT_DATE)
    val day1Rows = listRankIndexBlobs(day1.name, showDeleted = true).associateBy { it.name }
    for (original in originalRankRows.getValue(EVENT_DATE)) {
      assertThat(day1Rows.getValue(original.name).hasDeleteTime()).isFalse()
    }
    for (date in listOf(EVENT_DATE.plusDays(1), EVENT_DATE.plusDays(2))) {
      assertSnapshotsEvicted(originalsByDate.getValue(date).name, originalRankRows.getValue(date))
    }
    assertThat(listMetadata()).hasSize(3)
    assertThat(
        readLabeledPeople(MEMOIZED_MODEL_LINE)
          .filter { it.eventDate == EVENT_DATE.plusDays(1) }
          .map { it.personId }
      )
      .containsExactly("stable-person", "corrected-day-2")
    assertThat(
        readLabeledPeople(MEMOIZED_MODEL_LINE)
          .filter { it.eventDate == EVENT_DATE.plusDays(2) }
          .map { it.personId }
      )
      .containsExactly("stable-person", "day-3")
    assertThat(
        readLabeledPeople(MEMOIZED_MODEL_LINE)
          .filter { it.personId == "stable-person" }
          .map { it.vid }
          .toSet()
      )
      .hasSize(1)
    assertAvailabilityPublished(
      setOf(MEMOIZED_MODEL_LINE),
      setOf(EVENT_DATE, EVENT_DATE.plusDays(1), EVENT_DATE.plusDays(2)),
    )
  }

  @Test
  fun `main and advertiser corrections on one date share one deduplicated plan`() = runBlocking {
    writeRawFile("shared-day/main", "main.parquet", listOf("main-original"))
    finalizeRawUpload("shared-day/main")
    awaitPipelineIdle()
    writeRawFile("shared-day/advertiser-a", "advertiser.parquet", listOf("advertiser-original"))
    finalizeRawUpload("shared-day/advertiser-a")
    awaitPipelineIdle()

    writeRawFile("shared-day/main", "main.parquet", listOf("main-corrected"))
    finalizeRawUpload("shared-day/main")
    workItemTransport.awaitIdle()
    writeRawFile("shared-day/advertiser-a", "advertiser.parquet", listOf("advertiser-corrected"))
    finalizeRawUpload("shared-day/advertiser-a")
    workItemTransport.awaitIdle()
    assertThat(listCorrectionCandidates()).hasSize(2)

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.rawImpressionUploadCorrectionCandidatesList)
      .containsExactlyElementsIn(listCorrectionCandidates().map { it.name })
    approveHealingOperation(
      draft,
      draft.rawImpressionUploadCorrectionCandidatesList.associateWith {
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT
      },
    )
    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(listMetadata()).isEmpty()
  }

  @Test
  fun `two providers keep candidates operations and fences isolated`() = runBlocking {
    val firstProvider = "dataProviders/dp1"
    val secondProvider = "dataProviders/dp2"
    registerSyntheticCorrectionCandidate("dp1", "123e4567-e89b-42d3-a456-426614174081")
    registerSyntheticCorrectionCandidate("dp2", "123e4567-e89b-42d3-a456-426614174082")

    val firstCandidate = listCorrectionCandidates(firstProvider).single()
    val secondCandidate = listCorrectionCandidates(secondProvider).single()
    assertThat(firstCandidate.name).startsWith("$firstProvider/")
    assertThat(secondCandidate.name).startsWith("$secondProvider/")
    assertThat(firstCandidate.rawImpressionUpload).startsWith("$firstProvider/")
    assertThat(secondCandidate.rawImpressionUpload).startsWith("$secondProvider/")

    val controller =
      buildHealingController(dataProviderNames = listOf(firstProvider, secondProvider))
    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    val firstOperation = listHealingOperations(firstProvider).single()
    val secondOperation = listHealingOperations(secondProvider).single()
    assertThat(firstOperation.rawImpressionUploadCorrectionCandidatesList)
      .containsExactly(firstCandidate.name)
    assertThat(secondOperation.rawImpressionUploadCorrectionCandidatesList)
      .containsExactly(secondCandidate.name)
    assertThat(firstOperation.name).startsWith("$firstProvider/")
    assertThat(secondOperation.name).startsWith("$secondProvider/")
  }

  @Test
  fun `stale approval is rejected while an idempotent approval retry is stable`() = runBlocking {
    writeRawFile("approval", "input.parquet", listOf("original"))
    finalizeRawUpload("approval")
    awaitPipelineIdle()
    writeRawFile("approval", "input.parquet", listOf("corrected"))
    finalizeRawUpload("approval")
    workItemTransport.awaitIdle()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    val candidate = draft.rawImpressionUploadCorrectionCandidatesList.single()
    val decisions =
      mapOf(candidate to RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE)

    val invalid =
      runCatching {
          approveHealingOperation(
            draft,
            decisions,
            requestId = "123e4567-e89b-42d3-a456-426614174090",
            etag = "invalid-etag",
          )
        }
        .exceptionOrNull()
    assertThat(Status.fromThrowable(invalid).code).isEqualTo(Status.Code.ABORTED)

    val requestId = "123e4567-e89b-42d3-a456-426614174091"
    val approved = approveHealingOperation(draft, decisions, requestId)
    val retried = approveHealingOperation(draft, decisions, requestId)
    assertThat(retried).isEqualTo(approved)
    assertThat(retried.state).isEqualTo(UploadHealingOperation.State.APPROVED)
  }

  @Test
  fun `out of retention correction enters operator attention without eviction`() = runBlocking {
    writeRawFile("out-of-retention", "input.parquet", listOf("original"))
    finalizeRawUpload("out-of-retention")
    awaitPipelineIdle()
    val metadataBefore = listMetadata()
    writeRawFile("out-of-retention", "input.parquet", listOf("corrected"))
    finalizeRawUpload("out-of-retention")
    workItemTransport.awaitIdle()

    val controller = buildHealingController(correctionRetention = Duration.ofNanos(1))
    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)
    assertThat(listCorrectionCandidates().single().state)
      .isEqualTo(RawImpressionUploadCorrectionCandidate.State.MANUAL_INTERVENTION_REQUIRED)
    assertThat(listMetadata()).containsExactlyElementsIn(metadataBefore)
    Unit
  }

  @Test
  fun `failed replacement is repaired and operator retry completes healing`() = runBlocking {
    setVisibleModelLines(DIRECT_MODEL_LINE)
    writeRawFile("replacement-retry", "input.parquet", listOf("original"))
    val originalGeneration = finalizeRawUpload("replacement-retry")
    awaitPipelineIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalGeneration }
    val correctedInput = writeRawFile("replacement-retry", "input.parquet", listOf("corrected"))
    val correctedBytes = checkNotNull(fileStorage.getBlob(blobKey(correctedInput))).read().flatten()
    val correctedGeneration = generationOf(correctedInput)
    finalizeRawUpload("replacement-retry")
    workItemTransport.awaitIdle()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    approveHealingOperation(
      listHealingOperations().single(),
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
    )
    withholdNextVidLabelerPublications()

    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    workItemTransport.awaitIdle()
    endpointFailure.getAndSet(null)?.let { throw AssertionError("Replacement replay failed", it) }
    val replacement =
      listUploads().single {
        it.replacesRawImpressionUpload == original.name &&
          it.state != RawImpressionUpload.State.FAILED
      }
    writeRawFile("replacement-retry", "input.parquet", listOf("transient-broken-generation"))
    republishQueuedVidLabelers()
    workItemTransport.awaitIdle(allowFailures = true)
    assertThat(buildDispatchFailer().failUpload(replacement.name, "raw generation repaired"))
      .hasSize(1)
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.NEEDS_ATTENTION)

    fileStorage.writeBlob(blobKey(correctedInput), flowOf(correctedBytes))
    Files.setLastModifiedTime(
      java.nio.file.Paths.get("/$fileBucket/${blobKey(correctedInput)}"),
      FileTime.fromMillis(correctedGeneration),
    )
    assertThat(generationOf(correctedInput)).isEqualTo(correctedGeneration)
    withholdNextAvailabilityPublications()
    val dispatchRetry =
      buildFailedDispatchRetrier().retryFailed(replacement.name, DIRECT_MODEL_LINE)
    assertThat(dispatchRetry.newState).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    workItemTransport.awaitIdle(allowFailures = true)
    assertThat(listModelLines(replacement.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)

    retryHealingOperation(listHealingOperations().single())
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    republishQueuedAvailability()
    workItemTransport.awaitIdle(allowFailures = true)

    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet())
      .containsExactly(replacement.name)
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("corrected"))
  }

  @Test
  fun `restarted controller resumes one persisted operation`() = runBlocking {
    writeRawFile("controller-restart", "input.parquet", listOf("original"))
    finalizeRawUpload("controller-restart")
    awaitPipelineIdle()
    writeRawFile("controller-restart", "input.parquet", listOf("corrected"))
    finalizeRawUpload("controller-restart")
    workItemTransport.awaitIdle()

    val planner = buildHealingController()
    assertThat(planner.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    approveHealingOperation(
      draft,
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
    )

    val restarted = buildHealingController()
    assertThat(restarted.run().failedDataProviders).isEqualTo(0)
    if (listHealingOperations().single().state != UploadHealingOperation.State.COMPLETE) {
      assertThat(buildHealingController().run().failedDataProviders).isEqualTo(0)
    }

    assertThat(listHealingOperations()).hasSize(1)
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(listMetadata()).isEmpty()
  }

  @Test
  fun `obsolete availability delivery cannot restore evicted metadata`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("stale-person"))
    val originalDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalDoneGeneration }
    assertCompletedForBothPaths(original)
    val originalTasks = listAvailabilityTasks(original.name)
    assertThat(originalTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(2)
    val originalMetadataNames = listMetadata().associate { it.modelLine to it.name }

    writeRawFile("day-1", "input.parquet", listOf("corrected-person"))
    val correctionDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val correction = listUploads().single { it.doneBlobGeneration == correctionDoneGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    approveHealingOperation(
      listHealingOperations().single(),
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
    )

    workItemTransport.dropNextPublications(DataAvailabilitySyncWorkItems.QUEUE, count = 2)
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    workItemTransport.awaitIdle()
    endpointFailure.getAndSet(null)?.let {
      throw AssertionError("Corrected revision replay failed", it)
    }
    val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
    val replacementTasks = listAvailabilityTasks(replacement.name)
    assertThat(replacementTasks.map { it.state }.toSet()).containsExactly(WorkItem.State.QUEUED)
    assertThat(listMetadata()).isEmpty()

    val fencedTask = replacementTasks.first()
    assertThat(runCatching { processAvailabilityTask(fencedTask) }.isFailure).isTrue()
    val blockedTask = listAvailabilityTasks(replacement.name).single { it.name == fencedTask.name }
    assertThat(blockedTask.state).isEqualTo(WorkItem.State.RUNNING)
    assertThat(blockedTask.attemptCount).isEqualTo(1)
    assertThat(listMetadata()).isEmpty()

    for (task in originalTasks) {
      workItemTransport.redeliverWorkItem(task.name)
    }
    workItemTransport.awaitIdle(allowFailures = true)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).isEmpty()

    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    workItemTransport.redeliverWorkItem(fencedTask.name)
    workItemTransport.republishQueuedWorkItems(DataAvailabilitySyncWorkItems.QUEUE)
    awaitPipelineIdle()

    assertCompletedForBothPaths(replacement)
    assertThat(listAvailabilityTasks(replacement.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata().map { it.rawImpressionUpload }.toSet())
      .containsExactly(replacement.name)
    assertThat(listMetadata().associate { it.modelLine to it.name })
      .isEqualTo(originalMetadataNames)
    assertReadablePeople(MEMOIZED_MODEL_LINE, setOf("corrected-person"))
    assertReadablePeople(DIRECT_MODEL_LINE, setOf("corrected-person"))
  }

  @Test
  fun `no-replacement correction evicts metadata without rewriting WorkItems`() = runBlocking {
    writeRawFile("day-1", "input.parquet", listOf("stale-person"))
    val originalDoneGeneration = finalizeRawUpload("day-1")
    awaitPipelineIdle()
    val original = listUploads().single { it.doneBlobGeneration == originalDoneGeneration }
    assertCompletedForBothPaths(original)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).hasSize(2)

    writeRawFile("day-1", "input.parquet", listOf("replacement-person"))
    val correctionDoneGeneration = finalizeRawUpload("day-1")
    workItemTransport.awaitIdle()
    val correction = listUploads().single { it.doneBlobGeneration == correctionDoneGeneration }
    assertThat(correction.state).isEqualTo(RawImpressionUpload.State.CORRECTION_REQUIRED)
    assertThat(listModelLines(correction.name)).isEmpty()

    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    val draft = listHealingOperations().single()
    assertThat(draft.state).isEqualTo(UploadHealingOperation.State.APPROVAL_REQUIRED)
    approveHealingOperation(
      draft,
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
    )

    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    val completed = listHealingOperations().single()
    assertThat(completed.state).isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(listAvailabilityTasks(original.name).map { it.state }.toSet())
      .containsExactly(WorkItem.State.SUCCEEDED)
    assertThat(listMetadata()).isEmpty()
    assertThat(listMetadata(showDeleted = true).map { it.state }.toSet())
      .containsExactly(ImpressionMetadata.State.DELETED)
    Unit
  }

  @Test
  fun `eviction tolerates data only sidecar only neither and complete artifacts`() = runBlocking {
    val inputA = writeRawFile("cleanup", "a.parquet", listOf("person-a"))
    val inputB = writeRawFile("cleanup", "b.parquet", listOf("person-b"))
    finalizeRawUpload("cleanup")
    awaitPipelineIdle()
    val original = listUploads().single()
    val metadata = listMetadata()
    assertThat(metadata).hasSize(4)
    val artifacts = snapshotOutputArtifacts(metadata)

    val memoA = sidecarUri(inputA, MEMOIZED_MODEL_LINE, EVENT_DATE)
    val directA = sidecarUri(inputA, DIRECT_MODEL_LINE, EVENT_DATE)
    val memoB = sidecarUri(inputB, MEMOIZED_MODEL_LINE, EVENT_DATE)
    val directB = sidecarUri(inputB, DIRECT_MODEL_LINE, EVENT_DATE)
    checkNotNull(fileStorage.getBlob(blobKey(artifacts.getValue(memoA).dataUri))).delete()
    checkNotNull(fileStorage.getBlob(blobKey(directA))).delete()
    checkNotNull(fileStorage.getBlob(blobKey(artifacts.getValue(memoB).dataUri))).delete()
    checkNotNull(fileStorage.getBlob(blobKey(memoB))).delete()

    writeRawFile("cleanup", "a.parquet", listOf("corrected-person"))
    finalizeRawUpload("cleanup")
    workItemTransport.awaitIdle()
    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    approveHealingOperation(
      listHealingOperations().single(),
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
    )
    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(listMetadata()).isEmpty()
    assertThat(listMetadata(showDeleted = true)).hasSize(4)
    for (sidecar in listOf(memoA, directA, memoB, directB)) {
      assertThat(fileStorage.getBlob(blobKey(sidecar))).isNull()
      assertThat(fileStorage.getBlob(blobKey(artifacts.getValue(sidecar).dataUri))).isNull()
    }
    assertThat(listUploads().single { it.name == original.name }.state)
      .isEqualTo(RawImpressionUpload.State.FAILED)
    assertThat(listUploads().map { it.state })
      .contains(RawImpressionUpload.State.REMOVED_WITHOUT_REPLACEMENT)
  }

  @Test
  fun `newer output generation survives eviction of the planned generation`() = runBlocking {
    writeRawFile("output-race", "input.parquet", listOf("original"))
    finalizeRawUpload("output-race")
    awaitPipelineIdle()
    val targetMetadata = listMetadata().single { it.modelLine == DIRECT_MODEL_LINE }
    val targetArtifact = snapshotOutputArtifact(targetMetadata)

    writeRawFile("output-race", "input.parquet", listOf("corrected"))
    finalizeRawUpload("output-race")
    workItemTransport.awaitIdle()
    val controller = buildHealingController()
    assertThat(controller.run().failedDataProviders).isEqualTo(0)
    approveHealingOperation(
      listHealingOperations().single(),
      RawImpressionUploadCorrectionCandidate.Decision.DECISION_REMOVE_WITHOUT_REPLACEMENT,
    )
    beforeOutputDelete(targetArtifact.dataUri) {
      fileStorage.writeBlob(
        blobKey(targetArtifact.dataUri),
        flowOf(ByteString.copyFromUtf8("newer output generation")),
      )
    }

    assertThat(controller.run().failedDataProviders).isEqualTo(0)

    assertThat(listHealingOperations().single().state)
      .isEqualTo(UploadHealingOperation.State.COMPLETE)
    assertThat(fileStorage.getBlob(blobKey(targetArtifact.dataUri))).isNotNull()
    assertThat(fileStorage.getFreshnessToken(blobKey(targetArtifact.dataUri)))
      .isNotEqualTo(targetArtifact.dataGeneration)
  }
}
