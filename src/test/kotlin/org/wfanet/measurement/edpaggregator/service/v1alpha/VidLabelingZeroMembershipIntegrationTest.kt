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
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.Any
import com.google.protobuf.ByteString
import com.google.protobuf.TextFormat
import java.io.File
import java.time.Clock
import java.time.DateTimeException
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertFailsWith
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.hadoop.fs.Path
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.flatten
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.edpaggregator.StorageConfig
import org.wfanet.measurement.edpaggregator.resultsfulfiller.StorageEventReader
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.EVENT_DATE
import org.wfanet.measurement.edpaggregator.service.v1alpha.VidLabelingPipelineTestHarness.Companion.MEMOIZED_MODEL_LINE
import org.wfanet.measurement.edpaggregator.subpoolassigner.SubpoolAssignerApp
import org.wfanet.measurement.edpaggregator.subpoolassigner.VirtualPeoplePoolEmitLabeler
import org.wfanet.measurement.edpaggregator.testing.VidLabelingRpcThrottlersTestHelper
import org.wfanet.measurement.edpaggregator.v1alpha.BlobDetails
import org.wfanet.measurement.edpaggregator.v1alpha.GetRawImpressionUploadFileRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.MarkRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadCorrectionCandidate
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.UploadHealingOperation
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.LabeledImpressionsBlobKeys
import org.wfanet.measurement.edpaggregator.vidlabeler.ParquetImpressionConverter
import org.wfanet.measurement.edpaggregator.vidlabeler.PopulationAttributeWriter
import org.wfanet.measurement.edpaggregator.vidlabeler.VidLabelerApp
import org.wfanet.measurement.edpaggregator.vidlabeler.VirtualPeopleVidAssigner
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.virtualpeople.common.CompiledNode

internal class VidLabelingZeroMembershipIntegrationTest :
  VidLabelingPipelineTestHarness(
    PipelineHarnessConfig(
      numberOfShards = 2,
      maxFileBatchSizeBytes = 1L,
      redeliveryDelayMillis = 0L,
      modelLineActiveStarts =
        mapOf(
          MEMOIZED_MODEL_LINE to
            EVENT_DATE.atStartOfDay(ZoneOffset.UTC).toInstant().plusSeconds(3600)
        ),
    )
  ) {
  private val activeDate = EVENT_DATE.plusDays(1)
  private val transitionRequests =
    ConcurrentLinkedQueue<MarkRawImpressionUploadModelLineLabelingRequest>()
  private val transitionArrivals = AtomicInteger()
  private val bothTransitionsReached = CompletableDeferred<Unit>()
  private val releaseTransitions = CompletableDeferred<Unit>()

  @get:Rule
  val racingModelRowsServer = GrpcTestServerRule {
    addService(
      object :
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadModelLines(
          request: ListRawImpressionUploadModelLinesRequest
        ) = modelLineRowsStub.listRawImpressionUploadModelLines(request)

        override suspend fun markRawImpressionUploadModelLineLabeling(
          request: MarkRawImpressionUploadModelLineLabelingRequest
        ): RawImpressionUploadModelLine {
          transitionRequests.add(request)
          if (transitionArrivals.incrementAndGet() == 2) bothTransitionsReached.complete(Unit)
          withTimeout(30_000L) { releaseTransitions.await() }
          return modelLineRowsStub.markRawImpressionUploadModelLineLabeling(request)
        }
      }
    )
  }

  @get:Rule
  val missingDatesFilesServer = GrpcTestServerRule {
    addService(
      object :
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase() {
        override suspend fun listRawImpressionUploadFiles(
          request: ListRawImpressionUploadFilesRequest
        ) =
          filesStub.listRawImpressionUploadFiles(request).let { response ->
            response.copy {
              rawImpressionUploadFiles.clear()
              rawImpressionUploadFiles +=
                response.rawImpressionUploadFilesList.map { file -> file.copy { clearEventDate() } }
            }
          }

        override suspend fun getRawImpressionUploadFile(
          request: GetRawImpressionUploadFileRequest
        ) = filesStub.getRawImpressionUploadFile(request)
      }
    )
  }

  @Test
  fun `ordinary-only mixed model labels every file without rank jobs or snapshots`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      writeRawFile("ordinary", "one.parquet", listOf("ordinary-one"), activeDate)
      writeRawFile("ordinary", "two.parquet", listOf("ordinary-two"), activeDate)
      finalizeRawUpload("ordinary")
      awaitPipelineIdle()

      val upload = listUploads().single()
      assertZeroCompleted(upload)
      assertThat(poolAssignmentWorkItems()).hasSize(2)
      assertThat(vidLabelerWorkItems()).hasSize(2)
      val people = readLabeledPeople(MEMOIZED_MODEL_LINE)
      assertThat(people.map { it.personId }).containsExactly("ordinary-one", "ordinary-two")
      assertThat(people.all { it.vid in 10100L..10199L }).isTrue()
      for (row in listMetadata()) {
        assertMetadataMatchesSidecar(row)
        val details =
          BlobDetails.parseFrom(
            checkNotNull(fileStorage.getBlob(blobKey(row.blobUri))).read().flatten()
          )
        assertThat(details.encryptedDek.kekUri).isEqualTo(kekUri)
        val events =
          StorageEventReader(
              blobDetails = details,
              blobGeneration = 0L,
              kmsClient = kmsClient,
              impressionsStorageConfig = StorageConfig(rootDirectory = File("/")),
              descriptor = pipelineConfig.eventDescriptor,
            )
            .readEvents()
            .toList()
            .flatten()
        assertThat(events).hasSize(1)
        val event = TestEvent.parseFrom(events.single().message.toByteString())
        assertThat(event.person.gender).isEqualTo(Person.Gender.MALE)
        assertThat(event.person.ageGroup).isEqualTo(Person.AgeGroup.YEARS_35_TO_54)
      }

      assertAvailabilityPublished(setOf(MEMOIZED_MODEL_LINE), setOf(activeDate))
      assertOutputIntervalsContainEveryImpression()
    }

  @Test
  fun `no active rows completes registered date with no memoized output`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      writeRawEvents(
        "inactive",
        "input.parquet",
        listOf(RawEventFixture("ranked-event", "inactive", EVENT_DATE)),
      )
      finalizeRawUpload("inactive")
      awaitPipelineIdle()

      val upload = listUploads().single()
      assertZeroCompleted(upload)
      assertThat(vidLabelerWorkItems()).hasSize(1)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE)).isEmpty()
      assertThat(listMetadata()).isEmpty()
      val outputKeys = fileStorage.listBlobs("$rootKey/output/").toList().map { it.blobKey }
      assertThat(outputKeys)
        .containsExactly(
          blobKey(
            LabeledImpressionsBlobKeys.forDoneUri(
              outputPrefix,
              upload.name,
              MEMOIZED_MODEL_LINE,
              EVENT_DATE,
            )
          )
        )

      assertThat(listAvailabilityTasks(upload.name).single().eventDate.day)
        .isEqualTo(EVENT_DATE.dayOfMonth)
    }

  @Test
  fun `ranked upload waits for zero labeling and then completes on the same model line`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextVidLabelerPublications()
      writeRawFile("zero-first", "input.parquet", listOf("ordinary"), activeDate)
      finalizeRawUpload("zero-first")
      awaitPipelineIdle()
      val zero = listUploads().single()
      assertThat(listModelLines(zero.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      writeRawEvents(
        "ranked-next",
        "input.parquet",
        listOf(RawEventFixture("ranked-event", "ranked", activeDate.plusDays(1))),
      )
      finalizeRawUpload("ranked-next")
      awaitPipelineIdle()
      val queuedRanked = listUploads().single { it.name != zero.name }
      assertThat(listModelLines(queuedRanked.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.CREATED)
      assertThat(listRankIndexBlobs(queuedRanked.name)).isEmpty()
      assertThat(rankBuilderWorkItems()).isEmpty()
      assertThat(poolAssignmentWorkItems()).hasSize(2)
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      drainSequencer()
      assertZeroCompleted(listUploads().single { it.name == zero.name })
      val ranked = listUploads().single { it.name == queuedRanked.name }
      assertThat(ranked.state).isEqualTo(RawImpressionUpload.State.COMPLETED)
      assertThat(listRankIndexBlobs(ranked.name)).isNotEmpty()
      assertThat(rankBuilderWorkItems()).hasSize(1)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).single { it.personId == "ranked" }.vid)
        .isIn(10000L..10089L)
    }

  @Test
  fun `zero mode bypasses a previous ranked index cached in the same app`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      writeRawEvents(
        "cached-ranked",
        "input.parquet",
        listOf(RawEventFixture("ranked-event", "ranked", activeDate)),
      )
      finalizeRawUpload("cached-ranked")
      awaitPipelineIdle()
      val ranked = listUploads().single()
      assertThat(listRankIndexBlobs(ranked.name)).isNotEmpty()
      withholdNextVidLabelerPublications()
      writeRawFile("after-cache", "input.parquet", listOf("ordinary"), activeDate.plusDays(1))
      finalizeRawUpload("after-cache")
      awaitPipelineIdle()
      val pendingZero = listUploads().single { it.name != ranked.name }
      assertThat(listModelLines(pendingZero.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.RankIndexBlobService/ListRankIndexBlobs",
        count = 100,
      )
      try {
        republishQueuedVidLabelers()
        awaitPipelineIdle()
      } finally {
        edpaBeforeCallFaults.clear()
      }
      val zero = listUploads().single { it.name != ranked.name }
      assertZeroCompleted(zero)
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).single { it.personId == "ordinary" }.vid)
        .isIn(10100L..10199L)
      val initialVid = readLabeledPeople(MEMOIZED_MODEL_LINE).single { it.personId == "ranked" }.vid
      writeRawEvents(
        "ranked-after-zero",
        "input.parquet",
        listOf(RawEventFixture("ranked-event", "ranked-again", activeDate.plusDays(2))),
      )
      finalizeRawUpload("ranked-after-zero")
      awaitPipelineIdle()
      val following = listUploads().single { it.name != ranked.name && it.name != zero.name }
      assertThat(following.state).isEqualTo(RawImpressionUpload.State.COMPLETED)
      assertThat(listRankIndexBlobs(following.name)).isNotEmpty()
      assertThat(
          readLabeledPeople(MEMOIZED_MODEL_LINE).single { it.personId == "ranked-again" }.vid
        )
        .isEqualTo(initialVid)
    }

  @Test
  fun `lost shard success responses recover the empty handoff`() =
    runBlocking<Unit> {
      recoverInterruptedHandoff {
        edpaBeforeCallFaults.armAfterCommit(
          "wfa.measurement.edpaggregator.v1alpha.PoolAssignmentJobService/MarkPoolAssignmentJobSucceeded",
          count = 2,
        )
      }
    }

  @Test
  fun `labeling job creation response loss recovers durable batches`() =
    runBlocking<Unit> {
      recoverInterruptedHandoff {
        edpaBeforeCallFaults.armAfterCommit(
          "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/BatchCreateVidLabelingJobs"
        )
      }
    }

  @Test
  fun `labeling publication response loss recovers all files`() =
    runBlocking<Unit> { recoverInterruptedHandoff { loseNextVidLabelerPublicationResponse() } }

  @Test
  fun `crash before labeling state transition recovers published jobs`() =
    runBlocking<Unit> {
      recoverInterruptedHandoff {
        edpaBeforeCallFaults.arm(
          "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/MarkRawImpressionUploadModelLineLabeling"
        )
      }
    }

  @Test
  fun `labeling state response loss keeps committed transition and job identities`() =
    runBlocking<Unit> {
      recoverInterruptedHandoff {
        edpaBeforeCallFaults.armAfterCommit(
          "wfa.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineService/MarkRawImpressionUploadModelLineLabeling"
        )
      }
    }

  private suspend fun recoverInterruptedHandoff(armFault: () -> Unit) {
    installMixedModel()
    setVisibleModelLines(MEMOIZED_MODEL_LINE)
    withholdNextPoolAssignmentPublications(2)
    withholdNextVidLabelerPublications(2)
    writeRawFile("interrupted", "one.parquet", listOf("ordinary-one"), activeDate)
    writeRawFile("interrupted", "two.parquet", listOf("ordinary-two"), activeDate)
    finalizeRawUpload("interrupted")
    awaitPipelineIdle()
    armFault()
    republishQueuedPoolAssignments()
    awaitPipelineIdle()
    val upload = listUploads().single()
    assertThat(listModelLines(upload.name).single().state)
      .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    assertThat(vidLabelerWorkItems()).hasSize(2)
    val names = vidLabelerWorkItems().map { it.name }.toSet()
    assertThat(poolAssignmentWorkItems().any { workItemTransport.deliveryCount(it.name) > 1 })
      .isTrue()
    republishQueuedVidLabelers()
    awaitPipelineIdle()
    assertZeroCompleted(listUploads().single())
    assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
      .containsExactly("ordinary-one", "ordinary-two")
    val generations = outputGenerations(MEMOIZED_MODEL_LINE)
    poolAssignmentWorkItems().forEach { workItemTransport.redeliverWorkItem(it.name) }
    awaitPipelineIdle()
    assertThat(vidLabelerWorkItems().map { it.name }.toSet()).containsExactlyElementsIn(names)
    assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(generations)
  }

  @Test
  fun `monitor recovers empty handoff using the non-last succeeded shard`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextPoolAssignmentPublications(2)
      withholdNextVidLabelerPublications(2)
      writeRawFile("monitor", "one.parquet", listOf("ordinary-one"), activeDate)
      writeRawFile("monitor", "two.parquet", listOf("ordinary-two"), activeDate)
      finalizeRawUpload("monitor")
      awaitPipelineIdle()
      val originals =
        poolAssignmentWorkItems().sortedBy {
          it.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(SubpoolAssignerParams::class.java)
            .shardIndex
        }
      workItemTransport.republishWorkItem(originals[0].name)
      awaitPipelineIdle()
      val upload = listUploads().single()
      assertThat(listModelLines(upload.name).single().hasEncryptedMergedDek()).isFalse()
      assertThat(vidLabelerWorkItems()).isEmpty()
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/BatchCreateVidLabelingJobs"
      )
      holdNextPoolAssignmentRedelivery()
      workItemTransport.republishWorkItem(originals[1].name)
      awaitHeldPoolAssignmentRedelivery()
      val stuck = listModelLines(upload.name).single()
      assertThat(stuck.state).isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
      assertThat(stuck.hasEncryptedMergedDek()).isTrue()
      val monitor =
        buildVidLabelingMonitor(
          stalenessThreshold = Duration.ofSeconds(1),
          clock =
            Clock.fixed(
              Instant.ofEpochSecond(stuck.updateTime.seconds, stuck.updateTime.nanos.toLong())
                .plusSeconds(2),
              ZoneOffset.UTC,
            ),
        )
      assertThat(monitor.runHealth().recoveredTransitions).isEqualTo(1)
      val recovery =
        poolAssignmentWorkItems().single { item -> originals.none { it.name == item.name } }
      val recoveryParams =
        recovery.workItemParams
          .unpack(WorkItem.WorkItemParams::class.java)
          .appParams
          .unpack(SubpoolAssignerParams::class.java)
      assertThat(recoveryParams.shardIndex).isEqualTo(0)
      workItemTransport.awaitSucceeded(recovery.name)
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      assertThat(vidLabelerWorkItems()).hasSize(2)
      releaseHeldPoolAssignmentRedelivery()
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      assertZeroCompleted(listUploads().single())
      val generations = outputGenerations(MEMOIZED_MODEL_LINE)
      assertThat(monitor.runHealth().recoveredTransitions).isEqualTo(0)
      assertThat(monitor.runHealth().recoveredTransitions).isEqualTo(0)
      originals.forEach { workItemTransport.redeliverWorkItem(it.name) }
      awaitPipelineIdle()
      assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(generations)
      assertThat(vidLabelerWorkItems()).hasSize(2)
    }

  @Test
  fun `partial labeling job creation resumes the omitted file batch`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextPoolAssignmentPublications(2)
      withholdNextVidLabelerPublications(51)
      val people = (0 until 51).map { "ordinary-$it" }
      for ((index, person) in people.withIndex()) {
        writeRawFile("partial-creation", "$index.parquet", listOf(person), activeDate)
      }
      finalizeRawUpload("partial-creation")
      awaitPipelineIdle()
      // The shared dispatcher creates at most 50 jobs per request and publishes after all jobs
      // exist. Commit the first batch, then fail before creating the final file's job.
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/BatchCreateVidLabelingJobs",
        skip = 1,
      )
      holdNextPoolAssignmentRedelivery()
      republishQueuedPoolAssignments()
      awaitHeldPoolAssignmentRedelivery()
      val upload = listUploads().single()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
      val jobsStub = VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
      val firstBatch =
        jobsStub
          .listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = upload.name
              pageSize = 100
            }
          )
          .vidLabelingJobsList
      assertThat(firstBatch).hasSize(50)
      assertThat(vidLabelerWorkItems()).isEmpty()
      releaseHeldPoolAssignmentRedelivery()
      awaitPipelineIdle()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      assertThat(vidLabelerWorkItems()).hasSize(51)
      val completedJobs =
        jobsStub
          .listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = upload.name
              pageSize = 100
            }
          )
          .vidLabelingJobsList
      assertThat(completedJobs).hasSize(51)
      assertThat(completedJobs).containsAtLeastElementsIn(firstBatch)
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      assertZeroCompleted(listUploads().single())
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactlyElementsIn(people)
      assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).hasSize(51)
    }

  @Test
  fun `zero-membership healing defers dated availability until fence release`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      writeRawFile("zero-healing", "input.parquet", listOf("original"), activeDate)
      val originalGeneration = finalizeRawUpload("zero-healing")
      awaitPipelineIdle()
      val original = listUploads().single { it.doneBlobGeneration == originalGeneration }
      assertZeroCompleted(original)
      writeRawFile("zero-healing", "input.parquet", listOf("corrected"), activeDate)
      finalizeRawUpload("zero-healing")
      workItemTransport.awaitIdle()
      val controller = buildHealingController()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      approveHealingOperation(
        listHealingOperations().single(),
        RawImpressionUploadCorrectionCandidate.Decision.DECISION_APPLY_CANDIDATE,
      )
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      awaitPipelineIdle()
      val replacement = listUploads().single { it.replacesRawImpressionUpload == original.name }
      assertThat(listModelLines(replacement.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.AVAILABILITY_SYNCING)
      assertThat(listAvailabilityTasks(replacement.name)).isEmpty()
      assertThat(listRankIndexBlobs(replacement.name)).isEmpty()
      assertThat(listHealingOperations().single().state)
        .isNotEqualTo(UploadHealingOperation.State.COMPLETE)
      val generations = outputGenerations(MEMOIZED_MODEL_LINE)
      val phase0 =
        poolAssignmentWorkItems().filter {
          it.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(SubpoolAssignerParams::class.java)
            .rawImpressionUpload == replacement.name
        }
      phase0.forEach { workItemTransport.redeliverWorkItem(it.name) }
      awaitPipelineIdle()
      assertThat(listAvailabilityTasks(replacement.name)).isEmpty()
      assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(generations)
      withholdNextAvailabilityPublications()
      assertThat(controller.run().failedDataProviders).isEqualTo(0)
      assertThat(listHealingOperations().single().state)
        .isEqualTo(UploadHealingOperation.State.COMPLETE)
      assertThat(listAvailabilityTasks(replacement.name).single().state)
        .isEqualTo(WorkItem.State.QUEUED)
      republishQueuedAvailability()
      awaitPipelineIdle()
      assertZeroCompleted(listUploads().single { it.name == replacement.name })
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("corrected")
      assertAvailabilityPublished(setOf(MEMOIZED_MODEL_LINE), setOf(activeDate))
    }

  @Test
  fun `overlapping zero handoff recoveries keep one set of jobs and outputs`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextPoolAssignmentPublications(2)
      withholdNextVidLabelerPublications(2)
      writeRawFile("racing-zero", "one.parquet", listOf("ordinary-one"), activeDate)
      writeRawFile("racing-zero", "two.parquet", listOf("ordinary-two"), activeDate)
      finalizeRawUpload("racing-zero")
      awaitPipelineIdle()
      val originals =
        poolAssignmentWorkItems().sortedBy {
          it.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(SubpoolAssignerParams::class.java)
            .shardIndex
        }
      workItemTransport.republishWorkItem(originals[0].name)
      awaitPipelineIdle()
      edpaBeforeCallFaults.arm(
        "wfa.measurement.edpaggregator.v1alpha.VidLabelingJobService/BatchCreateVidLabelingJobs"
      )
      holdNextPoolAssignmentRedelivery()
      workItemTransport.republishWorkItem(originals[1].name)
      awaitHeldPoolAssignmentRedelivery()
      val upload = listUploads().single()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
      val apps = listOf(racingRecoveryApp(), racingRecoveryApp())
      val recoveries = apps.map { app -> async { app.runWork(originals[0].workItemParams) } }
      withTimeout(30_000L) { bothTransitionsReached.await() }
      assertThat(transitionRequests).hasSize(2)
      assertThat(transitionRequests.map { it.etag }.toSet()).hasSize(1)
      assertThat(transitionRequests.map { it.requestId }.toSet()).hasSize(1)
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
      val jobsStub = VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel)
      val jobs =
        jobsStub
          .listVidLabelingJobs(listVidLabelingJobsRequest { parent = upload.name })
          .vidLabelingJobsList
      assertThat(jobs).hasSize(2)
      assertThat(jobs.flatMap { it.rawImpressionUploadFilesList })
        .containsExactlyElementsIn(listUploadFiles(upload.name).map { it.name })
      assertThat(vidLabelerWorkItems()).hasSize(2)
      val workItemNames = vidLabelerWorkItems().map { it.name }.toSet()
      releaseTransitions.complete(Unit)
      withTimeout(30_000L) { recoveries.awaitAll() }
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      releaseHeldPoolAssignmentRedelivery()
      awaitPipelineIdle()
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      assertZeroCompleted(listUploads().single())
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("ordinary-one", "ordinary-two")
      val generations = outputGenerations(MEMOIZED_MODEL_LINE)
      apps.map { app -> async { app.runWork(originals[0].workItemParams) } }.awaitAll()
      awaitPipelineIdle()
      assertThat(transitionArrivals.get()).isEqualTo(2)
      assertThat(vidLabelerWorkItems().map { it.name }.toSet())
        .containsExactlyElementsIn(workItemNames)
      assertThat(
          jobsStub
            .listVidLabelingJobs(listVidLabelingJobsRequest { parent = upload.name })
            .vidLabelingJobsList
            .map { it.name }
        )
        .containsExactlyElementsIn(jobs.map { it.name })
      assertThat(outputGenerations(MEMOIZED_MODEL_LINE)).isEqualTo(generations)
    }

  private fun racingRecoveryApp() =
    SubpoolAssignerApp(
      subscriptionId = "queues/pool-assigner",
      queueSubscriber = workItemTransport,
      parser = WorkItem.parser(),
      workItemsClient = workItemsStub,
      workItemAttemptsClient = workItemAttemptsStub,
      vidRankBuilderQueue = "queues/rank-builder",
      vidLabelerQueue = "queues/vid-labeler",
      kmsClients = mapOf(providerName to kmsClient),
      getSubpoolMapStorageConfig = {
        StorageConfig(rootDirectory = fileStorageRoot, blobPrefix = it.blobPrefix)
      },
      getRawImpressionsStorageConfig = {
        StorageConfig(rootDirectory = fileStorageRoot, blobPrefix = it.blobPrefix)
      },
      getModelStorageConfig = {
        StorageConfig(rootDirectory = fileStorageRoot, blobPrefix = it.blobPrefix)
      },
      rawImpressionUploadsStub = uploadsStub,
      rawImpressionUploadModelLinesStub =
        RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
          racingModelRowsServer.channel
        ),
      rankerJobsStub = RankerJobServiceCoroutineStub(edpaPublicServer.channel),
      vidLabelingJobsStub = VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
      poolAssignmentJobsStub = PoolAssignmentJobServiceCoroutineStub(edpaPublicServer.channel),
      rawImpressionUploadFilesStub = filesStub,
      buildParquetStorageClient = { config, kms ->
        parquetClient(
          kms,
          Path(
            if (config.blobPrefix?.startsWith("gs://") == true) "gs://$fileBucket/"
            else "file:///$fileBucket"
          ),
        )
      },
      buildSubpoolMapStorageClient = { mapStorage },
      loadPoolEmitLabeler = { _, uri ->
        VirtualPeoplePoolEmitLabeler.fromCompiledNodeBlob(
          checkNotNull(fileStorage.getBlob(blobKey(uri))).read().flatten()
        )
      },
      getSubpoolMapKekUri = { kekUri },
      rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    )

  @Test
  fun `missing authoritative dates leave empty labeling retryable without publication`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextVidLabelerPublications()
      writeRawFile("missing-dates", "input.parquet", listOf("inactive"), EVENT_DATE)
      finalizeRawUpload("missing-dates")
      awaitPipelineIdle()
      val upload = listUploads().single()
      val workItem = vidLabelerWorkItems().single()
      assertThat(listModelLines(upload.name).single().hasEncryptedMergedDek()).isTrue()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      assertFailsWith<DateTimeException> {
        missingDatesLabelerApp().runWork(workItem.workItemParams)
      }
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      assertThat(listUploads().single().state).isNotEqualTo(RawImpressionUpload.State.COMPLETED)
      assertThat(listAvailabilityTasks(upload.name)).isEmpty()
      assertThat(dataProvidersService.requests).isEmpty()
      assertThat(listMetadata()).isEmpty()
      assertThat(File(fileStorageRoot, "$rootKey/output").exists()).isFalse()
      // The persisted registry is intact; the normal worker can finalize the succeeded job when
      // the authoritative dates become available again.
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      assertZeroCompleted(listUploads().single())
    }

  private fun missingDatesLabelerApp() =
    VidLabelerApp(
      subscriptionId = "queues/vid-labeler",
      queueSubscriber = workItemTransport,
      parser = WorkItem.parser(),
      workItemsClient = workItemsStub,
      workItemAttemptsClient = workItemAttemptsStub,
      kmsClients = mapOf(providerName to kmsClient),
      encryptKekUris = mapOf(providerName to kekUri),
      getStorageConfig = {
        StorageConfig(rootDirectory = File("/"), blobPrefix = it.impressionsBlobPrefix)
      },
      vidLabelingJobsStub = VidLabelingJobServiceCoroutineStub(edpaPublicServer.channel),
      rawImpressionUploadModelLinesStub = modelLineRowsStub,
      rankIndexBlobsStub = rankIndexBlobsStub,
      rawImpressionUploadFilesStub =
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
          missingDatesFilesServer.channel
        ),
      rawImpressionUploadsStub = uploadsStub,
      uploadHealingOperationsStub = operationsStub,
      buildParquetStorageClient = { config, kms ->
        parquetClient(
          kms,
          Path(
            if (config.blobPrefix?.startsWith("gs://") == true) "gs://$fileBucket/"
            else "file:///$fileBucket"
          ),
        )
      },
      buildVidRankMapStorageClient = { mapStorage },
      loadAssigner = { _, uri ->
        VirtualPeopleVidAssigner.fromCompiledNodeBlob(
          checkNotNull(fileStorage.getBlob(blobKey(uri))).read().flatten()
        )
      },
      buildImpressionConverter = { _, _ ->
        ParquetImpressionConverter(
          pipelineConfig.eventDescriptor,
          PopulationAttributeWriter(pipelineConfig.eventDescriptor, comparisonPopulationSpec),
        )
      },
      rpcThrottlers = VidLabelingRpcThrottlersTestHelper.alwaysReady(),
    )

  @Test
  fun `shard one finishing before shard zero hands off only after both succeed`() =
    runBlocking<Unit> {
      installMixedModel()
      setVisibleModelLines(MEMOIZED_MODEL_LINE)
      withholdNextPoolAssignmentPublications(2)
      withholdNextVidLabelerPublications()
      writeRawFile("reverse-shards", "input.parquet", listOf("ordinary"), activeDate)
      finalizeRawUpload("reverse-shards")
      awaitPipelineIdle()
      val upload = listUploads().single()
      val originals =
        poolAssignmentWorkItems().sortedBy {
          it.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(SubpoolAssignerParams::class.java)
            .shardIndex
        }
      workItemTransport.republishWorkItem(originals[1].name)
      awaitPipelineIdle()
      assertThat(listModelLines(upload.name).single().hasEncryptedMergedDek()).isFalse()
      assertThat(listModelLines(upload.name).single().state)
        .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
      assertThat(vidLabelerWorkItems()).isEmpty()
      assertThat(rankBuilderWorkItems()).isEmpty()
      workItemTransport.republishWorkItem(originals[0].name)
      awaitPipelineIdle()
      val parent = listModelLines(upload.name).single()
      assertThat(parent.state).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
      val shardJobs =
        PoolAssignmentJobServiceCoroutineStub(edpaPublicServer.channel)
          .listPoolAssignmentJobs(listPoolAssignmentJobsRequest { this.parent = upload.name })
          .poolAssignmentJobsList
      assertThat(parent.encryptedMergedDek)
        .isEqualTo(shardJobs.single { it.shardIndex == 0 }.encryptedDek)
      assertThat(vidLabelerWorkItems()).hasSize(1)
      val childNames = vidLabelerWorkItems().map { it.name }.toSet()
      republishQueuedVidLabelers()
      awaitPipelineIdle()
      assertZeroCompleted(listUploads().single())
      assertThat(readLabeledPeople(MEMOIZED_MODEL_LINE).map { it.personId })
        .containsExactly("ordinary")
      originals.forEach { workItemTransport.redeliverWorkItem(it.name) }
      awaitPipelineIdle()
      assertThat(vidLabelerWorkItems().map { it.name }.toSet())
        .containsExactlyElementsIn(childNames)
    }

  private suspend fun assertZeroCompleted(upload: RawImpressionUpload) {
    assertThat(upload.state).isEqualTo(RawImpressionUpload.State.COMPLETED)
    val parent = listModelLines(upload.name).single()
    assertThat(parent.state).isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(parent.hasEncryptedMergedDek()).isTrue()
    assertThat(parent.encryptedMergedDek.kekUri).isEqualTo(kekUri)
    assertThat(parent.poolOffsetsList).isEmpty()
    assertThat(parent.pendingAvailabilityDatesList).isEmpty()
    assertThat(parent.hasMaxEventDate()).isFalse()
    assertThat(listRankIndexBlobs(upload.name)).isEmpty()
    assertThat(
        rankBuilderWorkItems().filter {
          it.workItemParams
            .unpack(WorkItem.WorkItemParams::class.java)
            .appParams
            .unpack(VidRankBuilderParams::class.java)
            .rawImpressionUpload == upload.name
        }
      )
      .isEmpty()
    assertThat(listAvailabilityTasks(upload.name).single().state)
      .isEqualTo(WorkItem.State.SUCCEEDED)
  }

  private suspend fun installMixedModel() {
    val model = CompiledNode.newBuilder()
    TextFormat.merge(
      """
      name: "mixed"
      branch_node {
        branches {
          condition { op: EQUAL name: "labeler_input.event_id.id" value: "ranked-event" }
          node {
            name: "ranked"
            ranked_population_node {
              pools { population_offset: 10000 total_population: 100 }
              random_seed: "ranked-seed"
              ranked_size: 90
              unranked_mode: DISJOINT
            }
          }
        }
        branches {
          condition { op: TRUE }
          node {
            name: "ordinary"
            population_node {
              pools { population_offset: 10100 total_population: 100 }
              random_seed: "ordinary-seed"
            }
          }
        }
      }
      """
        .trimIndent(),
      model,
    )
    val encoder =
      Runfiles.preload()
        .withSourceRepository("")
        .rlocation(
          "_main/src/main/cc/wfa/measurement/edpaggregator/testing/encode_riegeli_test_record"
        )
    val process = ProcessBuilder(encoder).start()
    process.outputStream.use { it.write(Any.pack(model.build()).toByteArray()) }
    val encoded = process.inputStream.readBytes()
    val error = process.errorStream.bufferedReader().readText()
    check(process.waitFor() == 0) { error }
    fileStorage.writeBlob(blobKey(modelBlobUri), flowOf(ByteString.copyFrom(encoded)))
  }
}
