// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import java.io.PrintWriter
import java.io.StringWriter
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.logging.Handler
import java.util.logging.Level
import java.util.logging.LogRecord
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.common.telemetry.CloudLogEntry
import org.wfanet.measurement.common.telemetry.CloudLogReader
import org.wfanet.measurement.common.telemetry.CloudTraceReader
import org.wfanet.measurement.common.telemetry.CloudTraceSpan
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceLogging
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine

private fun main(args: Array<String>, dependencies: VidLabelingTraceDependencies): Int =
  runVidLabelingTrace(args, dependencies)

class VidLabelingTraceTest {
  @Test
  fun `collect joins separate traces by exact GCS object identity`() = runBlocking {
    val uri = "gs://output-bucket/model-line/direct/2026-09-01/done"
    val identity = VidLabelingTraceAttributes.gcsObjectIdentity(uri, 77L)
    val job = RAW_UPLOAD + "/vidLabelingJobs/direct"
    val graph =
      testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                job,
                NON_MEMOIZED_MODEL_LINE,
                "label_finalize",
                "SUCCEEDED",
                mapOf(
                  "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
                  "xmm.edpa.vid_labeling_job.name" to job,
                ),
              )
            )
        )
    val entries =
      listOf(
        entry(
          "label_finalize",
          objectIdentityFields(identity) +
            " xmm.model_line.name=" +
            NON_MEMOIZED_MODEL_LINE +
            " xmm.edpa.vid_labeling_job.name=" +
            job,
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        ),
        entry("data_watcher", objectIdentityFields(identity), traceId = MEMO_TRACE),
        entry(
          "data_availability_publish",
          objectIdentityFields(identity) + " xmm.model_line.name=" + NON_MEMOIZED_MODEL_LINE,
          traceId = DIRECT_TRACE,
        ),
      )
    val logReader = FakeCloudLogReader(entries)
    val finalStateResolver = VidLabelingFinalStateResolver { _, _, _ ->
      listOf(
        ExpectedTraceNode(
          "watcher",
          NON_MEMOIZED_MODEL_LINE,
          "data_watcher",
          "SUCCEEDED",
          mapOf(
            "xmm.gcs.object.path_hash" to identity.pathHash,
            "xmm.gcs.object.generation" to identity.generation.toString(),
          ),
        ),
        ExpectedTraceNode(
          "availability",
          NON_MEMOIZED_MODEL_LINE,
          "data_availability_publish",
          "PUBLISHED",
          mapOf(
            "xmm.gcs.object.path_hash" to identity.pathHash,
            "xmm.gcs.object.generation" to identity.generation.toString(),
          ),
        ),
      )
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { logReader },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = finalStateResolver,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.evidence.mapNotNull { it.traceId })
      .containsAtLeast(ROOT_TRACE, MEMO_TRACE, DIRECT_TRACE)
    Unit
  }

  @Test
  fun `collect discovers a new trace by exact GCS object identity`() = runBlocking {
    val identity =
      VidLabelingTraceAttributes.gcsObjectIdentity(
        "gs://output-bucket/model-line/direct/2026-09-01/done",
        77L,
      )
    val producerEntry =
      entry(
        "label_finalize",
        objectIdentityFields(identity),
        rawImpressionUpload = RAW_UPLOAD,
        traceId = ROOT_TRACE,
      )
    val traceQueries = mutableListOf<Set<String>>()
    val spanReader = CloudTraceReader { project, correlationValues, _, _, _, _ ->
      traceQueries += correlationValues.toSet()
      if (identity.pathHash !in correlationValues) {
        emptyList()
      } else {
        listOf(
          span(project, MEMO_AVAILABILITY_TRACE, "data_watcher", identity, generation = 77L),
          span(project, OTHER_TRACE, "data_watcher", identity, generation = 78L),
        )
      }
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(listOf(producerEntry)) },
        spanReader = spanReader,
        stateResolver = VidLabelingStateResolver { testGraph(emptyList()) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(vidTraceAttributesFor(identity.pathHash)).containsExactly("xmm.gcs.object.path_hash")
    assertThat(traceQueries.any { identity.pathHash in it }).isTrue()
    assertThat(collection.evidence.mapNotNull { it.traceId }).contains(MEMO_AVAILABILITY_TRACE)
    assertThat(collection.evidence.mapNotNull { it.traceId }).doesNotContain(OTHER_TRACE)
    Unit
  }

  @Test
  fun `collect matches the metadata sidecar lifecycle record emitted by the producer`() =
    runBlocking {
      val sidecarUri = "gs://output-bucket/model-line/direct/output.metadata.binpb"
      val pathHash = VidLabelingTraceAttributes.gcsObjectPathHash(sidecarUri)
      val records = mutableListOf<LogRecord>()
      val logger =
        Logger.getAnonymousLogger().apply {
          useParentHandlers = false
          addHandler(
            object : Handler() {
              override fun publish(record: LogRecord) {
                records += record
              }

              override fun flush() {}

              override fun close() {}
            }
          )
        }
      VidLabelingTraceLogging.logLabelOutput(
        logger,
        Level.INFO,
        "edpa.vid_labeling.label.metadata_sidecar",
        "dataProviders/123",
        RAW_UPLOAD,
        RAW_UPLOAD + "/vidLabelingJobs/direct",
        "non_memoized",
        NON_MEMOIZED_MODEL_LINE,
        "metadata_sidecar",
        pathHash,
        "written",
      )
      val graph =
        testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
          .copy(
            nodes =
              listOf(
                ExpectedTraceNode(
                  "gcs:$pathHash",
                  NON_MEMOIZED_MODEL_LINE,
                  VidLabelingTraceLogging.LABEL_OUTPUT_STAGE,
                  "PUBLISHED",
                  mapOf(
                    "xmm.model_line.name" to NON_MEMOIZED_MODEL_LINE,
                    "xmm.gcs.object.path_hash" to pathHash,
                  ),
                )
              )
          )
      val emittedEntry =
        CloudLogEntry(
          "project",
          Instant.parse("2026-09-01T00:00:01Z"),
          "vid-labeler",
          "INFO",
          ROOT_TRACE,
          records.single().message,
        )
      val collector =
        VidLabelingTraceCollector(
          logReaderFactory = { FakeCloudLogReader(listOf(emittedEntry)) },
          spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
          stateResolver = VidLabelingStateResolver { graph },
          finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
        )

      val collection = collector.collect(request())

      assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
      assertThat(collection.evidence.single().stage)
        .isEqualTo(VidLabelingTraceLogging.LABEL_OUTPUT_STAGE)
      Unit
    }

  @Test
  fun `artifact file names cannot collide`() {
    val first = "dataProviders/a/rawImpressionUploads/b__c"
    val second = "dataProviders/a__b/rawImpressionUploads/c"

    assertThat(VidLabelingTraceOutput.artifactFileName(first))
      .isNotEqualTo(VidLabelingTraceOutput.artifactFileName(second))
  }

  @Test
  fun `availability WorkItem resource name maps to generic WorkItem correlation attribute`() {
    val workItem = "workItems/das-01234567"

    assertThat(vidTraceAttributesFor(workItem)).contains("xmm.work_item.name")
  }

  @Test
  fun `lease resource name maps to lease correlation attribute`() {
    val lease = "dataProviders/123/dataAvailabilitySyncLeases/task-1-attempt-1"

    assertThat(vidTraceAttributesFor(lease))
      .containsExactly("xmm.edpa.data_availability_sync_lease.name")
  }

  @Test
  fun `render retains safe error classification`() {
    val collection =
      VidLabelingTraceCollection(
        RAW_UPLOAD,
        VidLabelingTraceStatus.PARTIAL,
        VidLabelingExecutionStatus.FAILED,
        emptyList(),
        listOf(
          VidLabelingEvidence(
            Instant.parse("2026-09-01T00:00:00Z"),
            "project",
            "log",
            "worker",
            "label",
            "failed",
            mapOf("xmm.error.type" to "IllegalStateException", "xmm.error.code" to "grpc.INTERNAL"),
          )
        ),
        emptyList(),
        emptyList(),
      )

    val output = VidLabelingTraceOutput.render(collection)

    assertThat(output).contains("xmm.error.type=IllegalStateException")
    assertThat(output).contains("xmm.error.code=grpc.INTERNAL")
    assertThat(output).contains("## Errors")
  }

  @Test
  fun `render reports failed availability WorkItem attempts before metadata exists`() {
    val collection =
      VidLabelingTraceCollection(
        RAW_UPLOAD,
        VidLabelingTraceStatus.COMPLETE,
        VidLabelingExecutionStatus.FAILED,
        emptyList(),
        emptyList(),
        emptyList(),
        emptyList(),
        listOf(
          VidLabelingAvailabilityWorkItem(
            "workItems/das-01234567",
            NON_MEMOIZED_MODEL_LINE,
            "FAILED",
            1,
            listOf(
              VidLabelingAvailabilityAttempt(
                "workItems/das-01234567/workItemAttempts/attempt-3",
                "FAILED",
                3,
                "METADATA_PERSISTENCE",
                "SpannerException",
              )
            ),
            "2026-09-01",
            "gs://bucket/secret/done",
            "0123456789abcdef",
            77,
          )
        ),
      )

    val output = VidLabelingTraceOutput.render(collection)

    assertThat(output).contains("## Availability WorkItems")
    assertThat(output).contains("FAILED | 1 | 3 | FAILED | METADATA_PERSISTENCE | SpannerException")
    assertThat(output).contains("0123456789abcdef@77")
    assertThat(output).doesNotContain("gs://")
  }

  @Test
  fun `queued availability WorkItem keeps completed labeling in progress`() = runBlocking {
    val workItem = availabilityWorkItem("QUEUED")
    val graph = testGraph(listOf(NON_MEMOIZED_MODEL_LINE))
    val resolvedNames = mutableSetOf<String>()
    val stateResolver =
      object : VidLabelingStateResolver {
        override suspend fun resolve(rawImpressionUpload: String) = graph

        override suspend fun resolveAvailabilityWorkItems(
          rawImpressionUpload: String,
          workItemNames: Set<String>,
        ): List<VidLabelingAvailabilityWorkItem> {
          resolvedNames += workItemNames
          return listOf(workItem)
        }
      }
    val entries =
      listOf(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE)) +
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        ) +
        entry(
          "availability_work_item_create",
          additionalFields =
            "xmm.work_item.name=${workItem.name} xmm.model_line.name=$NON_MEMOIZED_MODEL_LINE",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = DIRECT_AVAILABILITY_TRACE,
        ) +
        entry(
          "work_item_publication",
          additionalFields =
            "xmm.work_item.name=${workItem.name} xmm.work_item.publication_attempt=2",
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = stateResolver,
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.IN_PROGRESS)
    assertThat(collection.availabilityWorkItems.single().state).isEqualTo("QUEUED")
    assertThat(resolvedNames).containsExactly(workItem.name)
    assertThat(
        collection.modelLines
          .single()
          .stages
          .filter { it.name == "availability_work_item_create" }
          .any { it.evidenceCount > 0 }
      )
      .isTrue()
    assertThat(
        collection.evidence
          .first {
            it.stage == "work_item_publication" &&
              it.identifiers.containsKey("xmm.work_item.publication_attempt")
          }
          .identifiers
      )
      .containsEntry("xmm.work_item.publication_attempt", "2")
    Unit
  }

  @Test
  fun `failed availability WorkItem makes completed labeling failed`() = runBlocking {
    val collection = collectWithAvailabilityWorkItemState("FAILED")

    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.FAILED)
    assertThat(collection.availabilityWorkItems.single().state).isEqualTo("FAILED")
  }

  @Test
  fun `collect traverses mixed routes and excludes another upload`() = runBlocking {
    val entries = buildList {
      add(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
      addAll(routeEntries(MEMOIZED_MODEL_LINE, "memoized", MEMOIZED_STAGES, "memo", MEMO_TRACE))
      addAll(
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(MEMOIZED_MODEL_LINE),
          traceId = MEMO_AVAILABILITY_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
      addAll(
        routeEntries(
          MEMOIZED_MODEL_LINE,
          "memoized",
          MEMOIZED_STAGES,
          "other",
          OTHER_TRACE,
          OTHER_RAW_UPLOAD,
        )
      )
    }
    val logReader = FakeCloudLogReader(entries)
    val workItem = "workItems/memoized-ranker-retry"
    val spanQueries = mutableListOf<Pair<Set<String>, Set<String>>>()
    val spanReader = CloudTraceReader { project, correlationValues, traceIds, _, _, _ ->
      spanQueries += correlationValues.toSet() to traceIds.toSet()
      if (RAW_UPLOAD !in correlationValues && ROOT_TRACE !in traceIds) {
        emptyList()
      } else {
        listOf(
          CloudTraceSpan(
            project,
            ROOT_TRACE,
            "span-1",
            null,
            "edpa.vid_labeling.rank",
            "vid-rank-builder",
            Instant.parse("2026-09-01T00:00:01Z"),
            Instant.parse("2026-09-01T00:00:02Z"),
            mapOf(
              "xmm.lifecycle.stage" to "rank",
              "xmm.outcome" to "succeeded",
              "xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD,
              "xmm.model_line.name" to MEMOIZED_MODEL_LINE,
              "xmm.work_item.name" to workItem,
              "xmm.work_item.generation" to "2",
            ),
          )
        )
      }
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { logReader },
        spanReader = spanReader,
        stateResolver = VidLabelingStateResolver { testGraph() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.SUCCEEDED)
    assertThat(collection.modelLines.map { it.route })
      .containsExactly(VidLabelingRoute.MEMOIZED, VidLabelingRoute.NON_MEMOIZED)
    assertThat(collection.modelLines.flatMap { it.missingStages }).isEmpty()
    assertThat(collection.evidence.flatMap { it.identifiers.values })
      .doesNotContain(OTHER_RAW_UPLOAD)
    assertThat(collection.evidence.flatMap { it.identifiers.values }).contains(workItem)
    assertThat(collection.evidence.any { it.source == "span" }).isTrue()
    assertThat(spanQueries.any { (values, _) -> RAW_UPLOAD in values }).isTrue()
    assertThat(logReader.traceQueries.flatten())
      .containsAtLeast(ROOT_TRACE, MEMO_AVAILABILITY_TRACE, DIRECT_AVAILABILITY_TRACE)
    val artifact = VidLabelingTraceOutput.render(collection)
    assertThat(artifact).contains(MEMOIZED_MODEL_LINE)
    assertThat(artifact).contains(NON_MEMOIZED_MODEL_LINE)
    assertThat(artifact).doesNotContain("gs://private-bucket/raw-input")
  }

  @Test
  fun `availability evidence matches model line and interval`() = runBlocking {
    val intervalStart = "2026-09-01T00:00:00Z"
    val intervalEnd = "2026-09-02T00:00:00Z"
    val graph =
      testGraph()
        .copy(
          nodes =
            listOf(
              availabilityNode(
                "memoized-availability",
                MEMOIZED_MODEL_LINE,
                intervalStart,
                intervalEnd,
              ),
              availabilityNode(
                "non-memoized-availability",
                NON_MEMOIZED_MODEL_LINE,
                intervalStart,
                intervalEnd,
              ),
            )
        )
    val entry =
      entry(
        "data_availability_publish",
        modelLineFields(MEMOIZED_MODEL_LINE) +
          " xmm.edpa.availability.interval_start=$intervalStart" +
          " xmm.edpa.availability.interval_end=$intervalEnd",
        rawImpressionUpload = RAW_UPLOAD,
        traceId = ROOT_TRACE,
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(listOf(entry)) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.modelLines.single { it.modelLine == MEMOIZED_MODEL_LINE }.missingStages)
      .isEmpty()
    assertThat(
        collection.modelLines.single { it.modelLine == NON_MEMOIZED_MODEL_LINE }.missingStages
      )
      .containsExactly("non-memoized-availability")
    Unit
  }

  @Test
  fun `collect reports discovery caps as partial`() = runBlocking {
    val entries =
      listOf(
        entry(
          "upload_registration",
          "xmm.edpa.vid_labeling_job.name=" + RAW_UPLOAD + "/vidLabelingJobs/job-1",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        ),
        entry(
          "dispatch",
          "xmm.edpa.vid_labeling_job.name=" + RAW_UPLOAD + "/vidLabelingJobs/job-2",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = MEMO_TRACE,
        ),
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request(correlationValueLimit = 1, traceIdLimit = 1))

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.warnings.single { "Discovery limit" in it })
      .contains("correlation values and trace IDs")
    assertThat(collection.sourceStatuses.any { it.status == "truncated" }).isTrue()
  }

  @Test
  fun `collect reports exhausted expansion rounds as partial`() = runBlocking {
    val entries =
      listOf(
        entry(
          "upload_registration",
          "xmm.edpa.vid_labeling_job.name=" + RAW_UPLOAD + "/vidLabelingJobs/new-job",
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        )
      )
    val rootOnlyGraph = testGraph(emptyList())
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { rootOnlyGraph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request(expansionRounds = 1))

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.warnings).contains("Discovery limit reached for expansion rounds.")
  }

  @Test
  fun `collect reports a registered upload with no model lines as no work`() = runBlocking {
    val graph =
      testGraph(emptyList()).let {
        it.copy(upload = it.upload.toBuilder().setRegistrationComplete(true).build())
      }
    val entries =
      listOf(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.NO_WORK)
  }

  @Test
  fun `collect requires evidence for every concrete child`() = runBlocking {
    val modelLine = NON_MEMOIZED_MODEL_LINE
    val childOne = RAW_UPLOAD + "/vidLabelingJobs/one"
    val childTwo = RAW_UPLOAD + "/vidLabelingJobs/two"
    val graph =
      testGraph(listOf(modelLine))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                childOne,
                modelLine,
                "label",
                "SUCCEEDED",
                mapOf(
                  "xmm.edpa.vid_labeling_job.name" to childOne,
                  "xmm.model_line.name" to modelLine,
                ),
              ),
              ExpectedTraceNode(
                childTwo,
                modelLine,
                "label",
                "SUCCEEDED",
                mapOf(
                  "xmm.edpa.vid_labeling_job.name" to childTwo,
                  "xmm.model_line.name" to modelLine,
                ),
              ),
            )
        )
    val entries =
      listOf(
        entry(
          "label",
          "xmm.model_line.name=" + modelLine + " xmm.edpa.vid_labeling_job.name=" + childOne,
          rawImpressionUpload = RAW_UPLOAD,
          traceId = ROOT_TRACE,
        )
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.PARTIAL)
    assertThat(collection.modelLines.single().missingStages).contains(childTwo)
  }

  @Test
  fun `collect treats downstream stages after terminal failure as not applicable`() = runBlocking {
    val modelLine = NON_MEMOIZED_MODEL_LINE
    val job = RAW_UPLOAD + "/vidLabelingJobs/failed"
    val graph =
      testGraph(listOf(modelLine))
        .copy(
          nodes =
            listOf(
              ExpectedTraceNode(
                job,
                modelLine,
                "label",
                "FAILED",
                mapOf("xmm.edpa.vid_labeling_job.name" to job, "xmm.model_line.name" to modelLine),
              ),
              ExpectedTraceNode(
                modelLine + ":availability",
                modelLine,
                "data_availability_publish",
                "NOT_REACHED",
                mapOf("xmm.model_line.name" to modelLine),
                ExpectedNodeDisposition.NOT_APPLICABLE,
              ),
            )
        )
    val entries =
      listOf(
        entry(
          "label",
          "xmm.model_line.name=" + modelLine + " xmm.edpa.vid_labeling_job.name=" + job,
          rawImpressionUpload = RAW_UPLOAD,
          outcome = "failed",
          traceId = ROOT_TRACE,
        )
      )
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.COMPLETE)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.FAILED)
  }

  @Test
  fun `runVidLabelingTrace exercises CLI contract`() {
    val entries = buildList {
      add(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
      addAll(
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(listOf(NON_MEMOIZED_MODEL_LINE)) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )
    val output = StringWriter()

    val exitCode =
      main(
        arrayOf(
          "--raw-impression-upload=$RAW_UPLOAD",
          "--observability-project=project",
          "--start-time=2026-08-31T00:00:00Z",
          "--end-time=2026-09-02T00:00:00Z",
          "--allow-partial",
          "--edpa-public-api-target=unused",
          "--control-plane-api-target=unused",
          "--kingdom-public-api-target=unused",
          "--tls-cert-file=unused",
          "--tls-key-file=unused",
          "--cert-collection-file=unused",
        ),
        VidLabelingTraceDependencies(
          collector,
          Clock.fixed(Instant.parse("2026-09-02T00:00:00Z"), ZoneOffset.UTC),
          PrintWriter(output, true),
        ),
      )

    assertThat(exitCode).isEqualTo(0)
    assertThat(output.toString()).contains("# VID labeling trace")
  }

  @Test
  fun `latest successful terminal evidence wins over earlier failed attempt`() = runBlocking {
    val entries = buildList {
      add(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE))
      addAll(
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          seconds = -1,
          outcome = "failed",
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
      add(
        entry(
          "data_availability_publish",
          modelLineFields(NON_MEMOIZED_MODEL_LINE),
          seconds = 100,
          traceId = DIRECT_AVAILABILITY_TRACE,
        )
      )
    }
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph(listOf(NON_MEMOIZED_MODEL_LINE)) },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.SUCCEEDED)
    assertThat(collection.warnings)
      .contains("Earlier failed attempts were followed by successful terminal evidence.")
  }

  @Test
  fun `collect reports source failure independently from execution status`() = runBlocking {
    val collector =
      VidLabelingTraceCollector(
        logReaderFactory = { CloudLogReader { _, _, _, _ -> throw IllegalStateException() } },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { testGraph() },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )

    val collection = collector.collect(request())

    assertThat(collection.traceStatus).isEqualTo(VidLabelingTraceStatus.FAILED)
    assertThat(collection.executionStatus).isEqualTo(VidLabelingExecutionStatus.UNKNOWN)
    assertThat(collection.sourceStatuses.map { it.status }).contains("failed")
  }

  @Test
  fun `batch isolates malformed upload and continues with valid upload`() = runBlocking {
    val writtenFiles = mutableListOf<String>()

    val failed =
      collectVidLabelingTraceBatch(
        listOf("malformed", RAW_UPLOAD),
        allowPartial = true,
        collect = { rawImpressionUpload ->
          require(rawImpressionUpload == RAW_UPLOAD)
          VidLabelingTraceCollection(
            rawImpressionUpload,
            VidLabelingTraceStatus.COMPLETE,
            VidLabelingExecutionStatus.SUCCEEDED,
            emptyList(),
            emptyList(),
            emptyList(),
            emptyList(),
          )
        },
        write = { fileName, _ -> writtenFiles += fileName },
      )

    assertThat(failed).isTrue()
    assertThat(writtenFiles).hasSize(2)
    assertThat(writtenFiles[0]).startsWith("invalid__")
    assertThat(writtenFiles[1]).isEqualTo(VidLabelingTraceOutput.artifactFileName(RAW_UPLOAD))
  }

  private fun request(
    correlationValueLimit: Int = 500,
    traceIdLimit: Int = 500,
    expansionRounds: Int = 4,
  ) =
    VidLabelingTraceRequest(
      RAW_UPLOAD,
      listOf("observability-project"),
      Instant.parse("2026-08-31T00:00:00Z"),
      Instant.parse("2026-09-02T00:00:00Z"),
      correlationValueLimit = correlationValueLimit,
      traceIdLimit = traceIdLimit,
      expansionRounds = expansionRounds,
    )

  private suspend fun collectWithAvailabilityWorkItemState(
    state: String
  ): VidLabelingTraceCollection {
    val workItem = availabilityWorkItem(state)
    val graph =
      testGraph(listOf(NON_MEMOIZED_MODEL_LINE)).copy(availabilityWorkItems = listOf(workItem))
    val entries =
      listOf(entry("upload_registration", rawImpressionUpload = RAW_UPLOAD, traceId = ROOT_TRACE)) +
        routeEntries(
          NON_MEMOIZED_MODEL_LINE,
          "non_memoized",
          NON_MEMOIZED_STAGES,
          "direct",
          DIRECT_TRACE,
        )
    return VidLabelingTraceCollector(
        logReaderFactory = { FakeCloudLogReader(entries) },
        spanReader = CloudTraceReader { _, _, _, _, _, _ -> emptyList() },
        stateResolver = VidLabelingStateResolver { graph },
        finalStateResolver = NOOP_FINAL_STATE_RESOLVER,
      )
      .collect(request())
  }

  private fun availabilityWorkItem(state: String): VidLabelingAvailabilityWorkItem =
    VidLabelingAvailabilityWorkItem(
      "workItems/das-01234567",
      NON_MEMOIZED_MODEL_LINE,
      state,
      1,
      emptyList(),
      "2026-09-01",
      "gs://bucket/model-line/2026-09-01/done",
      "0123456789abcdef",
      77,
    )

  private fun testGraph(
    modelLineNames: List<String> = listOf(MEMOIZED_MODEL_LINE, NON_MEMOIZED_MODEL_LINE)
  ): VidLabelingAuthoritativeGraph {
    val upload =
      RawImpressionUpload.newBuilder()
        .setName(RAW_UPLOAD)
        .setState(RawImpressionUpload.State.COMPLETED)
        .setDoneBlobGeneration(1)
        .build()
    val modelLines =
      modelLineNames.mapIndexed { index, modelLine ->
        val id = if (modelLine == MEMOIZED_MODEL_LINE) "memo" else "direct"
        RawImpressionUploadModelLine.newBuilder()
          .setName(RAW_UPLOAD + "/rawImpressionUploadModelLines/" + id)
          .setCmmsModelLine(modelLine)
          .setState(RawImpressionUploadModelLine.State.COMPLETED)
          .build()
      }
    val nodes = buildList {
      add(
        ExpectedTraceNode(
          RAW_UPLOAD,
          null,
          "upload_registration",
          "COMPLETED",
          mapOf("xmm.edpa.raw_impression_upload.name" to RAW_UPLOAD),
        )
      )
      for ((index, modelLine) in modelLineNames.withIndex()) {
        val memoized = modelLine == MEMOIZED_MODEL_LINE
        val stages = if (memoized) MEMOIZED_STAGES else NON_MEMOIZED_STAGES
        val child =
          RAW_UPLOAD + "/rawImpressionUploadModelLines/" + if (memoized) "memo" else "direct"
        for (stage in stages) {
          val identifiers =
            if (stage in setOf("data_watcher", "data_availability_publish")) {
              mapOf(
                "xmm.model_line.name" to modelLine,
                "xmm.edpa.label.route" to if (memoized) "memoized" else "non_memoized",
              )
            } else {
              mapOf(
                "xmm.model_line.name" to modelLine,
                "xmm.edpa.raw_impression_upload_model_line.name" to child,
                "xmm.edpa.label.route" to if (memoized) "memoized" else "non_memoized",
              )
            }
          add(ExpectedTraceNode(child + ":" + stage, modelLine, stage, "SUCCEEDED", identifiers))
        }
      }
    }
    return VidLabelingAuthoritativeGraph(upload, modelLines, nodes)
  }

  private fun routeEntries(
    modelLine: String,
    route: String,
    stages: List<String>,
    id: String,
    routeTraceId: String,
    rawImpressionUpload: String = RAW_UPLOAD,
  ): List<CloudLogEntry> {
    val child = rawImpressionUpload + "/rawImpressionUploadModelLines/" + id
    val availabilityTrace =
      if (route == "memoized") MEMO_AVAILABILITY_TRACE else DIRECT_AVAILABILITY_TRACE
    return stages
      .filterNot { it == "data_availability_publish" }
      .mapIndexed { index, stage ->
        val traceId =
          when (stage) {
            "dispatch" -> if (rawImpressionUpload == RAW_UPLOAD) ROOT_TRACE else OTHER_TRACE
            "data_availability_metadata" -> availabilityTrace
            else -> routeTraceId
          }
        entry(
          stage,
          modelLineFields(modelLine) +
            " xmm.edpa.raw_impression_upload_model_line.name=" +
            child +
            " xmm.edpa.label.route=" +
            route,
          index + 1L,
          traceId = traceId,
        )
      }
  }

  private fun modelLineFields(modelLine: String): String = "xmm.model_line.name=" + modelLine

  private fun availabilityNode(
    id: String,
    modelLine: String,
    intervalStart: String,
    intervalEnd: String,
  ) =
    ExpectedTraceNode(
      id,
      modelLine,
      "data_availability_publish",
      "PUBLISHED",
      mapOf(
        "xmm.model_line.name" to modelLine,
        "xmm.edpa.availability.interval_start" to intervalStart,
        "xmm.edpa.availability.interval_end" to intervalEnd,
      ),
    )

  private fun span(
    project: String,
    traceId: String,
    stage: String,
    identity: VidLabelingTraceAttributes.GcsObjectIdentity,
    generation: Long,
  ) =
    CloudTraceSpan(
      project,
      traceId,
      "span-$traceId",
      null,
      "edpa.vid_labeling.$stage",
      "worker",
      Instant.parse("2026-09-01T00:00:01Z"),
      Instant.parse("2026-09-01T00:00:02Z"),
      mapOf(
        "xmm.lifecycle.stage" to stage,
        "xmm.outcome" to "failed",
        "xmm.gcs.object.path_hash" to identity.pathHash,
        "xmm.gcs.object.generation" to generation.toString(),
      ),
    )

  private fun objectIdentityFields(identity: VidLabelingTraceAttributes.GcsObjectIdentity): String =
    "xmm.gcs.object.path_hash=" +
      identity.pathHash +
      " xmm.gcs.object.generation=" +
      identity.generation

  private fun entry(
    stage: String,
    additionalFields: String = "",
    seconds: Long = 0,
    rawImpressionUpload: String? = null,
    outcome: String = "succeeded",
    traceId: String,
  ): CloudLogEntry {
    val uploadField =
      rawImpressionUpload?.let { " xmm.edpa.raw_impression_upload.name=" + it }.orEmpty()
    return CloudLogEntry(
      "observability-project",
      Instant.parse("2026-09-01T00:00:00Z").plusSeconds(seconds),
      "edpa",
      "INFO",
      "projects/observability-project/traces/" + traceId,
      "event=edpa.vid_labeling." +
        stage +
        " xmm.lifecycle.stage=" +
        stage +
        " xmm.outcome=" +
        outcome +
        uploadField +
        " " +
        additionalFields +
        " unsafe_uri=gs://private-bucket/raw-input",
    )
  }

  private class FakeCloudLogReader(private val entries: List<CloudLogEntry>) : CloudLogReader {
    val traceQueries = mutableListOf<Set<String>>()

    override suspend fun read(
      correlationValues: Collection<String>,
      startTime: Instant,
      endTime: Instant,
      limit: Int,
    ): List<CloudLogEntry> =
      entries.filter { entry ->
        val fieldValues =
          entry.message.split(' ').mapNotNull { token ->
            token.substringAfter('=', "").takeIf { it.isNotEmpty() }
          }
        correlationValues.any { it in fieldValues }
      }

    override suspend fun readTraceIds(
      traceIds: Collection<String>,
      startTime: Instant,
      endTime: Instant,
      limit: Int,
    ): List<CloudLogEntry> {
      val normalized = traceIds.map { it.substringAfterLast('/') }.toSet()
      traceQueries += normalized
      return entries.filter { it.trace?.substringAfterLast('/') in normalized }
    }
  }

  companion object {
    private val NOOP_FINAL_STATE_RESOLVER = VidLabelingFinalStateResolver { _, _, _ -> emptyList() }
    private const val RAW_UPLOAD = "dataProviders/123/rawImpressionUploads/upload-1"
    private const val OTHER_RAW_UPLOAD = "dataProviders/123/rawImpressionUploads/upload-2"
    private const val MEMOIZED_MODEL_LINE =
      "modelProviders/456/modelSuites/suite/modelLines/memoized"
    private const val NON_MEMOIZED_MODEL_LINE =
      "modelProviders/456/modelSuites/suite/modelLines/non-memoized"
    private const val ROOT_TRACE = "11111111111111111111111111111111"
    private const val MEMO_TRACE = "22222222222222222222222222222222"
    private const val DIRECT_TRACE = "33333333333333333333333333333333"
    private const val MEMO_AVAILABILITY_TRACE = "44444444444444444444444444444444"
    private const val DIRECT_AVAILABILITY_TRACE = "55555555555555555555555555555555"
    private const val OTHER_TRACE = "99999999999999999999999999999999"
    private val MEMOIZED_STAGES =
      listOf(
        "dispatch",
        "pool_assignment",
        "pool_assignment_finalize",
        "rank",
        "rank_finalize",
        "label",
        "label_finalize",
        "availability_work_item_create",
        "work_item_publication",
        "availability_work_item_process",
        "data_availability_metadata",
        "data_availability_publish",
      )
    private val NON_MEMOIZED_STAGES =
      listOf(
        "dispatch",
        "label",
        "label_finalize",
        "availability_work_item_create",
        "work_item_publication",
        "availability_work_item_process",
        "data_availability_metadata",
        "data_availability_publish",
      )
  }
}
